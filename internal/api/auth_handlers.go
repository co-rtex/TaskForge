package api

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/auth"
	"github.com/co-rtex/TaskForge/internal/jobs"
)

// The key-management routes live under /internal/v1 with the worker-control
// surface, and for the same reason: they are loopback-only operator plumbing,
// not part of the public user API.
//
// They are themselves unauthenticated in this milestone. That is a real security
// boundary and it is stated rather than hidden: anyone who can reach loopback --
// which is already anyone who can reach the worker-control surface -- can mint a
// tenant credential. It is recorded in
// docs/adr/0013-database-backed-api-key-authentication.md, and it is the reason
// nothing here may be exposed off loopback.

type createAPIKeyRequest struct {
	Scope string `json:"scope"`
	Name  string `json:"name"`
}

// APIKeyCreatedResponse is the ONLY response in TaskForge that ever contains a
// raw credential, and it is returned exactly once.
type APIKeyCreatedResponse struct {
	ID        string    `json:"id"`
	Scope     string    `json:"scope"`
	Name      string    `json:"name"`
	Prefix    string    `json:"prefix"`
	CreatedAt time.Time `json:"created_at"`
	// Key is the complete credential. It is not stored and cannot be recovered:
	// a caller who loses it revokes this key and mints another.
	Key string `json:"key"`
}

// APIKeySummaryResponse is the operator view of a key. It has no field for a
// secret or a digest, so a listing cannot leak one.
type APIKeySummaryResponse struct {
	ID        string     `json:"id"`
	Scope     string     `json:"scope"`
	Name      string     `json:"name"`
	Prefix    string     `json:"prefix"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at"`
}

// APIKeyListResponse is one bounded page of key metadata, newest first.
type APIKeyListResponse struct {
	Keys []APIKeySummaryResponse `json:"keys"`
}

// APIKeyRevokedResponse reports a revocation, including an idempotent repeat.
type APIKeyRevokedResponse struct {
	APIKeySummaryResponse
	// AlreadyRevoked is true when this key was revoked before this call. The
	// reported revoked_at is the original instant, not this one.
	AlreadyRevoked bool `json:"already_revoked"`
}

func toAPIKeySummary(key auth.Key) APIKeySummaryResponse {
	summary := APIKeySummaryResponse{
		ID:        key.ID.String(),
		Scope:     key.Scope,
		Name:      key.Name,
		Prefix:    key.Prefix,
		CreatedAt: key.CreatedAt.UTC(),
	}
	if key.RevokedAt != nil {
		utc := key.RevokedAt.UTC()
		summary.RevokedAt = &utc
	}
	return summary
}

// handleCreateAPIKey mints one credential and returns it exactly once.
//
// Responses:
//
//	201 the key was created; this body carries the only copy of the credential
//	422 scope or name was invalid
//	400 the body was not valid JSON, or carried an unknown field
//	503 the request exceeded its deadline before the outcome was known
//
// There is deliberately no Idempotency-Key and no 200 replay. Every call mints a
// distinct credential, because an idempotent create would have to return an
// existing secret to a repeat -- the one thing a write-only credential must
// never do.
func (s *Server) handleCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	var request createAPIKeyRequest
	if !s.decodeControlJSON(w, r, &request) {
		return
	}

	created, err := s.keys.Create(r.Context(), request.Scope, request.Name)
	switch {
	case err == nil:
		// The credential is returned to the caller and never written anywhere
		// else. This log line carries the id, the scope, and the non-secret
		// lookup prefix -- enough to find and revoke the key, and nothing that
		// could be presented as one.
		s.log.Info("api key created",
			slog.String("request_id", RequestIDFrom(r.Context())),
			slog.String("api_key_id", created.Key.ID.String()),
			slog.String("scope", created.Key.Scope),
			slog.String("api_key_prefix", created.Key.Prefix))
		// M5A warned here that a key's scope might have no worker able to
		// execute it, because every worker ran under one fixed configured
		// scope. Since M5B a worker key can be minted for any scope, so
		// whether jobs under this scope run depends on whether a worker has
		// registered under a matching worker key -- a fact this endpoint has
		// no way to know at mint time, and one that can change independently
		// of this call in either direction. There is nothing true to warn
		// about here anymore.
		writeJSON(w, s.log, http.StatusCreated, APIKeyCreatedResponse{
			ID:        created.Key.ID.String(),
			Scope:     created.Key.Scope,
			Name:      created.Key.Name,
			Prefix:    created.Key.Prefix,
			CreatedAt: created.Key.CreatedAt.UTC(),
			Key:       created.Raw,
		})

	case isAuthValidation(err):
		s.writeAuthValidation(w, r, err)
	case errors.Is(err, auth.ErrDeadlineExceeded):
		s.writeDeadlineAmbiguity(w, r, "create api key",
			"do NOT repeat this request blindly: key creation carries no request "+
				"identity, so a repeat mints a SECOND credential rather than "+
				"returning the first. List keys through GET /internal/v1/api-keys, "+
				"and revoke any key whose credential no caller received")
	default:
		s.internalError(w, r, "create api key", err)
	}
}

// handleRevokeAPIKey withdraws one credential.
//
// Responses:
//
//	200 the key is revoked, including an idempotent repeat
//	404 no key with that id
//	422 the key id was not a UUID
//	503 the request exceeded its deadline before the outcome was known
func (s *Server) handleRevokeAPIKey(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("key_id"))
	if err != nil {
		// 422, not the 404 the public job routes answer for a malformed id.
		// This surface is loopback operator plumbing with no tenant to hide ids
		// from, so a caller is better served by being told their id is malformed
		// than by a not-found that sends them looking for a deleted key.
		s.writeFieldError(w, r, CodeValidationFailed, "key_id", "must be a UUID")
		return
	}

	result, err := s.keys.Revoke(r.Context(), id)
	switch {
	case err == nil:
		s.log.Info("api key revoked",
			slog.String("request_id", RequestIDFrom(r.Context())),
			slog.String("api_key_id", result.Key.ID.String()),
			slog.String("scope", result.Key.Scope),
			slog.String("api_key_prefix", result.Key.Prefix),
			slog.Bool("already_revoked", result.AlreadyRevoked))
		writeJSON(w, s.log, http.StatusOK, APIKeyRevokedResponse{
			APIKeySummaryResponse: toAPIKeySummary(result.Key),
			AlreadyRevoked:        result.AlreadyRevoked,
		})
	case errors.Is(err, auth.ErrKeyNotFound):
		writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "api key not found", nil)
	case errors.Is(err, auth.ErrDeadlineExceeded):
		s.writeDeadlineAmbiguity(w, r, "revoke api key",
			"repeat the identical request for the same key id; revocation is keyed "+
				"by key id alone, so a repeat reports the original revocation "+
				"instant rather than moving it")
	default:
		s.internalError(w, r, "revoke api key", err)
	}
}

// handleListAPIKeys returns one bounded page of key metadata, newest first.
//
// Revoked keys are included: an operator auditing which credentials ever existed
// needs to see them, and hiding them would make the listing useless for the
// question it is actually asked.
//
// Responses:
//
//	200 one bounded page
//	422 the limit was invalid
//	503 the request exceeded its deadline
func (s *Server) handleListAPIKeys(w http.ResponseWriter, r *http.Request) {
	limit := auth.DefaultListLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > auth.MaxListLimit {
			s.writeFieldError(w, r, CodeValidationFailed, "limit",
				"must be an integer between 1 and "+strconv.Itoa(auth.MaxListLimit))
			return
		}
		limit = parsed
	}

	keys, err := s.keys.List(r.Context(), limit)
	switch {
	case err == nil:
		summaries := make([]APIKeySummaryResponse, 0, len(keys))
		for _, key := range keys {
			summaries = append(summaries, toAPIKeySummary(key))
		}
		writeJSON(w, s.log, http.StatusOK, APIKeyListResponse{Keys: summaries})
	case errors.Is(err, auth.ErrDeadlineExceeded):
		s.writeDeadlineAmbiguity(w, r, "list api keys",
			"repeat the identical request; listing keys reads rows and writes "+
				"nothing, so a retry cannot change any credential")
	default:
		s.internalError(w, r, "list api keys", err)
	}
}

func isAuthValidation(err error) bool {
	var validation *auth.ValidationError
	return errors.As(err, &validation)
}

// writeAuthValidation renders an auth validation failure in the one error shape
// every endpoint returns, converting auth's own field errors at the boundary the
// way writeWorkerValidation converts workers'.
func (s *Server) writeAuthValidation(w http.ResponseWriter, r *http.Request, err error) {
	var validation *auth.ValidationError
	if !errors.As(err, &validation) {
		s.internalError(w, r, "validate api key request", err)
		return
	}
	details := make([]jobs.FieldError, 0, len(validation.Fields))
	for _, field := range validation.Fields {
		details = append(details, jobs.FieldError{Field: field.Field, Message: field.Message})
	}
	writeError(w, r, s.log, http.StatusUnprocessableEntity, CodeValidationFailed,
		"the request was rejected by validation", details)
}
