package api

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/workerauth"
)

// The worker-key management routes live under /internal/v1, beside the
// public-key management routes and the worker-control surface itself, and
// for the same reason as both: loopback-only operator plumbing, not part of
// the public user API.
//
// They are themselves unauthenticated in this milestone, for the identical
// reason /internal/v1/api-keys is: this is how the first worker credential
// comes into existence, so requiring one would make the system
// unbootstrappable. It is recorded in
// docs/adr/0014-worker-control-authentication.md, and it is why nothing here
// may be exposed off loopback -- the same population that could already
// drive worker control directly can already mint a worker credential for any
// scope; this surface does not widen who is trusted, only what a caller with
// loopback access can do with that trust.

type createWorkerKeyRequest struct {
	Scope string `json:"scope"`
	Name  string `json:"name"`
}

// WorkerKeyCreatedResponse is the ONLY response that ever contains a raw
// worker credential, and it is returned exactly once.
type WorkerKeyCreatedResponse struct {
	ID        string    `json:"id"`
	Scope     string    `json:"scope"`
	Name      string    `json:"name"`
	Prefix    string    `json:"prefix"`
	CreatedAt time.Time `json:"created_at"`
	// Key is the complete credential. It is not stored and cannot be
	// recovered: a caller who loses it revokes this key and mints another.
	Key string `json:"key"`
}

// WorkerKeySummaryResponse is the operator view of a worker key. It has no
// field for a secret or a digest, so a listing cannot leak one.
type WorkerKeySummaryResponse struct {
	ID        string     `json:"id"`
	Scope     string     `json:"scope"`
	Name      string     `json:"name"`
	Prefix    string     `json:"prefix"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at"`
}

// WorkerKeyListResponse is one bounded page of worker-key metadata, newest
// first.
type WorkerKeyListResponse struct {
	Keys []WorkerKeySummaryResponse `json:"keys"`
}

// WorkerKeyRevokedResponse reports a revocation, including an idempotent
// repeat.
type WorkerKeyRevokedResponse struct {
	WorkerKeySummaryResponse
	// AlreadyRevoked is true when this key was revoked before this call. The
	// reported revoked_at is the original instant, not this one.
	AlreadyRevoked bool `json:"already_revoked"`
}

func toWorkerKeySummary(key workerauth.Key) WorkerKeySummaryResponse {
	summary := WorkerKeySummaryResponse{
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

// handleCreateWorkerKey mints one worker credential and returns it exactly
// once.
//
// Responses:
//
//	201 the key was created; this body carries the only copy of the credential
//	422 scope or name was invalid
//	400 the body was not valid JSON, or carried an unknown field
//	503 the request exceeded its deadline before the outcome was known
//
// There is deliberately no Idempotency-Key and no 200 replay, for the
// identical reason handleCreateAPIKey has none: an idempotent create would
// have to return an existing secret to a repeat.
func (s *Server) handleCreateWorkerKey(w http.ResponseWriter, r *http.Request) {
	var request createWorkerKeyRequest
	if !s.decodeControlJSON(w, r, &request) {
		return
	}

	created, err := s.workerKeys.Create(r.Context(), request.Scope, request.Name)
	switch {
	case err == nil:
		// The credential is returned to the caller and never written anywhere
		// else. This log line carries the id, the scope, and the non-secret
		// lookup prefix -- enough to find and revoke the key, and nothing that
		// could be presented as one.
		s.log.Info("worker key created",
			slog.String("request_id", RequestIDFrom(r.Context())),
			slog.String("worker_key_id", created.Key.ID.String()),
			slog.String("scope", created.Key.Scope),
			slog.String("worker_key_prefix", created.Key.Prefix))
		writeJSON(w, s.log, http.StatusCreated, WorkerKeyCreatedResponse{
			ID:        created.Key.ID.String(),
			Scope:     created.Key.Scope,
			Name:      created.Key.Name,
			Prefix:    created.Key.Prefix,
			CreatedAt: created.Key.CreatedAt.UTC(),
			Key:       created.Raw,
		})

	case isWorkerAuthValidation(err):
		s.writeWorkerAuthValidation(w, r, err)
	case errors.Is(err, workerauth.ErrDeadlineExceeded):
		s.writeDeadlineAmbiguity(w, r, "create worker key",
			"do NOT repeat this request blindly: key creation carries no request "+
				"identity, so a repeat mints a SECOND credential rather than "+
				"returning the first. List keys through GET /internal/v1/worker-keys, "+
				"and revoke any key whose credential no caller received")
	default:
		s.internalError(w, r, "create worker key", err)
	}
}

// handleRevokeWorkerKey withdraws one worker credential.
//
// Revocation does not force-expire a session already registered under this
// key, and does not touch reconciliation: the next control-plane call that
// session makes discovers the revocation through the same lookup that
// already resolves its scope, and its lease lapses and is reconciled through
// the existing, unmodified path exactly as any other abandoned session's
// would.
//
// Responses:
//
//	200 the key is revoked, including an idempotent repeat
//	404 no key with that id
//	422 the key id was not a UUID
//	503 the request exceeded its deadline before the outcome was known
func (s *Server) handleRevokeWorkerKey(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("key_id"))
	if err != nil {
		// 422, not the 404 the public job routes answer for a malformed id.
		// This surface is loopback operator plumbing with no tenant to hide
		// ids from, so a caller is better served by being told their id is
		// malformed than by a not-found that sends them looking for a deleted
		// key.
		s.writeFieldError(w, r, CodeValidationFailed, "key_id", "must be a UUID")
		return
	}

	result, err := s.workerKeys.Revoke(r.Context(), id)
	switch {
	case err == nil:
		s.log.Info("worker key revoked",
			slog.String("request_id", RequestIDFrom(r.Context())),
			slog.String("worker_key_id", result.Key.ID.String()),
			slog.String("scope", result.Key.Scope),
			slog.String("worker_key_prefix", result.Key.Prefix),
			slog.Bool("already_revoked", result.AlreadyRevoked))
		writeJSON(w, s.log, http.StatusOK, WorkerKeyRevokedResponse{
			WorkerKeySummaryResponse: toWorkerKeySummary(result.Key),
			AlreadyRevoked:           result.AlreadyRevoked,
		})
	case errors.Is(err, workerauth.ErrKeyNotFound):
		writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "worker key not found", nil)
	case errors.Is(err, workerauth.ErrDeadlineExceeded):
		s.writeDeadlineAmbiguity(w, r, "revoke worker key",
			"repeat the identical request for the same key id; revocation is keyed "+
				"by key id alone, so a repeat reports the original revocation "+
				"instant rather than moving it")
	default:
		s.internalError(w, r, "revoke worker key", err)
	}
}

// handleListWorkerKeys returns one bounded page of worker-key metadata,
// newest first.
//
// Revoked keys are included, for the identical reason handleListAPIKeys
// includes them: an operator auditing which credentials ever existed needs
// to see them.
//
// Responses:
//
//	200 one bounded page
//	422 the limit was invalid
//	503 the request exceeded its deadline
func (s *Server) handleListWorkerKeys(w http.ResponseWriter, r *http.Request) {
	limit := workerauth.DefaultListLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > workerauth.MaxListLimit {
			s.writeFieldError(w, r, CodeValidationFailed, "limit",
				"must be an integer between 1 and "+strconv.Itoa(workerauth.MaxListLimit))
			return
		}
		limit = parsed
	}

	keys, err := s.workerKeys.List(r.Context(), limit)
	switch {
	case err == nil:
		summaries := make([]WorkerKeySummaryResponse, 0, len(keys))
		for _, key := range keys {
			summaries = append(summaries, toWorkerKeySummary(key))
		}
		writeJSON(w, s.log, http.StatusOK, WorkerKeyListResponse{Keys: summaries})
	case errors.Is(err, workerauth.ErrDeadlineExceeded):
		s.writeDeadlineAmbiguity(w, r, "list worker keys",
			"repeat the identical request; listing keys reads rows and writes "+
				"nothing, so a retry cannot change any credential")
	default:
		s.internalError(w, r, "list worker keys", err)
	}
}

func isWorkerAuthValidation(err error) bool {
	var validation *workerauth.ValidationError
	return errors.As(err, &validation)
}

// writeWorkerAuthValidation renders a workerauth validation failure in the
// one error shape every endpoint returns, converting workerauth's own field
// errors at the boundary the way writeAuthValidation converts auth's.
func (s *Server) writeWorkerAuthValidation(w http.ResponseWriter, r *http.Request, err error) {
	var validation *workerauth.ValidationError
	if !errors.As(err, &validation) {
		s.internalError(w, r, "validate worker key request", err)
		return
	}
	details := make([]jobs.FieldError, 0, len(validation.Fields))
	for _, field := range validation.Fields {
		details = append(details, jobs.FieldError{Field: field.Field, Message: field.Message})
	}
	writeError(w, r, s.log, http.StatusUnprocessableEntity, CodeValidationFailed,
		"the request was rejected by validation", details)
}
