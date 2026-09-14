package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/workerauth"
)

// WorkerKeys is the credential surface PUT /internal/v1/worker-sessions/{id}
// authenticates against, and the loopback admin routes at
// /internal/v1/worker-keys manage.
//
// It is one interface rather than two for the identical reason APIKeys is:
// *workerauth.Store satisfies the whole of it, and a Server that can
// authenticate a registration can also mint and revoke the credentials that
// registration checks. IsRevoked is the one method APIKeys has no
// counterpart for: it is what every OTHER worker-control call uses, once per
// request, instead of re-presenting a credential -- see requireWorkerKey's
// doc comment and internal/workers.Store.SessionScope.
type WorkerKeys interface {
	Authenticate(ctx context.Context, raw string) (workerauth.Principal, error)
	Create(ctx context.Context, scope, name string) (workerauth.Created, error)
	Revoke(ctx context.Context, id uuid.UUID) (workerauth.RevokeResult, error)
	List(ctx context.Context, limit int) ([]workerauth.Key, error)
	IsRevoked(ctx context.Context, id uuid.UUID) (bool, error)
}

// workerPrincipalKey carries the authenticated worker-key principal through
// the request context, exactly as scopeKey does for a public API key. It
// continues the same unexported ctxKey chain requestIDKey and scopeKey
// already established, for the same reason: nothing outside this package can
// forge a value under it.
const workerPrincipalKey ctxKey = scopeKey + 1

// workerPrincipalFrom returns the worker-key principal bound to ctx by
// requireWorkerKey.
//
// It reports ok=false rather than falling back to any default, for the
// identical reason authenticatedScope does: a handler that somehow ran
// without this wrapper must refuse, not quietly register a session under
// some other scope.
func workerPrincipalFrom(ctx context.Context) (workerauth.Principal, bool) {
	principal, ok := ctx.Value(workerPrincipalKey).(workerauth.Principal)
	return principal, ok
}

// requireWorkerKey wraps the one handler that authenticates a worker
// credential: PUT /internal/v1/worker-sessions/{worker_session_id}.
//
// No other worker-control route is wrapped in this. Heartbeat, Claim, Start,
// Succeed, Fail, AcknowledgeCancellation, and RenewLease all resolve scope
// from the session identity the request already carries
// (internal/workers.Store.SessionScope) and check IsRevoked against the
// worker key that session recorded at registration, rather than requiring a
// fresh credential on every call. That asymmetry is deliberate: a session's
// identity, once authenticated, is the same kind of bearer capability every
// other worker-control call has used since M2 (a worker id and a session id
// neither one can forge), and re-verifying a secret on every heartbeat would
// buy nothing a revocation check does not already buy more cheaply.
//
// When no credential store is wired, this fails CLOSED, for the identical
// reason requireAPIKey does: a server that can authenticate nobody has no
// authenticated caller to serve, and there is no test-only bypass anywhere in
// this package for either surface.
func (s *Server) requireWorkerKey(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.workerKeys == nil {
			s.writeWorkerKeyUnauthorized(w, r)
			return
		}

		credential, ok := bearerCredential(r.Header.Get(authorizationHeader))
		if !ok {
			s.writeWorkerKeyUnauthorized(w, r)
			return
		}

		principal, err := s.workerKeys.Authenticate(r.Context(), credential)
		switch {
		case err == nil:
			next(w, r.WithContext(context.WithValue(r.Context(), workerPrincipalKey, principal)))
		case errors.Is(err, workerauth.ErrUnauthorized):
			s.writeWorkerKeyUnauthorized(w, r)
		case errors.Is(err, workerauth.ErrDeadlineExceeded):
			// Not 401, for the identical reason requireAPIKey's deadline case
			// is not: verification commits nothing, so an identical retry is
			// unconditionally safe, and answering "not authorized" to a
			// database timeout sends an operator hunting for a revocation that
			// never happened.
			s.writeDeadlineAmbiguity(w, r, "authenticate worker key",
				"repeat the identical request with the same credential; verifying "+
					"a worker key reads one row and writes nothing, so a retry can "+
					"neither duplicate work nor change what the key is allowed to do")
		default:
			s.internalError(w, r, "authenticate worker key", err)
		}
	}
}

// writeWorkerKeyUnauthorized renders the single 401 every worker-key
// authentication failure gets.
//
// It reuses CodeUnauthorized -- the same stable code a rejected public API
// key answers -- because both report the identical fact: no valid credential
// was presented. The message differs only to name the right credential for
// the endpoint the caller actually reached; naming it leaks nothing, since a
// caller already knows which URL they called.
func (s *Server) writeWorkerKeyUnauthorized(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeError(w, r, s.log, http.StatusUnauthorized, CodeUnauthorized,
		"a valid worker key must be presented as an Authorization: Bearer credential", nil)
}
