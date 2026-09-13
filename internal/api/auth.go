package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/auth"
)

// APIKeys is the credential surface the public routes authenticate against and
// the loopback admin routes manage.
//
// It is one interface rather than two because *auth.Store satisfies the whole of
// it and a Server that can authenticate can also mint. Splitting it would create
// a wiring shape -- authentication without management, or the reverse -- that no
// binary and no test actually wants.
type APIKeys interface {
	Authenticate(ctx context.Context, raw string) (auth.Principal, error)
	Create(ctx context.Context, scope, name string) (auth.Created, error)
	Revoke(ctx context.Context, id uuid.UUID) (auth.RevokeResult, error)
	List(ctx context.Context, limit int) ([]auth.Key, error)
}

// authorizationHeader is the one header a public request authenticates with.
const authorizationHeader = "Authorization"

// bearerScheme is matched case-insensitively: RFC 7235 defines the scheme token
// as case-insensitive, and a client that sends "bearer" is not making a mistake
// worth a 401 the operator then has to diagnose.
const bearerScheme = "bearer"

// maxAuthorizationHeaderLen bounds what is read from the header before anything
// else happens.
//
// It is the credential bound plus room for the scheme, so a near-miss is still
// rejected by shape with a real reason while an unbounded value is discarded
// outright -- the same rule sanitizeRequestID applies to X-Request-Id, for the
// same reason: a caller must not be able to choose how much work a rejected
// request costs.
const maxAuthorizationHeaderLen = len(bearerScheme) + 1 + auth.MaxCredentialLen

// scopeKey carries the authenticated scope through the request context.
//
// It is an unexported type so nothing outside this package can collide with it
// or forge a value, which is what makes "the scope in the context was put there
// by the authentication layer" a property rather than a convention.
const scopeKey ctxKey = requestIDKey + 1

// authenticatedScope returns the scope bound to ctx by requireAPIKey.
//
// It reports ok=false rather than falling back to a configured default. A
// handler that somehow ran without authentication must refuse, not quietly serve
// another tenant's scope: silently substituting DevScope here is exactly the
// defect this milestone exists to remove.
func authenticatedScope(ctx context.Context) (string, bool) {
	scope, ok := ctx.Value(scopeKey).(string)
	return scope, ok
}

// requireAPIKey wraps one public handler with authentication.
//
// It is applied per route rather than as an outer layer so the set of
// authenticated routes is visible at registration in Handler(), instead of being
// an inclusion list buried in a middleware that would have to be kept in sync
// with the mux.
//
// When no credential store is wired, this fails CLOSED: every public request is
// unauthenticated because nothing can authenticate it, so 401 is the honest
// answer. That is deliberate, and it is why there is no test-only bypass
// anywhere in this package -- a binary that forgot to call WithAuth serves a
// closed API rather than an open one.
func (s *Server) requireAPIKey(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.keys == nil {
			s.writeUnauthorized(w, r)
			return
		}

		credential, ok := bearerCredential(r.Header.Get(authorizationHeader))
		if !ok {
			s.writeUnauthorized(w, r)
			return
		}

		principal, err := s.keys.Authenticate(r.Context(), credential)
		switch {
		case err == nil:
			next(w, r.WithContext(context.WithValue(r.Context(), scopeKey, principal.Scope)))
		case errors.Is(err, auth.ErrUnauthorized):
			s.writeUnauthorized(w, r)
		case errors.Is(err, auth.ErrDeadlineExceeded):
			// Not 401. A database timeout is "I could not tell", and answering
			// "you are not authorized" to it would send an operator hunting for a
			// revoked key that was never revoked. Verification commits nothing,
			// so an identical retry is unambiguously safe here -- which is more
			// than the mutating routes can promise about their own 503.
			s.writeDeadlineAmbiguity(w, r, "authenticate api key",
				"repeat the identical request with the same credential; verifying "+
					"a key reads one row and writes nothing, so a retry can neither "+
					"duplicate work nor change what the key is allowed to do")
		default:
			s.internalError(w, r, "authenticate api key", err)
		}
	}
}

// bearerCredential extracts the credential from an Authorization header.
//
// It returns only "did this yield a credential", never why it did not: the
// caller answers one indistinguishable 401 for a missing header, a wrong scheme,
// and a malformed credential alike, matching auth.Store.Authenticate's single
// sentinel.
func bearerCredential(header string) (string, bool) {
	if header == "" || len(header) > maxAuthorizationHeaderLen {
		return "", false
	}
	scheme, credential, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, bearerScheme) {
		return "", false
	}
	// One optional space is already consumed by Cut; anything further is padding
	// a client should not be sending and a server should not be guessing about.
	credential = strings.TrimLeft(credential, " ")
	if credential == "" {
		return "", false
	}
	return credential, true
}

// writeUnauthorized renders the single 401 every authentication failure gets.
//
// The message names no cause on purpose. "unknown key", "revoked key", and
// "wrong secret" are three different sentences an attacker can use to tell
// which prefixes exist, and one of them tells a thief that the key they stole
// has been revoked.
//
// WWW-Authenticate is set because RFC 7235 requires a 401 to carry it, and
// because it tells a client which scheme to present rather than leaving them to
// guess. It names the scheme and nothing else: a realm or an error code here
// would be the same leak in a different header.
func (s *Server) writeUnauthorized(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeError(w, r, s.log, http.StatusUnauthorized, CodeUnauthorized,
		"a valid API key must be presented as an Authorization: Bearer credential", nil)
}

// scopeOrUnauthorized reads the authenticated scope, refusing the request if it
// is absent.
//
// requireAPIKey guarantees the value is there, so this never fires in a wired
// server. It exists so that if a public route is ever registered without the
// wrapper, the failure is a refusal rather than a silent read of whatever scope
// the process happened to be configured with.
func (s *Server) scopeOrUnauthorized(w http.ResponseWriter, r *http.Request) (string, bool) {
	scope, ok := authenticatedScope(r.Context())
	if !ok {
		s.writeUnauthorized(w, r)
		return "", false
	}
	return scope, true
}
