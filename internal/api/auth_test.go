package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/auth"
)

const testScope = "tenant-under-test"

// testRawKey is a well-formed credential: the tfk_ marker, a 22-character
// lookup segment, '.', and a 43-character secret. It is shaped correctly on
// purpose, so a test that expects a rejection is rejected by the credential
// store rather than incidentally by ParseKey.
var testRawKey = auth.KeyPrefix + strings.Repeat("a", auth.LookupLen) + "." + strings.Repeat("b", auth.SecretLen)

// fakeKeys is a credential store with no database behind it.
//
// It implements the whole APIKeys interface because that is what the Server
// wires, but every test here drives only the behavior it names.
type fakeKeys struct {
	authenticate func(context.Context, string) (auth.Principal, error)
	create       func(context.Context, string, string) (auth.Created, error)
	revoke       func(context.Context, uuid.UUID) (auth.RevokeResult, error)
	list         func(context.Context, int) ([]auth.Key, error)
}

func (f *fakeKeys) Authenticate(ctx context.Context, raw string) (auth.Principal, error) {
	if f.authenticate == nil {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	return f.authenticate(ctx, raw)
}

func (f *fakeKeys) Create(ctx context.Context, scope, name string) (auth.Created, error) {
	if f.create == nil {
		return auth.Created{}, auth.ErrUnauthorized
	}
	return f.create(ctx, scope, name)
}

func (f *fakeKeys) Revoke(ctx context.Context, id uuid.UUID) (auth.RevokeResult, error) {
	if f.revoke == nil {
		return auth.RevokeResult{}, auth.ErrKeyNotFound
	}
	return f.revoke(ctx, id)
}

func (f *fakeKeys) List(ctx context.Context, limit int) ([]auth.Key, error) {
	if f.list == nil {
		return nil, nil
	}
	return f.list(ctx, limit)
}

// acceptingKeys accepts exactly testRawKey and nothing else.
func acceptingKeys(scope string) *fakeKeys {
	return &fakeKeys{
		authenticate: func(_ context.Context, raw string) (auth.Principal, error) {
			if raw != testRawKey {
				return auth.Principal{}, auth.ErrUnauthorized
			}
			return auth.Principal{KeyID: uuid.New(), Scope: scope}, nil
		},
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

// publicRoutes is every route the public surface exposes. Each case must
// answer 401 without a credential, so a route added later without the wrapper
// shows up as a failure here rather than as an open endpoint.
var publicRoutes = []struct{ method, path string }{
	{http.MethodPost, "/v1/jobs"},
	{http.MethodGet, "/v1/jobs/" + "11111111-1111-1111-1111-111111111111"},
	{http.MethodGet, "/v1/jobs/11111111-1111-1111-1111-111111111111/result"},
	{http.MethodPost, "/v1/jobs/11111111-1111-1111-1111-111111111111/cancel"},
	{http.MethodPost, "/v1/jobs/11111111-1111-1111-1111-111111111111/retry"},
	{http.MethodGet, "/v1/dlq"},
	{http.MethodPost, "/v1/dlq/11111111-1111-1111-1111-111111111111/replay"},
}

// TestAuth_EveryPublicRouteRefusesAnUnauthenticatedRequest is the load-bearing
// assertion of this milestone.
//
// The job store is nil, so a route that let an unauthenticated request through
// to its handler would panic rather than quietly pass. Combined with the route
// list above, this is what makes "the public surface is authenticated" a tested
// property instead of a claim about the registration code.
func TestAuth_EveryPublicRouteRefusesAnUnauthenticatedRequest(t *testing.T) {
	handler := newTestServer(t)

	for _, route := range publicRoutes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(route.method, route.path, strings.NewReader(validBody))
			request.Header.Set("Idempotency-Key", "k1")
			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusUnauthorized, recorder.Code)
			body := decodeError(t, recorder)
			require.Equal(t, CodeUnauthorized, body.Error.Code)
			require.NotEmpty(t, body.Error.RequestID)
			require.Equal(t, "Bearer", recorder.Header().Get("WWW-Authenticate"))
		})
	}
}

// TestAuth_EveryPublicRouteConsultsTheCredentialStore proves the wrapper is
// actually applied to each route, which the 401 above does NOT prove on its own.
//
// scopeOrUnauthorized is a second line of defense inside every public handler,
// so a route registered WITHOUT requireAPIKey still answers 401 — the right
// status, reached the wrong way, and the earlier test cannot tell the two apart.
// Counting calls into the credential store can: an unwrapped route never makes
// one.
//
// The difference is not academic. Without the wrapper the handler runs, so an
// unauthenticated caller reaches body decoding and validation; and the next
// public handler written without that internal guard would simply be open.
func TestAuth_EveryPublicRouteConsultsTheCredentialStore(t *testing.T) {
	for _, route := range publicRoutes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			var consulted int
			handler := NewServer(nil, Config{MaxRequestBytes: 1024}, discardLogger()).
				WithAuth(&fakeKeys{
					authenticate: func(_ context.Context, raw string) (auth.Principal, error) {
						consulted++
						require.Equal(t, testRawKey, raw,
							"the wrapper must hand the store the credential as presented")
						return auth.Principal{KeyID: uuid.New(), Scope: testScope}, nil
					},
				}).
				WithResults(acceptingResults(), nil).
				Handler()

			recorder := httptest.NewRecorder()
			request := authorize(httptest.NewRequest(route.method, route.path, strings.NewReader(validBody)))
			request.Header.Set("Idempotency-Key", "k1")
			// The job store is nil, so an authenticated request panics on its way
			// into PostgreSQL. That is the proof it got past authentication, and
			// it is recovered into a 500 by the recovery middleware.
			handler.ServeHTTP(recorder, request)

			require.Equal(t, 1, consulted,
				"this route did not authenticate; it is very likely missing requireAPIKey")
		})
	}
}

// Authentication must precede body decoding and idempotency validation.
//
// Otherwise an unauthenticated caller can probe the validation surface — which
// fields exist, which are rejected, how large a body is accepted — without ever
// holding a credential.
func TestAuth_HappensBeforeTheRequestBodyIsLookedAt(t *testing.T) {
	handler := newTestServer(t)

	for name, body := range map[string]string{
		"malformed JSON": `{`,
		"unknown field":  `{"queue":"default","job_type":"demo.echo","payload":{},"priorty":1}`,
		"oversized body": `{"queue":"default","job_type":"demo.echo","payload":{"a":"` + strings.Repeat("x", 4096) + `"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			// No credential, and a body that would otherwise produce 400 or 413.
			request := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(body))
			request.Header.Set("Idempotency-Key", "k1")
			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusUnauthorized, recorder.Code,
				"an unauthenticated caller must not learn anything about validation")
			require.Equal(t, CodeUnauthorized, decodeError(t, recorder).Error.Code)
		})
	}

	t.Run("a missing Idempotency-Key is still 401, not 422", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(validBody)))
		require.Equal(t, http.StatusUnauthorized, recorder.Code)
	})
}

// A server built without WithAuth must fail CLOSED.
//
// This is the reason there is no test-only bypass anywhere in this package: a
// binary that forgets to wire a credential store serves a closed API, not an
// open one, so the mistake is a visible outage rather than a silent breach.
func TestAuth_AServerWithNoCredentialStoreRefusesEverything(t *testing.T) {
	handler := NewServer(nil, Config{MaxRequestBytes: 1024}, discardLogger()).Handler()

	for _, route := range publicRoutes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := authorize(httptest.NewRequest(route.method, route.path, strings.NewReader(validBody)))
			request.Header.Set("Idempotency-Key", "k1")
			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusUnauthorized, recorder.Code,
				"a valid-looking credential must not be honored by a server that can verify nothing")
			require.Equal(t, CodeUnauthorized, decodeError(t, recorder).Error.Code)
		})
	}

	t.Run("health probes stay reachable", func(t *testing.T) {
		for _, path := range []string{"/healthz", "/readyz"} {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
			require.Equal(t, http.StatusOK, recorder.Code,
				"an unauthenticated liveness probe must not depend on a credential store")
		}
	})

	t.Run("key management is not registered", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/internal/v1/api-keys",
			strings.NewReader(`{"scope":"s","name":"n"}`)))
		require.Equal(t, http.StatusNotFound, recorder.Code)
	})
}

// Every rejected credential must produce a byte-identical body and status.
//
// If a missing header, an unknown key, and a revoked key answered differently,
// a prefix would become an oracle: present a guess with any secret and the
// response tells you whether that prefix exists, and whether the key behind it
// is still live.
func TestAuth_EveryRejectionIsIndistinguishable(t *testing.T) {
	reject := func(err error) http.Handler {
		return NewServer(nil, Config{MaxRequestBytes: 1024}, discardLogger()).
			WithAuth(&fakeKeys{
				authenticate: func(context.Context, string) (auth.Principal, error) {
					return auth.Principal{}, err
				},
			}).Handler()
	}

	type response struct {
		status int
		code   string
		body   string
	}
	probe := func(handler http.Handler, header string) response {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/v1/dlq", nil)
		if header != "" {
			request.Header.Set(authorizationHeader, header)
		}
		handler.ServeHTTP(recorder, request)

		var envelope ErrorBody
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
		// The request id differs per request by design, so it is removed before
		// the bodies are compared.
		envelope.Error.RequestID = ""
		normalized, err := json.Marshal(envelope)
		require.NoError(t, err)
		return response{status: recorder.Code, code: envelope.Error.Code, body: string(normalized)}
	}

	unauthorized := reject(auth.ErrUnauthorized)
	cases := map[string]response{
		"no header":            probe(unauthorized, ""),
		"empty bearer":         probe(unauthorized, "Bearer "),
		"wrong scheme":         probe(unauthorized, "Basic "+testRawKey),
		"malformed credential": probe(unauthorized, "Bearer not-a-key"),
		"unknown or revoked":   probe(unauthorized, "Bearer "+testRawKey),
	}

	baseline := cases["no header"]
	require.Equal(t, http.StatusUnauthorized, baseline.status)
	require.Equal(t, CodeUnauthorized, baseline.code)
	for name, got := range cases {
		require.Equalf(t, baseline.status, got.status, "%s answered a different status", name)
		require.Equalf(t, baseline.body, got.body, "%s answered a different body", name)
	}

	// And the message itself must not name a cause.
	for _, forbidden := range []string{"revoked", "expired", "unknown", "malformed", "not found", "prefix"} {
		require.NotContains(t, strings.ToLower(baseline.body), forbidden,
			"the 401 message must not tell a caller which check failed")
	}
}

// An oversized Authorization header must be discarded on length alone, before
// the credential store is consulted at all. Without that bound a caller could
// choose how much work a rejected request costs.
func TestAuth_AnOversizedHeaderIsRejectedWithoutConsultingTheStore(t *testing.T) {
	var consulted bool
	handler := NewServer(nil, Config{MaxRequestBytes: 1024}, discardLogger()).
		WithAuth(&fakeKeys{
			authenticate: func(context.Context, string) (auth.Principal, error) {
				consulted = true
				return auth.Principal{}, auth.ErrUnauthorized
			},
		}).Handler()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/dlq", nil)
	request.Header.Set(authorizationHeader, "Bearer "+strings.Repeat("a", maxAuthorizationHeaderLen))
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.False(t, consulted, "an oversized header must never reach the credential store")
}

// The scheme token is case-insensitive per RFC 7235. A client sending "bearer"
// is not making a mistake worth a 401 an operator then has to diagnose.
func TestAuth_BearerSchemeIsMatchedCaseInsensitively(t *testing.T) {
	for _, header := range []string{"Bearer ", "bearer ", "BEARER ", "BeArEr "} {
		t.Run(strings.TrimSpace(header), func(t *testing.T) {
			credential, ok := bearerCredential(header + testRawKey)
			require.True(t, ok)
			require.Equal(t, testRawKey, credential)
		})
	}
}

func TestBearerCredential_RejectsWhatIsNotABearerCredential(t *testing.T) {
	for name, header := range map[string]string{
		"empty":              "",
		"scheme only":        "Bearer",
		"scheme and space":   "Bearer ",
		"scheme and spaces":  "Bearer    ",
		"wrong scheme":       "Basic " + testRawKey,
		"no scheme":          testRawKey,
		"oversized":          "Bearer " + strings.Repeat("a", maxAuthorizationHeaderLen),
		"credential is null": "Bearer \x00",
	} {
		t.Run(name, func(t *testing.T) {
			credential, ok := bearerCredential(header)
			if name == "credential is null" {
				// A control character is not rejected here: it is not a valid
				// credential shape, so auth.ParseKey refuses it, and this layer
				// deliberately makes no second judgment about content.
				require.True(t, ok)
				require.Equal(t, "\x00", credential)
				return
			}
			require.False(t, ok)
			require.Empty(t, credential)
		})
	}
}

// An authenticated request must reach its handler carrying the key's own
// scope, read from the credential store's answer rather than any
// process-wide default.
func TestAuth_AnAuthenticatedRequestCarriesTheKeyScope(t *testing.T) {
	var seen string
	var found bool
	server := NewServer(nil, Config{MaxRequestBytes: 1024}, discardLogger()).
		WithAuth(acceptingKeys("scope-from-the-key"))

	handler := server.requireAPIKey(func(_ http.ResponseWriter, r *http.Request) {
		seen, found = authenticatedScope(r.Context())
	})

	recorder := httptest.NewRecorder()
	handler(recorder, authorize(httptest.NewRequest(http.MethodGet, "/v1/dlq", nil)))

	require.True(t, found, "the authenticated scope must be bound to the request context")
	require.Equal(t, "scope-from-the-key", seen)
}

// A handler reached without the wrapper must refuse rather than fall back.
//
// requireAPIKey guarantees the scope is present, so this never fires in a wired
// server. It exists because the failure mode of the alternative is silent: a
// public route registered without the wrapper would otherwise read whatever
// scope the process was configured with and serve another tenant's data.
func TestAuth_AHandlerWithNoScopeInContextRefusesInsteadOfFallingBack(t *testing.T) {
	server := NewServer(nil, Config{MaxRequestBytes: 1024}, discardLogger())

	recorder := httptest.NewRecorder()
	scope, ok := server.scopeOrUnauthorized(recorder, httptest.NewRequest(http.MethodGet, "/v1/dlq", nil))

	require.False(t, ok)
	require.Empty(t, scope)
	require.Equal(t, http.StatusUnauthorized, recorder.Code)
}

// A deadline inside the credential lookup is 503, never 401.
//
// "I could not verify this" and "you are not authorized" are different facts,
// and answering the second to the first sends an operator hunting for a revoked
// key that was never revoked.
func TestAuth_ADeadlineDuringVerificationIsNotAnAuthenticationFailure(t *testing.T) {
	handler := NewServer(nil, Config{MaxRequestBytes: 1024}, discardLogger()).
		WithAuth(&fakeKeys{
			authenticate: func(context.Context, string) (auth.Principal, error) {
				return auth.Principal{}, auth.ErrDeadlineExceeded
			},
		}).Handler()

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authorize(httptest.NewRequest(http.MethodGet, "/v1/dlq", nil)))

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	body := decodeError(t, recorder)
	require.Equal(t, CodeServiceUnavailable, body.Error.Code)
	require.Contains(t, strings.ToLower(body.Error.Message), "deadline")
	// Verification reads one row and writes nothing, so this is the one 503 in
	// the API that may promise an identical retry is unconditionally safe.
	require.Contains(t, strings.ToLower(body.Error.Message), "writes nothing")
}

// The credential must never reach a log line, in any code path.
//
// The one that is easy to get wrong is the REJECTION path: a "bad key" log that
// echoes what was presented writes a live credential into the logs of every
// operator who tails them, because a mistyped header often differs from a valid
// key by one character.
func TestAuth_TheCredentialNeverReachesALogLine(t *testing.T) {
	var logged bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logged, nil))

	for name, keys := range map[string]*fakeKeys{
		"accepted": acceptingKeys(testScope),
		"rejected": {},
	} {
		t.Run(name, func(t *testing.T) {
			logged.Reset()
			handler := NewServer(nil, Config{MaxRequestBytes: 1024}, log).
				WithAuth(keys).Handler()

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, authorize(httptest.NewRequest(http.MethodGet, "/v1/dlq", nil)))

			require.NotEmpty(t, logged.String(), "the access log must still record the request")
			require.NotContains(t, logged.String(), testRawKey)
			// Not even the secret segment on its own.
			require.NotContains(t, logged.String(), strings.Repeat("b", auth.SecretLen))
			require.NotContains(t, recorder.Body.String(), testRawKey)
		})
	}
}
