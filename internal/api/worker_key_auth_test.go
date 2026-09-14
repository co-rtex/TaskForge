package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/workerauth"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// registrationRequest builds a well-formed PUT worker-sessions body, since
// these tests care about authentication, not registration field validation.
func registrationRequest() *http.Request {
	body := `{"worker_name":"w","hostname":"h","worker_group":"default",` +
		`"concurrency_limit":1,"capabilities":[],"supported_job_types":[]}`
	return httptest.NewRequest(http.MethodPut,
		"/internal/v1/worker-sessions/"+uuid.NewString(), strings.NewReader(body))
}

// TestRequireWorkerKey_RegistrationRefusesAnUnauthenticatedRequest is the
// registration-side counterpart to TestAuth_EveryPublicRouteRefusesAnUnauthenticatedRequest:
// the one worker-control route that authenticates must actually refuse a
// caller who presents nothing.
func TestRequireWorkerKey_RegistrationRefusesAnUnauthenticatedRequest(t *testing.T) {
	handler := NewServer(nil, Config{MaxRequestBytes: 2048}, discardLogger()).
		WithWorkerControl(&fakeWorkerControl{
			register: func(context.Context, string, workers.Registration) (workers.Session, error) {
				t.Fatal("Register must not be reached without a credential")
				return workers.Session{}, nil
			},
		}).
		WithWorkerAuth(acceptingWorkerKeys("tenant-a")).
		Handler()

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, registrationRequest())

	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.Equal(t, CodeUnauthorized, decodeError(t, recorder).Error.Code)
	require.Equal(t, "Bearer", recorder.Header().Get("WWW-Authenticate"))
}

// TestRequireWorkerKey_RejectsAnUnknownOrRevokedCredential proves the
// wrapper actually consults the worker-key store, and that its rejection is
// indistinguishable from the no-credential case.
func TestRequireWorkerKey_RejectsAnUnknownOrRevokedCredential(t *testing.T) {
	rejecting := &fakeWorkerKeys{
		authenticate: func(context.Context, string) (workerauth.Principal, error) {
			return workerauth.Principal{}, workerauth.ErrUnauthorized
		},
	}
	handler := NewServer(nil, Config{MaxRequestBytes: 2048}, discardLogger()).
		WithWorkerControl(&fakeWorkerControl{
			register: func(context.Context, string, workers.Registration) (workers.Session, error) {
				t.Fatal("Register must not be reached with a rejected credential")
				return workers.Session{}, nil
			},
		}).
		WithWorkerAuth(rejecting).
		Handler()

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authorizeWorker(registrationRequest()))

	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.Equal(t, CodeUnauthorized, decodeError(t, recorder).Error.Code)
}

// TestRequireWorkerKey_ADeadlineDuringVerificationIsNotAnAuthenticationFailure
// mirrors the identical assertion requireAPIKey already makes: "I could not
// verify this" and "you are not authorized" are different facts.
func TestRequireWorkerKey_ADeadlineDuringVerificationIsNotAnAuthenticationFailure(t *testing.T) {
	handler := NewServer(nil, Config{MaxRequestBytes: 2048}, discardLogger()).
		WithWorkerControl(&fakeWorkerControl{}).
		WithWorkerAuth(&fakeWorkerKeys{
			authenticate: func(context.Context, string) (workerauth.Principal, error) {
				return workerauth.Principal{}, workerauth.ErrDeadlineExceeded
			},
		}).
		Handler()

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authorizeWorker(registrationRequest()))

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Equal(t, CodeServiceUnavailable, decodeError(t, recorder).Error.Code)
}

// TestRequireWorkerKey_AServerWithNoWorkerKeyStoreRefusesRegistrationOnly is
// the load-bearing asymmetry assertion of this milestone: registration fails
// closed exactly like the public surface does without WithAuth, but the
// other seven worker-control routes -- which never re-present a credential --
// stay reachable through WithWorkerControl alone. A server that could
// authenticate nobody but still let heartbeat or claim through would be a
// silent hole; a server that also closed heartbeat because no worker-key
// store exists would be wrong in the other direction, since heartbeat
// authenticates the session, not a presented key.
func TestRequireWorkerKey_AServerWithNoWorkerKeyStoreRefusesRegistrationOnly(t *testing.T) {
	var heartbeatCalled bool
	handler := NewServer(nil, Config{MaxRequestBytes: 2048}, discardLogger()).
		WithWorkerControl(&fakeWorkerControl{
			register: func(context.Context, string, workers.Registration) (workers.Session, error) {
				t.Fatal("Register must not be reached without a worker-key store")
				return workers.Session{}, nil
			},
			heartbeat: func(context.Context, string, workers.HeartbeatRequest) (workers.HeartbeatResult, error) {
				heartbeatCalled = true
				return workers.HeartbeatResult{Status: workers.SessionHealthy, LastHeartbeatAt: time.Now()}, nil
			},
		}).
		Handler()

	registerRecorder := httptest.NewRecorder()
	handler.ServeHTTP(registerRecorder, authorizeWorker(registrationRequest()))
	require.Equal(t, http.StatusUnauthorized, registerRecorder.Code,
		"registration must fail closed with no worker-key store, exactly as requireAPIKey does with no key store")

	heartbeatBody := `{"worker_id":"` + uuid.NewString() + `"}`
	heartbeatRecorder := httptest.NewRecorder()
	handler.ServeHTTP(heartbeatRecorder, httptest.NewRequest(http.MethodPost,
		"/internal/v1/worker-sessions/"+uuid.NewString()+"/heartbeat", strings.NewReader(heartbeatBody)))
	require.Equal(t, http.StatusOK, heartbeatRecorder.Code,
		"heartbeat authenticates the session, not a presented credential, so it must stay reachable")
	require.True(t, heartbeatCalled)
}

// TestResolveWorkerControlScope_RefusesANonRegisterCallWhoseWorkerKeyWasRevoked
// is the test that would have caught a missing revocation check: a session
// registered under a worker key that has since been revoked must be refused
// on its very next call, per the confirmed Option B semantics, without ever
// re-presenting a credential.
func TestResolveWorkerControlScope_RefusesANonRegisterCallWhoseWorkerKeyWasRevoked(t *testing.T) {
	revokedKeyID := uuid.New()
	var claimCalled bool
	control := &fakeWorkerControl{
		sessionScope: func(context.Context, uuid.UUID) (string, *uuid.UUID, error) {
			return "tenant-a", &revokedKeyID, nil
		},
		claim: func(context.Context, string, workers.ClaimRequest) (workers.ClaimResult, error) {
			claimCalled = true
			return workers.ClaimResult{Disposition: workers.QueueEmpty}, nil
		},
	}
	handler := NewServer(nil, Config{MaxRequestBytes: 2048}, discardLogger()).
		WithWorkerControl(control).
		WithWorkerAuth(&fakeWorkerKeys{
			isRevoked: func(_ context.Context, id uuid.UUID) (bool, error) {
				require.Equal(t, revokedKeyID, id, "the revocation check must be keyed by the session's own worker key")
				return true, nil
			},
		}).
		Handler()

	body := `{"worker_id":"` + uuid.NewString() + `","worker_session_id":"` + uuid.NewString() +
		`","claim_request_id":"` + uuid.NewString() + `","queue":"default"}`
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/internal/v1/claims", strings.NewReader(body)))

	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.Equal(t, CodeUnauthorized, decodeError(t, recorder).Error.Code)
	require.False(t, claimCalled, "a revoked session's worker key must refuse the call before it reaches Claim")
}

// TestResolveWorkerControlScope_ANonRevokedWorkerKeySucceeds is the positive
// control for the test above: a session whose worker key is live reaches the
// control-plane call, and with the scope SessionScope reported.
func TestResolveWorkerControlScope_ANonRevokedWorkerKeySucceeds(t *testing.T) {
	liveKeyID := uuid.New()
	var gotScope string
	control := &fakeWorkerControl{
		sessionScope: func(context.Context, uuid.UUID) (string, *uuid.UUID, error) {
			return "tenant-a", &liveKeyID, nil
		},
		claim: func(_ context.Context, scope string, _ workers.ClaimRequest) (workers.ClaimResult, error) {
			gotScope = scope
			return workers.ClaimResult{Disposition: workers.QueueEmpty}, nil
		},
	}
	handler := NewServer(nil, Config{MaxRequestBytes: 2048}, discardLogger()).
		WithWorkerControl(control).
		WithWorkerAuth(&fakeWorkerKeys{
			isRevoked: func(_ context.Context, id uuid.UUID) (bool, error) {
				require.Equal(t, liveKeyID, id)
				return false, nil
			},
		}).
		Handler()

	body := `{"worker_id":"` + uuid.NewString() + `","worker_session_id":"` + uuid.NewString() +
		`","claim_request_id":"` + uuid.NewString() + `","queue":"default"}`
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/internal/v1/claims", strings.NewReader(body)))

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "tenant-a", gotScope)
}

// TestResolveWorkerControlScope_FailsClosedWhenNoWorkerKeyStoreCanCheckRevocation
// covers the session-was-authenticated-but-this-server-instance-cannot-verify-
// revocation case: a session was registered with a real worker key
// (SessionScope reports a non-nil id), but this Server was built without
// WithWorkerAuth. Trusting the session anyway would mean a credential minted
// on one, later-revoked, instance could never be checked once WithWorkerAuth
// is dropped from a deployment -- so this must refuse, not fall back to
// treating a real key id as though it were never revoked.
func TestResolveWorkerControlScope_FailsClosedWhenNoWorkerKeyStoreCanCheckRevocation(t *testing.T) {
	someKeyID := uuid.New()
	var claimCalled bool
	control := &fakeWorkerControl{
		sessionScope: func(context.Context, uuid.UUID) (string, *uuid.UUID, error) {
			return "tenant-a", &someKeyID, nil
		},
		claim: func(context.Context, string, workers.ClaimRequest) (workers.ClaimResult, error) {
			claimCalled = true
			return workers.ClaimResult{Disposition: workers.QueueEmpty}, nil
		},
	}
	handler := NewServer(nil, Config{MaxRequestBytes: 2048}, discardLogger()).
		WithWorkerControl(control).
		Handler()

	body := `{"worker_id":"` + uuid.NewString() + `","worker_session_id":"` + uuid.NewString() +
		`","claim_request_id":"` + uuid.NewString() + `","queue":"default"}`
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/internal/v1/claims", strings.NewReader(body)))

	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.False(t, claimCalled)
}

// TestResolveWorkerControlScope_ASessionWithNoWorkerKeyIsNeverTreatedAsRevoked
// pins the nil-workerKeyID reading migrations/0015_worker_keys.sql documents:
// a session registered through a path that never presented a worker key (or
// one that predates this milestone) is simply never checked, even when this
// server has no worker-key store wired at all.
func TestResolveWorkerControlScope_ASessionWithNoWorkerKeyIsNeverTreatedAsRevoked(t *testing.T) {
	var gotScope string
	control := &fakeWorkerControl{
		sessionScope: func(context.Context, uuid.UUID) (string, *uuid.UUID, error) {
			return "tenant-a", nil, nil
		},
		claim: func(_ context.Context, scope string, _ workers.ClaimRequest) (workers.ClaimResult, error) {
			gotScope = scope
			return workers.ClaimResult{Disposition: workers.QueueEmpty}, nil
		},
	}
	handler := NewServer(nil, Config{MaxRequestBytes: 2048}, discardLogger()).
		WithWorkerControl(control).
		Handler()

	body := `{"worker_id":"` + uuid.NewString() + `","worker_session_id":"` + uuid.NewString() +
		`","claim_request_id":"` + uuid.NewString() + `","queue":"default"}`
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/internal/v1/claims", strings.NewReader(body)))

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "tenant-a", gotScope)
}

// TestResolveWorkerControlScope_PropagatesAnUnknownSessionAsTheExistingConflict
// proves SessionScope's ErrSessionUnavailable reaches the caller through the
// same 409 an unmodified fenced call already produces for a bogus id, rather
// than a new error shape this milestone introduced.
func TestResolveWorkerControlScope_PropagatesAnUnknownSessionAsTheExistingConflict(t *testing.T) {
	control := &fakeWorkerControl{
		sessionScope: func(context.Context, uuid.UUID) (string, *uuid.UUID, error) {
			return "", nil, workers.ErrSessionUnavailable
		},
	}
	handler := NewServer(nil, Config{MaxRequestBytes: 2048}, discardLogger()).
		WithWorkerControl(control).
		Handler()

	body := `{"worker_id":"` + uuid.NewString() + `","worker_session_id":"` + uuid.NewString() +
		`","claim_request_id":"` + uuid.NewString() + `","queue":"default"}`
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/internal/v1/claims", strings.NewReader(body)))

	require.Equal(t, http.StatusConflict, recorder.Code)
	require.Equal(t, CodeSessionUnavailable, decodeError(t, recorder).Error.Code)
}

// TestHandleRegisterWorkerSession_PersistsThePrincipalsScopeAndKeyID proves
// the HTTP layer actually threads the authenticated principal into
// Registration -- both the scope Register is called with and the
// WorkerKeyID persisted on the row that SessionScope later reads back.
func TestHandleRegisterWorkerSession_PersistsThePrincipalsScopeAndKeyID(t *testing.T) {
	principalKeyID := uuid.New()
	var gotScope string
	var gotRegistration workers.Registration
	control := &fakeWorkerControl{
		register: func(_ context.Context, scope string, reg workers.Registration) (workers.Session, error) {
			gotScope = scope
			gotRegistration = reg
			return workers.Session{
				ID: reg.SessionID, WorkerID: uuid.New(), Name: reg.Name, Hostname: reg.Hostname,
				WorkerGroup: reg.WorkerGroup, Status: workers.SessionHealthy,
				RegisteredAt: time.Now(), LastHeartbeatAt: time.Now(),
			}, nil
		},
	}
	handler := NewServer(nil, Config{MaxRequestBytes: 2048}, discardLogger()).
		WithWorkerControl(control).
		WithWorkerAuth(&fakeWorkerKeys{
			authenticate: func(_ context.Context, raw string) (workerauth.Principal, error) {
				require.Equal(t, testRawWorkerKey, raw)
				return workerauth.Principal{KeyID: principalKeyID, Scope: "tenant-a"}, nil
			},
		}).
		Handler()

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authorizeWorker(registrationRequest()))

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "tenant-a", gotScope)
	require.NotNil(t, gotRegistration.WorkerKeyID)
	require.Equal(t, principalKeyID, *gotRegistration.WorkerKeyID)
}

