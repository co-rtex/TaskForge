package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	yaml "go.yaml.in/yaml/v3"

	"github.com/co-rtex/TaskForge/internal/auth"
	"github.com/co-rtex/TaskForge/internal/metrics"
	"github.com/co-rtex/TaskForge/internal/workerauth"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// guardProbe records every call any fake dependency receives, by name.
//
// A refused request must leave it EMPTY. Checking the status code alone would
// prove the guard answered, not that nothing behind it ran: a guard that
// responded 403 and then fell through to the handler would still pass that
// check. Every dependency a /internal handler can reach -- the API-key store,
// the worker-key store, and the worker-control service -- reports here, and
// "WorkerKeys.Authenticate" counts too, because authentication running at all
// means the guard was not outermost.
type guardProbe struct {
	mu    sync.Mutex
	calls []string
}

func (p *guardProbe) record(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, name)
}

func (p *guardProbe) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = nil
}

func (p *guardProbe) all() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

// reached reports whether a request got past authentication to real work: any
// recorded call other than the worker-key credential check itself.
func (p *guardProbe) reached() bool {
	for _, call := range p.all() {
		if call != "WorkerKeys.Authenticate" {
			return true
		}
	}
	return false
}

type probeKeys struct{ probe *guardProbe }

func (f probeKeys) Authenticate(context.Context, string) (auth.Principal, error) {
	f.probe.record("APIKeys.Authenticate")
	return auth.Principal{KeyID: uuid.New(), Scope: testScope}, nil
}
func (f probeKeys) Create(context.Context, string, string) (auth.Created, error) {
	f.probe.record("APIKeys.Create")
	return auth.Created{}, nil
}
func (f probeKeys) Revoke(context.Context, uuid.UUID) (auth.RevokeResult, error) {
	f.probe.record("APIKeys.Revoke")
	return auth.RevokeResult{}, nil
}
func (f probeKeys) List(context.Context, int) ([]auth.Key, error) {
	f.probe.record("APIKeys.List")
	return nil, nil
}

type probeWorkerKeys struct{ probe *guardProbe }

// Authenticate accepts testRawWorkerKey, so T3 can present a VALID worker key:
// the only thing that may stop the request is the guard.
func (f probeWorkerKeys) Authenticate(_ context.Context, raw string) (workerauth.Principal, error) {
	f.probe.record("WorkerKeys.Authenticate")
	if raw != testRawWorkerKey {
		return workerauth.Principal{}, workerauth.ErrUnauthorized
	}
	return workerauth.Principal{KeyID: uuid.New(), Scope: testScope}, nil
}
func (f probeWorkerKeys) Create(context.Context, string, string) (workerauth.Created, error) {
	f.probe.record("WorkerKeys.Create")
	return workerauth.Created{}, nil
}
func (f probeWorkerKeys) Revoke(context.Context, uuid.UUID) (workerauth.RevokeResult, error) {
	f.probe.record("WorkerKeys.Revoke")
	return workerauth.RevokeResult{}, nil
}
func (f probeWorkerKeys) List(context.Context, int) ([]workerauth.Key, error) {
	f.probe.record("WorkerKeys.List")
	return nil, nil
}
func (f probeWorkerKeys) IsRevoked(context.Context, uuid.UUID) (bool, error) {
	f.probe.record("WorkerKeys.IsRevoked")
	return false, nil
}

type probeControl struct{ probe *guardProbe }

func (f probeControl) Register(context.Context, string, workers.Registration) (workers.Session, error) {
	f.probe.record("Control.Register")
	return workers.Session{}, nil
}
func (f probeControl) Heartbeat(context.Context, string, workers.HeartbeatRequest) (workers.HeartbeatResult, error) {
	f.probe.record("Control.Heartbeat")
	return workers.HeartbeatResult{}, nil
}
func (f probeControl) Claim(context.Context, string, workers.ClaimRequest) (workers.ClaimResult, error) {
	f.probe.record("Control.Claim")
	return workers.ClaimResult{}, nil
}
func (f probeControl) RenewLease(context.Context, string, workers.RenewalRequest) (workers.RenewalResult, error) {
	f.probe.record("Control.RenewLease")
	return workers.RenewalResult{}, nil
}
func (f probeControl) Start(context.Context, string, workers.Fence) (workers.StartResult, error) {
	f.probe.record("Control.Start")
	return workers.StartResult{}, nil
}
func (f probeControl) Succeed(context.Context, string, workers.Fence, *workers.ResultRef) error {
	f.probe.record("Control.Succeed")
	return nil
}
func (f probeControl) Fail(context.Context, string, workers.FailureReport) (workers.OutcomeResult, error) {
	f.probe.record("Control.Fail")
	return workers.OutcomeResult{}, nil
}
func (f probeControl) AcknowledgeCancellation(context.Context, string, workers.CancelAcknowledgment) (workers.OutcomeResult, error) {
	f.probe.record("Control.AcknowledgeCancellation")
	return workers.OutcomeResult{}, nil
}
func (f probeControl) SessionScope(context.Context, uuid.UUID) (string, *uuid.UUID, error) {
	f.probe.record("Control.SessionScope")
	return testScope, nil, nil
}

// guardedServer is a server with the whole /internal surface wired to probes.
func guardedServer(log *slog.Logger, configure func(*Server)) (http.Handler, *guardProbe) {
	probe := &guardProbe{}
	if log == nil {
		log = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	server := NewServer(nil, Config{MaxRequestBytes: 4096}, log).
		WithAuth(probeKeys{probe}).
		WithWorkerAuth(probeWorkerKeys{probe}).
		WithWorkerControl(probeControl{probe})
	if configure != nil {
		configure(server)
	}
	return server.Handler(), probe
}

// internalOperation is one /internal operation api/openapi.yaml documents.
type internalOperation struct {
	method string // upper case
	path   string // as documented, with {placeholders}
}

func (o internalOperation) String() string { return o.method + " " + o.path }

// url fills every {placeholder} with a real UUID, so a request that reached its
// handler would pass path validation and hit the fake.
func (o internalOperation) url() string {
	return regexp.MustCompile(`\{[^}]+\}`).ReplaceAllString(o.path, "11111111-1111-4111-8111-111111111111")
}

// documentedInternalOperations is every /internal operation in the OpenAPI
// document, so the refusal tests cover exactly what is documented and a route
// added to the document is covered without anyone remembering to list it.
func documentedInternalOperations(t *testing.T) []internalOperation {
	t.Helper()
	var operations []internalOperation
	for path, methods := range loadOpenAPI(t).Paths {
		if !strings.HasPrefix(path, "/internal/v1/") {
			continue
		}
		for method := range methods {
			operations = append(operations, internalOperation{strings.ToUpper(method), path})
		}
	}
	require.NotEmpty(t, operations)
	return operations
}

const (
	guardID   = "11111111-1111-4111-8111-111111111111"
	guardBody = `{"worker_id":"` + guardID + `","worker_session_id":"` + guardID +
		`","claim_request_id":"` + guardID + `","queue":"default","job_id":"` + guardID +
		`","attempt_id":"` + guardID + `","lease_id":"` + guardID + `","renewal_request_id":"` + guardID +
		`","outcome_request_id":"` + guardID + `","failure_class":"RETRYABLE","error_code":"e","error_message":"m",` +
		`"scope":"s","name":"n","worker_name":"w","hostname":"h","worker_group":"g",` +
		`"concurrency_limit":1,"capabilities":[],"supported_job_types":["demo.echo"],"expected_renewal_version":0}`
)

// validRequest builds a request that, with no browser marking and a loopback
// Host, passes every check in front of its handler. It is a superset body: the
// handlers each decode their own request type, and unknown-field rejection is
// avoided by giving every operation exactly its own fields.
func (o internalOperation) validRequest(t *testing.T) *http.Request {
	t.Helper()
	var body string
	switch {
	case o.method == http.MethodGet, strings.HasSuffix(o.path, "/revoke"):
		body = ""
	default:
		body = o.bodyFor(t)
	}
	req := httptest.NewRequest(o.method, o.url(), strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(RequestIDHeader, "guard-req-1")
	if o.path == "/internal/v1/worker-sessions/{worker_session_id}" {
		req.Header.Set(authorizationHeader, "Bearer "+testRawWorkerKey)
	}
	return internalRequest(req)
}

// bodyFor picks only the fields the operation's own request type declares:
// handlers reject unknown fields, so a shared superset body would 400.
func (o internalOperation) bodyFor(t *testing.T) string {
	t.Helper()
	var all map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(guardBody), &all))
	fields := map[string][]string{
		"/internal/v1/api-keys":    {"scope", "name"},
		"/internal/v1/worker-keys": {"scope", "name"},
		"/internal/v1/claims":      {"worker_id", "worker_session_id", "claim_request_id", "queue"},
		"/internal/v1/leases/{lease_id}/renew": {"job_id", "attempt_id", "worker_id", "worker_session_id",
			"renewal_request_id", "expected_renewal_version"},
		"/internal/v1/worker-sessions/{worker_session_id}": {"worker_name", "hostname", "worker_group",
			"concurrency_limit", "capabilities", "supported_job_types"},
		"/internal/v1/worker-sessions/{worker_session_id}/heartbeat": {"worker_id"},
		"/internal/v1/attempts/{attempt_id}/start":                   {"job_id", "lease_id", "worker_id", "worker_session_id"},
		"/internal/v1/attempts/{attempt_id}/succeed":                 {"job_id", "lease_id", "worker_id", "worker_session_id"},
		"/internal/v1/attempts/{attempt_id}/fail": {"job_id", "lease_id", "worker_id", "worker_session_id",
			"outcome_request_id", "failure_class", "error_code", "error_message"},
		"/internal/v1/attempts/{attempt_id}/cancel": {"job_id", "lease_id", "worker_id", "worker_session_id",
			"outcome_request_id"},
	}[o.path]
	require.NotNilf(t, fields, "no body fields recorded for %s: add them so its handler can be reached", o)
	picked := map[string]json.RawMessage{}
	for _, field := range fields {
		picked[field] = all[field]
	}
	out, err := json.Marshal(picked)
	require.NoError(t, err)
	return string(out)
}

// requireOriginRefused asserts the full refusal: 403, the stable code, the
// fixed message, the JSON error shape, and the request id echoed both in the
// header and in the body.
func requireOriginRefused(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
	var body ErrorBody
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, CodeOriginRefused, body.Error.Code)
	require.Equal(t, originRefusedMessage, body.Error.Message)
	require.Equal(t, "guard-req-1", rec.Header().Get(RequestIDHeader), "the request id is echoed")
	require.Equal(t, "guard-req-1", body.Error.RequestID)
	require.Empty(t, body.Error.Details)
}

// browserMarkings is every way a request can be refused, as a mutation of an
// otherwise valid request.
var browserMarkings = []struct {
	name  string
	rule  string
	apply func(*http.Request)
}{
	{"Sec-Fetch-Site same-origin", ruleSecFetchSite, func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "same-origin") }},
	{"Sec-Fetch-Site same-site", ruleSecFetchSite, func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "same-site") }},
	{"Sec-Fetch-Site cross-site", ruleSecFetchSite, func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") }},
	{"Sec-Fetch-Site none", ruleSecFetchSite, func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "none") }},
	{"Sec-Fetch-Site empty", ruleSecFetchSite, func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "") }},
	{"Origin evil", ruleOrigin, func(r *http.Request) { r.Header.Set("Origin", "http://evil.example") }},
	{"Origin null", ruleOrigin, func(r *http.Request) { r.Header.Set("Origin", "null") }},
	{"Origin the API's own", ruleOrigin, func(r *http.Request) { r.Header.Set("Origin", "http://127.0.0.1:8080") }},
	{"Host evil.example", ruleHost, func(r *http.Request) { r.Host = "evil.example" }},
	{"Host evil.example:8080", ruleHost, func(r *http.Request) { r.Host = "evil.example:8080" }},
	{"Host 127.0.0.1.evil.example", ruleHost, func(r *http.Request) { r.Host = "127.0.0.1.evil.example" }},
	{"Host empty", ruleHost, func(r *http.Request) { r.Host = "" }},
	{"Host localhost:", ruleHost, func(r *http.Request) { r.Host = "localhost:" }},
	{"Host 127.0.0.1:8080:1", ruleHost, func(r *http.Request) { r.Host = "127.0.0.1:8080:1" }},
	{"Host [::1 (unclosed)", ruleHost, func(r *http.Request) { r.Host = "[::1" }},
	{"Host ::1 (unbracketed)", ruleHost, func(r *http.Request) { r.Host = "::1" }},
	{"Host [::1]x", ruleHost, func(r *http.Request) { r.Host = "[::1]x" }},
	{"Host 127.0.0.1:abc", ruleHost, func(r *http.Request) { r.Host = "127.0.0.1:abc" }},
}

// T1. Every documented /internal operation refuses every browser marking, and
// refuses it BEFORE anything behind it runs.
func TestInternalGuard_RefusesBrowserRequestsOnEveryDocumentedOperation(t *testing.T) {
	handler, probe := guardedServer(nil, nil)

	for _, operation := range documentedInternalOperations(t) {
		// A positive control per operation: with no marking the same request
		// DOES reach its dependencies. Without it, an operation whose request
		// this test built wrongly would be "refused" by validation, and the
		// empty-probe assertion below would prove nothing about the guard.
		t.Run("control "+operation.String(), func(t *testing.T) {
			probe.reset()
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, operation.validRequest(t))
			require.NotEqual(t, http.StatusForbidden, rec.Code, rec.Body.String())
			require.Truef(t, probe.reached(),
				"%s did not reach its dependencies (status %d, body %s); the refusal cases below would prove nothing for it",
				operation, rec.Code, rec.Body.String())
		})

		for _, marking := range browserMarkings {
			t.Run(operation.String()+" / "+marking.name, func(t *testing.T) {
				probe.reset()
				req := operation.validRequest(t)
				marking.apply(req)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)

				requireOriginRefused(t, rec)
				require.Emptyf(t, probe.all(),
					"a refused request reached a dependency: %v", probe.all())
			})
		}
	}
}

// T2. A request with no browser marking and a loopback Host in any accepted
// spelling reaches the handler and behaves exactly as it did before the guard:
// same status as the canonical 127.0.0.1:8080 request.
func TestInternalGuard_PassesLoopbackHostsUntouched(t *testing.T) {
	handler, probe := guardedServer(nil, nil)

	hosts := []string{
		"127.0.0.1:8080", "127.0.0.2:8080", "localhost:8080", "LOCALHOST.:8080",
		"[::1]:8080", "[::1]", "127.0.0.1", "localhost",
	}
	for _, operation := range documentedInternalOperations(t) {
		probe.reset()
		baseline := httptest.NewRecorder()
		handler.ServeHTTP(baseline, operation.validRequest(t))
		require.Truef(t, probe.reached(), "%s: baseline did not reach its dependencies", operation)

		for _, host := range hosts {
			t.Run(operation.String()+" / "+host, func(t *testing.T) {
				probe.reset()
				req := operation.validRequest(t)
				req.Host = host
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)

				require.NotEqual(t, http.StatusForbidden, rec.Code, "a loopback Host was refused: %s", rec.Body.String())
				require.True(t, probe.reached(), "the request never reached its handler")
				require.Equal(t, baseline.Code, rec.Code, "behavior changed for Host %q", host)
			})
		}
	}
}

// T3. Ordering: the guard runs before authentication and before the 405.
func TestInternalGuard_RunsBeforeAuthenticationAndBefore405(t *testing.T) {
	t.Run("a browser-marked registration with a VALID worker key is refused, not served and not 401", func(t *testing.T) {
		handler, probe := guardedServer(nil, nil)

		// The valid-key request, unmarked, really does register: the key is
		// good and the handler succeeds. That is the 200 the marked request
		// must not get.
		operation := internalOperation{http.MethodPut, "/internal/v1/worker-sessions/{worker_session_id}"}
		control := httptest.NewRecorder()
		handler.ServeHTTP(control, operation.validRequest(t))
		require.Equal(t, http.StatusOK, control.Code, "control: the valid key registers when unmarked")
		require.Contains(t, probe.all(), "Control.Register")

		for _, marking := range []string{"Sec-Fetch-Site", "Origin"} {
			probe.reset()
			req := operation.validRequest(t)
			require.Equal(t, "Bearer "+testRawWorkerKey, req.Header.Get(authorizationHeader), "the worker key is present and valid")
			req.Header.Set(marking, "x")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			requireOriginRefused(t, rec)
			require.NotEqual(t, http.StatusOK, rec.Code)
			require.NotEqual(t, http.StatusUnauthorized, rec.Code)
			require.Empty(t, probe.all(), "authentication or registration ran for a refused request")
		}
	})

	t.Run("a browser-marked DELETE on a POST/GET path is 403, not 405", func(t *testing.T) {
		handler, probe := guardedServer(nil, nil)

		// Control: unmarked, the same request is the structured 405.
		plain := internalRequest(httptest.NewRequest(http.MethodDelete, "/internal/v1/api-keys", nil))
		control := httptest.NewRecorder()
		handler.ServeHTTP(control, plain)
		require.Equal(t, http.StatusMethodNotAllowed, control.Code)
		require.Equal(t, "GET, POST", control.Header().Get("Allow"))

		marked := internalRequest(httptest.NewRequest(http.MethodDelete, "/internal/v1/api-keys", nil))
		marked.Header.Set(RequestIDHeader, "guard-req-1")
		marked.Header.Set("Origin", "http://evil.example")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, marked)

		requireOriginRefused(t, rec)
		require.Empty(t, rec.Header().Get("Allow"), "a refused request must not advertise which methods the path allows")
		require.Empty(t, probe.all())
	})
}

// T4. Scope (pins Q1): the guard touches only registered /internal routes.
func TestInternalGuard_DoesNotTouchAnyOtherRoute(t *testing.T) {
	handler := NewServer(nil, Config{MaxRequestBytes: 1024}, discardLogger()).
		WithAuth(acceptingKeys(testScope)).
		WithResults(acceptingResults(), nil).
		WithMetrics(metrics.New("taskforge-api")).
		WithDashboard(testDashboardAssets()).
		Handler()

	// Everything a browser attack would carry, at once.
	hostile := func(req *http.Request) *http.Request {
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		req.Header.Set("Origin", "http://evil.example")
		req.Host = "evil.example:8080"
		return req
	}
	type observed struct {
		code        int
		contentType string
		body        string
	}
	do := func(req *http.Request) observed {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		// Blank the one field that differs per request.
		body := regexp.MustCompile(`"request_id":"[^"]*"`).ReplaceAllString(rec.Body.String(), `"request_id":""`)
		if req.URL.Path == "/metrics" {
			// The scrape reports the counters this very test moves, so its body
			// legitimately differs between two requests. Status and type pin it.
			body = ""
		}
		return observed{rec.Code, rec.Header().Get("Content-Type"), body}
	}

	cases := []struct {
		name    string
		request func() *http.Request
		want    int
	}{
		// A valid key and a request that fails validation before any store
		// access: the answer is the handler's, 422, whatever the headers.
		{"/v1/jobs with a valid key", func() *http.Request {
			return authorize(httptest.NewRequest(http.MethodGet, "/v1/jobs?limit=0", nil))
		}, http.StatusUnprocessableEntity},
		{"/v1/jobs with no key", func() *http.Request {
			return httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
		}, http.StatusUnauthorized},
		{"/dashboard/", func() *http.Request {
			return httptest.NewRequest(http.MethodGet, "/dashboard/", nil)
		}, http.StatusOK},
		{"/metrics", func() *http.Request {
			return httptest.NewRequest(http.MethodGet, "/metrics", nil)
		}, http.StatusOK},
		{"/healthz", func() *http.Request {
			return httptest.NewRequest(http.MethodGet, "/healthz", nil)
		}, http.StatusOK},
		{"an unregistered /internal path", func() *http.Request {
			return httptest.NewRequest(http.MethodGet, "/internal/v1/nonexistent", nil)
		}, http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plain := do(tc.request())
			require.Equal(t, tc.want, plain.code, "baseline status")

			marked := do(hostile(tc.request()))
			require.Equal(t, plain, marked, "browser markings changed the response of a route the guard does not cover")
			require.NotContains(t, marked.body, CodeOriginRefused)
		})
	}

	// The unregistered /internal path is still the JSON 404.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, hostile(httptest.NewRequest(http.MethodGet, "/internal/v1/nonexistent", nil)))
	requireStructuredNotFound(t, rec)
}

// T5. A refused request is observable as what it was: a span and a metric
// labelled with the ROUTE PATTERN, and a log line naming the rule and request
// id but never a header value.
func TestInternalGuard_RefusalIsObservable(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m := metrics.New("taskforge-api")
	var logs bytes.Buffer
	handler, _ := guardedServer(slog.New(slog.NewJSONHandler(&logs, nil)), func(s *Server) {
		s.WithTracer(provider.Tracer("test")).WithMetrics(m)
	})

	refuse := func(method, path string, mark func(*http.Request)) {
		req := internalRequest(httptest.NewRequest(method, path, nil))
		req.Header.Set(RequestIDHeader, "guard-req-1")
		mark(req)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		requireOriginRefused(t, rec)
	}
	refuse(http.MethodPost, "/internal/v1/api-keys", func(r *http.Request) { r.Header.Set("Origin", "http://evil.example") })
	refuse(http.MethodDelete, "/internal/v1/api-keys", func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") })
	refuse(http.MethodGet, "/internal/v1/api-keys", func(r *http.Request) { r.Host = "evil.example:8080" })

	t.Run("span names", func(t *testing.T) {
		var names []string
		for _, span := range exporter.GetSpans() {
			names = append(names, span.Name)
		}
		require.Equal(t, []string{
			"POST /internal/v1/api-keys",
			"DELETE /internal/v1/api-keys", // the method-less fallback pattern, prefixed with the verb
			"GET /internal/v1/api-keys",
		}, names, "a refused request must keep its route pattern as its span name, not the unrouted fallback")
	})

	t.Run("metric route labels", func(t *testing.T) {
		got := map[string]bool{}
		for _, series := range scrape(t, handler)["taskforge_http_requests_total"].series {
			got[series["method"]+"|"+series["route"]+"|"+series["code"]] = true
		}
		require.Equal(t, map[string]bool{
			"POST|POST /internal/v1/api-keys|403": true,
			"GET|GET /internal/v1/api-keys|403":   true,
			"DELETE|/internal/v1/api-keys|403":    true, // the method-less fallback's own pattern
		}, got, "a refusal must keep its route pattern as its label, never the unrouted one")
	})

	t.Run("log lines", func(t *testing.T) {
		type line struct {
			Msg       string `json:"msg"`
			Rule      string `json:"rule"`
			RequestID string `json:"request_id"`
		}
		var rules []string
		for _, raw := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			var parsed line
			require.NoError(t, json.Unmarshal([]byte(raw), &parsed))
			if parsed.Msg != "internal request refused" {
				continue
			}
			require.Equal(t, "guard-req-1", parsed.RequestID)
			rules = append(rules, parsed.Rule)
		}
		require.Equal(t, []string{ruleOrigin, ruleSecFetchSite, ruleHost}, rules)

		// The attacker-chosen values never reach the log, from any line.
		for _, secret := range []string{"evil.example", "cross-site"} {
			require.NotContains(t, logs.String(), secret)
		}
	})
}

// Rule order: when a request breaks several rules, the first in the documented
// order names the refusal.
func TestInternalGuard_FirstRuleToFireNamesTheRefusal(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/claims", nil)
	req.Host = "evil.example"
	require.Equal(t, ruleHost, browserOriginRule(req))
	req.Header.Set("Origin", "null")
	require.Equal(t, ruleOrigin, browserOriginRule(req))
	req.Header.Set("Sec-Fetch-Site", "")
	require.Equal(t, ruleSecFetchSite, browserOriginRule(req))
}

func TestInternalGuard_EveryBrowserMarkingNamesItsRule(t *testing.T) {
	for _, marking := range browserMarkings {
		t.Run(marking.name, func(t *testing.T) {
			req := internalRequest(httptest.NewRequest(http.MethodPost, "/internal/v1/claims", nil))
			marking.apply(req)
			require.Equal(t, marking.rule, browserOriginRule(req))
		})
	}
}

// pattern registration is the invariant the whole guard rests on, and it is
// "visible in the route table" only if nothing registers around the helper.
// TestInternalGuard_RefusesBrowserRequestsOnEveryDocumentedOperation proves the
// result for every documented operation; this proves the cause, so an /internal
// pattern that is registered but undocumented cannot slip past both.
func TestServer_EveryInternalPatternIsRegisteredThroughTheGuard(t *testing.T) {
	source, err := os.ReadFile("server.go")
	require.NoError(t, err)

	registration := regexp.MustCompile(`mux\.Handle(Func)?\(`)
	internalPath := regexp.MustCompile(`"(?:[A-Z]+ )?/internal/`)
	var guarded int
	for number, line := range strings.Split(string(source), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") || !internalPath.MatchString(line) {
			continue
		}
		require.Falsef(t, registration.MatchString(line),
			"server.go:%d registers an /internal pattern directly on the mux; use s.handleInternal so the browser-origin guard wraps it:\n%s",
			number+1, line)
		if strings.Contains(line, "s.handleInternal(mux,") {
			guarded++
		}
	}
	// 14 documented operations and 12 method-less fallbacks (one per path with
	// a registered handler is not the rule -- the fallback list is per path).
	require.Equal(t, 26, guarded, "the number of /internal registrations changed; update this count deliberately")
}

// The contract: every /internal operation documents the guard's refusal, the
// enum names it, and the /v1 operations do not claim it.
func TestOpenAPI_EveryInternalOperationDocumentsTheOriginGuardRefusal(t *testing.T) {
	doc := loadOpenAPI(t)

	for path, methods := range doc.Paths {
		for method, operation := range methods {
			response, has403 := operation.Responses["403"]
			if !strings.HasPrefix(path, "/internal/v1/") {
				require.Falsef(t, has403,
					"%s %s documents a 403, but the origin guard covers /internal/v1 only", method, path)
				continue
			}
			require.Truef(t, has403, "%s %s does not document the origin guard's 403", method, path)
			example := response.Content["application/json"].Example.Error
			require.Equalf(t, CodeOriginRefused, example.Code, "%s %s", method, path)
			require.Equalf(t, originRefusedMessage, example.Message,
				"%s %s: the documented message must be the one the server sends", method, path)
		}
	}

	raw, err := os.ReadFile("../../api/openapi.yaml")
	require.NoError(t, err)
	var spec struct {
		Components struct {
			Schemas struct {
				Error struct {
					Properties struct {
						Error struct {
							Properties struct {
								Code struct {
									Enum []string `yaml:"enum"`
								} `yaml:"code"`
							} `yaml:"properties"`
						} `yaml:"error"`
					} `yaml:"properties"`
				} `yaml:"Error"`
			} `yaml:"schemas"`
		} `yaml:"components"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &spec))
	require.Contains(t, spec.Components.Schemas.Error.Properties.Error.Properties.Code.Enum, CodeOriginRefused)

	document := flatten(string(raw))
	require.Contains(t, document, "the browser-origin guard")
	require.Contains(t, document, "is **not authentication**",
		"the guard must be documented as not being authentication")
}
