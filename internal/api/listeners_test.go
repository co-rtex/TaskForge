package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/co-rtex/TaskForge/internal/hostallow"
)

// The tests in this file hold the listener column of the route table
// (docs/adr/0026-listeners-by-surface.md): which listener serves which route,
// the startup check that refuses a forbidden placement, the 405 fallbacks per
// listener, and the control listener's Host allowlist in the browser-origin
// guard. The admin listener's own behavior is pinned, unchanged, by
// testdata/route_behavior.golden and the ADR-0018 tests in origin_guard_test.go.

// testControlHost is the name the topology smoke's worker dials, and so the
// allowlist entry these tests use.
const testControlHost = "api"

func controlAllowlist(t *testing.T, entries ...string) hostallow.List {
	t.Helper()
	list, err := hostallow.Parse(entries)
	require.NoError(t, err)
	return list
}

// listenerBed is a fully wired server (every feature group on, each to a
// recording fake) with a handler built for each of the three listeners, and a
// tracer so a test can read the pattern each mux matched.
type listenerBed struct {
	server   *Server
	handlers map[Listener]http.Handler
	probe    *guardProbe
	exporter *tracetest.InMemoryExporter
}

func newListenerBed(t *testing.T, log *slog.Logger) *listenerBed {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	server, probe := wireRouteServer(t, provider.Tracer("listeners-test"))
	if log != nil {
		server.log = log
	}
	server.WithControlHosts(controlAllowlist(t, testControlHost))
	bed := &listenerBed{server: server, probe: probe, exporter: exporter, handlers: map[Listener]http.Handler{}}
	for _, l := range []Listener{ListenerAdmin, ListenerPublic, ListenerControl} {
		bed.handlers[l] = server.ListenerHandler(l)
	}
	return bed
}

// listenerHost is the Host a real client of each listener sends: loopback on
// admin, a public name on public, and the allowlisted worker name on control.
func listenerHost(l Listener) string {
	switch l {
	case ListenerPublic:
		return "taskforge.example:8090"
	case ListenerControl:
		return testControlHost + ":8091"
	}
	return testInternalHost
}

func (b *listenerBed) serve(t *testing.T, l Listener, req *http.Request) (*httptest.ResponseRecorder, string) {
	t.Helper()
	b.exporter.Reset()
	rec := httptest.NewRecorder()
	b.handlers[l].ServeHTTP(rec, req)
	spans := b.exporter.GetSpans()
	require.Lenf(t, spans, 1, "%s %s on %s must produce exactly one server span", req.Method, req.URL.Path, l)
	var pattern string
	for _, attr := range spans[0].Attributes {
		if string(attr.Key) == "http.route" {
			pattern = attr.Value.AsString()
		}
	}
	return rec, pattern
}

func listenerRequest(l Listener, method, path string) *http.Request {
	req := httptest.NewRequest(method, requestPath(path), http.NoBody)
	req.Host = listenerHost(l)
	return req
}

// The exact route set of each listener, written out. Comparing against the table
// would compare the table with itself.
var (
	wantPublicRoutes = []string{
		"GET /healthz",
		"GET /readyz",
		"GET /v1/dlq",
		"GET /v1/jobs",
		"GET /v1/jobs/{job_id}",
		"GET /v1/jobs/{job_id}/attempts",
		"GET /v1/jobs/{job_id}/result",
		"GET /v1/queues",
		"GET /v1/workers",
		"POST /v1/dlq/{job_id}/replay",
		"POST /v1/jobs",
		"POST /v1/jobs/{job_id}/cancel",
		"POST /v1/jobs/{job_id}/retry",
	}
	wantControlRoutes = []string{
		"GET /healthz",
		"GET /readyz",
		"POST /internal/v1/attempts/{attempt_id}/cancel",
		"POST /internal/v1/attempts/{attempt_id}/fail",
		"POST /internal/v1/attempts/{attempt_id}/start",
		"POST /internal/v1/attempts/{attempt_id}/succeed",
		"POST /internal/v1/claims",
		"POST /internal/v1/leases/{lease_id}/renew",
		"POST /internal/v1/worker-sessions/{worker_session_id}/heartbeat",
		"PUT /internal/v1/worker-sessions/{worker_session_id}",
	}
	wantAdminRoutes = []string{
		"GET /",
		"GET /dashboard/",
		"GET /healthz",
		"GET /internal/v1/api-keys",
		"GET /internal/v1/worker-keys",
		"GET /metrics",
		"GET /readyz",
		"GET /v1/dlq",
		"GET /v1/jobs",
		"GET /v1/jobs/{job_id}",
		"GET /v1/jobs/{job_id}/attempts",
		"GET /v1/jobs/{job_id}/result",
		"GET /v1/queues",
		"GET /v1/workers",
		"GET /{$}",
		"POST /internal/v1/api-keys",
		"POST /internal/v1/api-keys/{key_id}/revoke",
		"POST /internal/v1/attempts/{attempt_id}/cancel",
		"POST /internal/v1/attempts/{attempt_id}/fail",
		"POST /internal/v1/attempts/{attempt_id}/start",
		"POST /internal/v1/attempts/{attempt_id}/succeed",
		"POST /internal/v1/claims",
		"POST /internal/v1/leases/{lease_id}/renew",
		"POST /internal/v1/worker-keys",
		"POST /internal/v1/worker-keys/{key_id}/revoke",
		"POST /internal/v1/worker-sessions/{worker_session_id}/heartbeat",
		"POST /v1/dlq/{job_id}/replay",
		"POST /v1/jobs",
		"POST /v1/jobs/{job_id}/cancel",
		"POST /v1/jobs/{job_id}/retry",
		"PUT /internal/v1/worker-sessions/{worker_session_id}",
	}
)

// TestListeners_EachServesExactlyItsRoutes proves each listener's route set two
// ways. By the table: the routes whose column names the listener are exactly the
// literal list. By behavior: every route of the fully wired table, sent to every
// listener, matches its own pattern where the list has it and falls to the "/"
// catch-all's 404 everywhere else.
func TestListeners_EachServesExactlyItsRoutes(t *testing.T) {
	bed := newListenerBed(t, nil)
	enabled := bed.server.enabledRoutes()

	// "GET /" is not a table route: it is the admin list's reminder that the
	// bare root has a route there ("GET /{$}"). It is dropped before comparing.
	want := map[Listener][]string{
		ListenerAdmin:   withoutBareRoot(wantAdminRoutes),
		ListenerPublic:  wantPublicRoutes,
		ListenerControl: wantControlRoutes,
	}
	for l, patterns := range want {
		var got []string
		for _, rt := range routesOn(enabled, l) {
			got = append(got, rt.pattern())
		}
		sort.Strings(got)
		require.Equalf(t, patterns, got, "the %s listener's routes", l)
	}

	for _, rt := range enabled {
		for l, patterns := range want {
			label := fmt.Sprintf("%s on %s", rt.pattern(), l)
			rec, pattern := bed.serve(t, l, listenerRequest(l, rt.method, rt.path))
			if contains(patterns, rt.pattern()) {
				require.Equalf(t, rt.pattern(), pattern, "%s: served here, so its own pattern must match", label)
				continue
			}
			require.Equalf(t, "/", pattern, "%s: not served here, so the catch-all must answer", label)
			require.Equalf(t, http.StatusNotFound, rec.Code, label)
			require.Equalf(t, CodeNotFound, errorCodeOf(rec), label)
		}
	}
}

func withoutBareRoot(patterns []string) []string {
	var out []string
	for _, p := range patterns {
		if p != "GET /" {
			out = append(out, p)
		}
	}
	return out
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

// TestRoutes_EveryEntryNamesItsListeners is the table-consistency rule for the
// listener column: every entry sets it explicitly (none relies on the zero
// value's admin-only default), every entry is on the admin listener, and the
// column follows the route group: /v1 on public, the worker-control group on
// control, the probes on both, and everything else on admin alone. The real
// table passes the startup check.
func TestRoutes_EveryEntryNamesItsListeners(t *testing.T) {
	table := routeTableOf(t)
	for _, rt := range table {
		label := rt.pattern()
		require.NotZerof(t, rt.listeners, "%s has no listeners column", label)
		require.Truef(t, rt.listeners.has(ListenerAdmin), "%s: the admin listener serves every route", label)

		var want listenerSet
		switch {
		case strings.HasPrefix(rt.path, publicPathPrefix):
			want = adminAndPublic
		case rt.surface == surfaceProbe:
			want = everyListener
		case rt.group == groupControl:
			want = adminAndControl
		default:
			want = adminOnly
		}
		require.Equalf(t, want, rt.listeners, "%s (surface %s, group %s)", label, rt.surface, rt.group)
	}
	require.NoError(t, checkListenerPlacement(table))
}

// TestListenerPlacement_RefusesEachForbiddenPlacement proves checkListenerPlacement,
// the pure function behind the startup check, refuses each placement ADR-0026
// forbids, by group and by path, and names the route.
func TestListenerPlacement_RefusesEachForbiddenPlacement(t *testing.T) {
	noop := func(http.ResponseWriter, *http.Request) {}
	entry := func(path string, g group, ls listenerSet) route {
		return route{method: http.MethodGet, path: path, surface: surfaceUnlisted, chain: chainNone,
			group: g, listeners: ls, handler: noop, reason: "x"}
	}

	for name, tc := range map[string]struct {
		rt       route
		wantRule string
	}{
		"API-key administration on control": {
			entry("/internal/v1/api-keys", groupKeys, adminAndControl), "admin listener only"},
		"API-key administration on public": {
			entry("/internal/v1/api-keys", groupKeys, adminAndPublic), "admin listener only"},
		"worker-key administration on control": {
			entry("/internal/v1/worker-keys", groupWorkerKeys, adminAndControl), "admin listener only"},
		"worker-key administration on public": {
			entry("/internal/v1/worker-keys", groupWorkerKeys, everyListener), "admin listener only"},
		"the dashboard on public": {
			entry(DashboardPath, groupDashboard, adminAndPublic), "admin listener only"},
		"the bare-root redirect on control": {
			entry("/{$}", groupDashboard, adminAndControl), "admin listener only"},
		"metrics on public": {
			entry("/metrics", groupMetrics, adminAndPublic), "admin listener only"},
		"metrics on control": {
			entry("/metrics", groupMetrics, adminAndControl), "admin listener only"},
		// A wrong group declaration does not get a route past the check: the path
		// alone decides that these are admin-only.
		"a dashboard path declared always-on, on public": {
			entry(DashboardPath+"x", groupAlways, adminAndPublic), "admin listener only"},
		"the metrics path declared always-on, on control": {
			entry("/metrics", groupAlways, adminAndControl), "admin listener only"},
		"a key path declared control-group, on control": {
			entry("/internal/v1/api-keys/{key_id}/revoke", groupControl, adminAndControl), "admin listener only"},
		"an /internal route on public": {
			entry("/internal/v1/claims", groupControl, adminAndPublic), "no /internal route is served there"},
		"an /internal route on public alone": {
			entry("/internal/v1/debug", groupAlways, ListenerPublic.bit()), "no /internal route is served there"},
		"a /v1 route on control": {
			entry("/v1/jobs", groupAlways, adminAndControl), "no /v1 route is served there"},
		"a /v1 route on every listener": {
			entry("/v1/jobs", groupAlways, everyListener), "no /v1 route is served there"},
	} {
		t.Run(name, func(t *testing.T) {
			err := checkListenerPlacement([]route{tc.rt})
			require.Error(t, err)
			require.Contains(t, err.Error(), fmt.Sprintf("%q", tc.rt.pattern()))
			require.Contains(t, err.Error(), tc.wantRule)
		})
	}

	t.Run("every breach is reported, not only the first", func(t *testing.T) {
		err := checkListenerPlacement([]route{
			entry("/internal/v1/api-keys", groupKeys, adminAndControl),
			entry("/internal/v1/claims", groupControl, adminAndPublic),
			entry("/v1/jobs", groupAlways, adminAndControl),
		})
		require.Error(t, err)
		for _, named := range []string{"GET /internal/v1/api-keys", "GET /internal/v1/claims", "GET /v1/jobs"} {
			require.Contains(t, err.Error(), fmt.Sprintf("%q", named))
		}
	})

	t.Run("what the rules allow is accepted", func(t *testing.T) {
		require.NoError(t, checkListenerPlacement([]route{
			entry("/internal/v1/api-keys", groupKeys, adminOnly),
			entry("/internal/v1/api-keys", groupKeys, 0), // the zero value is admin only
			entry("/metrics", groupMetrics, adminOnly),
			entry(DashboardPath, groupDashboard, adminOnly),
			entry("/healthz", groupAlways, everyListener),
			entry("/v1/jobs", groupAlways, adminAndPublic),
			entry("/internal/v1/claims", groupControl, adminAndControl),
		}))
	})
}

// TestListeners_HandlerRefusesAForbiddenPlacement proves the startup check is
// wired in: building ANY listener's handler, the admin one included, panics on
// a table that places a route where ADR-0026 forbids, so a process with no
// network listener configured still refuses such a table.
func TestListeners_HandlerRefusesAForbiddenPlacement(t *testing.T) {
	for name, tc := range map[string]struct {
		move     func(route) (route, bool)
		wantRule string
	}{
		"API-key administration moved onto control": {func(rt route) (route, bool) {
			if rt.group != groupKeys {
				return rt, false
			}
			rt.listeners = adminAndControl
			return rt, true
		}, "admin listener only"},
		"claims moved onto public": {func(rt route) (route, bool) {
			if rt.path != "/internal/v1/claims" {
				return rt, false
			}
			rt.listeners = everyListener
			return rt, true
		}, "no /internal route is served there"},
	} {
		for _, l := range []Listener{ListenerAdmin, ListenerPublic, ListenerControl} {
			t.Run(name+", building "+l.String(), func(t *testing.T) {
				server, _ := wireRouteServer(t, nil)
				server.WithControlHosts(controlAllowlist(t, testControlHost))
				var moved int
				server.routeTableHook = func(table []route) []route {
					for i := range table {
						var ok bool
						if table[i], ok = tc.move(table[i]); ok {
							moved++
						}
					}
					return table
				}
				panicked := func() (p any) {
					defer func() { p = recover() }()
					server.ListenerHandler(l)
					return nil
				}()
				require.NotZero(t, moved, "the hook moved nothing; the test would prove nothing")
				require.NotNil(t, panicked, "building the %s listener must refuse the table", l)
				require.Contains(t, fmt.Sprint(panicked), tc.wantRule)
			})
		}
	}
}

// TestListeners_ControlNeedsAnAllowlist proves the control listener cannot be
// built with no Host rule, while the other two need none.
func TestListeners_ControlNeedsAnAllowlist(t *testing.T) {
	server, _ := wireRouteServer(t, nil)
	require.NotPanics(t, func() { server.ListenerHandler(ListenerAdmin) })
	require.NotPanics(t, func() { server.ListenerHandler(ListenerPublic) })
	require.PanicsWithValue(t,
		"api: the control listener needs a Host allowlist; call WithControlHosts before building it",
		func() { server.ListenerHandler(ListenerControl) })
}

// TestListeners_FallbacksFollowTheirRoutes proves each listener derives its 405
// fallbacks from its own routes: a path it serves answers a wrong method with a
// 405 and the Allow header of the methods it holds, a path it does not serve
// answers the catch-all's 404, and an /internal fallback on control goes through
// the control guard like the routes beside it.
func TestListeners_FallbacksFollowTheirRoutes(t *testing.T) {
	bed := newListenerBed(t, nil)
	type want struct {
		status       int
		allow, code  string
		pattern      string
		probeUntouch bool
		hostOverride string
		listener     Listener
		method, path string
		label        string
	}
	for _, tc := range []want{
		{label: "a probe's wrong method on public", listener: ListenerPublic, method: http.MethodPost, path: "/healthz",
			status: 405, allow: "GET", code: CodeMethodNotAllowed, pattern: "/healthz"},
		{label: "a probe's wrong method on control", listener: ListenerControl, method: http.MethodPost, path: "/readyz",
			status: 405, allow: "GET", code: CodeMethodNotAllowed, pattern: "/readyz"},
		{label: "a /v1 path's wrong method on public", listener: ListenerPublic, method: http.MethodDelete, path: "/v1/jobs",
			status: 405, allow: "GET, POST", code: CodeMethodNotAllowed, pattern: "/v1/jobs"},
		{label: "a /v1 path on control has no fallback", listener: ListenerControl, method: http.MethodDelete, path: "/v1/jobs",
			status: 404, code: CodeNotFound, pattern: "/"},
		{label: "a control path's wrong method on control", listener: ListenerControl, method: http.MethodDelete, path: "/internal/v1/claims",
			status: 405, allow: "POST", code: CodeMethodNotAllowed, pattern: "/internal/v1/claims"},
		{label: "registration's wrong method on control", listener: ListenerControl, method: http.MethodGet,
			path:   "/internal/v1/worker-sessions/{worker_session_id}",
			status: 405, allow: "PUT", code: CodeMethodNotAllowed, pattern: "/internal/v1/worker-sessions/{worker_session_id}"},
		{label: "a control fallback refuses a Host off the allowlist first", listener: ListenerControl, method: http.MethodDelete,
			path: "/internal/v1/claims", hostOverride: "127.0.0.1:8091",
			status: 403, code: CodeOriginRefused, pattern: "/internal/v1/claims"},
		{label: "a control path on public has no fallback", listener: ListenerPublic, method: http.MethodDelete, path: "/internal/v1/claims",
			status: 404, code: CodeNotFound, pattern: "/"},
		{label: "a key path on control has no fallback", listener: ListenerControl, method: http.MethodPut, path: "/internal/v1/api-keys",
			status: 404, code: CodeNotFound, pattern: "/"},
		{label: "the dashboard on public has no fallback", listener: ListenerPublic, method: http.MethodPost, path: DashboardPath,
			status: 404, code: CodeNotFound, pattern: "/"},
		{label: "the dashboard's wrong method on admin, as always", listener: ListenerAdmin, method: http.MethodPost, path: DashboardPath,
			status: 405, allow: "GET", code: CodeMethodNotAllowed, pattern: DashboardPath},
	} {
		t.Run(tc.label, func(t *testing.T) {
			bed.probe.reset()
			req := listenerRequest(tc.listener, tc.method, tc.path)
			if tc.hostOverride != "" {
				req.Host = tc.hostOverride
			}
			rec, pattern := bed.serve(t, tc.listener, req)
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			require.Equal(t, tc.allow, rec.Header().Get("Allow"))
			require.Equal(t, tc.code, errorCodeOf(rec))
			require.Equal(t, tc.pattern, pattern)
			require.Empty(t, bed.probe.all(), "a fallback or a 404 reaches no dependency")
		})
	}
}

// --- The control listener's Host rule (ADR-0026 amending ADR-0018 rule 3) ---

// controlClaimRequest is a POST /internal/v1/claims that passes every check in
// front of its handler when sent with an allowed Host.
func controlClaimRequest(t *testing.T, host string) *http.Request {
	t.Helper()
	req := internalOperation{http.MethodPost, "/internal/v1/claims"}.validRequest(t)
	req.Host = host
	return req
}

// TestControlGuard_AcceptsTheAllowlistedHostInEveryForm proves the forms a
// worker's client can send for an allowlisted name pass rule 3 and reach the
// handler: with and without a port, in any case, and fully qualified.
func TestControlGuard_AcceptsTheAllowlistedHostInEveryForm(t *testing.T) {
	bed := newListenerBed(t, nil)
	for _, host := range []string{"api", "api:8091", "API", "api.", "Api.:8091"} {
		t.Run(host, func(t *testing.T) {
			bed.probe.reset()
			rec, _ := bed.serve(t, ListenerControl, controlClaimRequest(t, host))
			require.NotEqual(t, http.StatusForbidden, rec.Code, rec.Body.String())
			require.Contains(t, bed.probe.all(), "Control.Claim", "an allowed Host reaches the handler")
		})
	}
}

// TestControlGuard_RefusesEveryHostOffTheAllowlist proves rule 3 on control is
// an exact match that fails closed: loopback is refused because it is not on
// the list, as are an empty Host, a Host that does not parse, every near-miss of
// the allowed name, and an IPv6 literal that is not listed. Nothing behind the
// guard runs.
func TestControlGuard_RefusesEveryHostOffTheAllowlist(t *testing.T) {
	bed := newListenerBed(t, nil)
	for _, host := range []string{
		"127.0.0.1", "127.0.0.1:8091", "localhost", "localhost:8091", "[::1]:8091",
		"",
		"api:", "api:abc", "api:8091:1", "api:99999", "[api]", "[api]:8091", "api]",
		"api.evil", "api.evil:8091", "xapi", "xapi:8091", "api.example", "apix", "evil.api", "api..",
		"[2001:db8::1]", "[2001:db8::1]:8091", "2001:db8::1",
	} {
		t.Run(fmt.Sprintf("%q", host), func(t *testing.T) {
			bed.probe.reset()
			rec, _ := bed.serve(t, ListenerControl, controlClaimRequest(t, host))
			requireOriginRefused(t, rec)
			require.Empty(t, bed.probe.all(), "nothing behind the guard may run")
		})
	}
}

// TestControlGuard_RefusesAForeignHostOnEveryControlRoute proves the rule
// covers every operation the control listener serves, not only claims, each
// with a positive control that the same request with the allowed Host reaches
// its dependencies.
func TestControlGuard_RefusesAForeignHostOnEveryControlRoute(t *testing.T) {
	bed := newListenerBed(t, nil)
	var covered int
	for _, operation := range documentedInternalOperations(t) {
		if !contains(wantControlRoutes, operation.String()) {
			continue
		}
		covered++
		t.Run(operation.String(), func(t *testing.T) {
			bed.probe.reset()
			req := operation.validRequest(t)
			req.Host = listenerHost(ListenerControl)
			rec, _ := bed.serve(t, ListenerControl, req)
			require.NotEqual(t, http.StatusForbidden, rec.Code, rec.Body.String())
			require.True(t, bed.probe.reached(), "the allowed Host must reach %s's dependencies", operation)

			bed.probe.reset()
			req = operation.validRequest(t)
			req.Host = "evil.example:8091"
			rec, _ = bed.serve(t, ListenerControl, req)
			requireOriginRefused(t, rec)
			require.Empty(t, bed.probe.all())
		})
	}
	require.Equal(t, 8, covered, "the eight worker-control operations")
}

// TestControlGuard_AcceptsAnAllowlistedIPv6LiteralOnlyBracketed proves IPv6
// brackets are handled: a listed address passes in its bracketed forms, and its
// unbracketed form, which no client sends as a Host, is refused.
func TestControlGuard_AcceptsAnAllowlistedIPv6LiteralOnlyBracketed(t *testing.T) {
	probe := &guardProbe{}
	server := NewServer(nil, Config{MaxRequestBytes: 4096}, discardLogger()).
		WithAuth(probeKeys{probe}).WithWorkerAuth(probeWorkerKeys{probe}).WithWorkerControl(probeControl{probe}).
		WithControlHosts(controlAllowlist(t, "2001:db8::7", "10.0.4.9"))
	handler := server.ListenerHandler(ListenerControl)

	for host, allowed := range map[string]bool{
		"[2001:db8::7]": true, "[2001:db8::7]:8091": true, "[2001:DB8:0::7]:8091": true,
		"10.0.4.9": true, "10.0.4.9:8091": true,
		"2001:db8::7": false, "[2001:db8::8]:8091": false, "10.0.4.90": false, "api": false,
	} {
		probe.reset()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, controlClaimRequest(t, host))
		if allowed {
			require.NotEqualf(t, http.StatusForbidden, rec.Code, "%q: %s", host, rec.Body.String())
			continue
		}
		requireOriginRefused(t, rec)
		require.Emptyf(t, probe.all(), "%q", host)
	}
}

// TestGuard_RulesOneAndTwoFireBeforeRuleThreeOnBothListeners proves the order
// is unchanged and per-listener only in rule 3: with an allowed Host,
// Sec-Fetch-Site and Origin are still refused on control, and when a request
// breaks several rules the first in the documented order names the refusal, on
// admin and on control alike, in the log line.
func TestGuard_RulesOneAndTwoFireBeforeRuleThreeOnBothListeners(t *testing.T) {
	var logs bytes.Buffer
	bed := newListenerBed(t, slog.New(slog.NewJSONHandler(&logs, nil)))

	type sent struct {
		listener Listener
		host     string
		headers  map[string]string
		wantRule string
	}
	cases := []sent{
		{ListenerControl, "api:8091", map[string]string{"Origin": "http://api:8091"}, ruleOrigin},
		{ListenerControl, "api:8091", map[string]string{"Sec-Fetch-Site": "same-origin"}, ruleSecFetchSite},
		{ListenerControl, "evil.example", map[string]string{"Origin": "null"}, ruleOrigin},
		{ListenerControl, "evil.example", map[string]string{"Origin": "null", "Sec-Fetch-Site": "cross-site"}, ruleSecFetchSite},
		{ListenerControl, "evil.example", nil, ruleHost},
		{ListenerAdmin, testInternalHost, map[string]string{"Origin": "http://127.0.0.1:8080"}, ruleOrigin},
		{ListenerAdmin, "api:8091", map[string]string{"Origin": "null"}, ruleOrigin},
		{ListenerAdmin, "api:8091", map[string]string{"Origin": "null", "Sec-Fetch-Site": ""}, ruleSecFetchSite},
		// The allowlist belongs to control: on admin, the allowlisted name is a
		// foreign Host like any other.
		{ListenerAdmin, "api:8091", nil, ruleHost},
	}
	for _, tc := range cases {
		bed.probe.reset()
		logs.Reset()
		req := controlClaimRequest(t, tc.host)
		for k, v := range tc.headers {
			req.Header.Set(k, v)
		}
		rec, _ := bed.serve(t, tc.listener, req)
		requireOriginRefused(t, rec)
		require.Empty(t, bed.probe.all())

		var line struct {
			Msg  string `json:"msg"`
			Rule string `json:"rule"`
		}
		var rules []string
		for _, raw := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			require.NoError(t, json.Unmarshal([]byte(raw), &line))
			if line.Msg == "internal request refused" {
				rules = append(rules, line.Rule)
			}
		}
		require.Equalf(t, []string{tc.wantRule}, rules, "%s Host %q headers %v", tc.listener, tc.host, tc.headers)
	}

	// The same order through the pure function, with the control rule.
	control := allowlistHostRule(controlAllowlist(t, testControlHost))
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/claims", nil)
	req.Host = "evil.example"
	require.Equal(t, ruleHost, browserOriginRuleFor(req, control))
	req.Header.Set("Origin", "null")
	require.Equal(t, ruleOrigin, browserOriginRuleFor(req, control))
	req.Header.Set("Sec-Fetch-Site", "")
	require.Equal(t, ruleSecFetchSite, browserOriginRuleFor(req, control))
	req.Host = "api"
	req.Header.Del("Origin")
	req.Header.Del("Sec-Fetch-Site")
	require.Empty(t, browserOriginRuleFor(req, control))
}
