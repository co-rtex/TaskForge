package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/co-rtex/TaskForge/internal/metrics"
)

// The tests in this file hold the route table (routes.go) to what it claims. The
// table is data; nothing it says is true until a test drives a server built from
// it. Each test states what it proves and, in the PR that introduced it, which
// mutation shows it can fail.

// routeBed is a server with every feature group wired on, except those a test
// turns off, each dependency a recording fake, and a tracer so a test can read
// the route pattern the mux matched.
type routeBed struct {
	server   *Server
	handler  http.Handler
	probe    *guardProbe
	exporter *tracetest.InMemoryExporter
}

// newRouteBed wires a server. Groups named in off are left unwired, which is
// exactly what leaves their routes unregistered in production.
func newRouteBed(t *testing.T, off ...group) *routeBed {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	disabled := map[group]bool{}
	for _, g := range off {
		require.NotEqualf(t, groupAlways, g, "the %s group cannot be turned off", g)
		disabled[g] = true
	}

	probe := &guardProbe{}
	server := NewServer(nil, Config{MaxRequestBytes: 4096}, discardLogger()).
		WithTracer(provider.Tracer("routes-test")).
		WithResults(acceptingResults(), nil)
	if !disabled[groupKeys] {
		server.WithAuth(probeKeys{probe})
	}
	if !disabled[groupWorkerKeys] {
		server.WithWorkerAuth(probeWorkerKeys{probe})
	}
	if !disabled[groupControl] {
		server.WithWorkerControl(probeControl{probe})
	}
	if !disabled[groupMetrics] {
		server.WithMetrics(metrics.New("taskforge-api"))
	}
	if !disabled[groupDashboard] {
		server.WithDashboard(testDashboardAssets())
	}
	return &routeBed{server: server, handler: server.Handler(), probe: probe, exporter: exporter}
}

// serve sends one request and returns the response with the route pattern the
// mux matched, read from the span withSpanRoute names. "" means nothing matched.
func (b *routeBed) serve(t *testing.T, req *http.Request) (*httptest.ResponseRecorder, string) {
	t.Helper()
	b.exporter.Reset()
	rec := httptest.NewRecorder()
	b.handler.ServeHTTP(rec, req)

	spans := b.exporter.GetSpans()
	require.Lenf(t, spans, 1, "%s %s must produce exactly one server span", req.Method, req.URL.Path)
	var pattern string
	for _, attr := range spans[0].Attributes {
		if string(attr.Key) == "http.route" {
			pattern = attr.Value.AsString()
		}
	}
	return rec, pattern
}

// requestPath turns a table path into one a client would send: every parameter
// is filled with a real UUID, and "/{$}" is the bare root it matches.
func requestPath(path string) string {
	if path == "/{$}" {
		return "/"
	}
	return pathParam.ReplaceAllString(path, goldenID)
}

// routeRequest is a request to a table route as a real client sends it: no
// credential, no browser marking, the loopback Host every default base URL has.
func routeRequest(method, path string) *http.Request {
	return internalRequest(httptest.NewRequest(method, requestPath(path), http.NoBody))
}

func errorCodeOf(rec *httptest.ResponseRecorder) string {
	var body ErrorBody
	if json.Unmarshal(rec.Body.Bytes(), &body) != nil {
		return ""
	}
	return body.Error.Code
}

// tableRoutes is the enabled routes of a fully wired server, optionally only
// those of the given surfaces. It is what the tests that used to loop over hand
// lists of routes loop over instead.
func tableRoutes(t *testing.T, surfaces ...surface) []route {
	t.Helper()
	var routes []route
	for _, rt := range newRouteBed(t).server.enabledRoutes() {
		if len(surfaces) == 0 {
			routes = append(routes, rt)
			continue
		}
		for _, want := range surfaces {
			if rt.surface == want {
				routes = append(routes, rt)
			}
		}
	}
	require.NotEmpty(t, routes)
	return routes
}

// openAPIMethods are the keys of a path item that name an operation.
var openAPIMethods = map[string]bool{
	"get": true, "put": true, "post": true, "delete": true,
	"patch": true, "head": true, "options": true, "trace": true,
}

// specMethodsByPath is every operation api/openapi.yaml documents, as its
// upper-case methods sorted, by path.
func specMethodsByPath(t *testing.T) map[string][]string {
	t.Helper()
	byPath := map[string][]string{}
	for path, operations := range loadOpenAPI(t).Paths {
		for method := range operations {
			require.Truef(t, openAPIMethods[method],
				"%s: %q is not an operation; teach specMethodsByPath what it is", path, method)
			byPath[path] = append(byPath[path], strings.ToUpper(method))
		}
		sort.Strings(byPath[path])
	}
	require.NotEmpty(t, byPath)
	return byPath
}

// specOperations is the same set as "METHOD /path".
func specOperations(t *testing.T) map[string]bool {
	t.Helper()
	operations := map[string]bool{}
	for path, methods := range specMethodsByPath(t) {
		for _, method := range methods {
			operations[method+" "+path] = true
		}
	}
	return operations
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TestRoutes_TableIsInternallyConsistent proves the table does not contradict
// itself: each entry's wrapper chain is the one its surface requires, only
// registration carries a worker key, a reason is set on exactly the unlisted
// entries, a surface agrees with the path it is on, and every entry at one path
// agrees about what that path is.
//
// The chain is data on each entry, so nothing structural stops it disagreeing
// with the surface. The behavioral tests below catch the consequence; this one
// names the cause.
func TestRoutes_TableIsInternallyConsistent(t *testing.T) {
	table := newRouteBed(t).server.routeTable()
	require.NotEmpty(t, table)

	type pathFacts struct {
		surface    surface
		group      group
		noFallback bool
	}
	seen := map[string]bool{}
	facts := map[string]pathFacts{}
	var workerKeyEntries []string

	for _, rt := range table {
		name := rt.pattern()
		require.Falsef(t, seen[name], "%s is declared twice", name)
		seen[name] = true

		require.Containsf(t, goldenMethods, rt.method, "%s: unexpected method", name)
		require.Truef(t, strings.HasPrefix(rt.path, "/"), "%s: a path starts with /", name)
		require.NotNilf(t, rt.handler, "%s has no handler", name)

		switch rt.surface {
		case surfacePublic:
			require.Equalf(t, chainAPIKey, rt.chain, "%s is public, so it must be wrapped in requireAPIKey", name)
			require.Equalf(t, groupAlways, rt.group, "%s: the public surface is not behind a feature group", name)
			require.Truef(t, strings.HasPrefix(rt.path, "/v1/"), "%s: a public route lives under /v1/", name)
		case surfaceInternal:
			require.Containsf(t, []chain{chainGuard, chainGuardWorkerKey}, rt.chain,
				"%s is internal, so it must go through the browser-origin guard", name)
			require.Containsf(t, []group{groupKeys, groupWorkerKeys, groupControl}, rt.group,
				"%s: an internal route is gated by keys, workerKeys or control", name)
			require.Truef(t, strings.HasPrefix(rt.path, "/internal/v1/"), "%s: an internal route lives under /internal/v1/", name)
		case surfaceProbe:
			require.Equalf(t, chainNone, rt.chain, "%s is a probe and needs no credential", name)
			require.Equalf(t, groupAlways, rt.group, "%s", name)
			require.Containsf(t, []string{"/healthz", "/readyz"}, rt.path, "%s", name)
		case surfaceUnlisted:
			require.Equalf(t, chainNone, rt.chain, "%s is unlisted and carries no wrapper", name)
			// A zero or unknown group is never enabled, so without this an unlisted
			// entry that forgot its group would simply not be registered.
			require.Containsf(t, []group{groupMetrics, groupDashboard}, rt.group,
				"%s: an unlisted route is gated by metrics or dashboard", name)
		default:
			require.Failf(t, "invalid surface", "%s has surface %v", name, rt.surface)
		}
		if rt.chain == chainGuardWorkerKey {
			workerKeyEntries = append(workerKeyEntries, name)
		}

		if rt.surface == surfaceUnlisted {
			require.NotEmptyf(t, strings.TrimSpace(rt.reason), "%s is unlisted but gives no reason", name)
		} else {
			require.Emptyf(t, rt.reason, "%s is documented in the spec, so it has no reason to be absent from it", name)
		}

		mine := pathFacts{rt.surface, rt.group, rt.noFallback}
		if other, ok := facts[rt.path]; ok {
			require.Equalf(t, other, mine, "the entries at %s disagree about what the path is", rt.path)
		}
		facts[rt.path] = mine
	}

	require.Equal(t, []string{"PUT /internal/v1/worker-sessions/{worker_session_id}"}, workerKeyEntries,
		"registration, and only registration, carries a worker key")
}

// TestRoutes_TableMatchesTheSpec proves the route table and api/openapi.yaml
// describe the same operations, in both directions.
//
// With every feature group on, the (method, path) of every entry that is not
// unlisted must be exactly the set of operations the document defines. A route
// added to one and not the other fails, and the failure says which side has what
// the other lacks. Before the table, the two maps that tried to keep the
// document from falling behind the handlers each walked from a hand list into
// the document, never from the server into the list.
func TestRoutes_TableMatchesTheSpec(t *testing.T) {
	inTable := map[string]bool{}
	for _, rt := range newRouteBed(t).server.enabledRoutes() {
		if rt.surface != surfaceUnlisted {
			inTable[rt.method+" "+rt.path] = true
		}
	}
	inSpec := specOperations(t)
	require.NotEmpty(t, inTable)
	require.NotEmpty(t, inSpec)

	var onlyTable, onlySpec []string
	for _, operation := range sortedKeys(inTable) {
		if !inSpec[operation] {
			onlyTable = append(onlyTable, operation)
		}
	}
	for _, operation := range sortedKeys(inSpec) {
		if !inTable[operation] {
			onlySpec = append(onlySpec, operation)
		}
	}
	if len(onlyTable) > 0 || len(onlySpec) > 0 {
		require.Failf(t, "the route table and api/openapi.yaml disagree",
			"in the route table but not in the spec (%d): %v\n"+
				"in the spec but not in the route table (%d): %v\n"+
				"Document the route in api/openapi.yaml, or, if it is deliberately not an API route, "+
				"declare it surfaceUnlisted with a reason.",
			len(onlyTable), onlyTable, len(onlySpec), onlySpec)
	}
}

// TestRoutes_UnlistedEntriesAreDeliberate proves a route that is registered but
// absent from the spec is absent on purpose: each unlisted entry gives a reason,
// is really registered (the mux matches its own pattern for it), and appears in
// the spec neither as that operation nor as that path.
//
// Without it, "unlisted" would be a way to register an undocumented route
// silently, which is the failure the table-versus-spec test exists to prevent.
func TestRoutes_UnlistedEntriesAreDeliberate(t *testing.T) {
	bed := newRouteBed(t)
	inSpec := specOperations(t)
	specPaths := specMethodsByPath(t)

	var unlisted []route
	for _, rt := range bed.server.enabledRoutes() {
		if rt.surface == surfaceUnlisted {
			unlisted = append(unlisted, rt)
		}
	}
	require.NotEmpty(t, unlisted, "the server has unlisted routes; the test would otherwise prove nothing")

	for _, rt := range unlisted {
		t.Run(rt.pattern(), func(t *testing.T) {
			require.NotEmpty(t, strings.TrimSpace(rt.reason), "an unlisted route must say why it is not in the spec")

			_, pattern := bed.serve(t, routeRequest(rt.method, rt.path))
			require.Equal(t, rt.pattern(), pattern, "the route is declared unlisted but the mux does not match it")

			require.Falsef(t, inSpec[rt.method+" "+rt.path], "%s is declared unlisted but api/openapi.yaml documents it", rt.pattern())
			require.NotContainsf(t, specPaths, rt.path, "%s is declared unlisted but api/openapi.yaml defines its path", rt.pattern())
		})
	}
}

// TestRoutes_PublicRoutesRequireAnAPIKey proves, by behavior, that every public
// entry is wrapped in requireAPIKey: with no key it answers 401 unauthorized
// with WWW-Authenticate, and with a key presented it asks the credential store
// about it.
//
// The second half is what the first cannot show. Every public handler also
// refuses an unauthenticated caller itself, so a route registered without the
// wrapper still answers 401 to a request with no credential. Only a presented
// credential reaching the store proves the wrapper is there.
func TestRoutes_PublicRoutesRequireAnAPIKey(t *testing.T) {
	bed := newRouteBed(t)
	for _, rt := range tableRoutes(t, surfacePublic) {
		t.Run(rt.pattern(), func(t *testing.T) {
			bed.probe.reset()
			rec, _ := bed.serve(t, routeRequest(rt.method, rt.path))
			require.Equal(t, http.StatusUnauthorized, rec.Code)
			require.Equal(t, CodeUnauthorized, errorCodeOf(rec))
			require.Equal(t, "Bearer", rec.Header().Get("WWW-Authenticate"))
			require.Empty(t, bed.probe.all(), "a request with no credential must not reach any dependency")

			bed.probe.reset()
			bed.serve(t, authorize(routeRequest(rt.method, rt.path)))
			require.Contains(t, bed.probe.all(), "APIKeys.Authenticate",
				"a presented credential never reached the store: the route is missing requireAPIKey")
		})
	}
}

// TestRoutes_InternalRoutesAreGuardedOutermost proves, by behavior, that the
// browser-origin guard is the OUTERMOST layer of every internal entry and of
// every internal path's 405 fallback.
//
// With a browser Origin, each internal entry answers 403 origin_refused, ahead
// of any 401 and ahead of any dependency; each internal path, asked for a method
// the table does not hold for it, answers 403 and not the 405, and does not
// advertise Allow. Internal-ness is read from the table's surface, never from
// the chain the derivation reports for a fallback, so a fallback derived outside
// the guard is caught rather than skipped.
func TestRoutes_InternalRoutesAreGuardedOutermost(t *testing.T) {
	bed := newRouteBed(t)
	internal := tableRoutes(t, surfaceInternal)

	refuse := func(t *testing.T, req *http.Request) {
		t.Helper()
		bed.probe.reset()
		req.Header.Set("Origin", "https://evil.example")
		rec, _ := bed.serve(t, req)
		require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
		require.Equal(t, CodeOriginRefused, errorCodeOf(rec))
		require.Empty(t, rec.Header().Get("Allow"), "a refused request must not say which methods the path allows")
		require.Empty(t, bed.probe.all(), "a refused request reached a dependency: %v", bed.probe.all())
	}

	paths := map[string]bool{}
	for _, rt := range internal {
		paths[rt.path] = true
		t.Run("route "+rt.pattern(), func(t *testing.T) {
			refuse(t, routeRequest(rt.method, rt.path))
		})
		if rt.chain == chainGuardWorkerKey {
			// Registration. The guard must also come before the worker key is
			// even looked at: a VALID key on a browser-marked request still
			// reaches nothing.
			t.Run("route "+rt.pattern()+" with a valid worker key", func(t *testing.T) {
				req := routeRequest(rt.method, rt.path)
				req.Header.Set(authorizationHeader, "Bearer "+testRawWorkerKey)
				refuse(t, req)
			})
		}
	}
	for _, path := range sortedKeys(paths) {
		t.Run("fallback "+path, func(t *testing.T) {
			// No internal route uses DELETE, so it is a method every internal
			// path's fallback answers.
			refuse(t, routeRequest(http.MethodDelete, path))
		})
	}
}

// TestRoutes_RegistrationAloneRequiresAWorkerKey proves the asymmetry the design
// keeps on purpose: worker registration answers 401 without a valid worker key
// and never reaches the control service, while every other internal route needs
// no credential and reaches its dependency.
//
// A positive control comes first: with a valid key the same route does register,
// so the 401s that follow are the wrapper's and not a malformed request's.
func TestRoutes_RegistrationAloneRequiresAWorkerKey(t *testing.T) {
	bed := newRouteBed(t)

	var registration route
	for _, rt := range tableRoutes(t, surfaceInternal) {
		if rt.chain == chainGuardWorkerKey {
			registration = rt
		}
	}
	require.Equal(t, "PUT /internal/v1/worker-sessions/{worker_session_id}", registration.pattern())
	operation := internalOperation{registration.method, registration.path}

	t.Run("control: a valid worker key registers", func(t *testing.T) {
		bed.probe.reset()
		rec, _ := bed.serve(t, operation.validRequest(t))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Contains(t, bed.probe.all(), "Control.Register")
	})

	for name, apply := range map[string]func(*http.Request){
		"no credential": func(*http.Request) {},
		// A well-formed credential the worker-key store does not accept: the API
		// key, which authenticates a different surface.
		"a credential the worker-key store rejects": func(r *http.Request) {
			r.Header.Set(authorizationHeader, "Bearer "+testRawKey)
		},
	} {
		t.Run(name, func(t *testing.T) {
			bed.probe.reset()
			req := operation.validRequest(t)
			req.Header.Del(authorizationHeader)
			apply(req)
			rec, _ := bed.serve(t, req)
			require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
			require.Equal(t, CodeUnauthorized, errorCodeOf(rec))
			require.NotContains(t, bed.probe.all(), "Control.Register", "registration ran without a valid worker key")
		})
	}

	for _, rt := range tableRoutes(t, surfaceInternal) {
		if rt.chain != chainGuard {
			continue
		}
		t.Run("no credential is needed for "+rt.pattern(), func(t *testing.T) {
			bed.probe.reset()
			req := internalOperation{rt.method, rt.path}.validRequest(t)
			require.Empty(t, req.Header.Get(authorizationHeader))
			rec, _ := bed.serve(t, req)
			require.NotEqual(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
			require.Truef(t, bed.probe.reached(), "%s never reached its dependency without a credential", rt.pattern())
		})
	}
}

// TestRoutes_ProbesAndUnlistedRoutesNeedNoCredentialAndNoGuard proves the other
// two surfaces are unwrapped: a probe, /metrics and the dashboard answer a
// request with no credential, even one a browser marks and addresses to a name
// that is not loopback.
//
// The guard is for /internal only (ADR-0018). A guard that crept onto a probe
// would take a load balancer's health check down with it.
func TestRoutes_ProbesAndUnlistedRoutesNeedNoCredentialAndNoGuard(t *testing.T) {
	bed := newRouteBed(t)
	for _, rt := range tableRoutes(t, surfaceProbe, surfaceUnlisted) {
		t.Run(rt.pattern(), func(t *testing.T) {
			req := httptest.NewRequest(rt.method, requestPath(rt.path), http.NoBody)
			req.Host = "taskforge.example"
			req.Header.Set("Origin", "https://evil.example")
			req.Header.Set("Sec-Fetch-Site", "cross-site")
			rec, pattern := bed.serve(t, req)

			require.Equal(t, rt.pattern(), pattern)
			require.NotEqual(t, http.StatusUnauthorized, rec.Code)
			require.NotEqual(t, http.StatusForbidden, rec.Code)
			require.NotEqual(t, CodeOriginRefused, errorCodeOf(rec))
		})
	}
}

// TestRoutes_FallbacksAreDerivedFromTheTable proves the method-less 405 fallbacks
// say what the table and the spec say: for every spec path, a method the
// document does not define answers 405 method_not_allowed, whose Allow header is
// exactly the document's methods for that path, sorted; and the same holds for
// every other path the table holds, the dashboard's included.
//
// It also pins the two deliberate exceptions: /metrics and "/" have no fallback,
// so a wrong method on either still reaches the catch-all's 404, as it did
// before the table existed.
func TestRoutes_FallbacksAreDerivedFromTheTable(t *testing.T) {
	bed := newRouteBed(t)

	firstUndefined := func(defined []string) string {
		for _, candidate := range []string{
			http.MethodDelete, http.MethodPatch, http.MethodPut, http.MethodPost, http.MethodGet,
		} {
			if !containsString(defined, candidate) {
				return candidate
			}
		}
		require.Fail(t, "a path defines every method", "%v", defined)
		return ""
	}
	expect405 := func(t *testing.T, path string, defined []string) {
		t.Helper()
		bed.probe.reset()
		rec, pattern := bed.serve(t, routeRequest(firstUndefined(defined), path))
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code, rec.Body.String())
		require.Equal(t, CodeMethodNotAllowed, errorCodeOf(rec))
		require.Equal(t, strings.Join(defined, ", "), rec.Header().Get("Allow"))
		require.Equal(t, path, pattern, "a fallback is the method-less pattern, which is the bare path")
		require.Empty(t, bed.probe.all())
	}

	for path, methods := range specMethodsByPath(t) {
		t.Run("spec "+path, func(t *testing.T) { expect405(t, path, methods) })
	}

	t.Run("every table path with a fallback", func(t *testing.T) {
		held := map[string][]string{}
		var noFallback []string
		for _, rt := range bed.server.enabledRoutes() {
			if rt.noFallback {
				noFallback = append(noFallback, rt.pattern())
				continue
			}
			held[rt.path] = append(held[rt.path], rt.method)
		}
		require.NotEmpty(t, held)
		for path, methods := range held {
			sort.Strings(methods)
			expect405(t, path, methods)
		}

		sort.Strings(noFallback)
		require.Equal(t, []string{"GET /metrics", "GET /{$}"}, noFallback,
			"the paths without a fallback are two deliberate exceptions; a third is a decision, not a default")
	})

	t.Run("a wrong method on a path without a fallback is the catch-all's 404", func(t *testing.T) {
		for _, rt := range bed.server.enabledRoutes() {
			if !rt.noFallback {
				continue
			}
			rec, pattern := bed.serve(t, routeRequest(http.MethodDelete, rt.path))
			require.Equal(t, http.StatusNotFound, rec.Code, rt.pattern())
			require.Equal(t, CodeNotFound, errorCodeOf(rec), rt.pattern())
			require.Empty(t, rec.Header().Get("Allow"), rt.pattern())
			require.Equal(t, "/", pattern, rt.pattern())
		}
	})
}

func containsString(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

// TestRoutes_FeatureGroupsGateTheirRoutesAndNothingElse proves a feature group
// gates exactly its own routes. With one group off on its own, every route of
// that group, and its 405 fallback, answers the "/" catch-all's 404; every other
// route answers exactly as it does with everything on.
//
// "Exactly" compares the status, the Allow header, the error code and the
// matched pattern. A group that gated a neighbour, or a fallback that outlived
// its routes and told a caller a path exists, fails here.
func TestRoutes_FeatureGroupsGateTheirRoutesAndNothingElse(t *testing.T) {
	type behavior struct {
		status         int
		allow, code    string
		matchedPattern string
	}
	observe := func(t *testing.T, bed *routeBed, method, path string) behavior {
		t.Helper()
		bed.probe.reset()
		rec, pattern := bed.serve(t, routeRequest(method, path))
		return behavior{rec.Code, rec.Header().Get("Allow"), errorCodeOf(rec), pattern}
	}

	baseline := newRouteBed(t)
	table := baseline.server.enabledRoutes()
	require.NotEmpty(t, table)

	groups := []group{groupMetrics, groupDashboard, groupKeys, groupWorkerKeys, groupControl}
	for _, off := range groups {
		t.Run(off.String()+" off", func(t *testing.T) {
			bed := newRouteBed(t, off)
			var gated int
			for _, rt := range table {
				// The route's own method, and DELETE, which no route uses and every
				// path's fallback therefore answers.
				for _, method := range []string{rt.method, http.MethodDelete} {
					want := observe(t, baseline, method, rt.path)
					got := observe(t, bed, method, rt.path)
					label := fmt.Sprintf("%s %s with %s off", method, rt.path, off)
					if rt.group == off {
						gated++
						require.Equal(t, behavior{http.StatusNotFound, "", CodeNotFound, "/"}, got,
							"%s: a gated route and its fallback must fall to the catch-all", label)
						continue
					}
					require.Equal(t, want, got, "%s: a group changed a route that is not its own", label)
				}
			}
			require.NotZerof(t, gated, "no route belongs to the %s group; the test would prove nothing", off)
		})
	}
}
