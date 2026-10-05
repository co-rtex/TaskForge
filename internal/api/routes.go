package api

import (
	"fmt"
	"net/http"
	"sort"
)

// This file is the one place a route is declared. Handler() registers every
// pattern from routeTable and nowhere else, except the "/" catch-all that
// registerRoutes adds last. TestRoutes_NothingRegistersAroundTheTable holds that
// with a syntax-tree check, so a route added any other way fails a test rather
// than escaping every list that is meant to cover it.
//
// To add a route: add an entry to routeTable, and either document it in
// api/openapi.yaml (surfacePublic, surfaceInternal, surfaceProbe) or give it a
// reason it is not there (surfaceUnlisted). TestRoutes_TableMatchesTheSpec and
// TestRoutes_UnlistedEntriesAreDeliberate fail until one of the two is true.
//
// See docs/adr/0022-the-route-table-is-the-single-source-of-routes.md.

// surface says what a route is for. It decides how the route is wrapped and
// whether it must appear in api/openapi.yaml.
type surface int

const (
	// surfacePublic is the /v1 surface a client authenticates to with an API
	// key. In the spec.
	surfacePublic surface = iota + 1
	// surfaceInternal is the /internal/v1 surface: operator and worker plumbing
	// behind the browser-origin guard (ADR-0018). In the spec.
	surfaceInternal
	// surfaceProbe is the unauthenticated health probes. In the spec.
	surfaceProbe
	// surfaceUnlisted is a registered route that api/openapi.yaml deliberately
	// does not document. It must carry a reason.
	surfaceUnlisted
)

func (s surface) String() string {
	switch s {
	case surfacePublic:
		return "public"
	case surfaceInternal:
		return "internal"
	case surfaceProbe:
		return "probe"
	case surfaceUnlisted:
		return "unlisted"
	}
	return fmt.Sprintf("surface(%d)", int(s))
}

// chain names the wrappers a route's handler gets, outermost first. The zero
// value is deliberately not a chain, so an entry that forgot one is a panic at
// registration rather than an unwrapped route.
type chain int

const (
	// chainNone registers the handler as written.
	chainNone chain = iota + 1
	// chainAPIKey is requireAPIKey(handler): the public surface.
	chainAPIKey
	// chainGuard is refuseBrowserOrigin(handler), through handleInternal: every
	// /internal route except registration.
	chainGuard
	// chainGuardWorkerKey is refuseBrowserOrigin(requireWorkerKey(handler)),
	// through handleInternal: worker registration only. The guard is OUTSIDE the
	// credential check, so a browser-marked request is refused before it can be
	// authenticated.
	chainGuardWorkerKey
)

func (c chain) String() string {
	switch c {
	case chainNone:
		return "none"
	case chainAPIKey:
		return "api-key"
	case chainGuard:
		return "guard"
	case chainGuardWorkerKey:
		return "guard+worker-key"
	}
	return fmt.Sprintf("chain(%d)", int(c))
}

// group is the feature that has to be wired for a route to be registered. A
// route whose group is off is not registered, and its path falls through to the
// "/" catch-all like any other unrouted path.
type group int

const (
	// groupAlways routes exist on every server.
	groupAlways group = iota + 1
	// groupMetrics is enabled by WithMetrics.
	groupMetrics
	// groupDashboard is enabled by WithDashboard.
	groupDashboard
	// groupKeys is enabled by WithAuth: the API-key management routes.
	groupKeys
	// groupWorkerKeys is enabled by WithWorkerAuth: the worker-key management
	// routes.
	groupWorkerKeys
	// groupControl is enabled by WithWorkerControl: the eight worker-control
	// routes.
	groupControl
)

func (g group) String() string {
	switch g {
	case groupAlways:
		return "always"
	case groupMetrics:
		return "metrics"
	case groupDashboard:
		return "dashboard"
	case groupKeys:
		return "keys"
	case groupWorkerKeys:
		return "workerKeys"
	case groupControl:
		return "control"
	}
	return fmt.Sprintf("group(%d)", int(g))
}

// groupEnabled reports whether the feature behind g is wired on this server.
func (s *Server) groupEnabled(g group) bool {
	switch g {
	case groupAlways:
		return true
	case groupMetrics:
		return s.metrics != nil
	case groupDashboard:
		return s.dashboard != nil
	case groupKeys:
		return s.keys != nil
	case groupWorkerKeys:
		return s.workerKeys != nil
	case groupControl:
		return s.control != nil
	}
	return false
}

// route is one registered method and path, and everything that decides how it
// is registered and what it is held to.
type route struct {
	// method and path together are the exact pattern ServeMux sees:
	// method + " " + path. They are separate fields because the 405 fallback is
	// derived from the methods the table holds for a path. The pattern string is
	// observable (it is the span name and the HTTP metric's route label) and must
	// not change, which is what testdata/route_behavior.golden pins.
	method string
	path   string

	surface surface
	chain   chain
	group   group
	handler http.HandlerFunc

	// noFallback marks a path that has NO method-less 405 fallback. Every other
	// path gets one, derived from the methods the table holds for it. A path
	// marked here keeps answering a wrong method with the "/" catch-all's 404,
	// exactly as it did before the table existed: the fallback is deliberately
	// not derived for it, and every route at a path must agree. See the entries
	// that set it for why each one does.
	noFallback bool

	// reason is why an unlisted route is not in api/openapi.yaml. It is set on
	// every surfaceUnlisted route and on no other.
	reason string
}

// pattern is the string registered with ServeMux.
func (r route) pattern() string { return r.method + " " + r.path }

// routeTable is every route this server can register, in every feature group,
// as data. It is built per server because each handler is a method value bound
// to s; which entries are registered is decided by groupEnabled, not here.
//
// Wrappers are applied by rule from chain, and chain follows surface:
//
//   - public routes are wrapped in requireAPIKey, so the set of authenticated
//     routes is readable here rather than in an inclusion list inside a
//     middleware that would drift from the mux;
//   - every internal route, and every internal 405 fallback, goes through
//     handleInternal, which puts the browser-origin guard outside everything else
//     the route carries;
//   - registration alone carries requireWorkerKey, inside the guard. Every other
//     worker-control route resolves its scope from the session identity the
//     request already carries (see requireWorkerKey for why that asymmetry is
//     deliberate), so adding the wrapper to one of them would be a change in
//     authentication, not a tidy-up;
//   - probes and unlisted routes are unwrapped.
func (s *Server) routeTable() []route {
	// Resolved once, when the table is built, rather than per request. With
	// metrics off it stays nil and no groupMetrics route is registered.
	var metricsHandler http.HandlerFunc
	if s.metrics != nil {
		metricsHandler = s.metrics.Handler().ServeHTTP
	}

	return []route{
		// --- Public surface. ---------------------------------------------------
		{method: http.MethodPost, path: "/v1/jobs", surface: surfacePublic, chain: chainAPIKey, group: groupAlways, handler: s.handleSubmitJob},
		{method: http.MethodGet, path: "/v1/jobs", surface: surfacePublic, chain: chainAPIKey, group: groupAlways, handler: s.handleListJobs},
		{method: http.MethodGet, path: "/v1/jobs/{job_id}", surface: surfacePublic, chain: chainAPIKey, group: groupAlways, handler: s.handleGetJob},
		{method: http.MethodGet, path: "/v1/jobs/{job_id}/attempts", surface: surfacePublic, chain: chainAPIKey, group: groupAlways, handler: s.handleListJobAttempts},
		{method: http.MethodGet, path: "/v1/jobs/{job_id}/result", surface: surfacePublic, chain: chainAPIKey, group: groupAlways, handler: s.handleGetJobResult},
		{method: http.MethodPost, path: "/v1/jobs/{job_id}/cancel", surface: surfacePublic, chain: chainAPIKey, group: groupAlways, handler: s.handleCancelJob},
		// Operator retry IS DLQ replay: same service, same idempotency namespace.
		// Two routes exist because operators reach for both names, not because
		// there are two operations.
		{method: http.MethodPost, path: "/v1/jobs/{job_id}/retry", surface: surfacePublic, chain: chainAPIKey, group: groupAlways, handler: s.handleReplayJob},
		{method: http.MethodGet, path: "/v1/dlq", surface: surfacePublic, chain: chainAPIKey, group: groupAlways, handler: s.handleListDLQ},
		{method: http.MethodPost, path: "/v1/dlq/{job_id}/replay", surface: surfacePublic, chain: chainAPIKey, group: groupAlways, handler: s.handleReplayJob},
		// The operator read surface (M6A). PROJECT_SPEC.md section 4 items 4 and 7
		// name these; the dashboard, the CLI, and the SDK are all consumers of
		// exactly these four routes and nothing else.
		{method: http.MethodGet, path: "/v1/workers", surface: surfacePublic, chain: chainAPIKey, group: groupAlways, handler: s.handleListWorkers},
		{method: http.MethodGet, path: "/v1/queues", surface: surfacePublic, chain: chainAPIKey, group: groupAlways, handler: s.handleListQueues},

		// --- Health probes. ----------------------------------------------------
		// Unauthenticated. They reveal nothing tenant-specific, and a liveness
		// probe that needed a credential would report a healthy process as dead
		// the moment that credential was revoked.
		{method: http.MethodGet, path: "/healthz", surface: surfaceProbe, chain: chainNone, group: groupAlways, handler: s.handleLiveness},
		{method: http.MethodGet, path: "/readyz", surface: surfaceProbe, chain: chainNone, group: groupAlways, handler: s.handleReadiness},

		// --- Registered, and deliberately not in api/openapi.yaml. --------------
		{
			method: http.MethodGet, path: "/metrics", surface: surfaceUnlisted, chain: chainNone, group: groupMetrics,
			handler: metricsHandler,
			// Unauthenticated, like the probes beside it, on this same
			// already-loopback-bound listener. See WithMetrics.
			reason: "Prometheus text exposition for an operator's scraper on the loopback listener; not part of the client API",
			// No fallback: a wrong method on /metrics reaches the "/" catch-all and
			// answers its structured 404 today. Deriving a 405 here would change
			// that, so it is preserved, not fixed.
			noFallback: true,
		},
		{
			method: http.MethodGet, path: DashboardPath, surface: surfaceUnlisted, chain: chainNone, group: groupDashboard,
			handler: s.handleDashboard,
			// Static files and nothing else: the dashboard reads only the /v1
			// routes above, with the operator's own key, like any other client.
			// One subtree pattern, so every dashboard path shares one bounded span
			// name and metric route label however many client routes it has.
			reason: "static files of the embedded operator dashboard, served as a subtree under /dashboard/; not API resources",
			// This path DOES get a derived 405 fallback, as it always had.
		},
		{
			method: http.MethodGet, path: "/{$}", surface: surfaceUnlisted, chain: chainNone, group: groupDashboard,
			handler: s.handleDashboardRoot,
			// {$} matches the bare root only. Every other unrouted path still falls
			// through to the catch-all.
			reason: "sends the bare root to the embedded dashboard's mount point; part of serving its static files, not an API resource",
			// No fallback: a wrong method on "/" reaches the catch-all's structured
			// 404 today. A 405 here would also tell a caller "/" is a route. Preserved.
			noFallback: true,
		},

		// --- Internal surface: API-key management (ADR-0013). -------------------
		{method: http.MethodPost, path: "/internal/v1/api-keys", surface: surfaceInternal, chain: chainGuard, group: groupKeys, handler: s.handleCreateAPIKey},
		{method: http.MethodGet, path: "/internal/v1/api-keys", surface: surfaceInternal, chain: chainGuard, group: groupKeys, handler: s.handleListAPIKeys},
		{method: http.MethodPost, path: "/internal/v1/api-keys/{key_id}/revoke", surface: surfaceInternal, chain: chainGuard, group: groupKeys, handler: s.handleRevokeAPIKey},

		// --- Internal surface: worker-key management (ADR-0014). ----------------
		{method: http.MethodPost, path: "/internal/v1/worker-keys", surface: surfaceInternal, chain: chainGuard, group: groupWorkerKeys, handler: s.handleCreateWorkerKey},
		{method: http.MethodGet, path: "/internal/v1/worker-keys", surface: surfaceInternal, chain: chainGuard, group: groupWorkerKeys, handler: s.handleListWorkerKeys},
		{method: http.MethodPost, path: "/internal/v1/worker-keys/{key_id}/revoke", surface: surfaceInternal, chain: chainGuard, group: groupWorkerKeys, handler: s.handleRevokeWorkerKey},

		// --- Internal surface: worker control. ---------------------------------
		// Registration alone is wrapped in requireWorkerKey: every other
		// worker-control route below resolves its scope from the session identity
		// the request already carries rather than a fresh credential. See
		// requireWorkerKey's doc comment for why that asymmetry is deliberate.
		{method: http.MethodPut, path: "/internal/v1/worker-sessions/{worker_session_id}", surface: surfaceInternal, chain: chainGuardWorkerKey, group: groupControl, handler: s.handleRegisterWorkerSession},
		{method: http.MethodPost, path: "/internal/v1/worker-sessions/{worker_session_id}/heartbeat", surface: surfaceInternal, chain: chainGuard, group: groupControl, handler: s.handleHeartbeat},
		{method: http.MethodPost, path: "/internal/v1/claims", surface: surfaceInternal, chain: chainGuard, group: groupControl, handler: s.handleClaim},
		{method: http.MethodPost, path: "/internal/v1/leases/{lease_id}/renew", surface: surfaceInternal, chain: chainGuard, group: groupControl, handler: s.handleRenewLease},
		{method: http.MethodPost, path: "/internal/v1/attempts/{attempt_id}/start", surface: surfaceInternal, chain: chainGuard, group: groupControl, handler: s.handleStartAttempt},
		{method: http.MethodPost, path: "/internal/v1/attempts/{attempt_id}/succeed", surface: surfaceInternal, chain: chainGuard, group: groupControl, handler: s.handleSucceedAttempt},
		{method: http.MethodPost, path: "/internal/v1/attempts/{attempt_id}/fail", surface: surfaceInternal, chain: chainGuard, group: groupControl, handler: s.handleFailAttempt},
		{method: http.MethodPost, path: "/internal/v1/attempts/{attempt_id}/cancel", surface: surfaceInternal, chain: chainGuard, group: groupControl, handler: s.handleCancelAttempt},
	}
}

// enabledRoutes is the table filtered to the groups wired on this server: the
// routes Handler() will actually register.
func (s *Server) enabledRoutes() []route {
	var enabled []route
	for _, rt := range s.routeTable() {
		if s.groupEnabled(rt.group) {
			enabled = append(enabled, rt)
		}
	}
	return enabled
}

// fallback is a method-less pattern that answers every method the table does not
// hold for a path with the structured 405.
type fallback struct {
	path    string
	methods []string // sorted: this is the Allow header, in order
	chain   chain
}

// fallbacksFor derives the 405 fallbacks from the routes that are registered.
//
// ServeMux answers an unmatched method with a plain-text 405 and an unmatched
// path with a plain-text 404, neither of which matches the structured error
// shape every other response uses. A method-less pattern registered beside the
// real ones reclaims the first case: a pattern that names a method is more
// specific, so it still wins for that method, and every other method falls
// through to the fallback.
//
// Deriving it from the table, per path, means a route added to a path widens
// that path's Allow header by construction instead of by someone remembering a
// second list, and a path whose routes are all in a disabled group has no
// fallback either, so it falls to the "/" catch-all like any unrouted path.
//
// An internal path's fallback goes through the guard like the routes beside it:
// 405 is answered before authentication, deliberately, but after the
// browser-origin guard. A browser-marked DELETE is refused 403, not told which
// methods exist. "This path does not accept DELETE" is a fact about the route
// table, not about the caller, so it discloses nothing a reader of the OpenAPI
// document does not already have.
//
// A path whose entries set noFallback gets none.
func fallbacksFor(routes []route) []fallback {
	skip := map[string]bool{}
	for _, rt := range routes {
		if rt.noFallback {
			skip[rt.path] = true
		}
	}

	var order []string
	byPath := map[string]*fallback{}
	for _, rt := range routes {
		if skip[rt.path] {
			continue
		}
		fb, seen := byPath[rt.path]
		if !seen {
			fb = &fallback{path: rt.path, chain: chainNone}
			if rt.surface == surfaceInternal {
				fb.chain = chainGuard
			}
			byPath[rt.path] = fb
			order = append(order, rt.path)
		}
		fb.methods = append(fb.methods, rt.method)
	}

	fallbacks := make([]fallback, 0, len(order))
	for _, path := range order {
		fb := byPath[path]
		sort.Strings(fb.methods)
		fallbacks = append(fallbacks, *fb)
	}
	return fallbacks
}

// registerRoutes registers every enabled route, then every derived fallback,
// then the "/" catch-all. It is the only code that iterates the table.
func (s *Server) registerRoutes(mux *http.ServeMux) {
	enabled := s.enabledRoutes()
	for _, rt := range enabled {
		s.register(mux, rt.pattern(), rt.chain, rt.handler)
	}
	for _, fb := range fallbacksFor(enabled) {
		s.register(mux, fb.path, fb.chain, s.methodNotAllowed(fb.methods...))
	}
	// The one registration that is not in the table: it answers every path
	// nothing else matched with the structured 404, and so exists on every
	// server whatever its configuration.
	mux.HandleFunc("/", s.handleNotFound)
}

// register applies a chain to a handler and registers it. It is the only place
// a chain is turned into wrappers.
func (s *Server) register(mux *http.ServeMux, pattern string, c chain, handler http.HandlerFunc) {
	switch c {
	case chainNone:
		mux.HandleFunc(pattern, handler)
	case chainAPIKey:
		mux.HandleFunc(pattern, s.requireAPIKey(handler))
	case chainGuard:
		s.handleInternal(mux, pattern, handler)
	case chainGuardWorkerKey:
		s.handleInternal(mux, pattern, s.requireWorkerKey(handler))
	default:
		panic(fmt.Sprintf("api: route %q has no wrapper chain; set one in routeTable", pattern))
	}
}
