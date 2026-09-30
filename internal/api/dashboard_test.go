package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const (
	testDashboardIndex = `<!doctype html><script type="module" src="/dashboard/assets/index-abc123.js"></script>`
	testDashboardJS    = `console.log("dashboard")`
)

// testDashboardAssets has the shape of Vite's real build output: one entry
// document, content-hashed assets under assets/, and the committed .gitkeep
// that go:embed's all: prefix carries along with them.
func testDashboardAssets() fstest.MapFS {
	return fstest.MapFS{
		".gitkeep":               {Data: nil},
		"index.html":             {Data: []byte(testDashboardIndex)},
		"assets/index-abc123.js": {Data: []byte(testDashboardJS)},
		"favicon.svg":            {Data: []byte(`<svg xmlns="http://www.w3.org/2000/svg"/>`)},
	}
}

func newDashboardTestServer(t *testing.T) http.Handler {
	t.Helper()
	return NewServer(nil, Config{MaxRequestBytes: 1024}, discardLogger()).
		WithAuth(acceptingKeys(testScope)).
		WithResults(acceptingResults(), nil).
		WithDashboard(testDashboardAssets()).
		Handler()
}

func serve(h http.Handler, method, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

// The JSON 404 an unrouted path gets is the reference body. Request ids differ
// per request, so the comparison blanks that one field and nothing else.
func requireStructuredNotFound(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
	body := decodeError(t, rec)
	require.Equal(t, CodeNotFound, body.Error.Code)
	require.Equal(t, "no such endpoint", body.Error.Message)
	require.NotEmpty(t, body.Error.RequestID)
	require.Empty(t, rec.Header().Get("Content-Security-Policy"),
		"a JSON error is not a dashboard document")
}

// Without WithDashboard nothing changes: the root, the dashboard's own mount,
// and every path under it are the same structured 404 as any other unrouted
// path. This is what keeps every pre-M6D test valid without modification.
func TestDashboard_UnsetLeavesTheCatchAllUnchanged(t *testing.T) {
	h := newTestServer(t)
	for _, path := range []string{"/", "/dashboard", "/dashboard/", "/dashboard/jobs", "/dashboard/assets/index-abc123.js", "/nope"} {
		t.Run(path, func(t *testing.T) {
			requireStructuredNotFound(t, serve(h, http.MethodGet, path))
		})
	}
}

func TestDashboard_ServesTheEntryDocument(t *testing.T) {
	h := newDashboardTestServer(t)
	for _, path := range []string{"/dashboard/", "/dashboard/index.html"} {
		t.Run(path, func(t *testing.T) {
			rec := serve(h, http.MethodGet, path)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, testDashboardIndex, rec.Body.String())
			require.Equal(t, "text/html; charset=utf-8", rec.Header().Get("Content-Type"))
			// The entry document names the current hashed assets, so it must be
			// revalidated; a cached copy would point at assets a rebuild removed.
			require.Equal(t, "no-cache", rec.Header().Get("Cache-Control"))
			requireDashboardSecurityHeaders(t, rec)
		})
	}
}

// A client-side route has no file behind it. It is answered with the entry
// document so the dashboard's own router can render it, which is what makes a
// deep link or a browser reload on /dashboard/jobs/<id> work.
func TestDashboard_ClientRoutesFallBackToTheEntryDocument(t *testing.T) {
	h := newDashboardTestServer(t)
	for _, path := range []string{
		"/dashboard/jobs",
		"/dashboard/jobs/11111111-1111-4111-8111-111111111111",
		"/dashboard/workers",
		"/dashboard/queues",
		"/dashboard/dlq",
		"/dashboard/no-such-view",
	} {
		t.Run(path, func(t *testing.T) {
			rec := serve(h, http.MethodGet, path)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, testDashboardIndex, rec.Body.String())
			require.Equal(t, "no-cache", rec.Header().Get("Cache-Control"))
		})
	}
}

func TestDashboard_ServesHashedAssetsImmutably(t *testing.T) {
	h := newDashboardTestServer(t)
	rec := serve(h, http.MethodGet, "/dashboard/assets/index-abc123.js")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, testDashboardJS, rec.Body.String())
	require.Equal(t, "text/javascript; charset=utf-8", rec.Header().Get("Content-Type"))
	// The file name carries its content hash, so a changed file is a new URL.
	require.Equal(t, "public, max-age=31536000, immutable", rec.Header().Get("Cache-Control"))
	requireDashboardSecurityHeaders(t, rec)

	// A top-level file is not content-hashed and must not be cached forever.
	rec = serve(h, http.MethodGet, "/dashboard/favicon.svg")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "no-cache", rec.Header().Get("Cache-Control"))
}

// A missing asset must never be answered with the entry document: the browser
// would try to execute HTML as a script and report a baffling syntax error
// instead of the 404 that actually happened.
func TestDashboard_AMissingAssetIsAStructuredNotFound(t *testing.T) {
	h := newDashboardTestServer(t)
	requireStructuredNotFound(t, serve(h, http.MethodGet, "/dashboard/assets/index-gone.js"))
	requireStructuredNotFound(t, serve(h, http.MethodGet, "/dashboard/assets/"))
}

// Invalid UTF-8 in a client route never reaches the dashboard's router: the
// server answers the JSON 404 itself. (A malformed escape is rejected earlier
// still, by net/http, as a 400.) The router guards its own decode regardless.
func TestDashboard_InvalidUTF8PathIsAStructuredNotFound(t *testing.T) {
	requireStructuredNotFound(t, serve(newDashboardTestServer(t), http.MethodGet, "/dashboard/jobs/%E0"))
}

// Dotfiles ride along in the embedded tree (the committed .gitkeep is one) and
// are never served.
func TestDashboard_HiddenFilesAreNotServed(t *testing.T) {
	h := newDashboardTestServer(t)
	requireStructuredNotFound(t, serve(h, http.MethodGet, "/dashboard/.gitkeep"))
	requireStructuredNotFound(t, serve(h, http.MethodGet, "/dashboard/assets/.hidden"))
}

// The acceptance criterion this mount exists to satisfy: a mistyped API path
// is still the API's own JSON 404 while the dashboard is enabled, never the
// dashboard's HTML. Mounting at /dashboard/ rather than / is what makes this
// structural rather than a list of exclusions that could drift.
func TestDashboard_UnroutedAPIPathsStayStructuredNotFound(t *testing.T) {
	h := newDashboardTestServer(t)
	for _, path := range []string{
		"/v1/nonexistent",
		"/v1/jobs/11111111-1111-4111-8111-111111111111/nope",
		"/internal/v1/nonexistent",
		"/nope",
		"/dashboardx",
		"/index.html",
		"/assets/index-abc123.js",
	} {
		t.Run(path, func(t *testing.T) {
			requireStructuredNotFound(t, serve(h, http.MethodGet, path))
		})
	}
}

// Enabling the dashboard must not touch a single API route's behavior.
func TestDashboard_EnabledLeavesAPIRoutesUnchanged(t *testing.T) {
	h := newDashboardTestServer(t)

	rec := serve(h, http.MethodGet, "/v1/jobs")
	require.Equal(t, http.StatusUnauthorized, rec.Code, "public routes still require a key")
	require.Equal(t, CodeUnauthorized, decodeError(t, rec).Error.Code)

	rec = serve(h, http.MethodGet, "/healthz")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = serve(h, http.MethodDelete, "/v1/queues")
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	require.Equal(t, CodeMethodNotAllowed, decodeError(t, rec).Error.Code)
}

func TestDashboard_RootRedirectsToTheDashboardOnlyWhenEnabled(t *testing.T) {
	rec := serve(newDashboardTestServer(t), http.MethodGet, "/")
	require.Equal(t, http.StatusFound, rec.Code)
	require.Equal(t, "/dashboard/", rec.Header().Get("Location"))

	requireStructuredNotFound(t, serve(newTestServer(t), http.MethodGet, "/"))
}

// ServeMux's own trailing-slash redirect. Its status is the standard library's
// choice and has changed between Go releases (301, then 307), so this pins
// where it goes rather than which code carries it.
func TestDashboard_MountWithoutTrailingSlashRedirects(t *testing.T) {
	rec := serve(newDashboardTestServer(t), http.MethodGet, "/dashboard")
	require.Contains(t, []int{http.StatusMovedPermanently, http.StatusTemporaryRedirect, http.StatusPermanentRedirect}, rec.Code)
	require.Equal(t, "/dashboard/", rec.Header().Get("Location"))
}

// The dashboard is read-only, and so is its mount.
func TestDashboard_WritesAreMethodNotAllowed(t *testing.T) {
	h := newDashboardTestServer(t)
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		rec := serve(h, method, "/dashboard/jobs")
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		require.Equal(t, "GET", rec.Header().Get("Allow"))
		require.Equal(t, CodeMethodNotAllowed, decodeError(t, rec).Error.Code)
	}
}

func TestDashboard_HeadIsServedWithoutABody(t *testing.T) {
	rec := serve(newDashboardTestServer(t), http.MethodHead, "/dashboard/")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Empty(t, rec.Body.String())
}

// Every dashboard path shares one bounded span name -- the same property
// TestTracing_UnroutedRequestsShareOneBoundedSpanName pins for the catch-all.
// A client route carrying a job id must not put that id into a span name, and,
// because withHTTPMetrics reads the same pattern, into a metric label.
func TestDashboard_PathsShareOneBoundedSpanName(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	h := NewServer(nil, Config{MaxRequestBytes: 1024}, discardLogger()).
		WithAuth(acceptingKeys(testScope)).
		WithDashboard(testDashboardAssets()).
		WithTracer(provider.Tracer("test")).
		Handler()

	const jobID = "11111111-1111-4111-8111-111111111111"
	for _, path := range []string{"/dashboard/", "/dashboard/jobs/" + jobID, "/dashboard/assets/index-abc123.js"} {
		serve(h, http.MethodGet, path)
	}

	spans := exporter.GetSpans()
	require.Len(t, spans, 3)
	for _, span := range spans {
		require.Equal(t, "GET /dashboard/", span.Name)
		require.NotContains(t, span.Name, jobID)
	}
}

// An entry document the operator's build did not produce is a misconfiguration,
// reported as the sanitized 500 every other one is, not a panic.
func TestDashboard_MissingEntryDocumentIsASanitizedInternalError(t *testing.T) {
	h := NewServer(nil, Config{MaxRequestBytes: 1024}, discardLogger()).
		WithDashboard(fstest.MapFS{"assets/a.js": {Data: []byte("x")}}).
		Handler()
	rec := serve(h, http.MethodGet, "/dashboard/")
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, CodeInternal, decodeError(t, rec).Error.Code)
}

func requireDashboardSecurityHeaders(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	csp := rec.Header().Get("Content-Security-Policy")
	// Every directive the XSS argument in ADR-0017 leans on. Scripts, styles,
	// and fetches are same-origin only, and nothing may frame the page.
	for _, directive := range []string{
		"default-src 'none'",
		"script-src 'self'",
		"style-src 'self'",
		"connect-src 'self'",
		"base-uri 'none'",
		"form-action 'none'",
		"frame-ancestors 'none'",
	} {
		require.True(t, strings.Contains(csp, directive), "CSP %q lacks %q", csp, directive)
	}
	require.NotContains(t, csp, "unsafe-inline")
	require.NotContains(t, csp, "unsafe-eval")
	require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	require.Equal(t, "no-referrer", rec.Header().Get("Referrer-Policy"))
	require.Equal(t, "DENY", rec.Header().Get("X-Frame-Options"))
}
