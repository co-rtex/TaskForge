package api

import (
	"bytes"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"time"
)

// DashboardPath is where the operator dashboard is mounted.
//
// A prefix rather than "/", deliberately. "/" is already the catch-all that
// answers every unrouted path with the structured JSON 404, and a single-page
// app mounted there would need a fallback to its entry document for every path
// that is not a file -- which would swallow a mistyped API path like
// /v1/nonexistent into an HTML page unless the fallback carried a list of
// API-shaped exclusions, and that list would drift from the route table. Under
// a prefix, the question never arises: everything outside it is exactly what
// it was before M6D. See docs/adr/0017-dashboard-toolchain-and-serving.md.
const DashboardPath = "/dashboard/"

// dashboardAssetsDir is Vite's build.assetsDir. Everything under it is
// content-hashed, so it is safe to cache forever, and a miss there is a real
// 404 rather than a client-side route.
const dashboardAssetsDir = "assets/"

const dashboardIndex = "index.html"

// dashboardCSP confines the dashboard to its own origin. It is the concrete
// mitigation the sessionStorage credential in ADR-0017 relies on: no script,
// style, or fetch may come from anywhere else, no inline script can run, and
// nothing may frame the page.
const dashboardCSP = "default-src 'none'; script-src 'self'; style-src 'self'; " +
	"img-src 'self' data:; font-src 'self'; connect-src 'self'; manifest-src 'self'; " +
	"base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

// WithDashboard serves the operator dashboard's static assets under
// DashboardPath, and redirects the bare root to it.
//
// It is optional in exactly the way WithMetrics is: without it, neither
// pattern is registered, and "/", "/dashboard/", and everything under it are
// answered by the same structured 404 as any other unrouted path. With it,
// every path outside DashboardPath other than the bare root is still answered
// exactly as before -- including a mistyped API path, which stays a JSON 404.
//
// The dashboard is a static client of the public /v1 routes and nothing else.
// Serving it adds no endpoint, no credential, and no server-side session: the
// browser presents the operator's own API key to the same authenticated routes
// taskforge-cli uses, from the same origin, so there is no CORS policy to
// decide either.
func (s *Server) WithDashboard(assets fs.FS) *Server {
	s.dashboard = assets
	return s
}

// handleDashboardRoot sends the bare root to the dashboard. A 302 rather than
// a 301: whether "/" is the dashboard is this binary's configuration, and a
// browser must not remember it past a restart without one.
func (s *Server) handleDashboardRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, DashboardPath, http.StatusFound)
}

// handleDashboard serves one embedded file, or the entry document for a
// client-side route.
func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, DashboardPath)
	if name == "" || name == dashboardIndex {
		s.serveDashboardIndex(w, r)
		return
	}
	// ServeMux has already cleaned the path, so ".." cannot reach here; this
	// rejects whatever else fs.FS itself would refuse. Dotfiles -- the
	// committed .gitkeep rides along in the embedded tree -- are never served.
	if !fs.ValidPath(name) || hasHiddenSegment(name) {
		s.handleNotFound(w, r)
		return
	}
	if info, err := fs.Stat(s.dashboard, name); err == nil && info.Mode().IsRegular() {
		cache := "no-cache"
		if strings.HasPrefix(name, dashboardAssetsDir) {
			cache = "public, max-age=31536000, immutable"
		}
		s.serveDashboardFile(w, r, name, cache)
		return
	}
	// A missing asset is a real 404. Answering it with the entry document
	// would make the browser execute HTML as a script and report a syntax
	// error instead of the missing file.
	if strings.HasPrefix(name, dashboardAssetsDir) {
		s.handleNotFound(w, r)
		return
	}
	// Anything else is one of the dashboard's own routes, which only its
	// client-side router can render. This is what makes a reload or a deep
	// link on /dashboard/jobs/<id> work.
	s.serveDashboardIndex(w, r)
}

func (s *Server) serveDashboardIndex(w http.ResponseWriter, r *http.Request) {
	// The entry document names the current content-hashed assets, so a cached
	// copy could point at assets a rebuild has since removed.
	s.serveDashboardFile(w, r, dashboardIndex, "no-cache")
}

func (s *Server) serveDashboardFile(w http.ResponseWriter, r *http.Request, name, cacheControl string) {
	file, err := s.dashboard.Open(name)
	if err != nil {
		s.internalError(w, r, "open dashboard asset", err)
		return
	}
	defer file.Close()
	content, err := readSeeker(file)
	if err != nil {
		s.internalError(w, r, "read dashboard asset", err)
		return
	}
	header := w.Header()
	header.Set("Cache-Control", cacheControl)
	header.Set("Content-Security-Policy", dashboardCSP)
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Frame-Options", "DENY")
	// A zero modtime: embedded files have none, so ServeContent sends no
	// Last-Modified and answers no If-Modified-Since. It still sets the type
	// from the extension and handles HEAD and Range.
	http.ServeContent(w, r, name, time.Time{}, content)
}

// readSeeker adapts an opened file for http.ServeContent. embed.FS files
// already seek; the fallback keeps any other fs.FS usable without widening
// WithDashboard's parameter.
func readSeeker(file fs.File) (io.ReadSeeker, error) {
	if seeker, ok := file.(io.ReadSeeker); ok {
		return seeker, nil
	}
	body, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(body), nil
}

func hasHiddenSegment(name string) bool {
	for _, segment := range strings.Split(name, "/") {
		if strings.HasPrefix(segment, ".") {
			return true
		}
	}
	return false
}
