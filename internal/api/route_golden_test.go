package api

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/co-rtex/TaskForge/internal/metrics"
)

// The route-behavior golden pins what Handler() does with a request, for every
// path the server knows about, in two feature configurations. It exists so that
// changing HOW routes are registered (the route table, M8D1) cannot change WHAT
// the server answers without a diff in a committed file.
//
// What a row records, per request: the status code, the Allow header, the code
// of the structured error body (if the body is one), and the route pattern the
// mux matched. Nothing that varies between runs is recorded: no request id, no
// timestamp, no body.
//
// To regenerate it after a deliberate change to routing behavior:
//
//	go test ./internal/api -run TestRouteBehavior_MatchesTheGolden -update-route-golden
//
// and review the diff of testdata/route_behavior.golden: every changed row is a
// behavior change.
var updateRouteGolden = flag.Bool("update-route-golden", false,
	"rewrite internal/api/testdata/route_behavior.golden from the current server instead of comparing")

const routeGoldenPath = "testdata/route_behavior.golden"

// goldenID fills every path parameter, so a request that reaches a handler body
// passes path validation and runs real handler code against a fake.
const goldenID = "11111111-1111-4111-8111-111111111111"

var goldenMethods = []string{
	http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch,
}

// goldenUnlisted are the routes api/openapi.yaml does not document but Handler()
// registers when the feature behind them is on. They are written out here, not
// derived from anything in the server, because this test must be able to run
// against a server that has no table.
var goldenUnlisted = []string{"/metrics", "/dashboard/", "/"}

// goldenUnknown are paths nothing registers. They reach the "/" catch-all.
var goldenUnknown = []string{"/v1/nope", "/internal/v1/nope", "/nope"}

// goldenRegistrationPath is the one route that carries a worker key. Its
// credentialed variant presents one; every other route gets an API key.
const goldenRegistrationPath = "/internal/v1/worker-sessions/" + goldenID

var pathParam = regexp.MustCompile(`\{[^}]+\}`)

// goldenVariant is one way of presenting a request.
//
// "plain" and "credentialed" carry the loopback Host a real client sends;
// "origin" adds a browser's Origin to it; "foreign-host" is the same request
// addressed to a name that is not loopback. "credentialed" is not in the
// owner's list of three: without it nothing here can tell a public route
// wrapped in requireAPIKey from one whose handler merely refuses an
// unauthenticated caller itself, because both answer the same 401 to a request
// with no credential (see TestAuth_EveryPublicRouteConsultsTheCredentialStore).
type goldenVariant struct {
	name  string
	apply func(req *http.Request)
}

var goldenVariants = []goldenVariant{
	{"plain", func(req *http.Request) { req.Host = testInternalHost }},
	{"origin", func(req *http.Request) {
		req.Host = testInternalHost
		req.Header.Set("Origin", "https://evil.example")
	}},
	{"foreign-host", func(req *http.Request) { req.Host = "taskforge.example" }},
	{"credentialed", func(req *http.Request) {
		req.Host = testInternalHost
		key := testRawKey
		if req.URL.Path == goldenRegistrationPath {
			key = testRawWorkerKey
		}
		req.Header.Set(authorizationHeader, "Bearer "+key)
	}},
}

// goldenRow is one recorded request and its observed behavior.
type goldenRow struct {
	config, variant, method, path string
	status                        int
	allow, code, pattern          string
}

func (r goldenRow) String() string {
	return fmt.Sprintf("%s %s %s %s -> %d allow=%q code=%q pattern=%q",
		r.config, r.variant, r.method, r.path, r.status, r.allow, r.code, r.pattern)
}

// goldenPaths is every path of the matrix: each spec path with its parameters
// filled, each unlisted path, and each unknown one. The spec paths are read from
// the document so that a path added to it enters the matrix without anyone
// remembering to list it.
func goldenPaths(t *testing.T) []string {
	t.Helper()
	seen := map[string]bool{}
	for path := range loadOpenAPI(t).Paths {
		seen[pathParam.ReplaceAllString(path, goldenID)] = true
	}
	for _, path := range append(append([]string{}, goldenUnlisted...), goldenUnknown...) {
		seen[path] = true
	}
	paths := make([]string, 0, len(seen))
	for path := range seen {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// goldenServer builds the server for one configuration. "all-on" wires every
// feature group, each to a fake that records and answers deterministically;
// "all-off" wires none of them. Results are wired in both because the route is
// registered unconditionally and is not a feature group.
func goldenServer(config string, tracer *sdktrace.TracerProvider) (http.Handler, *metrics.Metrics) {
	server := NewServer(nil, Config{MaxRequestBytes: 4096}, discardLogger()).
		WithTracer(tracer.Tracer("route-golden")).
		WithResults(acceptingResults(), nil)
	if config != "all-on" {
		return server.Handler(), nil
	}
	probe := &guardProbe{}
	m := metrics.New("taskforge-api")
	server.WithAuth(probeKeys{probe}).
		WithWorkerAuth(probeWorkerKeys{probe}).
		WithWorkerControl(probeControl{probe}).
		WithMetrics(m).
		WithDashboard(testDashboardAssets())
	return server.Handler(), m
}

// observe sends one request and reports what the golden records.
//
// The matched pattern is captured through the channel the server itself
// publishes it on. withSpanRoute writes r.Pattern into the routeHolder that
// withHTTPMetrics put on the context and into the span it names, in one place
// (see middleware.go). Here:
//
//   - the span's http.route attribute is read from an in-memory exporter, in
//     both configurations;
//   - with metrics off, withHTTPMetrics is absent and a routeHolder this test
//     puts on the context under routeCtxKey is the one withSpanRoute writes, so
//     it is read directly and must equal the span's value on every request;
//   - with metrics on, withHTTPMetrics replaces any holder an outer layer
//     installed, so the same value is checked in aggregate instead: the
//     route labels of taskforge_http_requests_total must equal the rows (see
//     requireMetricsAgreeWithRows).
//
// The recorded value is the span's. Three independent readings of one fact that
// must agree is what makes it "the value withSpanRoute publishes".
func observe(t *testing.T, handler http.Handler, exporter *tracetest.InMemoryExporter,
	withHolder bool, req *http.Request) (status int, allow, code, pattern string) {
	t.Helper()
	exporter.Reset()
	holder := &routeHolder{}
	if withHolder {
		req = req.WithContext(context.WithValue(req.Context(), routeCtxKey{}, holder))
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	spans := exporter.GetSpans()
	require.Lenf(t, spans, 1, "%s %s must produce exactly one server span", req.Method, req.URL.Path)
	var route string
	for _, attr := range spans[0].Attributes {
		if string(attr.Key) == "http.route" {
			route = attr.Value.AsString()
		}
	}
	// The span is named for its pattern, prefixed with the verb when the pattern
	// has none, and keeps the bare method when nothing matched.
	wantName := req.Method
	if route != "" {
		wantName = route
		if !strings.Contains(wantName, " ") {
			wantName = req.Method + " " + wantName
		}
	}
	require.Equalf(t, wantName, spans[0].Name, "%s %s: span name and http.route disagree", req.Method, req.URL.Path)
	if withHolder {
		require.Equalf(t, route, holder.route,
			"%s %s: the routeHolder and the span's http.route are two readings of one value", req.Method, req.URL.Path)
	}

	var envelope ErrorBody
	if json.Unmarshal(rec.Body.Bytes(), &envelope) == nil {
		code = envelope.Error.Code
	}
	return rec.Code, rec.Header().Get("Allow"), code, route
}

// goldenRows drives the whole matrix against one configuration.
func goldenRows(t *testing.T, config string) []goldenRow {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	handler, m := goldenServer(config, provider)

	var rows []goldenRow
	for _, variant := range goldenVariants {
		for _, path := range goldenPaths(t) {
			for _, method := range goldenMethods {
				req := httptest.NewRequest(method, path, http.NoBody)
				variant.apply(req)
				status, allow, code, pattern := observe(t, handler, exporter, m == nil, req)
				rows = append(rows, goldenRow{config, variant.name, method, path, status, allow, code, pattern})
			}
		}
	}
	if m != nil {
		requireMetricsAgreeWithRows(t, m, rows)
	}
	return rows
}

// requireMetricsAgreeWithRows is the third reading of the matched pattern: the
// route label withHTTPMetrics took from the routeHolder, summed over the whole
// run, must be exactly the multiset of (method, pattern, status) the rows hold,
// with "unmatched" standing in for an empty pattern.
func requireMetricsAgreeWithRows(t *testing.T, m *metrics.Metrics, rows []goldenRow) {
	t.Helper()
	want := map[string]float64{}
	for _, row := range rows {
		route := row.pattern
		if route == "" {
			route = "unmatched"
		}
		want[row.method+"|"+route+"|"+strconv.Itoa(row.status)]++
	}

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	// A variable, not a call chain: TextToMetricFamilies has a pointer receiver.
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(rec.Body)
	require.NoError(t, err)
	family := families["taskforge_http_requests_total"]
	require.NotNil(t, family, "the request counter must be exposed")
	got := map[string]float64{}
	for _, metric := range family.GetMetric() {
		labels := map[string]string{}
		for _, label := range metric.GetLabel() {
			labels[label.GetName()] = label.GetValue()
		}
		got[labels["method"]+"|"+labels["route"]+"|"+labels["code"]] += metric.GetCounter().GetValue()
	}
	require.Equal(t, want, got,
		"the metrics route label and the span's http.route must be the same value for every request")
}

// renderGolden is the committed file's exact content.
func renderGolden(rows []goldenRow) string {
	var out strings.Builder
	out.WriteString("# Route-behavior golden for internal/api Handler(). Generated, not hand-edited:\n")
	out.WriteString("#   go test ./internal/api -run TestRouteBehavior_MatchesTheGolden -update-route-golden\n")
	out.WriteString("# One row per request: config variant method path -> status allow code pattern\n")
	out.WriteString("# pattern is the mux's matched route pattern; \"\" means nothing matched.\n")
	out.WriteString("# See route_golden_test.go for the matrix and what each variant means.\n")
	for _, row := range rows {
		out.WriteString(row.String())
		out.WriteString("\n")
	}
	return out.String()
}

// TestRouteBehavior_MatchesTheGolden proves Handler()'s observable routing
// behavior is exactly what the committed golden file records, for every path in
// the matrix, five methods, four request variants and two feature
// configurations.
//
// A route table that registered one pattern differently, dropped a wrapper,
// moved the browser-origin guard off the outside of an /internal route, or
// derived a 405 fallback with a different Allow header would change a row and
// fail here, naming it.
func TestRouteBehavior_MatchesTheGolden(t *testing.T) {
	var rows []goldenRow
	for _, config := range []string{"all-on", "all-off"} {
		rows = append(rows, goldenRows(t, config)...)
	}
	got := renderGolden(rows)

	if *updateRouteGolden {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(routeGoldenPath, []byte(got), 0o644))
		t.Logf("rewrote %s with %d rows", routeGoldenPath, len(rows))
		return
	}

	raw, err := os.ReadFile(routeGoldenPath)
	require.NoError(t, err, "the golden file is missing; generate it with -update-route-golden")
	require.Empty(t, diffGolden(string(raw), got),
		"routing behavior changed; if that is deliberate, regenerate the golden and review every changed row")
}

// diffGolden lists, by row key, every row that differs between two renderings,
// capped so a wholesale change stays readable.
func diffGolden(want, got string) string {
	if want == got {
		return ""
	}
	index := func(text string) map[string]string {
		rows := map[string]string{}
		for _, line := range strings.Split(text, "\n") {
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			key, _, _ := strings.Cut(line, " ->")
			rows[key] = line
		}
		return rows
	}
	wantRows, gotRows := index(want), index(got)
	var diffs []string
	for key, line := range wantRows {
		switch other, ok := gotRows[key]; {
		case !ok:
			diffs = append(diffs, "- missing now: "+line)
		case other != line:
			diffs = append(diffs, "- golden:      "+line+"\n+ now:         "+other)
		}
	}
	for key, line := range gotRows {
		if _, ok := wantRows[key]; !ok {
			diffs = append(diffs, "+ new:         "+line)
		}
	}
	sort.Strings(diffs)
	const shown = 40
	total := len(diffs)
	if total > shown {
		diffs = append(diffs[:shown], fmt.Sprintf("... and %d more", total-shown))
	}
	if total == 0 {
		return "the files differ in their comment lines or row order only"
	}
	return fmt.Sprintf("%d rows differ:\n%s", total, strings.Join(diffs, "\n"))
}

// goldenLine parses one row back out of the committed file.
var goldenLine = regexp.MustCompile(
	`^(\S+) (\S+) (\S+) (\S+) -> (\d+) allow=("[^"]*") code=("[^"]*") pattern=("[^"]*")$`)

func parseGolden(t *testing.T) []goldenRow {
	t.Helper()
	raw, err := os.ReadFile(routeGoldenPath)
	require.NoError(t, err)
	var rows []goldenRow
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		m := goldenLine.FindStringSubmatch(line)
		require.NotNilf(t, m, "unparseable golden row: %s", line)
		status, err := strconv.Atoi(m[5])
		require.NoError(t, err)
		unquote := func(s string) string {
			v, err := strconv.Unquote(s)
			require.NoError(t, err)
			return v
		}
		rows = append(rows, goldenRow{m[1], m[2], m[3], m[4], status, unquote(m[6]), unquote(m[7]), unquote(m[8])})
	}
	return rows
}

// TestRouteBehavior_GoldenIsNotVacuous proves the golden can actually tell a
// wrong server from a right one: it holds matched patterns, the four statuses
// that distinguish the wrappers, and a visible difference between the two
// feature configurations for every gated path.
//
// A golden whose rows were all 404 with an empty pattern would pass against any
// registration code at all.
func TestRouteBehavior_GoldenIsNotVacuous(t *testing.T) {
	rows := parseGolden(t)
	require.NotEmpty(t, rows)

	byKey := map[string]goldenRow{}
	var matched int
	statuses := map[string]map[int]int{} // config -> status -> count
	var refused, notFound, notAllowed, unauthorized int
	for _, row := range rows {
		byKey[row.config+" "+row.variant+" "+row.method+" "+row.path] = row
		if row.pattern != "" {
			matched++
		}
		if statuses[row.config] == nil {
			statuses[row.config] = map[int]int{}
		}
		statuses[row.config][row.status]++
		switch {
		case row.status == http.StatusUnauthorized && row.code == CodeUnauthorized:
			unauthorized++
		case row.status == http.StatusForbidden && row.code == CodeOriginRefused:
			refused++
		case row.status == http.StatusNotFound && row.code == CodeNotFound:
			notFound++
		case row.status == http.StatusMethodNotAllowed && row.code == CodeMethodNotAllowed && row.allow != "":
			notAllowed++
		}
	}
	require.Greater(t, matched, len(rows)/2, "most rows must name the pattern the mux matched")
	require.NotZero(t, unauthorized, "no 401 row")
	require.NotZero(t, refused, "no 403 origin_refused row")
	require.NotZero(t, notFound, "no 404 row")
	require.NotZero(t, notAllowed, "no 405 row with an Allow header")
	require.Len(t, statuses, 2, "both configurations must be present")

	// Every path a feature group gates must answer differently with the group on
	// and off, and with it off must be the catch-all's 404.
	for _, gated := range []struct{ method, path string }{
		{http.MethodGet, "/metrics"},
		{http.MethodGet, "/dashboard/"},
		{http.MethodGet, "/"},
		{http.MethodGet, "/internal/v1/api-keys"},
		{http.MethodPost, "/internal/v1/api-keys/" + goldenID + "/revoke"},
		{http.MethodGet, "/internal/v1/worker-keys"},
		{http.MethodPost, "/internal/v1/worker-keys/" + goldenID + "/revoke"},
		{http.MethodPut, "/internal/v1/worker-sessions/" + goldenID},
		{http.MethodPost, "/internal/v1/worker-sessions/" + goldenID + "/heartbeat"},
		{http.MethodPost, "/internal/v1/claims"},
		{http.MethodPost, "/internal/v1/leases/" + goldenID + "/renew"},
		{http.MethodPost, "/internal/v1/attempts/" + goldenID + "/start"},
	} {
		on := byKey["all-on plain "+gated.method+" "+gated.path]
		off := byKey["all-off plain "+gated.method+" "+gated.path]
		require.NotZerof(t, on.status, "%s %s is missing from the golden", gated.method, gated.path)
		require.NotEqualf(t, on, off, "%s %s must differ between the configurations", gated.method, gated.path)
		require.Equalf(t, http.StatusNotFound, off.status, "%s %s with its group off", gated.method, gated.path)
		require.Equalf(t, "/", off.pattern, "%s %s with its group off falls to the catch-all", gated.method, gated.path)
	}
}
