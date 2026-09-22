package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/metrics"
)

// meteredTestServer builds a server with its own metrics registry.
func meteredTestServer(t *testing.T) (http.Handler, *metrics.Metrics) {
	t.Helper()
	m := metrics.New("taskforge-api")
	handler := NewServer(nil, Config{MaxRequestBytes: 1024},
		slog.New(slog.NewJSONHandler(io.Discard, nil))).
		WithAuth(acceptingKeys(testScope)).
		WithResults(acceptingResults(), nil).
		WithMetrics(m).
		Handler()
	return handler, m
}

// scrape returns the parsed exposition output of GET /metrics.
func scrape(t *testing.T, handler http.Handler) map[string]*expfmtFamily {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	// NewTextParser, not a zero TextParser: the zero value leaves its
	// validation scheme unset and panics on the first metric name.
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(rec.Body)
	require.NoError(t, err, "/metrics must emit parseable exposition format")

	out := map[string]*expfmtFamily{}
	for name, family := range families {
		f := &expfmtFamily{}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			f.series = append(f.series, labels)
		}
		out[name] = f
	}
	return out
}

type expfmtFamily struct{ series []map[string]string }

// The HTTP metric's route label is the matched ROUTE PATTERN, identical to the
// value the span name already uses.
//
// This is the assertion that keeps M6C from inventing a second categorization
// scheme. An implementation that re-derived the route in its own middleware
// would read r.Pattern before the mux populated it and silently label every
// request the same way -- the exact failure M6B already found once.
func TestMetrics_HTTPRouteLabelIsThePatternNotTheRawPath(t *testing.T) {
	handler, _ := meteredTestServer(t)

	const jobID = "11111111-1111-4111-8111-111111111111"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, authorize(httptest.NewRequest(http.MethodGet, "/v1/jobs/"+jobID, nil)))

	families := scrape(t, handler)
	requests := families["taskforge_http_requests_total"]
	require.NotNil(t, requests, "the request counter must be exposed")

	var found bool
	for _, series := range requests.series {
		if series["route"] == "GET /v1/jobs/{job_id}" {
			found = true
			require.Equal(t, "GET", series["method"])
			require.Equal(t, "taskforge-api", series["service"])
		}
		require.NotContainsf(t, series["route"], jobID,
			"a job id reached the route label: %v", series)
	}
	require.True(t, found, "a request to a matched route must be labelled with its pattern")
}

// Every unmatched path shares one bounded label value. A scan of random URLs
// must not mint a time series per URL.
func TestMetrics_UnmatchedPathsShareOneBoundedRouteLabel(t *testing.T) {
	handler, _ := meteredTestServer(t)

	for _, path := range []string{"/nope", "/also/nope", "/" + strings.Repeat("x", 200)} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	}

	for _, series := range scrape(t, handler)["taskforge_http_requests_total"].series {
		require.NotContains(t, series["route"], "nope")
		require.NotContains(t, series["route"], "xxx")
	}
}

// The status code reaches the counter, which requires the recorder to be
// wrapped before the handler runs rather than read afterwards.
func TestMetrics_HTTPCounterRecordsTheStatusCode(t *testing.T) {
	handler, _ := meteredTestServer(t)

	// Unauthenticated: a 401 the handler never sees.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/jobs", nil))
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	var codes []string
	for _, series := range scrape(t, handler)["taskforge_http_requests_total"].series {
		codes = append(codes, series["code"])
	}
	require.Contains(t, codes, "401",
		"a refused request must still be counted, with its real status")
}

// A panicking handler is still counted, with the sanitized 500 it produced.
//
// Same reasoning as the span rename: the recording happens in a defer, so an
// unwinding panic does not skip it. Without that, the requests an operator most
// needs to see would be the ones missing from the metric.
func TestMetrics_PanickingHandlerIsStillCounted(t *testing.T) {
	handler, _ := meteredTestServer(t)

	// The job store is nil, so this route panics and withRecovery converts it.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, authorize(httptest.NewRequest(http.MethodGet,
		"/v1/jobs/11111111-1111-4111-8111-111111111111", nil)))
	require.Equal(t, http.StatusInternalServerError, rec.Code)

	var found bool
	for _, series := range scrape(t, handler)["taskforge_http_requests_total"].series {
		if series["route"] == "GET /v1/jobs/{job_id}" && series["code"] == "500" {
			found = true
		}
	}
	require.True(t, found,
		"a panicking handler must still be counted, with its route and its 500")
}

// /metrics itself is unauthenticated, exactly like the health probes it sits
// beside on this already-loopback-bound listener.
func TestMetrics_EndpointIsUnauthenticated(t *testing.T) {
	handler, _ := meteredTestServer(t)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code,
		"/metrics must not require a credential, like /healthz and /readyz")
}

// No label on any exposed series may carry an identifier or a tenancy value.
// This is the same rule internal/metrics enforces on declared labels, checked
// here against what is actually on the wire.
func TestMetrics_NoExposedSeriesCarriesAForbiddenLabel(t *testing.T) {
	handler, _ := meteredTestServer(t)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, authorize(httptest.NewRequest(http.MethodGet,
		"/v1/jobs/11111111-1111-4111-8111-111111111111?secret=shh", nil)))

	for name, family := range scrape(t, handler) {
		for _, series := range family.series {
			for label, value := range series {
				for _, forbidden := range metrics.ForbiddenLabels {
					require.NotEqualf(t, forbidden, label,
						"%s exposed forbidden label %q", name, label)
				}
				require.NotContainsf(t, value, "shh",
					"%s label %q leaked a query parameter", name, label)
				require.NotContainsf(t, value, testRawKey,
					"%s label %q leaked the credential", name, label)
			}
		}
	}
}

// A server built without metrics has no /metrics route and behaves exactly as
// it did before M6C -- which is what every pre-existing test relies on.
func TestMetrics_AbsentWithoutInstrumentation(t *testing.T) {
	handler := newTestServer(t) // no WithMetrics

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)

	// And an ordinary request is unaffected.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, authorize(httptest.NewRequest(http.MethodGet, "/v1/jobs?limit=0", nil)))
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
}
