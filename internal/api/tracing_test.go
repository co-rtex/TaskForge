package api

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/co-rtex/TaskForge/internal/telemetry"
)

// tracedTestServer builds a server recording into its own in-memory exporter.
//
// It sets no global provider, so these tests neither depend on nor disturb
// global state -- which is why Server.WithTracer exists.
func tracedTestServer(t *testing.T) (http.Handler, *tracetest.InMemoryExporter) {
	t.Helper()
	handler, exporter, _ := tracedTestServerWithLogs(t)
	return handler, exporter
}

// tracedTestServerWithLogs additionally captures the server's structured log,
// for the one test that must PROVE which code path it exercised rather than
// assume it.
func tracedTestServerWithLogs(t *testing.T) (http.Handler, *tracetest.InMemoryExporter, *bytes.Buffer) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	var logs bytes.Buffer
	handler := NewServer(nil, Config{MaxRequestBytes: 1024},
		slog.New(slog.NewJSONHandler(&logs, nil))).
		WithAuth(acceptingKeys(testScope)).
		WithResults(acceptingResults(), nil).
		WithTracer(provider.Tracer("test")).
		Handler()
	return handler, exporter, &logs
}

// A span is named for the matched ROUTE PATTERN, never the raw path.
//
// This is the assertion the whole two-part middleware split exists to make
// true. net/http only populates Request.Pattern inside ServeMux.ServeHTTP, so
// an implementation that read it in the outer middleware would silently name
// every span by method alone -- which still "works" and is still bounded, and
// would therefore never be noticed without this test.
func TestTracing_SpanIsNamedForTheRoutePatternNotTheRawPath(t *testing.T) {
	handler, exporter := tracedTestServer(t)

	const jobID = "11111111-1111-4111-8111-111111111111"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, authorize(httptest.NewRequest(http.MethodGet, "/v1/jobs/"+jobID, nil)))

	spans := exporter.GetSpans()
	require.Len(t, spans, 1, "exactly one server span per request")
	require.Equal(t, "GET /v1/jobs/{job_id}", spans[0].Name,
		"the span must be named for the pattern; the raw path would put a uuid in every name")
	require.NotContains(t, spans[0].Name, jobID)
}

// A panicking handler must still leave a correctly named span.
//
// This is the regression guard for a real defect. withSpanRoute originally
// renamed the span on the line AFTER next.ServeHTTP. A handler that panics
// unwinds straight past that to withRecovery -- which sits outside it -- so the
// span kept its provisional method-only name for exactly the requests an
// operator most wants to find in a trace. The rename is now deferred.
//
// The defect was originally surfaced by TestTracing_SpanIsNamedForTheRoutePattern
// above, but only INCIDENTALLY: that test panics because tracedTestServer passes
// a nil job store, which is a property of the harness rather than anything the
// test states. Give the harness a real store and that coverage disappears with
// nothing failing. This test makes the dependency explicit by asserting the
// recovery actually happened, so the coverage cannot evaporate silently.
func TestTracing_SpanSurvivesAPanickingHandler(t *testing.T) {
	handler, exporter, logs := tracedTestServerWithLogs(t)

	const jobID = "11111111-1111-4111-8111-111111111111"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, authorize(httptest.NewRequest(http.MethodGet, "/v1/jobs/"+jobID, nil)))

	// First: prove this request really did panic. Without this, the test could
	// pass while exercising an ordinary non-panicking path and prove nothing.
	require.Contains(t, logs.String(), "panic recovered",
		"this test is meaningless unless the handler actually panicked; "+
			"if the test harness gained a real job store, panic a handler explicitly instead")

	// The panic is still converted to a sanitized 500, unchanged by tracing.
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	body := decodeError(t, rec)
	require.Equal(t, CodeInternal, body.Error.Code)
	require.Equal(t, "internal error", body.Error.Message)
	require.NotEmpty(t, body.Error.RequestID)

	// And the span -- the actual regression -- is named for the matched route,
	// not the provisional method-only name it would keep without the defer.
	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	require.Equal(t, "GET /v1/jobs/{job_id}", spans[0].Name,
		"a panicking handler must not leave the span with its provisional name")
	require.NotContains(t, spans[0].Name, jobID)

	// The route attribute is set on the same deferred path, so it must survive too.
	var route string
	for _, attr := range spans[0].Attributes {
		if string(attr.Key) == "http.route" {
			route = attr.Value.Emit()
		}
	}
	require.Equal(t, "GET /v1/jobs/{job_id}", route,
		"http.route is set alongside the rename and must survive a panic identically")
}

// An unrouted path must not become its own span name, or a scan of random
// URLs would mint an unbounded set of them.
func TestTracing_UnroutedRequestsShareOneBoundedSpanName(t *testing.T) {
	handler, exporter := tracedTestServer(t)

	for _, path := range []string{"/nope", "/also/nope", "/" + strings.Repeat("x", 200)} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	}

	spans := exporter.GetSpans()
	require.Len(t, spans, 3)
	for _, span := range spans {
		// "/" is the catch-all pattern handleNotFound is registered at, so this
		// IS the matched route and is bounded. What matters is that no part of
		// the requested path reaches the name.
		require.Equal(t, "GET /", span.Name,
			"every unmatched path must share one bounded span name")
	}
}

// Span attributes are bounded and carry nothing sensitive. The raw path, the
// query string, and every header are deliberately absent.
func TestTracing_SpanAttributesAreBoundedAndCarryNoRequestDetail(t *testing.T) {
	handler, exporter := tracedTestServer(t)

	rec := httptest.NewRecorder()
	request := authorize(httptest.NewRequest(http.MethodGet,
		"/v1/jobs/11111111-1111-4111-8111-111111111111?secret=shh", nil))
	handler.ServeHTTP(rec, request)

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)

	keys := map[string]string{}
	for _, attr := range spans[0].Attributes {
		keys[string(attr.Key)] = attr.Value.Emit()
	}
	require.Equal(t, "GET", keys["http.request.method"])
	require.Equal(t, "GET /v1/jobs/{job_id}", keys["http.route"])
	require.NotEmpty(t, keys["taskforge.request_id"], "a span must be joinable to a log line")
	require.Len(t, keys, 3, "no attribute beyond the bounded set")

	for key, value := range keys {
		require.NotContainsf(t, value, "shh", "attribute %s leaked a query parameter", key)
		require.NotContainsf(t, value, testRawKey, "attribute %s leaked the credential", key)
	}
}

// An inbound traceparent is adopted, so a client's trace continues through
// this API rather than restarting at it.
func TestTracing_ContinuesAnInboundTrace(t *testing.T) {
	handler, exporter := tracedTestServer(t)
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"

	rec := httptest.NewRecorder()
	request := authorize(httptest.NewRequest(http.MethodGet,
		"/v1/jobs/11111111-1111-4111-8111-111111111111", nil))
	request.Header.Set(telemetry.TraceparentHeader,
		"00-"+traceID+"-00f067aa0ba902b7-01")
	handler.ServeHTTP(rec, request)

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	require.Equal(t, traceID, spans[0].SpanContext.TraceID().String())
	require.Equal(t, "00f067aa0ba902b7", spans[0].Parent.SpanID().String(),
		"the inbound span id must become this span's parent")
}

// A traceparent this server cannot parse is dropped and a fresh root is used.
// It is never propagated onward and never persisted -- the same rule the
// database CHECK on outbox_events.traceparent backstops.
func TestTracing_DropsAMalformedInboundTraceparent(t *testing.T) {
	handler, exporter := tracedTestServer(t)

	rec := httptest.NewRecorder()
	request := authorize(httptest.NewRequest(http.MethodGet,
		"/v1/jobs/11111111-1111-4111-8111-111111111111", nil))
	request.Header.Set(telemetry.TraceparentHeader, "00-not-a-trace-id-01")
	handler.ServeHTTP(rec, request)

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	require.True(t, spans[0].SpanContext.TraceID().IsValid(), "a fresh root still has a valid id")
	require.False(t, spans[0].Parent.IsValid(),
		"an untrusted traceparent must not become a parent")
}

// Health probes are traced like everything else. They are the one surface
// where an operator most often asks "why is this slow", and excluding them
// would be a special case with nothing to recommend it.
func TestTracing_CoversUnauthenticatedRoutesToo(t *testing.T) {
	handler, exporter := tracedTestServer(t)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	require.Equal(t, "GET /healthz", spans[0].Name)
}

// A request refused before it reaches a handler is still one span, named for
// the route it was refused on -- otherwise a 401 storm would be invisible.
func TestTracing_RefusedRequestsAreStillTraced(t *testing.T) {
	handler, exporter := tracedTestServer(t)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/jobs", nil))
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	require.Equal(t, "GET /v1/jobs", spans[0].Name)
}

// With no tracer configured, NewServer takes the global delegating tracer,
// which is a no-op until a provider is installed. Nothing should panic and no
// behavior should change -- this is the default every existing test runs under.
func TestTracing_DisabledServerBehavesIdentically(t *testing.T) {
	handler := newTestServer(t) // no WithTracer; global provider is the OTel no-op

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, authorize(httptest.NewRequest(http.MethodGet, "/v1/jobs?limit=0", nil)))

	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	require.Equal(t, CodeValidationFailed, decodeError(t, rec).Error.Code)
}
