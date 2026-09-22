//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/co-rtex/TaskForge/internal/outbox"
	"github.com/co-rtex/TaskForge/internal/telemetry"
	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
)

// A caller-supplied root. Fixing it up front means the expected trace id is
// known before anything runs, so every later assertion compares against a
// constant rather than against whatever the first span happened to produce.
const (
	callerTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	callerSpanID  = "00f067aa0ba902b7"
	callerParent  = "00-" + callerTraceID + "-" + callerSpanID + "-01"
)

// recordSpans installs a real tracer provider recording into an in-memory
// exporter, and restores the previous global afterwards.
//
// In-memory rather than a collector on purpose: the acceptance criterion is
// that one trace id reaches every hop, and that is a property of this system,
// not of a trace backend. Requiring a running collector would make the suite
// depend on infrastructure that proves nothing extra.
//
// Every component in these tests -- API, jobs store, worker runner -- takes its
// tracer from the global delegating provider, exactly as the five binaries do,
// so one provider here records all of them.
func recordSpans(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		require.NoError(t, provider.Shutdown(context.Background()))
	})
	return exporter
}

// submitTraced submits a job carrying an explicit W3C traceparent.
func submitTraced(t *testing.T, base, key, body, traceparent string) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, base+"/v1/jobs", strings.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", key)
	if traceparent != "" {
		request.Header.Set(telemetry.TraceparentHeader, traceparent)
	}
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusCreated, response.StatusCode)
}

// spanNames returns every recorded span's name, for failure messages that say
// what WAS recorded rather than only what was missing.
func spanNames(exporter *tracetest.InMemoryExporter) []string {
	spans := exporter.GetSpans()
	names := make([]string, 0, len(spans))
	for _, span := range spans {
		names = append(names, span.Name)
	}
	return names
}

// requireSpan finds exactly one span by name and returns it.
func requireSpan(t *testing.T, exporter *tracetest.InMemoryExporter, name string) tracetest.SpanStub {
	t.Helper()
	var found []tracetest.SpanStub
	for _, span := range exporter.GetSpans() {
		if span.Name == name {
			found = append(found, span)
		}
	}
	require.Lenf(t, found, 1, "expected exactly one %q span; recorded: %v", name, spanNames(exporter))
	return found[0]
}

// readOutboxTrace reads the persisted trace columns for a job's event.
func readOutboxTrace(t *testing.T, jobID string) (traceparent, tracestate *string) {
	t.Helper()
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT traceparent, tracestate FROM outbox_events WHERE job_id = $1`, jobID).
		Scan(&traceparent, &tracestate))
	return traceparent, tracestate
}

// TestTracing_OneTraceIDReachesEveryHop is M6B's acceptance criterion.
//
// It asserts one trace id on all five hops the roadmap names -- the API
// request, the submission transaction, the persisted outbox row, the published
// broker envelope, and the worker's claim and execution -- with the database
// and the broker read directly rather than inferred from in-process context.
// Those two reads are what make this a statement about the system rather than
// about Go's context propagation.
func TestTracing_OneTraceIDReachesEveryHop(t *testing.T) {
	exporter := recordSpans(t)

	executed := make(chan struct{}, 1)
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.HandlerFunc(
		func(ctx context.Context, execution workerruntime.Execution) (json.RawMessage, error) {
			select {
			case executed <- struct{}{}:
			default:
			}
			return json.RawMessage(`{"ok":true}`), nil
		})))

	stack := startE2EStack(t, registry, time.Minute)
	stack.startWorker(t, "trace-worker", 2)

	submitTraced(t, stack.baseURL, "trace-e2e",
		`{"queue":"default","job_type":"demo.echo","payload":{"m":1}}`, callerParent)

	select {
	case <-executed:
	case <-time.After(30 * time.Second):
		t.Fatalf("handler never ran; spans recorded: %v", spanNames(exporter))
	}

	// Hop 1 -- the API request span continues the caller's trace.
	apiSpan := requireSpan(t, exporter, "POST /v1/jobs")
	require.Equal(t, callerTraceID, apiSpan.SpanContext.TraceID().String())
	require.Equal(t, callerSpanID, apiSpan.Parent.SpanID().String(),
		"the caller's span id must become the API span's parent")

	// Hop 2 -- the submission transaction, a child of the request.
	submitSpan := requireSpan(t, exporter, "jobs.Submit")
	require.Equal(t, callerTraceID, submitSpan.SpanContext.TraceID().String())
	require.Equal(t, apiSpan.SpanContext.SpanID(), submitSpan.Parent.SpanID(),
		"the transaction span must be a child of the request span")

	// Hop 3 -- the trace committed to PostgreSQL with the event. Read from the
	// database, not from memory: this is the hop that survives the API process
	// exiting, and it is the whole reason migration 0018 exists.
	var jobID string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT id::text FROM jobs WHERE queue = 'default' ORDER BY created_at DESC LIMIT 1`).Scan(&jobID))
	traceparent, _ := readOutboxTrace(t, jobID)
	require.NotNil(t, traceparent, "the outbox row must carry the submitting transaction's trace")
	require.Contains(t, *traceparent, callerTraceID,
		"the persisted traceparent must name the caller's trace")
	require.Regexp(t, `^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`, *traceparent)

	// Hop 4 -- the worker's claim, continuing the trace it read off the
	// envelope. Its parent is the SUBMISSION's span, not the API span, because
	// that is the span whose context was persisted.
	claimSpan := requireSpan(t, exporter, "worker.Claim")
	require.Equal(t, callerTraceID, claimSpan.SpanContext.TraceID().String(),
		"the claim must continue the submitting trace, not start its own")

	// Hop 5 -- handler execution.
	executionSpan := requireSpan(t, exporter, "worker.Execute")
	require.Equal(t, callerTraceID, executionSpan.SpanContext.TraceID().String())

	// The four spans above really are one trace, not four that happen to agree.
	for _, span := range []tracetest.SpanStub{apiSpan, submitSpan, claimSpan, executionSpan} {
		require.Equalf(t, callerTraceID, span.SpanContext.TraceID().String(),
			"span %q escaped the submission trace", span.Name)
	}

	// And the boundary in the other direction, which matters just as much: a
	// worker's OWN lifecycle is not part of any client's submission. Its
	// registration is an independent operation that happened to run in the same
	// process, and folding it into this trace would make a trace mean "things
	// that happened around then" rather than "this submission".
	registration := requireSpan(t, exporter, "PUT /internal/v1/worker-sessions/{worker_session_id}")
	require.NotEqual(t, callerTraceID, registration.SpanContext.TraceID().String(),
		"worker registration continues no client request and must not join its trace")
}

// TestTracing_PublishedEnvelopeCarriesTheTrace reads the trace off the broker
// message itself.
//
// Separate from the end-to-end test because a running worker consumes the
// message: this one runs no worker, so the envelope can be received and
// inspected as a consumer in another process would see it. This is the hop
// ARCHITECTURE.md section 3 claims exists, and which did not until M6B.
func TestTracing_PublishedEnvelopeCarriesTheTrace(t *testing.T) {
	recordSpans(t)
	reset(t)
	srv := newAPI(t)
	broker := newBroker(t, "")

	submitTraced(t, srv.URL, "trace-envelope",
		`{"queue":"default","job_type":"demo.echo","payload":{"m":1}}`, callerParent)

	publisher := newPublisher(t, broker)
	_, err := publisher.RunOnce(context.Background())
	require.NoError(t, err)

	bodies := receiveAll(t, broker, 500*time.Millisecond)
	require.Len(t, bodies, 1, "exactly one notification was published")

	var envelope outbox.Envelope
	require.NoError(t, json.Unmarshal(bodies[0], &envelope))
	require.NotNil(t, envelope.Trace, "the published envelope must carry trace context")
	require.Contains(t, envelope.Trace.Traceparent, callerTraceID)

	// The wire shape is exactly what a consumer in another process parses.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(bodies[0], &raw))
	trace, ok := raw["trace"].(map[string]any)
	require.True(t, ok, "trace must be an object on the wire")
	require.Contains(t, trace["traceparent"], callerTraceID)

	// The envelope still carries no authoritative job state -- adding trace
	// metadata must not have widened what the broker learns.
	require.NotContains(t, string(bodies[0]), "payload")
	require.NotContains(t, string(bodies[0]), "scope")
}

// TestTracing_MalformedInboundTraceparentIsNeverPersisted closes the loop
// between the propagator and the database CHECK.
//
// The propagator drops an untrusted value before anything reaches PostgreSQL,
// so the constraint never fires in this path. Both halves are asserted: the row
// is written with a fresh root rather than the junk, and the constraint would
// in fact refuse the junk if anything ever tried to write it directly.
func TestTracing_MalformedInboundTraceparentIsNeverPersisted(t *testing.T) {
	recordSpans(t)
	reset(t)
	srv := newAPI(t)

	submitTraced(t, srv.URL, "trace-malformed",
		`{"queue":"default","job_type":"demo.echo","payload":{"m":1}}`,
		"00-not-a-real-trace-id-01")

	var jobID string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT id::text FROM jobs ORDER BY created_at DESC LIMIT 1`).Scan(&jobID))

	traceparent, _ := readOutboxTrace(t, jobID)
	require.NotNil(t, traceparent, "a fresh root is still a trace and is still persisted")
	require.NotContains(t, *traceparent, "not-a-real",
		"an untrusted traceparent must never reach the database")
	require.Regexp(t, `^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`, *traceparent)

	// The backstop itself: the constraint refuses a malformed value outright.
	_, err := testPool.Exec(context.Background(),
		`UPDATE outbox_events SET traceparent = $1 WHERE job_id = $2`,
		"00-not-a-real-trace-id-01", jobID)
	require.Error(t, err, "the CHECK must refuse a malformed traceparent")
	require.Contains(t, err.Error(), "outbox_events_traceparent_check")
}

// TestTracing_AnUntracedEventStillPublishesAndExecutesUnderAFreshRoot covers
// the NULL path: an outbox row with no trace context must publish, be
// consumed, and execute, with the worker starting a fresh root rather than
// failing or inventing a parent.
//
// NOTE for whoever writes the next tracing test. This does NOT work by
// disabling tracing partway through the binary, and an earlier draft that
// tried to was order-dependent: otel.SetTracerProvider binds package-level
// delegating tracers through a sync.Once (otel/internal/global/state.go), so
// once any test installs a recording provider, every package-level tracer in
// the process stays bound to it. Swapping the global back to noop afterwards
// does not un-bind them. Production calls SetTracerProvider exactly once at
// startup, so this is purely a test-isolation property -- but a test that
// ignores it passes alone and fails in the suite.
//
// So the untraced event here is produced the way production produces one: by a
// server-initiated notification, which deliberately persists no trace context.
func TestTracing_AnUntracedEventStillPublishesAndExecutesUnderAFreshRoot(t *testing.T) {
	exporter := recordSpans(t)

	executed := make(chan struct{}, 1)
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.HandlerFunc(
		func(ctx context.Context, execution workerruntime.Execution) (json.RawMessage, error) {
			select {
			case executed <- struct{}{}:
			default:
			}
			return json.RawMessage(`{"ok":true}`), nil
		})))

	// A short re-notify window so the scheduler promotes promptly.
	stack := startE2EStack(t, registry, time.Minute)
	stack.startWorker(t, "untraced-worker", 2)

	// Delayed submission writes NO outbox event; the scheduler writes one when
	// it promotes, and that write is server-initiated and carries no trace.
	scheduled := time.Now().Add(300 * time.Millisecond).UTC().Format(time.RFC3339Nano)
	submitTraced(t, stack.baseURL, "trace-untraced", fmt.Sprintf(
		`{"queue":"default","job_type":"demo.echo","payload":{"m":1},"scheduled_at":%q}`, scheduled),
		callerParent)

	select {
	case <-executed:
	case <-time.After(30 * time.Second):
		t.Fatalf("handler never ran; spans recorded: %v", spanNames(exporter))
	}

	var jobID string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT id::text FROM jobs ORDER BY created_at DESC LIMIT 1`).Scan(&jobID))

	traceparent, tracestate := readOutboxTrace(t, jobID)
	require.Nil(t, traceparent, "a server-initiated promotion persists no trace context")
	require.Nil(t, tracestate)

	// The worker still claimed and executed it, under a trace of its own rather
	// than the submitting client's -- NULL means "new root", not "error".
	claimSpan := requireSpan(t, exporter, "worker.Claim")
	executionSpan := requireSpan(t, exporter, "worker.Execute")
	require.True(t, claimSpan.SpanContext.TraceID().IsValid())
	require.NotEqual(t, callerTraceID, claimSpan.SpanContext.TraceID().String(),
		"an untraced event must not be adopted into the submitting client's trace")
	require.Equal(t,
		claimSpan.SpanContext.TraceID().String(),
		executionSpan.SpanContext.TraceID().String(),
		"claim and execution still share one trace with each other")
}

// TestTracing_ServerInitiatedNotificationsCarryNoTrace pins the deliberate
// deferral rather than leaving it to be inferred from four nil arguments.
//
// Replay, scheduler promotion, and abandonment requeue are server-initiated:
// none continues a client's submission, so none persists a trace context.
// Instrumenting them is a separate decision, and this asserts the boundary M6B
// actually shipped.
func TestTracing_ServerInitiatedNotificationsCarryNoTrace(t *testing.T) {
	recordSpans(t)
	reset(t)
	srv := newAPI(t)

	// A delayed job gets no event at submission; the scheduler writes one when
	// it promotes the job, and that write is server-initiated.
	scheduled := time.Now().Add(500 * time.Millisecond).UTC().Format(time.RFC3339Nano)
	submitTraced(t, srv.URL, "trace-promotion", fmt.Sprintf(
		`{"queue":"default","job_type":"demo.echo","payload":{"m":1},"scheduled_at":%q}`, scheduled),
		callerParent)

	var jobID string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT id::text FROM jobs ORDER BY created_at DESC LIMIT 1`).Scan(&jobID))
	require.Equal(t, 0, countRows(t, "outbox_events"), "a delayed job is notified by the scheduler, not by submission")

	engine := newScheduler(t, time.Minute)
	eventually(t, 10*time.Second, "the scheduler promotes the delayed job", func() bool {
		_, err := engine.RunOnce(context.Background())
		require.NoError(t, err)
		return countRows(t, "outbox_events") == 1
	})

	traceparent, tracestate := readOutboxTrace(t, jobID)
	require.Nil(t, traceparent,
		"a scheduler promotion is server-initiated and continues no client request")
	require.Nil(t, tracestate)
}
