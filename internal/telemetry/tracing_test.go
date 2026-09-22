package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// recordingProvider installs a provider recording into an in-memory exporter
// and restores the previous global on cleanup, so one test's provider cannot
// leak into another's.
func recordingProvider(t *testing.T) (*tracetest.InMemoryExporter, trace.Tracer) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	return exporter, provider.Tracer(TracerName)
}

// The default really is inert. This is the load-bearing claim behind "a
// deployment that has not opted in pays nothing": not merely that spans are
// dropped, but that no provider is registered at all, so nothing samples and
// nothing is queued for export.
func TestStartTracing_DisabledRegistersNoProvider(t *testing.T) {
	before := otel.GetTracerProvider()

	tracing, err := StartTracing(context.Background(), TracingConfig{
		Exporter: ExporterNone, Service: "taskforge-test",
	}, io.Discard)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tracing.Shutdown(context.Background())) })

	require.Same(t, before, otel.GetTracerProvider(),
		"disabled tracing must leave the global provider untouched")

	// And a span started from the returned tracer records nothing.
	_, span := tracing.Tracer.Start(context.Background(), "ignored")
	require.False(t, span.SpanContext().IsSampled())
	require.False(t, span.IsRecording())
	span.End()
}

// An empty exporter string is the same as "none": a process whose environment
// simply does not mention tracing must start, not fail.
func TestStartTracing_EmptyExporterIsDisabled(t *testing.T) {
	tracing, err := StartTracing(context.Background(), TracingConfig{}, io.Discard)
	require.NoError(t, err)
	require.NotNil(t, tracing.Tracer)
	require.NoError(t, tracing.Shutdown(context.Background()))
}

// A typo is refused rather than silently downgraded. A misconfiguration that
// quietly disabled tracing could only be found by noticing an absence.
func TestStartTracing_RejectsAnUnrecognizedExporter(t *testing.T) {
	_, err := StartTracing(context.Background(), TracingConfig{
		Exporter: "jaeger", Service: "taskforge-test",
	}, io.Discard)
	require.Error(t, err)
	require.Contains(t, err.Error(), "TASKFORGE_OTEL_EXPORTER")
}

// otlp without an endpoint is a misconfiguration, not a default.
func TestStartTracing_OTLPRequiresAnEndpoint(t *testing.T) {
	_, err := StartTracing(context.Background(), TracingConfig{
		Exporter: ExporterOTLP, Service: "taskforge-test",
	}, io.Discard)
	require.Error(t, err)
	require.Contains(t, err.Error(), "TASKFORGE_OTEL_ENDPOINT")
}

// The stdout exporter really exports, and the span carries this process's
// service name -- without which a trace spanning five binaries cannot say
// which one emitted what.
func TestStartTracing_StdoutExportsWithTheServiceName(t *testing.T) {
	var out strings.Builder
	tracing, err := StartTracing(context.Background(), TracingConfig{
		Exporter: ExporterStdout, Service: "taskforge-outbox",
	}, &out)
	require.NoError(t, err)

	_, span := tracing.Tracer.Start(context.Background(), "unit.span")
	span.End()

	// Shutdown flushes the batcher; nothing is guaranteed on stdout before it.
	require.NoError(t, tracing.Shutdown(context.Background()))

	require.NotEmpty(t, out.String(), "the stdout exporter must actually write")
	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(out.String()), &decoded),
		"the stdout exporter must emit valid JSON so it can be shipped")
	require.Equal(t, "unit.span", decoded["Name"])
	require.Contains(t, out.String(), "taskforge-outbox")
}

// Shutdown is safe on a disabled Tracing, safe twice, and safe on a nil
// receiver -- every binary defers it on a path that can run more than once.
func TestShutdown_IsSafeWhenDisabledAndWhenRepeated(t *testing.T) {
	var nilTracing *Tracing
	require.NoError(t, nilTracing.Shutdown(context.Background()))

	disabled, err := StartTracing(context.Background(), TracingConfig{Exporter: ExporterNone}, io.Discard)
	require.NoError(t, err)
	require.NoError(t, disabled.Shutdown(context.Background()))
	require.NoError(t, disabled.Shutdown(context.Background()))

	enabled, err := StartTracing(context.Background(), TracingConfig{
		Exporter: ExporterStdout, Service: "taskforge-test",
	}, io.Discard)
	require.NoError(t, err)
	require.NoError(t, enabled.Shutdown(context.Background()))
	require.NoError(t, enabled.Shutdown(context.Background()),
		"a second shutdown must not error; every binary defers this")
}

// Shutdown must still flush when the process context has already been canceled
// by SIGTERM, which is the only way it is ever actually called in production.
func TestShutdown_FlushesAfterItsContextIsCanceled(t *testing.T) {
	var out strings.Builder
	tracing, err := StartTracing(context.Background(), TracingConfig{
		Exporter: ExporterStdout, Service: "taskforge-test",
	}, &out)
	require.NoError(t, err)

	_, span := tracing.Tracer.Start(context.Background(), "span.during.shutdown")
	span.End()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, tracing.Shutdown(canceled))
	require.Contains(t, out.String(), "span.during.shutdown",
		"a span must still flush when shutdown is handed an already-canceled context")
}

// --- propagation ------------------------------------------------------------

// The round trip is what makes the outbox work: a context's span is rendered
// to W3C strings in one process and rebuilt in another.
func TestTraceContext_RoundTripsAcrossAProcessBoundary(t *testing.T) {
	_, tracer := recordingProvider(t)

	producerCtx, span := tracer.Start(context.Background(), "producer")
	defer span.End()

	traceparent, _ := InjectTraceContext(producerCtx)
	require.NotEmpty(t, traceparent, "a recording span must render a traceparent")
	require.Regexp(t, `^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`, traceparent,
		"the rendered value must match the CHECK migration 0018 puts on the column")

	// A different process: nothing in common but the strings.
	consumerCtx := ExtractTraceContext(context.Background(), traceparent, "")
	require.Equal(t,
		trace.SpanContextFromContext(producerCtx).TraceID(),
		trace.SpanContextFromContext(consumerCtx).TraceID(),
		"the consumer must continue the producer's trace, not start its own")
}

// With tracing disabled there is no span, so nothing is rendered -- which is
// what persists NULL and means "start a new root".
func TestInjectTraceContext_IsEmptyWithoutARecordingSpan(t *testing.T) {
	traceparent, tracestate := InjectTraceContext(context.Background())
	require.Empty(t, traceparent)
	require.Empty(t, tracestate)
}

// A value this process cannot trust is dropped, never adopted. This is the
// single rule at every boundary trace context crosses in this system.
func TestExtractTraceContext_DropsAnythingUnparseable(t *testing.T) {

	for name, traceparent := range map[string]string{
		"empty":              "",
		"not hex":            "00-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-zzzzzzzzzzzzzzzz-01",
		"too short":          "00-abc-def-01",
		"missing fields":     "00-4bf92f3577b34da6a3ce929d0e0e4736",
		"all-zero trace id":  "00-00000000000000000000000000000000-0000000000000000-01",
		"uppercase":          "00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01",
		"trailing garbage":   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
		"control characters": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\n",
	} {
		t.Run(name, func(t *testing.T) {
			ctx := ExtractTraceContext(context.Background(), traceparent, "")
			require.False(t, trace.SpanContextFromContext(ctx).IsValid(),
				"an untrusted traceparent must never produce a valid span context")
		})
	}
}

// A well-formed traceparent IS adopted -- the negative test above would pass
// vacuously if extraction never worked at all.
func TestExtractTraceContext_AdoptsAWellFormedValue(t *testing.T) {
	const valid = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	ctx := ExtractTraceContext(context.Background(), valid, "vendor=value")
	sc := trace.SpanContextFromContext(ctx)
	require.True(t, sc.IsValid())
	require.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", sc.TraceID().String())
	require.Equal(t, "00f067aa0ba902b7", sc.SpanID().String())
	require.Equal(t, "vendor=value", sc.TraceState().String())
}

// TraceIDFrom is what puts a trace id on a log line beside the request id.
func TestTraceIDFrom(t *testing.T) {
	require.Empty(t, TraceIDFrom(context.Background()), "no span means no id, not a zero id")

	_, tracer := recordingProvider(t)
	ctx, span := tracer.Start(context.Background(), "logged")
	defer span.End()

	id := TraceIDFrom(ctx)
	require.Len(t, id, 32)
	require.Equal(t, span.SpanContext().TraceID().String(), id)
}

// Inject and Extract must work with NO global propagator installed at all.
// That is the whole point of this package holding a concrete propagator: a
// process that never called StartTracing, or one whose global was overwritten
// by something else, still propagates trace context correctly.
func TestPropagation_DoesNotDependOnTheGlobalPropagator(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator()) // a no-op global
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })

	const valid = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := ExtractTraceContext(context.Background(), valid, "")
	require.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736",
		trace.SpanContextFromContext(ctx).TraceID().String(),
		"extraction must not depend on whatever is in the global propagator")

	_, tracer := recordingProvider(t)
	spanCtx, span := tracer.Start(context.Background(), "producer")
	defer span.End()
	traceparent, _ := InjectTraceContext(spanCtx)
	require.NotEmpty(t, traceparent, "injection must not depend on the global either")
}
