package telemetry

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TracerName is the instrumentation scope every TaskForge span is recorded
// under. One name across all five binaries, so a trace assembled from several
// processes reads as one system rather than five.
const TracerName = "github.com/co-rtex/TaskForge"

// Exporter selection. Values accepted by TASKFORGE_OTEL_EXPORTER.
const (
	// ExporterNone is the default. No exporter is constructed, no background
	// goroutine is started, and no network address is dialed -- a deployment
	// that has not opted in pays nothing and behaves exactly as it did before
	// tracing existed.
	ExporterNone = "none"
	// ExporterStdout writes spans as JSON to a supplied writer. Useful for a
	// developer and for tests that want to see a real exporter run.
	ExporterStdout = "stdout"
	// ExporterOTLP sends spans over OTLP/HTTP to TASKFORGE_OTEL_ENDPOINT.
	ExporterOTLP = "otlp"
)

// TracingConfig selects an exporter. It is deliberately not the process-wide
// config.Config: internal/telemetry must stay importable by every binary
// without importing the whole configuration surface.
type TracingConfig struct {
	// Exporter is one of ExporterNone, ExporterStdout, or ExporterOTLP.
	// Anything unrecognized is an error rather than a silent downgrade: a typo
	// in TASKFORGE_OTEL_EXPORTER that quietly disabled tracing would be found
	// only by noticing an absence, which is the hardest kind of bug to notice.
	Exporter string
	// Endpoint is the OTLP collector address. Only read for ExporterOTLP.
	Endpoint string
	// Service names the emitting process, e.g. "taskforge-api".
	Service string
}

// Tracing is a live tracer provider plus the shutdown its owner must call.
type Tracing struct {
	provider *sdktrace.TracerProvider
	// Tracer is what callers start spans from. When tracing is disabled this
	// is a no-op tracer, so instrumentation code has no enabled/disabled
	// branch anywhere -- there is exactly one code path, and it is the one
	// that runs in production.
	Tracer trace.Tracer
}

// StartTracing configures tracing for one process.
//
// It sets the global propagator in every mode, including ExporterNone, because
// propagation is about the wire contract rather than about export: a process
// that does not export spans still has to pass a caller's traceparent through
// to the next hop, or the trace breaks at that process for everyone else.
//
// It sets the global TracerProvider only when an exporter is configured. With
// ExporterNone the OTel default (a no-op provider) is left in place, so nothing
// is registered, nothing is sampled, and no goroutine exists to leak.
//
// stdoutWriter is only read for ExporterStdout; pass io.Discard elsewhere.
func StartTracing(ctx context.Context, cfg TracingConfig, stdoutWriter io.Writer) (*Tracing, error) {
	// Published globally so any third-party instrumentation interoperates. This
	// package's own Inject/Extract use the concrete propagator above rather
	// than reading this back -- see its comment.
	otel.SetTextMapPropagator(propagator)

	if cfg.Exporter == "" || cfg.Exporter == ExporterNone {
		// noop.NewTracerProvider() rather than otel.Tracer(...): taking a tracer
		// from the global provider would silently start recording if some other
		// package set a global provider later, which is exactly the surprise
		// "disabled" must not have.
		return &Tracing{Tracer: noop.NewTracerProvider().Tracer(TracerName)}, nil
	}

	exporter, err := newSpanExporter(ctx, cfg, stdoutWriter)
	if err != nil {
		return nil, err
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(serviceResource(cfg.Service)),
	)
	otel.SetTracerProvider(provider)
	return &Tracing{provider: provider, Tracer: provider.Tracer(TracerName)}, nil
}

func newSpanExporter(ctx context.Context, cfg TracingConfig, stdoutWriter io.Writer) (sdktrace.SpanExporter, error) {
	switch cfg.Exporter {
	case ExporterStdout:
		return stdouttrace.New(stdouttrace.WithWriter(stdoutWriter))
	case ExporterOTLP:
		if strings.TrimSpace(cfg.Endpoint) == "" {
			return nil, fmt.Errorf("TASKFORGE_OTEL_ENDPOINT must be set when TASKFORGE_OTEL_EXPORTER is %q", ExporterOTLP)
		}
		return otlptracehttp.New(ctx,
			otlptracehttp.WithEndpointURL(cfg.Endpoint),
		)
	default:
		return nil, fmt.Errorf(
			"TASKFORGE_OTEL_EXPORTER must be one of %q, %q, or %q; got %q",
			ExporterNone, ExporterStdout, ExporterOTLP, cfg.Exporter)
	}
}

// serviceResource tags every span this process emits with its service name.
//
// Without it a trace assembled from five binaries shows five anonymous
// producers, and the one question a reader always has -- which process emitted
// this span -- has no answer in the data. The name matches the one
// NewLogger already tags this process's log lines with, so a span and a log
// line from the same process agree.
func serviceResource(service string) *resource.Resource {
	if service == "" {
		service = "taskforge"
	}
	return resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(service),
	)
}

// Shutdown flushes and stops the exporter.
//
// Every binary calls this on its graceful-shutdown path. It is bounded rather
// than open-ended: a collector that has gone away must not hold a process open
// past the shutdown budget its operator configured. Safe to call on a Tracing
// built with ExporterNone, and safe to call more than once.
func (t *Tracing) Shutdown(ctx context.Context) error {
	if t == nil || t.provider == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := t.provider.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shut down tracer provider: %w", err)
	}
	return nil
}

// --- W3C trace context, carried two ways -----------------------------------
//
// The same traceparent/tracestate pair travels over HTTP headers between a
// client and the API, and inside the outbox envelope between the API and a
// worker. propagation.MapCarrier covers both, so one propagator serializes and
// parses each of them and there is no second, hand-rolled encoding to drift.

// TraceHeaders are the W3C fields. Named here so the outbox package and the
// HTTP layer agree on spelling without importing each other.
const (
	TraceparentHeader = "traceparent"
	TracestateHeader  = "tracestate"
)

// propagator is the concrete W3C propagator these helpers use.
//
// Deliberately NOT otel.GetTextMapPropagator(). Reading the global would make
// this package's correctness depend on a process having called StartTracing
// first, and on nothing else having overwritten the global afterwards -- a
// dependency that is invisible at every call site and fails silently by simply
// not propagating. TaskForge speaks W3C trace context and nothing else, so the
// concrete value is both simpler and impossible to break from a distance.
//
// StartTracing still publishes the same propagator globally, so third-party
// instrumentation a future milestone might add interoperates with these.
var propagator = propagation.TraceContext{}

// InjectTraceContext renders ctx's active span as W3C traceparent/tracestate.
//
// Both are empty when ctx carries no recording span, which is the ordinary
// case with tracing disabled. A caller persisting the result stores NULL, and
// NULL means "start a new root" rather than being an error.
func InjectTraceContext(ctx context.Context) (traceparent, tracestate string) {
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	return carrier.Get(TraceparentHeader), carrier.Get(TracestateHeader)
}

// ExtractTraceContext returns a context continuing the trace the given W3C
// values name.
//
// An empty, malformed, or otherwise unparseable traceparent yields ctx
// unchanged, so the caller's next span becomes a fresh root. That is the
// single rule everywhere trace context crosses a boundary in this system: a
// value we cannot trust is dropped, never propagated and never persisted.
func ExtractTraceContext(ctx context.Context, traceparent, tracestate string) context.Context {
	if traceparent == "" {
		return ctx
	}
	carrier := propagation.MapCarrier{TraceparentHeader: traceparent}
	if tracestate != "" {
		carrier.Set(TracestateHeader, tracestate)
	}
	return propagator.Extract(ctx, carrier)
}

// TraceIDFrom returns the hex trace id of ctx's active span, or "".
//
// Used to put a trace id in a structured log line beside the request id. The
// two identities are complementary and neither replaces the other: a request
// id is a user-facing token echoed in an error body, while a trace id links
// this process's work to every other process's.
func TraceIDFrom(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasTraceID() {
		return ""
	}
	return sc.TraceID().String()
}
