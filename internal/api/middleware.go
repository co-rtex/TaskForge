package api

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/co-rtex/TaskForge/internal/metrics"
	"github.com/co-rtex/TaskForge/internal/telemetry"
)

type ctxKey int

const requestIDKey ctxKey = iota

// RequestIDHeader is echoed on every response and included in every error body,
// so a user-reported failure can be found in the logs.
const RequestIDHeader = "X-Request-Id"

// RequestIDFrom returns the request id bound to ctx, or "".
func RequestIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

// withRequestID assigns a request id, preferring a caller-supplied one so a
// trace can be followed across services.
//
// The client value is length-limited and only accepted if printable: it is
// echoed into logs and response headers, so an unbounded or control-character
// value would be a log-injection vector.
func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := sanitizeRequestID(r.Header.Get(RequestIDHeader))
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

func sanitizeRequestID(v string) string {
	const maxLen = 128
	if v == "" || len(v) > maxLen {
		return ""
	}
	for _, r := range v {
		if r < 0x20 || r == 0x7f {
			return ""
		}
	}
	return v
}

// withTracing starts one server span per request, continuing an inbound W3C
// trace when the caller supplied one.
//
// It sits alongside withRequestID rather than replacing it. The two identities
// answer different questions and neither subsumes the other: a request id is a
// user-facing token echoed in every error body so a reported failure can be
// found in the logs, while a trace id links this request to work done in other
// processes. withLogging emits both.
//
// Every route is traced, public and internal alike. Instrumenting only
// submission would cost the same code and leave every later milestone with one
// route's worth of precedent instead of the whole surface.
//
// A span must be named for the ROUTE PATTERN ("GET /v1/jobs/{job_id}"), never
// the raw path: the raw path carries a job id, which would make every request
// its own span name and spans impossible to group. That requirement is what
// splits the tracing middleware in two.
//
// net/http populates Request.Pattern inside ServeMux.ServeHTTP, on the exact
// *Request pointer the mux is handed -- verified, not assumed. So the pattern
// does not exist yet when an outer middleware runs, and it is written to a
// pointer no outer middleware holds, because withTimeout hands the mux its own
// copy. The span therefore starts outermost, where the whole request is inside
// it and where withLogging can read its trace id, and is RENAMED by a second,
// innermost middleware once the mux has matched.
func withTracing(tracer trace.Tracer, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A malformed or absent traceparent yields the context unchanged, so
		// the span below becomes a fresh root. A value this server cannot parse
		// is never propagated onward and never persisted.
		ctx := telemetry.ExtractTraceContext(r.Context(),
			r.Header.Get(telemetry.TraceparentHeader),
			r.Header.Get(telemetry.TracestateHeader))

		// Provisional, and bounded: the method alone. withSpanRoute replaces it
		// with the matched pattern. A request that matches nothing keeps this
		// name, which is exactly right -- an unbounded set of 404 paths must
		// never become an unbounded set of span names.
		ctx, span := tracer.Start(ctx, r.Method, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()

		// Deliberately bounded attributes: the method, and later the route
		// pattern. Never the raw path, never a query parameter, never a header.
		// The request id is included so a trace and a log line can be tied
		// together from either direction.
		span.SetAttributes(attribute.String("http.request.method", r.Method))
		if id := RequestIDFrom(ctx); id != "" {
			span.SetAttributes(attribute.String("taskforge.request_id", id))
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// routeHolder carries the matched route pattern OUTWARD.
//
// Context flows inward, so the innermost middleware cannot hand a value back
// up the chain. A pointer placed in the context by the outer middleware and
// written by the inner one is how the two halves of an HTTP metric meet: only
// withSpanRoute can know the route, and only withHTTPMetrics can know the
// final status.
type routeHolder struct{ route string }

type routeCtxKey struct{}

// withHTTPMetrics records one request, from OUTSIDE withRecovery.
//
// Its position is the whole design, and it was a real defect before it moved
// here. withSpanRoute sits innermost, so a handler that panics unwinds past it
// and withRecovery -- which is further out -- writes the sanitized 500 to a
// writer the inner recorder never sees. Recording the status there reported
// 200 for a request that returned 500: not a missing metric but a wrong one,
// which is worse, because an operator watching error rates would see none.
//
// So the status is observed here, outside withRecovery where the real final
// status is written, and the route arrives from withSpanRoute through the
// holder. Each half is measured where it is actually knowable.
func withHTTPMetrics(m *metrics.Metrics, next http.Handler) http.Handler {
	if m == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		holder := &routeHolder{}
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		defer func() {
			route := holder.route
			if route == "" {
				// Matched nothing. One bounded stand-in for every unrouted
				// path: an unmatched URL must never become a label value.
				route = "unmatched"
			}
			m.HTTPRequests.WithLabelValues(
				r.Method, route, strconv.Itoa(recorder.status)).Inc()
			m.HTTPDuration.WithLabelValues(
				r.Method, route).Observe(time.Since(started).Seconds())
		}()

		next.ServeHTTP(recorder, r.WithContext(
			context.WithValue(r.Context(), routeCtxKey{}, holder)))
	})
}

// withSpanRoute names the active span, and publishes the matched route, once
// the mux has matched.
//
// It wraps the mux DIRECTLY and passes r through untouched, which is the whole
// trick: ServeMux writes Pattern onto the pointer it is given, so reading it
// back after ServeHTTP returns is reading the mux's own answer rather than
// re-deriving it. Any middleware between this and the mux that copied the
// request would break that, which is why this sits innermost.
//
// M6C reuses that single computation rather than repeating it. A span name and
// an HTTP metric need the same value at the same point, and an implementation
// that re-derived it in a second middleware would read r.Pattern where it is
// still empty -- the exact failure M6B already found and fixed once.
//
// Renaming after the response is written is safe: withTracing defers End, so
// the span is still open.
func withSpanRoute(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Deferred, not sequential. A handler that panics unwinds straight
		// through ServeHTTP, and withRecovery -- which sits OUTSIDE this --
		// turns that into a sanitized 500. Doing this work on the line after
		// the call would therefore be skipped for exactly the requests an
		// operator most wants to find, in a trace or in a metric alike.
		defer func() {
			if r.Pattern == "" {
				return // matched nothing; the bounded fallbacks stand
			}
			// Published outward for withHTTPMetrics. Same value as the span's
			// http.route attribute, by construction rather than by agreement.
			if holder, ok := r.Context().Value(routeCtxKey{}).(*routeHolder); ok {
				holder.route = r.Pattern
			}

			name := r.Pattern
			// A pattern registered without a method (the catch-all "/") has no
			// verb in it. Prefixing keeps every span name the same shape.
			if !strings.Contains(name, " ") {
				name = r.Method + " " + name
			}
			span := trace.SpanFromContext(r.Context())
			span.SetName(name)
			span.SetAttributes(attribute.String("http.route", r.Pattern))
		}()
		next.ServeHTTP(w, r)
	})
}

// statusRecorder captures the response status for access logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// withLogging emits one structured line per request.
//
// It records method, path, status, and duration only. Request bodies are never
// logged: they are job payloads, which are user data of unbounded size.
func withLogging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		fields := []any{
			slog.String("request_id", RequestIDFrom(r.Context())),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.status),
			slog.Duration("duration", time.Since(start)),
		}
		// Added beside the request id, never in place of it. Empty whenever
		// tracing is disabled, which is the default, so the line is unchanged
		// for a deployment that has not opted in.
		if traceID := telemetry.TraceIDFrom(r.Context()); traceID != "" {
			fields = append(fields, slog.String("trace_id", traceID))
		}
		log.Info("http request", fields...)
	})
}

// withRecovery converts a panic into a sanitized 500 so one bad request cannot
// take the process down or leak a stack trace to a client.
func withRecovery(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("panic recovered",
					slog.String("request_id", RequestIDFrom(r.Context())),
					slog.String("path", r.URL.Path),
					slog.Any("panic", rec))
				writeError(w, r, log, http.StatusInternalServerError, CodeInternal,
					"internal error", nil)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// withBodyLimit caps how much a client can send. Without it, one request could
// exhaust memory.
func withBodyLimit(limit int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}
		next.ServeHTTP(w, r)
	})
}

// withTimeout ensures PostgreSQL and every other context-aware dependency sees
// a bounded request context even when the caller supplied no deadline.
func withTimeout(timeout time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
