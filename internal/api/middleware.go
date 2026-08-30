package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
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
		log.Info("http request",
			slog.String("request_id", RequestIDFrom(r.Context())),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.status),
			slog.Duration("duration", time.Since(start)))
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
