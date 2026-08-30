package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTestServer builds a server with no job store.
//
// Every case here is rejected during validation, before any database access, so
// a nil store is the strongest possible assertion that these paths never touch
// PostgreSQL. Anything that must reach the store is an integration test.
func newTestServer(t *testing.T, checks ...ReadinessCheck) http.Handler {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewServer(nil, Config{MaxRequestBytes: 1024, DevScope: "test"}, log, checks...).Handler()
}

func post(t *testing.T, h http.Handler, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) ErrorBody {
	t.Helper()
	var body ErrorBody
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	return body
}

const validBody = `{"queue":"default","job_type":"demo.echo","payload":{"a":1}}`

func TestSubmit_RequiresIdempotencyKey(t *testing.T) {
	rec := post(t, newTestServer(t), validBody, nil)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)

	body := decodeError(t, rec)
	require.Equal(t, CodeValidationFailed, body.Error.Code)
	require.Equal(t, "Idempotency-Key", body.Error.Details[0].Field)
}

func TestSubmit_RejectsMalformedJSON(t *testing.T) {
	for _, in := range []string{`{`, `not json`, `[]`, ``} {
		t.Run(in, func(t *testing.T) {
			rec := post(t, newTestServer(t), in, map[string]string{"Idempotency-Key": "k1"})
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Equal(t, CodeMalformedJSON, decodeError(t, rec).Error.Code)
		})
	}
}

// json.Decoder would otherwise read only the first document and silently ignore
// the rest, accepting a request the caller did not mean to send.
func TestSubmit_RejectsMultipleJSONDocuments(t *testing.T) {
	rec := post(t, newTestServer(t), validBody+` {"queue":"other"}`, map[string]string{"Idempotency-Key": "k1"})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, CodeMalformedJSON, decodeError(t, rec).Error.Code)
}

// Silently dropping a misspelled field would give the caller a job that differs
// from what they asked for, and would change the idempotency fingerprint
// invisibly.
func TestSubmit_RejectsUnknownFields(t *testing.T) {
	rec := post(t, newTestServer(t),
		`{"queue":"default","job_type":"demo.echo","payload":{},"priorty":10}`,
		map[string]string{"Idempotency-Key": "k1"})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, CodeMalformedJSON, decodeError(t, rec).Error.Code)
}

func TestSubmit_RejectsOversizedBody(t *testing.T) {
	huge := `{"queue":"default","job_type":"demo.echo","payload":{"a":"` + strings.Repeat("x", 4096) + `"}}`
	rec := post(t, newTestServer(t), huge, map[string]string{"Idempotency-Key": "k1"})
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	require.Equal(t, CodePayloadTooLarge, decodeError(t, rec).Error.Code)
}

func TestSubmit_RejectsScheduledAtAsNotImplemented(t *testing.T) {
	rec := post(t, newTestServer(t),
		`{"queue":"default","job_type":"demo.echo","payload":{},"scheduled_at":"2030-01-01T00:00:00Z"}`,
		map[string]string{"Idempotency-Key": "k1"})
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)

	body := decodeError(t, rec)
	require.Equal(t, "scheduled_at", body.Error.Details[0].Field)
	require.Contains(t, body.Error.Details[0].Message, "not implemented")
}

func TestErrorsCarryRequestID(t *testing.T) {
	rec := post(t, newTestServer(t), `{`, map[string]string{"Idempotency-Key": "k1"})
	require.NotEmpty(t, decodeError(t, rec).Error.RequestID)
	require.NotEmpty(t, rec.Header().Get(RequestIDHeader))
}

func TestRequestID_ClientValueIsEchoedWhenSafe(t *testing.T) {
	rec := post(t, newTestServer(t), `{`, map[string]string{
		"Idempotency-Key": "k1",
		RequestIDHeader:   "client-trace-123",
	})
	require.Equal(t, "client-trace-123", rec.Header().Get(RequestIDHeader))
}

func TestRequestTimeoutIsPresentForEveryHandler(t *testing.T) {
	var deadlinePresent bool
	handler := withTimeout(time.Second, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, deadlinePresent = r.Context().Deadline()
		w.WriteHeader(http.StatusNoContent)
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusNoContent, recorder.Code)
	require.True(t, deadlinePresent)
}

// A client-supplied request id is echoed into logs and headers, so an unbounded
// or control-character value would be a log-injection vector.
func TestRequestID_UnsafeClientValueIsReplaced(t *testing.T) {
	for name, unsafe := range map[string]string{
		"newline":  "abc\ndef",
		"carriage": "abc\rdef",
		"nul":      "abc\x00def",
		"too long": strings.Repeat("x", 129),
	} {
		t.Run(name, func(t *testing.T) {
			rec := post(t, newTestServer(t), `{`, map[string]string{
				"Idempotency-Key": "k1",
				RequestIDHeader:   unsafe,
			})
			got := rec.Header().Get(RequestIDHeader)
			require.NotEqual(t, unsafe, got)
			require.NotEmpty(t, got)
			require.NotContains(t, got, "\n")
			require.NotContains(t, got, "\r")
		})
	}
}

// Unrouted methods and paths must return the same structured error shape as
// every other failure, not ServeMux's plain-text default.
func TestRouting(t *testing.T) {
	h := newTestServer(t)
	tests := []struct {
		method, path string
		want         int
		wantCode     string
		wantAllow    string
	}{
		{http.MethodGet, "/v1/jobs", http.StatusMethodNotAllowed, CodeMethodNotAllowed, "POST"},
		{http.MethodDelete, "/v1/jobs", http.StatusMethodNotAllowed, CodeMethodNotAllowed, "POST"},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed, CodeMethodNotAllowed, "GET"},
		{http.MethodDelete, "/v1/jobs/" + strings.Repeat("a", 8), http.StatusMethodNotAllowed, CodeMethodNotAllowed, "GET"},
		{http.MethodGet, "/v1/nope", http.StatusNotFound, CodeNotFound, ""},
		{http.MethodGet, "/", http.StatusNotFound, CodeNotFound, ""},
	}
	for _, tc := range tests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
			require.Equal(t, tc.want, rec.Code)

			body := decodeError(t, rec)
			require.Equal(t, tc.wantCode, body.Error.Code)
			require.NotEmpty(t, body.Error.RequestID)
			if tc.wantAllow != "" {
				require.Equal(t, tc.wantAllow, rec.Header().Get("Allow"))
			}
		})
	}
}

// A job id that is not a UUID answers 404 rather than 400, so a caller cannot
// tell a malformed id from someone else's id.
func TestGetJob_MalformedIDIsNotFound(t *testing.T) {
	rec := httptest.NewRecorder()
	newTestServer(t).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/jobs/not-a-uuid", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, CodeNotFound, decodeError(t, rec).Error.Code)
}

// Liveness must not depend on dependencies: a liveness probe that fails on a
// database blip would restart a healthy process and make an outage worse.
func TestLiveness_IgnoresFailingDependencies(t *testing.T) {
	h := newTestServer(t, ReadinessCheck{
		Name:  "postgres",
		Check: func(context.Context) error { return errors.New("down") },
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestReadiness_ReflectsDependencyHealth(t *testing.T) {
	t.Run("all healthy", func(t *testing.T) {
		h := newTestServer(t, ReadinessCheck{Name: "postgres", Check: func(context.Context) error { return nil }})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		require.Equal(t, http.StatusOK, rec.Code)

		var body struct {
			Status     string            `json:"status"`
			Components map[string]string `json:"components"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		require.Equal(t, "ready", body.Status)
		require.Equal(t, "ok", body.Components["postgres"])
	})

	t.Run("dependency down", func(t *testing.T) {
		h := newTestServer(t, ReadinessCheck{
			Name:  "postgres",
			Check: func(context.Context) error { return errors.New("connection refused") },
		})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var body struct {
			Status     string            `json:"status"`
			Components map[string]string `json:"components"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		require.Equal(t, "not_ready", body.Status)
		require.Equal(t, "unavailable", body.Components["postgres"])
		// The underlying error must not leak to a client.
		require.NotContains(t, rec.Body.String(), "connection refused")
	})
}
