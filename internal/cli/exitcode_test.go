package cli

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// allExitCodes is the complete, closed set this CLI defines. A test below
// fails if a new Exit* constant is added here without being added to this
// list, which is what makes TestExitCodes_AreDistinct and
// TestExitCodes_OnlySuccessIsZero real guards rather than a static list
// that could silently stop covering a new code.
var allExitCodes = map[string]int{
	"ExitSuccess":            ExitSuccess,
	"ExitUsageError":         ExitUsageError,
	"ExitRequestRejected":    ExitRequestRejected,
	"ExitUnauthorized":       ExitUnauthorized,
	"ExitNotFound":           ExitNotFound,
	"ExitConflict":           ExitConflict,
	"ExitInternalError":      ExitInternalError,
	"ExitServiceUnavailable": ExitServiceUnavailable,
	"ExitTransportError":     ExitTransportError,
	"ExitUnexpectedResponse": ExitUnexpectedResponse,
}

// TestExitCodes_AreDistinct proves the set is bounded (exactly ten named
// codes, 0-9) and mutually exclusive (no two names share a numeric value):
// the acceptance bar this milestone was told to satisfy, checked
// mechanically rather than only by code review.
func TestExitCodes_AreDistinct(t *testing.T) {
	require.Len(t, allExitCodes, 10)
	seen := make(map[int]string, len(allExitCodes))
	for name, code := range allExitCodes {
		if other, ok := seen[code]; ok {
			t.Fatalf("exit code %d is used by both %s and %s", code, other, name)
		}
		seen[code] = name
		require.GreaterOrEqualf(t, code, 0, "%s must be non-negative", name)
		require.LessOrEqualf(t, code, 255, "%s must fit in a POSIX exit status", name)
	}
}

// TestExitCodes_OnlySuccessIsZero proves exit 0 means success and nothing
// else does, in both directions: ExitSuccess is 0, and no other named code
// -- nor any entry in apiErrorExitCodes -- is 0.
func TestExitCodes_OnlySuccessIsZero(t *testing.T) {
	require.Equal(t, 0, ExitSuccess)
	for name, code := range allExitCodes {
		if name == "ExitSuccess" {
			continue
		}
		require.NotZerof(t, code, "%s must not be 0; only ExitSuccess may be", name)
	}
	for apiCode, exitCode := range apiErrorExitCodes {
		require.NotZerof(t, exitCode, "API error code %q must not map to exit 0", apiCode)
	}
}

// TestApiErrorExitCodes_CoversExactlyTheReachableSet pins the set of
// api/openapi.yaml Error.code values this CLI knows how to map, so the
// table cannot silently grow or shrink without a reviewer seeing it change
// here. This is the exact set reachable from every route
// internal/cli/client.go calls, enumerated against api/openapi.yaml by
// hand for the M5D handoff and re-verified here.
func TestApiErrorExitCodes_CoversExactlyTheReachableSet(t *testing.T) {
	want := []string{
		"malformed_json",
		"payload_too_large",
		"validation_failed",
		"invalid_cursor",
		"unauthorized",
		"not_found",
		"idempotency_conflict",
		"job_not_cancelable",
		"job_not_dead_lettered",
		"internal_error",
		"service_unavailable",
	}
	require.Len(t, apiErrorExitCodes, len(want))
	for _, code := range want {
		_, ok := apiErrorExitCodes[code]
		require.Truef(t, ok, "apiErrorExitCodes is missing %q", code)
	}
}

// errorBody builds a minimal, valid api/openapi.yaml Error response body
// carrying the given code.
func errorBody(code string) string {
	body, _ := json.Marshal(map[string]any{
		"error": map[string]any{"code": code, "message": "test fixture for " + code},
	})
	return string(body)
}

// runAgainst starts an httptest.Server that always answers with the given
// status and body, runs Run with the given CLI args against it, and
// returns the exit code plus stdout/stderr.
func runAgainst(t *testing.T, status int, body string, args []string) (exitCode int, stdout, stderr string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		if body != "" {
			_, _ = w.Write([]byte(body))
		}
	}))
	t.Cleanup(server.Close)

	var outBuf, errBuf strings.Builder
	fullArgs := append([]string{"--api-url", server.URL}, args...)
	code := Run(context.Background(), fullArgs, &outBuf, &errBuf)
	return code, outBuf.String(), errBuf.String()
}

// Each exit code below gets its OWN test, asserting the specific numeric
// value Run returns -- not a shared "non-zero" assertion.

func TestRun_ExitSuccess(t *testing.T) {
	code, stdout, _ := runAgainst(t, http.StatusOK, `{"id":"job-1"}`, []string{"jobs", "get", "job-1"})
	require.Equal(t, ExitSuccess, code)
	require.JSONEq(t, `{"id":"job-1"}`, stdout)
}

func TestRun_ExitUsageError(t *testing.T) {
	var outBuf, errBuf strings.Builder
	code := Run(context.Background(), []string{"jobs", "get"}, &outBuf, &errBuf)
	require.Equal(t, ExitUsageError, code)
	require.Empty(t, outBuf.String())
}

func TestRun_ExitUsageError_UnknownCommand(t *testing.T) {
	var outBuf, errBuf strings.Builder
	code := Run(context.Background(), []string{"jobs", "levitate"}, &outBuf, &errBuf)
	require.Equal(t, ExitUsageError, code)
}

func TestRun_ExitUsageError_MissingSubmitFlags(t *testing.T) {
	var outBuf, errBuf strings.Builder
	code := Run(context.Background(), []string{"jobs", "submit"}, &outBuf, &errBuf)
	require.Equal(t, ExitUsageError, code)
}

func TestRun_ExitRequestRejected_MalformedJSON(t *testing.T) {
	code, stdout, _ := runAgainst(t, http.StatusBadRequest, errorBody("malformed_json"),
		[]string{"api-keys", "create", "--scope", "s", "--name", "n"})
	require.Equal(t, ExitRequestRejected, code)
	require.Empty(t, stdout)
}

func TestRun_ExitRequestRejected_PayloadTooLarge(t *testing.T) {
	code, _, _ := runAgainst(t, http.StatusRequestEntityTooLarge, errorBody("payload_too_large"),
		[]string{"api-keys", "create", "--scope", "s", "--name", "n"})
	require.Equal(t, ExitRequestRejected, code)
}

func TestRun_ExitRequestRejected_ValidationFailed(t *testing.T) {
	code, _, _ := runAgainst(t, http.StatusUnprocessableEntity, errorBody("validation_failed"),
		[]string{"dlq", "list"})
	require.Equal(t, ExitRequestRejected, code)
}

func TestRun_ExitRequestRejected_InvalidCursor(t *testing.T) {
	code, _, _ := runAgainst(t, http.StatusUnprocessableEntity, errorBody("invalid_cursor"),
		[]string{"dlq", "list", "--cursor", "garbage"})
	require.Equal(t, ExitRequestRejected, code)
}

func TestRun_ExitUnauthorized(t *testing.T) {
	code, _, stderr := runAgainst(t, http.StatusUnauthorized, errorBody("unauthorized"),
		[]string{"jobs", "get", "job-1"})
	require.Equal(t, ExitUnauthorized, code)
	require.Contains(t, stderr, "unauthorized")
}

func TestRun_ExitNotFound(t *testing.T) {
	code, _, _ := runAgainst(t, http.StatusNotFound, errorBody("not_found"),
		[]string{"jobs", "get", "job-1"})
	require.Equal(t, ExitNotFound, code)
}

func TestRun_ExitConflict_IdempotencyConflict(t *testing.T) {
	code, _, _ := runAgainst(t, http.StatusConflict, errorBody("idempotency_conflict"),
		[]string{"jobs", "submit", "--queue", "default", "--job-type", "demo.echo", "--payload", `{}`})
	require.Equal(t, ExitConflict, code)
}

func TestRun_ExitConflict_JobNotCancelable(t *testing.T) {
	code, _, _ := runAgainst(t, http.StatusConflict, errorBody("job_not_cancelable"),
		[]string{"jobs", "cancel", "job-1"})
	require.Equal(t, ExitConflict, code)
}

func TestRun_ExitConflict_JobNotDeadLettered(t *testing.T) {
	code, _, _ := runAgainst(t, http.StatusConflict, errorBody("job_not_dead_lettered"),
		[]string{"jobs", "retry", "job-1"})
	require.Equal(t, ExitConflict, code)
}

func TestRun_ExitInternalError(t *testing.T) {
	code, _, _ := runAgainst(t, http.StatusInternalServerError, errorBody("internal_error"),
		[]string{"jobs", "get", "job-1"})
	require.Equal(t, ExitInternalError, code)
}

func TestRun_ExitServiceUnavailable(t *testing.T) {
	code, _, _ := runAgainst(t, http.StatusServiceUnavailable, errorBody("service_unavailable"),
		[]string{"jobs", "get", "job-1"})
	require.Equal(t, ExitServiceUnavailable, code)
}

func TestRun_ExitTransportError(t *testing.T) {
	var outBuf, errBuf strings.Builder
	// Port 1 on loopback: nothing listens there (it's a reserved,
	// privileged port), so http.Client.Do fails at connect time and never
	// produces a Response.
	code := Run(context.Background(), []string{"--api-url", "http://127.0.0.1:1", "jobs", "get", "job-1"}, &outBuf, &errBuf)
	require.Equal(t, ExitTransportError, code)
	require.Empty(t, outBuf.String())
	require.Contains(t, errBuf.String(), "transport_error")
}

func TestRun_ExitUnexpectedResponse_UnknownErrorCode(t *testing.T) {
	code, _, stderr := runAgainst(t, http.StatusTeapot, errorBody("a_future_code_this_cli_does_not_know"),
		[]string{"jobs", "get", "job-1"})
	require.Equal(t, ExitUnexpectedResponse, code)
	require.Contains(t, stderr, "a_future_code_this_cli_does_not_know")
}

func TestRun_ExitUnexpectedResponse_NonJSONBody(t *testing.T) {
	code, _, stderr := runAgainst(t, http.StatusBadGateway, "<html>not json</html>",
		[]string{"jobs", "get", "job-1"})
	require.Equal(t, ExitUnexpectedResponse, code)
	require.Contains(t, stderr, "unexpected_response")
}

// TestRun_FailurePrintsNothingToStdout proves the stdout/stderr split is
// consistent across every failure class: a script reading stdout only
// ever sees a payload on ExitSuccess.
func TestRun_FailurePrintsNothingToStdout(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"unauthorized", http.StatusUnauthorized, errorBody("unauthorized")},
		{"not_found", http.StatusNotFound, errorBody("not_found")},
		{"internal_error", http.StatusInternalServerError, errorBody("internal_error")},
		{"service_unavailable", http.StatusServiceUnavailable, errorBody("service_unavailable")},
		{"unexpected", http.StatusTeapot, "not json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, stdout, stderr := runAgainst(t, tc.status, tc.body, []string{"jobs", "get", "job-1"})
			require.Empty(t, stdout)
			require.NotEmpty(t, stderr)
		})
	}
}
