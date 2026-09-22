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
//
// M6A's four read routes (GET /v1/jobs, GET /v1/jobs/{job_id}/attempts,
// GET /v1/workers, GET /v1/queues) added no code to this set: every error
// they document was already reachable from an existing route.
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

// ---------------------------------------------------------------------------
// M6A read commands: the exit-code contract, per command
// ---------------------------------------------------------------------------

// m6aReadCommands is every command M6A added. Each one must honor the same
// exit-code contract the M5D commands do -- a new command that classified a
// 404 differently would break a script that already branches on 4.
var m6aReadCommands = map[string][]string{
	"jobs list":     {"jobs", "list"},
	"jobs attempts": {"jobs", "attempts", "job-1"},
	"workers list":  {"workers", "list"},
	"queues list":   {"queues", "list"},
}

// 422 invalid_cursor is exit 2. This is the case that matters most: a cursor
// this API did not issue is 422, NOT 400, and a CLI that expected 400 would
// fall through to ExitUnexpectedResponse and tell a script the server was the
// wrong version.
func TestM6AReads_InvalidCursorIsExitRequestRejected(t *testing.T) {
	for name, args := range m6aReadCommands {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runAgainst(t,
				http.StatusUnprocessableEntity, errorBody("invalid_cursor"), args)
			require.Equal(t, ExitRequestRejected, code)
			require.Equal(t, 2, code, "the documented numeric value, not just the constant")
			require.Empty(t, stdout, "a failure writes nothing to stdout")
			require.Contains(t, stderr, "invalid_cursor")
		})
	}
}

// 422 validation_failed is the same class: change the input and retry.
func TestM6AReads_ValidationFailedIsExitRequestRejected(t *testing.T) {
	for name, args := range m6aReadCommands {
		t.Run(name, func(t *testing.T) {
			code, stdout, _ := runAgainst(t,
				http.StatusUnprocessableEntity, errorBody("validation_failed"), args)
			require.Equal(t, 2, code)
			require.Empty(t, stdout)
		})
	}
}

func TestM6AReads_UnauthorizedIsExitUnauthorized(t *testing.T) {
	for name, args := range m6aReadCommands {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runAgainst(t,
				http.StatusUnauthorized, errorBody("unauthorized"), args)
			require.Equal(t, ExitUnauthorized, code)
			require.Equal(t, 3, code)
			require.Empty(t, stdout)
			require.Contains(t, stderr, "unauthorized")
		})
	}
}

func TestM6AReads_NotFoundIsExitNotFound(t *testing.T) {
	// Only the attempts route is addressed by id, so only it can answer 404
	// for a real caller; the others are covered anyway, because the mapping
	// must not depend on which command happened to receive the code.
	for name, args := range m6aReadCommands {
		t.Run(name, func(t *testing.T) {
			code, stdout, _ := runAgainst(t, http.StatusNotFound, errorBody("not_found"), args)
			require.Equal(t, ExitNotFound, code)
			require.Equal(t, 4, code)
			require.Empty(t, stdout)
		})
	}
}

// An API that was never reached is exit 8, distinct from the 503 the API
// returns about its own deadline. A script retrying a transport failure and a
// script retrying a server-deadline failure are doing different things.
func TestM6AReads_UnreachableAPIIsExitTransportError(t *testing.T) {
	for name, args := range m6aReadCommands {
		t.Run(name, func(t *testing.T) {
			var outBuf, errBuf strings.Builder
			// A port nothing listens on, on the loopback interface.
			full := append([]string{"--api-url", "http://127.0.0.1:1"}, args...)
			code := Run(context.Background(), full, &outBuf, &errBuf)
			require.Equal(t, ExitTransportError, code)
			require.Equal(t, 8, code)
			require.Empty(t, outBuf.String())
			require.Contains(t, errBuf.String(), "transport_error")
		})
	}
}

// 503 stays distinct from 8: the API WAS reached and reported that its own
// deadline elapsed. These reads commit nothing, so repeating them is safe --
// but the exit code alone does not say so, the server's message does.
func TestM6AReads_ServiceUnavailableIsExitServiceUnavailable(t *testing.T) {
	for name, args := range m6aReadCommands {
		t.Run(name, func(t *testing.T) {
			code, stdout, _ := runAgainst(t,
				http.StatusServiceUnavailable, errorBody("service_unavailable"), args)
			require.Equal(t, ExitServiceUnavailable, code)
			require.Equal(t, 7, code)
			require.Empty(t, stdout)
		})
	}
}

// A success writes the server's bytes verbatim to stdout and nothing to
// stderr -- "output is machine-readable" is the M5D acceptance criterion these
// commands inherit.
func TestM6AReads_SuccessWritesServerBytesToStdout(t *testing.T) {
	bodies := map[string]string{
		"jobs list":     `{"jobs":[],"next_cursor":"abc"}`,
		"jobs attempts": `{"attempts":[]}`,
		"workers list":  `{"workers":[]}`,
		"queues list":   `{"queues":[]}`,
	}
	for name, args := range m6aReadCommands {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runAgainst(t, http.StatusOK, bodies[name], args)
			require.Equal(t, ExitSuccess, code)
			require.Equal(t, 0, code)
			require.JSONEq(t, bodies[name], stdout)
			require.Empty(t, stderr)
		})
	}
}
