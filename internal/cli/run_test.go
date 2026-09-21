package cli

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// recordingServer captures the last request it received and answers with a
// fixed status and body, so a test can assert both what taskforge-cli sent
// and what it did with the response.
type recordingServer struct {
	*httptest.Server
	lastMethod string
	lastPath   string
	lastBody   []byte
}

func newRecordingServer(t *testing.T, status int, body string) *recordingServer {
	t.Helper()
	rs := &recordingServer{}
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rs.lastMethod = r.Method
		rs.lastPath = r.URL.String()
		reqBody, _ := io.ReadAll(r.Body)
		rs.lastBody = reqBody
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(rs.Close)
	return rs
}

func run(t *testing.T, server *httptest.Server, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var outBuf, errBuf strings.Builder
	fullArgs := append([]string{"--api-url", server.URL}, args...)
	code = Run(context.Background(), fullArgs, &outBuf, &errBuf)
	return code, outBuf.String(), errBuf.String()
}

func TestCommands_HappyPaths(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want map[int]bool // acceptable success statuses for this route
	}{
		{"jobs submit", []string{"jobs", "submit", "--queue", "default", "--job-type", "demo.echo", "--payload", `{"message":"hi"}`}, jobSuccess},
		{"jobs get", []string{"jobs", "get", "job-1"}, readSuccess},
		{"jobs result", []string{"jobs", "result", "job-1"}, readSuccess},
		{"jobs cancel", []string{"jobs", "cancel", "job-1"}, readSuccess},
		{"jobs retry", []string{"jobs", "retry", "job-1"}, jobSuccess},
		{"dlq list", []string{"dlq", "list"}, readSuccess},
		{"dlq replay", []string{"dlq", "replay", "job-1"}, jobSuccess},
		{"api-keys create", []string{"api-keys", "create", "--scope", "s", "--name", "n"}, map[int]bool{201: true}},
		{"api-keys list", []string{"api-keys", "list"}, readSuccess},
		{"api-keys revoke", []string{"api-keys", "revoke", "key-1"}, readSuccess},
		{"worker-keys create", []string{"worker-keys", "create", "--scope", "s", "--name", "n"}, map[int]bool{201: true}},
		{"worker-keys list", []string{"worker-keys", "list"}, readSuccess},
		{"worker-keys revoke", []string{"worker-keys", "revoke", "key-1"}, readSuccess},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var status int
			for s := range tc.want {
				status = s
			}
			server := newRecordingServer(t, status, `{"ok":true}`)
			code, stdout, stderr := run(t, server.Server, tc.args...)
			require.Equal(t, ExitSuccess, code, "stderr: %s", stderr)
			require.JSONEq(t, `{"ok":true}`, stdout)
			require.Empty(t, stderr)
		})
	}
}

func TestCmdJobsSubmit_SendsExactRequestBody(t *testing.T) {
	server := newRecordingServer(t, http.StatusCreated, `{"id":"job-1"}`)
	code, _, stderr := run(t, server.Server,
		"jobs", "submit",
		"--queue", "default",
		"--job-type", "demo.echo",
		"--payload", `{"message":"hi"}`,
		"--priority", "70",
		"--max-attempts", "5",
		"--timeout-seconds", "45",
		"--capability", "cpu",
		"--capability", "gpu",
		"--idempotency-key", "fixed-key",
	)
	require.Equal(t, ExitSuccess, code, "stderr: %s", stderr)
	require.Equal(t, http.MethodPost, server.lastMethod)
	require.Equal(t, "/v1/jobs", server.lastPath)

	var got SubmitJobRequest
	require.NoError(t, json.Unmarshal(server.lastBody, &got))
	require.Equal(t, "default", got.Queue)
	require.Equal(t, "demo.echo", got.JobType)
	require.Equal(t, map[string]any{"message": "hi"}, got.Payload)
	require.Equal(t, 70, got.Priority)
	require.Equal(t, 5, got.MaxAttempts)
	require.Equal(t, 45, got.TimeoutSeconds)
	require.Equal(t, []string{"cpu", "gpu"}, got.RequiredCapabilities)
	require.Nil(t, got.ScheduledAt)
}

func TestCmdJobsSubmit_GeneratesIdempotencyKeyWhenOmitted(t *testing.T) {
	var gotKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	code, _, stderr := run(t, server, "jobs", "submit", "--queue", "default", "--job-type", "demo.echo", "--payload", `{}`)
	require.Equal(t, ExitSuccess, code, "stderr: %s", stderr)
	require.NotEmpty(t, gotKey)
}

func TestCmdJobsSubmit_RejectsMalformedPayloadWithoutCallingTheAPI(t *testing.T) {
	server := newRecordingServer(t, http.StatusOK, `{}`)
	code, stdout, stderr := run(t, server.Server,
		"jobs", "submit", "--queue", "default", "--job-type", "demo.echo", "--payload", `not json`)
	require.Equal(t, ExitUsageError, code)
	require.Empty(t, stdout)
	require.Contains(t, stderr, "usage_error")
	require.Empty(t, server.lastPath, "the API must never be called for a CLI-side validation failure")
}

func TestCmdAPIKeysCreate_NeverSendsAuthorizationHeader(t *testing.T) {
	var sawAuth bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = r.Header.Get("Authorization") != ""
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"key":"tfk_x.y"}`))
	}))
	defer server.Close()

	var outBuf, errBuf strings.Builder
	args := []string{"--api-url", server.URL, "--api-key", "tfk_should.notbesent", "api-keys", "create", "--scope", "s", "--name", "n"}
	code := Run(context.Background(), args, &outBuf, &errBuf)
	require.Equal(t, ExitSuccess, code, "stderr: %s", errBuf.String())
	require.False(t, sawAuth, "--api-key must not be presented to the unauthenticated key-management routes")
}

func TestCmdDLQList_PassesLimitAndCursor(t *testing.T) {
	server := newRecordingServer(t, http.StatusOK, `{"entries":[]}`)
	code, _, stderr := run(t, server.Server, "dlq", "list", "--limit", "5", "--cursor", "abc")
	require.Equal(t, ExitSuccess, code, "stderr: %s", stderr)
	require.Equal(t, "/v1/dlq?cursor=abc&limit=5", server.lastPath)
}

func TestRun_MissingJobIDArgument(t *testing.T) {
	server := newRecordingServer(t, http.StatusOK, `{}`)
	for _, verb := range []string{"get", "result", "cancel", "retry"} {
		t.Run(verb, func(t *testing.T) {
			code, stdout, stderr := run(t, server.Server, "jobs", verb)
			require.Equal(t, ExitUsageError, code)
			require.Empty(t, stdout)
			require.Contains(t, stderr, "usage_error")
		})
	}
}

func TestRun_NoArguments(t *testing.T) {
	var outBuf, errBuf strings.Builder
	code := Run(context.Background(), nil, &outBuf, &errBuf)
	require.Equal(t, ExitUsageError, code)
	require.Empty(t, outBuf.String())
}

func TestRun_Help(t *testing.T) {
	var outBuf, errBuf strings.Builder
	code := Run(context.Background(), []string{"--help"}, &outBuf, &errBuf)
	require.Equal(t, ExitSuccess, code)
}

func TestRun_APIKeyFlagOverridesEnv(t *testing.T) {
	t.Setenv("TASKFORGE_CLI_API_KEY", "tfk_env.value")
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	var outBuf, errBuf strings.Builder
	args := []string{"--api-url", server.URL, "--api-key", "tfk_flag.value", "jobs", "get", "job-1"}
	code := Run(context.Background(), args, &outBuf, &errBuf)
	require.Equal(t, ExitSuccess, code, "stderr: %s", errBuf.String())
	require.Equal(t, "Bearer tfk_flag.value", gotAuth)
}

func TestRun_APIKeyFromEnvWhenFlagAbsent(t *testing.T) {
	t.Setenv("TASKFORGE_CLI_API_KEY", "tfk_env.value")
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	var outBuf, errBuf strings.Builder
	args := []string{"--api-url", server.URL, "jobs", "get", "job-1"}
	code := Run(context.Background(), args, &outBuf, &errBuf)
	require.Equal(t, ExitSuccess, code, "stderr: %s", errBuf.String())
	require.Equal(t, "Bearer tfk_env.value", gotAuth)
}

func TestRun_APIURLFromEnvWhenFlagAbsent(t *testing.T) {
	server := newRecordingServer(t, http.StatusOK, `{}`)
	t.Setenv("TASKFORGE_CLI_API_URL", server.URL)

	var outBuf, errBuf strings.Builder
	code := Run(context.Background(), []string{"jobs", "get", "job-1"}, &outBuf, &errBuf)
	require.Equal(t, ExitSuccess, code, "stderr: %s", errBuf.String())
	require.Equal(t, "/v1/jobs/job-1", server.lastPath)
}

// TestRun_IgnoresTheServersBindAddressVariable proves TASKFORGE_API_ADDR --
// taskforge-api's own bind address -- no longer influences where the CLI
// sends requests, even when it is set to a live server.
func TestRun_IgnoresTheServersBindAddressVariable(t *testing.T) {
	server := newRecordingServer(t, http.StatusOK, `{}`)
	t.Setenv("TASKFORGE_API_ADDR", strings.TrimPrefix(server.URL, "http://"))
	t.Setenv("TASKFORGE_CLI_API_URL", "http://127.0.0.1:1")

	var outBuf, errBuf strings.Builder
	code := Run(context.Background(), []string{"jobs", "get", "job-1"}, &outBuf, &errBuf)
	require.Equal(t, ExitTransportError, code, "the CLI must dial TASKFORGE_CLI_API_URL, not TASKFORGE_API_ADDR")
	require.Empty(t, server.lastPath)
}

func TestRun_InvalidAPIURLIsAUsageErrorBeforeAnyRequest(t *testing.T) {
	for name, args := range map[string][]string{
		"flag": {"--api-url", "127.0.0.1:8080", "jobs", "get", "job-1"},
		"env":  {"jobs", "get", "job-1"},
	} {
		t.Run(name, func(t *testing.T) {
			if name == "env" {
				t.Setenv("TASKFORGE_CLI_API_URL", "127.0.0.1:8080")
			}
			var outBuf, errBuf strings.Builder
			code := Run(context.Background(), args, &outBuf, &errBuf)
			require.Equal(t, ExitUsageError, code)
			require.Empty(t, outBuf.String())
			require.Contains(t, errBuf.String(), "absolute http(s) URL")
		})
	}
}
