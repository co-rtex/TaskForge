package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/google/uuid"
)

const usageText = `taskforge-cli -- command-line client for the TaskForge public API and its
loopback-only credential-management routes.

Usage:
  taskforge-cli [--api-url URL] [--api-key KEY] <resource> <verb> [flags]

Global flags:
  --api-url string   API base URL, an absolute http(s) URL. Overrides
                      TASKFORGE_CLI_API_URL. Default: http://127.0.0.1:8080.
  --api-key string   API key presented as "Authorization: Bearer". Overrides
                      TASKFORGE_CLI_API_KEY. Not sent to the /internal/v1
                      key-management routes, which are themselves
                      unauthenticated.

Resources and verbs:
  jobs submit    --queue --job-type --payload [--priority] [--max-attempts]
                 [--timeout-seconds] [--scheduled-at] [--capability]...
                 [--idempotency-key]
  jobs get       <job_id>
  jobs result    <job_id>
  jobs cancel    <job_id>
  jobs retry     <job_id> [--idempotency-key]
  dlq list       [--limit] [--cursor]
  dlq replay     <job_id> [--idempotency-key]
  api-keys create   --scope --name
  api-keys list     [--limit]
  api-keys revoke   <key_id>
  worker-keys create --scope --name
  worker-keys list   [--limit]
  worker-keys revoke <key_id>

A success response is written as JSON to stdout. A failure writes a JSON
error object to stderr and prints nothing to stdout; the process exit code
is stable and documented in internal/cli/exitcode.go and README.md.
`

// command runs one resource+verb, with args already stripped of the global
// flags and the resource/verb tokens themselves. It returns the process
// exit code and never calls os.Exit itself.
type command func(ctx context.Context, client *Client, args []string, stdout, stderr io.Writer) int

var commands = map[string]command{
	"jobs submit": cmdJobsSubmit,
	"jobs get":    cmdJobsGet,
	"jobs result": cmdJobsResult,
	"jobs cancel": cmdJobsCancel,
	"jobs retry":  cmdJobsRetry,

	"dlq list":   cmdDLQList,
	"dlq replay": cmdDLQReplay,

	"api-keys create": cmdAPIKeysCreate,
	"api-keys list":   cmdAPIKeysList,
	"api-keys revoke": cmdAPIKeysRevoke,

	"worker-keys create": cmdWorkerKeysCreate,
	"worker-keys list":   cmdWorkerKeysList,
	"worker-keys revoke": cmdWorkerKeysRevoke,
}

// Run parses args (os.Args[1:], not including the program name), executes
// the named command against the resolved API address, writes the outcome
// to stdout (success) or stderr (failure) as one JSON object, and returns
// the process exit code -- one of the Exit* constants in exitcode.go. Run
// never calls os.Exit and never dials the network directly outside Client,
// so it is fully testable against an httptest.Server.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	globalFlags := flag.NewFlagSet("taskforge-cli", flag.ContinueOnError)
	globalFlags.SetOutput(stderr)
	apiURL := globalFlags.String("api-url", "", "API base URL (overrides TASKFORGE_CLI_API_URL)")
	apiKey := globalFlags.String("api-key", "", "API key (overrides TASKFORGE_CLI_API_KEY)")
	globalFlags.Usage = func() { fmt.Fprint(stderr, usageText) }

	if err := globalFlags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return ExitSuccess
		}
		return ExitUsageError
	}

	rest := globalFlags.Args()
	if len(rest) < 2 {
		fmt.Fprint(stderr, usageText)
		return ExitUsageError
	}
	resource, verb, rest := rest[0], rest[1], rest[2:]

	cmd, ok := commands[resource+" "+verb]
	if !ok {
		fmt.Fprintf(stderr, "unknown command: %s %s\n\n", resource, verb)
		fmt.Fprint(stderr, usageText)
		return ExitUsageError
	}

	baseURL, err := ResolveBaseURL(*apiURL, os.Getenv(APIURLEnv))
	if err != nil {
		return usageErrorf(stderr, "%v", err)
	}
	key := *apiKey
	if key == "" {
		key = os.Getenv("TASKFORGE_CLI_API_KEY")
	}
	client := NewClient(baseURL, key)

	return cmd(ctx, client, rest, stdout, stderr)
}

// apiErrorEnvelope is the minimal shape this CLI needs to read out of an
// api/openapi.yaml Error response -- just enough to classify the exit
// code. The bytes written to stderr are always resp.Body verbatim, never
// this re-encoded, so a field this CLI does not know about is never lost
// from what the caller sees.
type apiErrorEnvelope struct {
	Error struct {
		Code string `json:"code"`
	} `json:"error"`
}

// emit interprets a completed HTTP response against the set of status
// codes that mean success for this particular operation, writes the
// server's JSON body verbatim to the correct stream, and returns the exit
// code. It never re-marshals resp.Body: the server's bytes are the
// contract (see docs/adr/0015-result-storage.md's identical "proxy, not
// re-derive" reasoning for GET /v1/jobs/{job_id}/result).
func emit(resp *Response, successStatus map[int]bool, stdout, stderr io.Writer) int {
	if successStatus[resp.StatusCode] {
		writeLine(stdout, resp.Body)
		return ExitSuccess
	}

	var envelope apiErrorEnvelope
	if err := json.Unmarshal(resp.Body, &envelope); err != nil || envelope.Error.Code == "" {
		writeLine(stderr, unexpectedResponseBody(resp))
		return ExitUnexpectedResponse
	}
	exitCode, ok := exitCodeForAPIError(envelope.Error.Code)
	if !ok {
		writeLine(stderr, resp.Body)
		return ExitUnexpectedResponse
	}
	writeLine(stderr, resp.Body)
	return exitCode
}

func unexpectedResponseBody(resp *Response) []byte {
	body, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"code":        "unexpected_response",
			"message":     "the API returned a response this CLI does not recognize",
			"http_status": resp.StatusCode,
			"body":        string(resp.Body),
		},
	})
	return body
}

func writeLine(w io.Writer, body []byte) {
	fmt.Fprintln(w, strings.TrimRight(string(body), "\n"))
}

// transportResult reports a *TransportError to stderr as a JSON error
// object and returns ExitTransportError. Any other error from a Client
// method is a taskforge-cli construction bug and is treated identically,
// since Client.do never returns anything else (see
// TestClient_EveryCommandBuildsAValidRequest).
func transportResult(err error, stderr io.Writer) int {
	body, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"code":    "transport_error",
			"message": err.Error(),
		},
	})
	writeLine(stderr, body)
	return ExitTransportError
}

// usageErrorf reports a CLI-side usage mistake -- no HTTP request was
// made.
func usageErrorf(stderr io.Writer, format string, args ...any) int {
	body, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"code":    "usage_error",
			"message": fmt.Sprintf(format, args...),
		},
	})
	writeLine(stderr, body)
	return ExitUsageError
}

// parseFlags parses a subcommand's own flag.FlagSet. flag.ErrHelp (from
// -h/--help) is treated as a usage error here, not success: unlike the
// top-level --help, a subcommand's help request still means "no operation
// was performed", and ExitUsageError already means exactly that.
func parseFlags(fs *flag.FlagSet, stderr io.Writer, args []string) (ok bool, exitCode int) {
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return false, ExitUsageError
	}
	return true, ExitSuccess
}

func newIdempotencyKeyIfEmpty(key string) string {
	if key != "" {
		return key
	}
	return uuid.NewString()
}
