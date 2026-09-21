package cli

// Exit codes are taskforge-cli's stable, scripted contract --
// docs/ROADMAP.md's M5D acceptance criterion is "CLI exit codes are stable
// and output is machine-readable". Once documented here, a code's meaning
// must never change: a script that branches on it today must keep working
// tomorrow. Adding a new code for a genuinely new failure class is fine;
// repurposing an existing one is a breaking change to this CLI's contract,
// exactly as a route's response shape is to the HTTP API's.
//
// The set is grouped by REMEDIATION -- what a caller's script should
// actually do differently -- not by HTTP status or by api/openapi.yaml's
// Error.code enum one-for-one. Two API error codes share an exit code only
// when a caller would take the identical next action for both; see each
// constant's comment for exactly which codes it covers and why. No exit
// code is a general "something else went wrong" bucket: ExitUnexpectedResponse
// is deliberately the narrowest code here (see its own comment), and every
// other code has an exhaustive, enumerated membership checked by
// TestApiErrorExitCodes_CoversExactlyTheReachableSet.
const (
	// ExitSuccess is the only exit code that means the requested operation
	// completed. No other code is ever used for a partial or ambiguous
	// success, and this code is never returned for anything else --
	// TestExitCodes_OnlySuccessIsZero checks both directions.
	ExitSuccess = 0

	// ExitUsageError means taskforge-cli itself rejected the invocation --
	// an unknown resource or verb, a missing or malformed flag, or a
	// missing required argument. No HTTP request was made.
	// Remediation: fix the command line; nothing about the API is at
	// fault.
	ExitUsageError = 1

	// ExitRequestRejected means the API told this specific request it was
	// malformed or invalid, and a DIFFERENT request is needed before
	// retrying: HTTP 400 malformed_json, HTTP 413 payload_too_large, and
	// HTTP 422 validation_failed / invalid_cursor. All four share one
	// remediation -- change the input and resubmit -- which is what makes
	// them one class rather than four; retrying the identical request
	// will fail identically every time.
	ExitRequestRejected = 2

	// ExitUnauthorized means the presented credential (or its absence) was
	// refused: HTTP 401 unauthorized. Remediation: obtain or configure a
	// valid API key (see --api-key / TASKFORGE_CLI_API_KEY).
	ExitUnauthorized = 3

	// ExitNotFound means the named resource does not exist within the
	// credential's scope: HTTP 404 not_found. Remediation: check the id
	// and the scope of the credential in use -- a malformed id and a job
	// in another scope are indistinguishable by design (docs/CURRENT_STATE.md),
	// so this code alone does not tell a script which.
	ExitNotFound = 4

	// ExitConflict means the operation cannot be applied given the
	// resource's current durable state, and the API is telling the caller
	// not to blindly retry: HTTP 409 idempotency_conflict,
	// job_not_cancelable, and job_not_dead_lettered. The remediation
	// differs by code (present a fresh Idempotency-Key vs. inspect the
	// job's current status), which the server's own printed message
	// states -- but "retrying the identical request will not help" is the
	// one fact all three share, which is what makes them one class here.
	ExitConflict = 5

	// ExitInternalError means the API's own request failed for a reason it
	// sanitizes rather than explains: HTTP 500 internal_error.
	// Remediation: the message's request_id locates the server-side
	// cause in its logs; this is an operator/server-side problem, not a
	// caller mistake, and is not necessarily safe to retry.
	ExitInternalError = 6

	// ExitServiceUnavailable means the request's own server-side deadline
	// elapsed before its outcome was known: HTTP 503 service_unavailable.
	// Every endpoint that can return this documents its own retry guidance
	// in the response message (see api/openapi.yaml) -- most are
	// unconditionally safe to repeat, but POST /internal/v1/api-keys and
	// POST /internal/v1/worker-keys explicitly are NOT, because key
	// creation carries no idempotency identity and a blind retry mints a
	// second credential. The exit code alone does not tell a script
	// whether to retry; the printed message (passed through verbatim from
	// the server) does.
	ExitServiceUnavailable = 7

	// ExitTransportError means the request never reached the API at all:
	// DNS failure, connection refused, TLS failure, or a client-side
	// timeout below the HTTP layer (see Client.do and TransportError).
	// Remediation: check --api-url / TASKFORGE_CLI_API_URL and network
	// reachability. This is distinct from ExitServiceUnavailable, which
	// means the API WAS reached and itself reported that its own deadline
	// elapsed.
	ExitTransportError = 8

	// ExitUnexpectedResponse means the API returned a response this CLI
	// does not recognize: an HTTP status this CLI did not expect for the
	// operation called, a response body that is not valid JSON where JSON
	// was expected, or an error `code` outside apiErrorExitCodes. It
	// signals a version mismatch between this CLI and the server it
	// talked to, and is deliberately never used as a fallback for a
	// recognized failure that simply "wasn't handled": every Error.code
	// value that a route this CLI calls can actually return, per
	// api/openapi.yaml, is enumerated in apiErrorExitCodes.
	ExitUnexpectedResponse = 9
)

// apiErrorExitCodes maps every api/openapi.yaml Error.code value that a
// route this CLI calls can actually return to its exit code. It is
// exhaustive for that reachable set -- verified line by line against every
// response documented for POST/GET /v1/jobs, /v1/jobs/{job_id},
// /v1/jobs/{job_id}/result, /v1/jobs/{job_id}/cancel,
// /v1/jobs/{job_id}/retry, GET /v1/dlq, POST /v1/dlq/{job_id}/replay, and
// the four /internal/v1/{api-keys,worker-keys}* routes -- not for the full
// 22-value enum in api/openapi.yaml's Error schema, most of which
// (worker_session_conflict, claim_conflict, fence_rejected, lease_expired,
// renewal_conflict, attempt_timed_out, outcome_conflict,
// cancellation_requested, worker_session_unavailable, unknown_queue,
// method_not_allowed) belongs to the worker-control surface or to
// documented-but-unreachable-here cases this CLI never triggers.
// TestApiErrorExitCodes_CoversExactlyTheReachableSet pins this set so it
// cannot silently drift from the routes above.
var apiErrorExitCodes = map[string]int{
	"malformed_json":        ExitRequestRejected,
	"payload_too_large":     ExitRequestRejected,
	"validation_failed":     ExitRequestRejected,
	"invalid_cursor":        ExitRequestRejected,
	"unauthorized":          ExitUnauthorized,
	"not_found":             ExitNotFound,
	"idempotency_conflict":  ExitConflict,
	"job_not_cancelable":    ExitConflict,
	"job_not_dead_lettered": ExitConflict,
	"internal_error":        ExitInternalError,
	"service_unavailable":   ExitServiceUnavailable,
}

// exitCodeForAPIError resolves a server-reported error code to this CLI's
// exit code. ok is false for any code outside apiErrorExitCodes, which the
// caller must treat as ExitUnexpectedResponse rather than guessing at a
// meaning this CLI was not built to recognize.
func exitCodeForAPIError(code string) (exitCode int, ok bool) {
	exitCode, ok = apiErrorExitCodes[code]
	return exitCode, ok
}
