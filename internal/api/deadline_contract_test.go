package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	yaml "go.yaml.in/yaml/v3"

	"github.com/co-rtex/TaskForge/internal/workers"
)

// deadline503Message returns the shared 503 body the API actually emits, taken
// from a real response rather than from a constant, so the assertions below
// cannot drift away from what a client receives.
func deadline503Message(t *testing.T) string {
	t.Helper()
	control := &fakeWorkerControl{
		claim: func(context.Context, string, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{}, fmt.Errorf("lock queue capacity: %w", workers.ErrDeadlineExceeded)
		},
	}
	body := fmt.Sprintf(
		`{"worker_id":%q,"worker_session_id":%q,"claim_request_id":%q,"queue":"default"}`,
		uuid.NewString(), uuid.NewString(), uuid.NewString())
	recorder := httptest.NewRecorder()
	newWorkerControlHandler(control).ServeHTTP(recorder,
		httptest.NewRequest(http.MethodPost, "/internal/v1/claims", strings.NewReader(body)))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)

	var envelope ErrorBody
	require.NoError(t, json.NewDecoder(recorder.Body).Decode(&envelope))
	require.Equal(t, CodeServiceUnavailable, envelope.Error.Code)
	return envelope.Error.Message
}

// TestDeadline503Message_IsEndpointNeutral guards the one string that four
// different operations share. It previously told every caller to send a claim
// request id and to read a job back, which is wrong for registration and for
// the fenced attempt transitions.
func TestDeadline503Message_IsEndpointNeutral(t *testing.T) {
	message := strings.ToLower(deadline503Message(t))

	for _, forbidden := range []string{
		"claim request id",
		"claim_request_id",
		"read the job",
		"read back",
		"job back",
		"waiting on",
		"lock",
		"nothing was committed",
		"nothing committed",
		"rolled back",
		"no changes were made",
	} {
		require.NotContains(t, message, forbidden,
			"the shared message must not make an endpoint-specific or over-strong promise")
	}

	require.Contains(t, message, "deadline")
	require.Contains(t, message, "retry the identical request",
		"the shared message must give guidance every operation can follow")
	require.Contains(t, message, "idempotent",
		"it may explain why an identical retry is safe")
}

type openAPIDoc struct {
	Paths map[string]map[string]struct {
		Description string `yaml:"description"`
		Responses   map[string]struct {
			Description string `yaml:"description"`
			Content     map[string]struct {
				Example struct {
					Error struct {
						Code    string `yaml:"code"`
						Message string `yaml:"message"`
					} `yaml:"error"`
				} `yaml:"example"`
			} `yaml:"content"`
		} `yaml:"responses"`
	} `yaml:"paths"`
}

func loadOpenAPI(t *testing.T) openAPIDoc {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "api", "openapi.yaml"))
	require.NoError(t, err)
	var doc openAPIDoc
	require.NoError(t, yaml.Unmarshal(raw, &doc))
	require.NotEmpty(t, doc.Paths)
	return doc
}

// flatten collapses YAML block-scalar line wrapping so a contract assertion
// tests the wording, not where the source file happened to break a line.
func flatten(text string) string {
	return strings.ToLower(strings.Join(strings.Fields(text), " "))
}

func describe503(t *testing.T, doc openAPIDoc, method, path string) (string, string) {
	t.Helper()
	operation, ok := doc.Paths[path][method]
	require.Truef(t, ok, "%s %s is missing from the spec", method, path)
	response, ok := operation.Responses["503"]
	require.Truef(t, ok, "%s %s has no documented 503", method, path)
	example := response.Content["application/json"].Example.Error
	require.Equal(t, "service_unavailable", example.Code)
	return response.Description, example.Message
}

// TestOpenAPI_Deadline503GuidanceIsPerEndpoint proves each worker-control
// operation documents the identity or fence its own caller should replay,
// instead of the one-size-fits-all text that was wrong for three of the four.
func TestOpenAPI_Deadline503GuidanceIsPerEndpoint(t *testing.T) {
	doc := loadOpenAPI(t)

	t.Run("registration names its session identity and nothing else", func(t *testing.T) {
		description, _ := describe503(t, doc, "put", "/internal/v1/worker-sessions/{worker_session_id}")
		lower := strings.ToLower(description)
		require.Contains(t, lower, "worker_session_id")
		require.Contains(t, lower, "registration")
		require.NotContains(t, lower, "claim_request_id",
			"registration has no claim request id to replay")
		require.NotContains(t, lower, "claim request id")
		require.NotContains(t, lower, "read the job",
			"registration produces no job to read back")
		require.NotContains(t, lower, "job back")
		require.NotContains(t, lower, "attempt fence")
	})

	t.Run("claim names its claim identity", func(t *testing.T) {
		description, _ := describe503(t, doc, "post", "/internal/v1/claims")
		lower := strings.ToLower(description)
		require.Contains(t, lower, "claim_request_id")
		require.Contains(t, lower, "worker_session_id")
	})

	for _, transition := range []string{"start", "succeed"} {
		t.Run(transition+" names its attempt fence", func(t *testing.T) {
			description, _ := describe503(t, doc, "post",
				"/internal/v1/attempts/{attempt_id}/"+transition)
			lower := strings.ToLower(description)
			require.Contains(t, lower, "attempt fence")
			for _, field := range []string{"attempt_id", "job_id", "lease_id", "worker_id", "worker_session_id"} {
				require.Containsf(t, lower, field, "%s must name %s as part of its fence", transition, field)
			}
			require.NotContains(t, lower, "claim_request_id",
				transition+" replays a fence, not a claim identity")
		})
	}

	t.Run("heartbeat names its session identity and promises no rollback", func(t *testing.T) {
		description, _ := describe503(t, doc, "post",
			"/internal/v1/worker-sessions/{worker_session_id}/heartbeat")
		lower := strings.ToLower(description)
		require.Contains(t, lower, "worker_session_id")
		require.Contains(t, lower, "worker_id")
		require.NotContains(t, lower, "claim_request_id",
			"a heartbeat has no claim identity to replay")
		require.NotContains(t, lower, "attempt fence")
		// A heartbeat that advances the receipt time a second time is correct
		// behavior, so the contract must not promise the retry changes nothing.
		require.Contains(t, lower, "advance",
			"the contract must say a heartbeat replay may advance the receipt time again")
		require.Contains(t, lower, "never create a session")
	})

	// Failure reporting and cancellation acknowledgment are the other two
	// operations where a fresh identity on a retry is actively harmful, and the
	// contract has to forbid it rather than merely not recommend it.
	for _, outcome := range []string{"fail", "cancel"} {
		t.Run(outcome+" forbids a fresh outcome identity on retry", func(t *testing.T) {
			description, _ := describe503(t, doc, "post",
				"/internal/v1/attempts/{attempt_id}/"+outcome)
			lower := strings.ToLower(description)
			for _, field := range []string{
				"attempt_id", "job_id", "lease_id", "worker_id", "worker_session_id",
				"outcome_request_id",
			} {
				require.Containsf(t, lower, field, "%s must name %s as part of what to replay", outcome, field)
			}
			require.Contains(t, flatten(description), "complete identical request",
				outcome+" must require the whole request to be repeated, not just its identifiers")
			require.Contains(t, flatten(description), "forbidden",
				"a fresh outcome request id after an ambiguous response must be forbidden, not discouraged")
			require.NotContains(t, lower, "claim_request_id",
				outcome+" replays an outcome identity, not a claim identity")
		})
	}

	// The failure contract additionally has to say WHY a fresh identity is
	// forbidden, because "just reuse it" is the kind of rule that gets optimized
	// away by someone who does not know what it costs.
	t.Run("failure explains what a fresh identity would cost", func(t *testing.T) {
		description, _ := describe503(t, doc, "post", "/internal/v1/attempts/{attempt_id}/fail")
		flat := flatten(description)
		require.Contains(t, flat, "attempt budget")
		require.Contains(t, flat, "jitter")
		require.Contains(t, flat, "failure_class")
		require.Contains(t, flat, "error_code")
	})

	// Renewal is the one operation where retrying with a NEW identity would
	// silently double the caller's authority. The contract has to say so.
	t.Run("renewal names its full fence, identity, and expected generation", func(t *testing.T) {
		description, _ := describe503(t, doc, "post", "/internal/v1/leases/{lease_id}/renew")
		lower := strings.ToLower(description)
		for _, field := range []string{
			"lease_id", "job_id", "attempt_id", "worker_id", "worker_session_id",
			"renewal_request_id", "expected_renewal_version",
		} {
			require.Containsf(t, lower, field, "renewal must name %s as part of what to replay", field)
		}
		require.Contains(t, flatten(description), "extend the lease twice",
			"the contract must warn that a fresh renewal identity on a retry double-extends")
		require.NotContains(t, lower, "claim_request_id",
			"renewal replays a renewal identity, not a claim identity")
	})
}

// TestOpenAPI_DocumentsEveryImplementedRouteAndErrorCode keeps the spec from
// silently falling behind the handlers. AGENTS.md section 8 forbids documenting
// unbuilt behavior; this is the other direction — built behavior that never made
// it into the document.
func TestOpenAPI_DocumentsEveryImplementedRouteAndErrorCode(t *testing.T) {
	doc := loadOpenAPI(t)

	t.Run("every implemented worker-control route is documented", func(t *testing.T) {
		for path, method := range map[string]string{
			"/internal/v1/worker-sessions/{worker_session_id}":           "put",
			"/internal/v1/worker-sessions/{worker_session_id}/heartbeat": "post",
			"/internal/v1/claims":                        "post",
			"/internal/v1/leases/{lease_id}/renew":       "post",
			"/internal/v1/attempts/{attempt_id}/start":   "post",
			"/internal/v1/attempts/{attempt_id}/succeed": "post",
			"/internal/v1/attempts/{attempt_id}/fail":    "post",
			"/internal/v1/attempts/{attempt_id}/cancel":  "post",
		} {
			operations, ok := doc.Paths[path]
			require.Truef(t, ok, "%s is implemented but missing from the spec", path)
			_, ok = operations[method]
			require.Truef(t, ok, "%s %s is implemented but missing from the spec", method, path)
		}
	})

	// The enum is the client's branching contract, so a code the handlers can
	// emit but the spec never lists is a broken contract.
	t.Run("every stable error code appears in the spec enum", func(t *testing.T) {
		raw, err := os.ReadFile(filepath.Join("..", "..", "api", "openapi.yaml"))
		require.NoError(t, err)
		var document struct {
			Components struct {
				Schemas struct {
					Error struct {
						Properties struct {
							Error struct {
								Properties struct {
									Code struct {
										Enum []string `yaml:"enum"`
									} `yaml:"code"`
								} `yaml:"properties"`
							} `yaml:"error"`
						} `yaml:"properties"`
					} `yaml:"Error"`
				} `yaml:"schemas"`
			} `yaml:"components"`
		}
		require.NoError(t, yaml.Unmarshal(raw, &document))
		documented := document.Components.Schemas.Error.Properties.Error.Properties.Code.Enum
		require.NotEmpty(t, documented)

		for _, code := range []string{
			CodeValidationFailed, CodeMalformedJSON, CodeUnknownQueue, CodeIdempotencyConflict,
			CodeNotFound, CodePayloadTooLarge, CodeMethodNotAllowed, CodeInternal,
			CodeServiceUnavailable, CodeSessionConflict, CodeSessionUnavailable,
			CodeClaimConflict, CodeFenceRejected, CodeLeaseExpired, CodeStateConflict,
			CodeRenewalConflict, CodeAttemptTimedOut, CodeOutcomeConflict,
			CodeNotCancelable, CodeNotDeadLettered, CodeInvalidCursor,
			CodeCancellationRequested,
		} {
			require.Containsf(t, documented, code, "error code %q is emitted but undocumented", code)
		}
	})
}

// TestOpenAPI_Deadline503NeverOverPromises pins the two claims the old contract
// made that could not be honored: that the request had been waiting on a lock,
// and that nothing was committed.
func TestOpenAPI_Deadline503NeverOverPromises(t *testing.T) {
	doc := loadOpenAPI(t)
	// Every worker-control operation, including the two M3 added. A 503 that
	// over-promises on one endpoint is exactly as wrong as on any other.
	operations := []struct{ method, path string }{
		{"put", "/internal/v1/worker-sessions/{worker_session_id}"},
		{"post", "/internal/v1/worker-sessions/{worker_session_id}/heartbeat"},
		{"post", "/internal/v1/claims"},
		{"post", "/internal/v1/leases/{lease_id}/renew"},
		{"post", "/internal/v1/attempts/{attempt_id}/start"},
		{"post", "/internal/v1/attempts/{attempt_id}/succeed"},
		{"post", "/internal/v1/attempts/{attempt_id}/fail"},
		{"post", "/internal/v1/attempts/{attempt_id}/cancel"},
	}

	for _, operation := range operations {
		method, path := operation.method, operation.path
		t.Run(method+" "+path, func(t *testing.T) {
			description, message := describe503(t, doc, method, path)
			combined := flatten(description + " " + message)

			// Ambiguity is acknowledged, not papered over.
			require.Contains(t, combined, "during commit",
				"the spec must name COMMIT as a place the deadline can land")
			require.Contains(t, combined, "ambiguous",
				"a deadline during COMMIT must be described as ambiguous")
			require.Contains(t, combined, "never asserts that nothing was committed",
				"the spec must say explicitly that it does not promise rollback")

			// The deadline is not described as necessarily a lock wait: the spec
			// lists lock acquisition as one of three places it can land.
			require.Contains(t, combined, "while acquiring a lock, while executing a statement, or during commit")

			// The example body is the endpoint-neutral shared message.
			flatMessage := flatten(message)
			require.Contains(t, flatMessage, "retry the identical request")
			require.NotContains(t, flatMessage, "read the job back")
			require.NotContains(t, flatMessage, "claim request id")
			require.NotContains(t, flatMessage, "nothing was committed")
		})
	}
}

// TestOpenAPI_DocumentsEveryImplementedPublicRoute is the public half of the
// same guard. A route a client can call but the spec never mentions is exactly
// as broken a contract as an undocumented internal one.
func TestOpenAPI_DocumentsEveryImplementedPublicRoute(t *testing.T) {
	doc := loadOpenAPI(t)
	for path, method := range map[string]string{
		"/v1/jobs":                 "post",
		"/v1/jobs/{job_id}":        "get",
		"/v1/jobs/{job_id}/cancel": "post",
		"/v1/jobs/{job_id}/retry":  "post",
		"/v1/dlq":                  "get",
		"/v1/dlq/{job_id}/replay":  "post",
		"/healthz":                 "get",
		"/readyz":                  "get",
	} {
		operations, ok := doc.Paths[path]
		require.Truef(t, ok, "%s is implemented but missing from the spec", path)
		_, ok = operations[method]
		require.Truef(t, ok, "%s %s is implemented but missing from the spec", method, path)
	}
}

// TestOpenAPI_ReplayRoutesShareOneIdempotencyNamespace pins the property that
// makes two routes for one operation safe. If the spec ever stopped saying they
// share an identity namespace, an operator could reasonably expect
// /retry and /replay to be independent — and get two jobs from one intent.
func TestOpenAPI_ReplayRoutesShareOneIdempotencyNamespace(t *testing.T) {
	doc := loadOpenAPI(t)

	retry, ok := doc.Paths["/v1/jobs/{job_id}/retry"]["post"]
	require.True(t, ok)
	require.Contains(t, flatten(retry.Responses["200"].Description), "already created the replacement")

	replay, ok := doc.Paths["/v1/dlq/{job_id}/replay"]["post"]
	require.True(t, ok)
	require.Contains(t, flatten(replay.Responses["201"].Description), "replacement job was created")

	// The shared namespace is described in the document's own idempotency
	// section, where a reader looks for it.
	raw, err := os.ReadFile(filepath.Join("..", "..", "api", "openapi.yaml"))
	require.NoError(t, err)
	description := flatten(string(raw))
	require.Contains(t, description, "same operation and require an `idempotency-key`")
	require.Contains(t, description, "one identity namespace")
	require.Contains(t, description,
		"different keys deliberately create different replacement jobs")
}

// TestOpenAPI_CancellationContractIsExplicitAboutAttempts guards the two facts
// an operator most needs and a spec most easily loses: cancelling a job that has
// not been claimed creates no attempt at all, and cancelling one that has does
// not immediately erase its history.
func TestOpenAPI_CancellationContractIsExplicitAboutAttempts(t *testing.T) {
	doc := loadOpenAPI(t)
	operation, ok := doc.Paths["/v1/jobs/{job_id}/cancel"]["post"]
	require.True(t, ok)
	description := flatten(operation.Description)

	require.Contains(t, description, "no attempt is created")
	require.Contains(t, description, "cancel_requested`")
	require.Contains(t, description, "never produces a retry and never creates a dead-letter entry")
	require.Contains(t, description, "no request identity")
}

// TestOpenAPI_PublicMutatorsDocumentTheirOwnAmbiguity extends the worker-control
// rule to the public surface.
//
// The three public mutating routes could previously only answer 500 when their
// deadline elapsed, which tells an operator a bug happened and gives no
// guidance at all. An operator cancelling a runaway job then has to guess, and
// the cautious guess — do not retry — is the one that leaves the job running.
func TestOpenAPI_PublicMutatorsDocumentTheirOwnAmbiguity(t *testing.T) {
	doc := loadOpenAPI(t)

	publicMutators := []struct{ method, path string }{
		{"post", "/v1/jobs/{job_id}/cancel"},
		{"post", "/v1/jobs/{job_id}/retry"},
		{"post", "/v1/dlq/{job_id}/replay"},
	}

	for _, operation := range publicMutators {
		method, path := operation.method, operation.path
		t.Run(method+" "+path, func(t *testing.T) {
			description, message := describe503(t, doc, method, path)
			combined := flatten(description + " " + message)

			require.Contains(t, combined, "while acquiring a lock, while executing a statement, or during commit")
			require.Contains(t, combined, "ambiguous")
			require.Contains(t, combined, "never asserts that nothing was committed",
				"the spec must not promise a rollback it cannot guarantee")
			require.Contains(t, flatten(message), "deadline")
		})
	}

	// Cancellation's identity is the job id, and it has no request identity at
	// all. Telling a caller to reuse an Idempotency-Key here would be nonsense.
	t.Run("cancellation names the job id as its whole identity", func(t *testing.T) {
		description, message := describe503(t, doc, "post", "/v1/jobs/{job_id}/cancel")
		flat := flatten(description)
		require.Contains(t, flat, "job_id")
		require.Contains(t, flat, "never cancels twice")
		require.Contains(t, flat, "already_requested")
		require.NotContains(t, flat, "idempotency-key",
			"cancellation carries no request identity, so it must not ask for one")
		require.Contains(t, flatten(message), "keyed by scope and job id alone")
	})

	// Retry and replay share one identity namespace, and a fresh key after an
	// ambiguous response is the specific mistake that duplicates work.
	for _, path := range []string{"/v1/jobs/{job_id}/retry", "/v1/dlq/{job_id}/replay"} {
		t.Run(path+" forbids a fresh key after an ambiguous response", func(t *testing.T) {
			description, message := describe503(t, doc, "post", path)
			flat := flatten(description)
			require.Contains(t, flat, "idempotency-key")
			require.Contains(t, flat, "complete identical request",
				"the whole request must be repeated, not just its identifiers")
			require.Contains(t, flat, "forbidden",
				"a fresh key must be forbidden, not merely discouraged")
			require.Contains(t, flat, "second replacement job",
				"the contract must say what a fresh key would actually cost")
			require.Contains(t, flat, "one identity namespace",
				"an ambiguous response from either route may be retried through either")
			require.Contains(t, flatten(message), "idempotency-key")
		})
	}
}

// TestOpenAPI_StartDocumentsTheCancelFirstRefusal pins the one 409 on this route
// whose correct handling is the opposite of every other 409's.
//
// Every other conflict Start can report means this worker no longer holds the
// attempt, so dropping it is right. A cancellation that won before Start means
// the worker still holds it and is the only party that can end it promptly. A
// client that cannot tell them apart leaves the job in CANCEL_REQUESTED for the
// rest of its lease window.
func TestOpenAPI_StartDocumentsTheCancelFirstRefusal(t *testing.T) {
	doc := loadOpenAPI(t)
	operation, ok := doc.Paths["/internal/v1/attempts/{attempt_id}/start"]["post"]
	require.True(t, ok)

	response, ok := operation.Responses["409"]
	require.True(t, ok)
	flat := flatten(response.Description)
	require.Contains(t, flat, "cancellation_requested")
	require.Contains(t, flat, "cancel_requested")
	require.Contains(t, flat, "/internal/v1/attempts/{attempt_id}/cancel",
		"the contract must name the acknowledgment route the worker should call")
	require.Contains(t, flat, "same fence")

	example := response.Content["application/json"].Example.Error
	require.Equal(t, CodeCancellationRequested, example.Code)
}
