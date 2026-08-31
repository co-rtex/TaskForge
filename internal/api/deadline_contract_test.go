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
		Responses map[string]struct {
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
			CodeRenewalConflict,
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
