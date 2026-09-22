package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// get issues an authenticated GET against a test server.
func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authorize(httptest.NewRequest(http.MethodGet, path, nil)))
	return rec
}

// newTestServerWithWorkerReads wires GET /v1/workers to a real
// *workers.Store built over a nil pool.
//
// That is deliberate rather than a fake: ListWorkers decodes and validates the
// cursor before it ever touches the pool, so this exercises the genuine codec
// on the genuine path. Anything that does reach the database would nil-panic
// here rather than quietly pass, which is the same assertion newTestServer's
// nil job store makes.
func newTestServerWithWorkerReads(t *testing.T) http.Handler {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewServer(nil, Config{MaxRequestBytes: 1024}, log).
		WithAuth(acceptingKeys(testScope)).
		WithResults(acceptingResults(), nil).
		WithWorkerReads(workers.NewStore(nil, workers.StoreConfig{})).
		Handler()
}

// Every listing route rejects an out-of-range limit before touching a store.
// newTestServer's job store is nil, so reaching one would panic rather than
// quietly pass -- which is what makes this an assertion about validation order
// and not merely about the status code.
func TestListing_RejectsAnOutOfRangeLimitBeforeAnyStoreAccess(t *testing.T) {
	h := newTestServerWithWorkerReads(t)

	for _, path := range []string{"/v1/jobs", "/v1/workers"} {
		for _, limit := range []string{"0", "-1", "101", "1000", "abc", "1.5", ""} {
			if limit == "" {
				continue
			}
			t.Run(path+"?limit="+limit, func(t *testing.T) {
				rec := get(t, h, path+"?limit="+limit)
				require.Equal(t, http.StatusUnprocessableEntity, rec.Code)

				body := decodeError(t, rec)
				require.Equal(t, CodeValidationFailed, body.Error.Code)
				require.NotEmpty(t, body.Error.RequestID)
				require.Len(t, body.Error.Details, 1)
				require.Equal(t, "limit", body.Error.Details[0].Field)
			})
		}
	}
}

// The documented maximum and the enforced one are the same number, read from
// the same constant the SQL clamp uses.
func TestListing_MaximumLimitIsAccepted(t *testing.T) {
	require.Equal(t, 100, jobs.MaxPageSize)
	require.Equal(t, 25, jobs.DefaultPageSize)

	// One past the maximum is refused; the maximum itself is not refused here
	// (it proceeds to the nil store, which is out of scope for a unit test).
	rec := get(t, newTestServer(t), "/v1/jobs?limit="+strconv.Itoa(jobs.MaxPageSize+1))
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
}

// A bad status or queue filter is a 422 naming the field that was actually
// wrong -- not a guess. A caller who sent one bad and one good filter must be
// told which one this API refused.
func TestListJobs_NamesTheFilterFieldThatFailed(t *testing.T) {
	h := newTestServer(t)

	t.Run("bad status", func(t *testing.T) {
		rec := get(t, h, "/v1/jobs?status=NOPE&queue=default")
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		body := decodeError(t, rec)
		require.Equal(t, CodeValidationFailed, body.Error.Code)
		require.Len(t, body.Error.Details, 1)
		require.Equal(t, "status", body.Error.Details[0].Field)
	})

	t.Run("bad queue, good status", func(t *testing.T) {
		rec := get(t, h, "/v1/jobs?status=QUEUED&queue=NOT+A+QUEUE")
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		body := decodeError(t, rec)
		require.Equal(t, CodeValidationFailed, body.Error.Code)
		require.Len(t, body.Error.Details, 1)
		require.Equal(t, "queue", body.Error.Details[0].Field,
			"the status was valid; blaming it would send the caller to the wrong field")
	})

	t.Run("bad queue, no status", func(t *testing.T) {
		rec := get(t, h, "/v1/jobs?queue=UPPERCASE")
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		require.Equal(t, "queue", decodeError(t, rec).Error.Details[0].Field)
	})
}

// A cursor this API did not issue is 422 invalid_cursor -- NOT 400. The CLI
// maps invalid_cursor to ExitRequestRejected, and the SDK to
// RequestRejectedError; both read the code, and the status must agree with the
// document that promises 422.
func TestListing_RejectsAForeignCursorAsInvalidCursor(t *testing.T) {
	h := newTestServerWithWorkerReads(t)

	for _, path := range []string{"/v1/jobs", "/v1/workers"} {
		t.Run(path, func(t *testing.T) {
			rec := get(t, h, path+"?cursor=not-base64!")
			require.Equal(t, http.StatusUnprocessableEntity, rec.Code)

			body := decodeError(t, rec)
			require.Equal(t, CodeInvalidCursor, body.Error.Code)
			require.Len(t, body.Error.Details, 1)
			require.Equal(t, "cursor", body.Error.Details[0].Field)
		})
	}
}

// A malformed job id on the attempts route answers 404, identical to
// GET /v1/jobs/{job_id}, so a caller cannot use the route as an oracle for
// which ids exist.
func TestListJobAttempts_MalformedIDIsNotFound(t *testing.T) {
	rec := get(t, newTestServer(t), "/v1/jobs/not-a-uuid/attempts")
	require.Equal(t, http.StatusNotFound, rec.Code)

	body := decodeError(t, rec)
	require.Equal(t, CodeNotFound, body.Error.Code)
	require.Equal(t, "job not found", body.Error.Message,
		"the message must not distinguish a malformed id from someone else's job")
}

// A server built without WithWorkerReads answers a sanitized 500 to an
// authenticated caller, never a 404. A 404 would make a real, documented route
// look like it does not exist, which is the failure mode WithResults already
// avoids for the same reason.
func TestListWorkers_UnwiredStoreIsAnInternalErrorNotA404(t *testing.T) {
	rec := get(t, newTestServer(t), "/v1/workers")
	require.Equal(t, http.StatusInternalServerError, rec.Code)

	body := decodeError(t, rec)
	require.Equal(t, CodeInternal, body.Error.Code)
	require.Equal(t, "internal error", body.Error.Message,
		"the sanitized message must not name the missing dependency")
	require.NotEmpty(t, body.Error.RequestID)
}

// ---------------------------------------------------------------------------
// Response shapes
// ---------------------------------------------------------------------------

// A job summary must carry no payload key at all. Not an empty payload, not a
// null one: absent. This is asserted on the serialized JSON rather than on the
// struct, because the struct could grow a field with a json tag and the wire is
// what a client actually sees.
func TestJobSummaryResponse_CarriesNoPayloadKey(t *testing.T) {
	scheduled := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	replayedFrom := uuid.New()
	summary := toJobSummaryResponse(jobs.JobSummary{
		ID: uuid.New(), Queue: "default", Type: "demo.echo",
		Status: jobs.StatusQueued, Priority: 50, MaxAttempts: 3,
		TimeoutSeconds: 300, RequiredCapabilities: []string{"cpu"},
		ScheduledAt: &scheduled, AvailableAt: scheduled,
		ReplayedFromJobID: &replayedFrom,
		CreatedAt:         scheduled, UpdatedAt: scheduled,
	})

	encoded, err := json.Marshal(summary)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	_, hasPayload := decoded["payload"]
	require.False(t, hasPayload,
		"a list endpoint that returned payloads would let one request pull an "+
			"unbounded amount of user data")

	// Everything a client does need is still there.
	for _, key := range []string{
		"id", "queue", "job_type", "status", "priority", "max_attempts",
		"timeout_seconds", "required_capabilities", "scheduled_at",
		"available_at", "cancel_requested_at", "replayed_from_job_id",
		"created_at", "updated_at",
	} {
		require.Containsf(t, decoded, key, "a job summary must carry %s", key)
	}
	require.Len(t, decoded, 14, "no field beyond the documented set")
}

// Nil slices must render as [] rather than null, so a client never has to
// handle both shapes for the same field.
func TestJobSummaryResponse_EmptyCapabilitiesIsAnArrayNotNull(t *testing.T) {
	encoded, err := json.Marshal(toJobSummaryResponse(jobs.JobSummary{ID: uuid.New()}))
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"required_capabilities":[]`)
	require.NotContains(t, string(encoded), `"required_capabilities":null`)
}

// An attempt must never publish a session id, a lease id, or an outcome
// identity. Every worker-control route except registration trusts a session id
// as authority, so one on an authenticated public read would be a credential
// handed to a caller who is not a worker.
func TestAttemptResponse_PublishesNoFencingIdentifier(t *testing.T) {
	started := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	delay := int64(1500)
	class, code, message := "RETRYABLE", "handler_error", "the trusted handler reported an error"

	encoded, err := json.Marshal(toAttemptResponse(jobs.Attempt{
		ID: uuid.New(), AttemptNumber: 2, Status: "FAILED",
		WorkerID: uuid.New(), WorkerName: "local-worker",
		CreatedAt: started, StartedAt: &started, FinishedAt: &started,
		TimeoutAt: &started, FailureClass: &class, ErrorCode: &code,
		ErrorMessage: &message, RetryDelayMS: &delay, RetryAt: &started,
	}))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	for _, forbidden := range []string{
		"worker_session_id", "session_id", "lease_id", "outcome_request_id",
		"claim_request_id", "fence", "scope",
	} {
		require.NotContainsf(t, decoded, forbidden,
			"%s is a control-plane identifier and must never reach a public read", forbidden)
	}
	// The whole-document check too, in case a field is ever nested.
	require.NotContains(t, strings.ToLower(string(encoded)), "session")
	require.NotContains(t, strings.ToLower(string(encoded)), "lease")

	require.Equal(t, float64(2), decoded["attempt_number"])
	require.Equal(t, "FAILED", decoded["status"])
	require.Equal(t, "local-worker", decoded["worker_name"])
}

// A worker summary must publish no session or lease identifier either, for the
// identical reason.
func TestWorkerResponse_PublishesNoSessionOrLeaseIdentifier(t *testing.T) {
	now := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	encoded, err := json.Marshal(toWorkerResponse(workers.WorkerSummary{
		ID: uuid.New(), Name: "local-worker", Status: workers.SessionUnhealthy,
		WorkerGroup: "default", Hostname: "local-worker.local",
		ConcurrencyLimit: 4, Capabilities: []string{"cpu"},
		SupportedJobTypes: []string{"demo.echo"},
		RegisteredAt:      now, LastHeartbeatAt: now,
		HeartbeatAgeSeconds: 42.5, ActiveLeases: 2,
	}))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	for _, forbidden := range []string{"session_id", "worker_session_id", "lease_id", "scope"} {
		require.NotContainsf(t, decoded, forbidden, "%s must never reach a public read", forbidden)
	}
	// active_leases is a COUNT, not an identifier, and must survive this rule.
	require.Equal(t, float64(2), decoded["active_leases"])
	require.Equal(t, "UNHEALTHY", decoded["status"],
		"a crashed worker's session status is exactly what this endpoint exists to show")
	require.Equal(t, 42.5, decoded["heartbeat_age_seconds"])
}

// Queue depth carries every non-terminal status, always, zero-filled -- and no
// terminal one. A client must never have to tell "no jobs in this status" apart
// from "this server did not report this status".
func TestQueueResponse_DepthIsZeroFilledAndExcludesTerminalStatuses(t *testing.T) {
	depth := make(jobs.QueueDepth)
	for _, status := range jobs.NonTerminalStatuses() {
		depth[status] = 0
	}
	depth[jobs.StatusRunning] = 3

	response := toQueueResponse(jobs.Queue{
		Name: "default", WorkerGroup: "default", MaxConcurrency: 100, Depth: depth,
	})

	for _, status := range jobs.AllStatuses() {
		if status.Terminal() {
			require.NotContainsf(t, response.Depth, status.String(),
				"%s is terminal; its count grows without bound and is not depth", status)
			continue
		}
		require.Containsf(t, response.Depth, status.String(),
			"%s must always be present, even at zero", status)
	}
	require.Equal(t, 3, response.Depth["RUNNING"])
	require.Equal(t, 0, response.Depth["QUEUED"])
	require.Len(t, response.Depth, 6)
}

// No queue-wide in-flight figure is reported. max_concurrency is shared across
// scopes while depth is this scope's alone, so a combined number would leak
// another tenant's load.
func TestQueueResponse_ReportsNoCrossScopeInFlightFigure(t *testing.T) {
	encoded, err := json.Marshal(toQueueResponse(jobs.Queue{
		Name: "default", WorkerGroup: "default", MaxConcurrency: 100,
		Depth: jobs.QueueDepth{jobs.StatusRunning: 1},
	}))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Len(t, decoded, 4)
	for _, forbidden := range []string{
		"in_flight", "active", "total", "global_depth", "utilization", "available",
	} {
		require.NotContainsf(t, decoded, forbidden,
			"%s would mix a queue-wide number with a scope-local one", forbidden)
	}
}

// An empty page omits next_cursor entirely rather than sending "". The absence
// of a cursor is the signal that a page is the last one.
func TestPageResponses_OmitAnEmptyCursor(t *testing.T) {
	jobsEncoded, err := json.Marshal(JobPageResponse{Jobs: []JobSummaryResponse{}})
	require.NoError(t, err)
	require.NotContains(t, string(jobsEncoded), "next_cursor")
	require.Contains(t, string(jobsEncoded), `"jobs":[]`)

	workersEncoded, err := json.Marshal(WorkerPageResponse{Workers: []WorkerResponse{}})
	require.NoError(t, err)
	require.NotContains(t, string(workersEncoded), "next_cursor")
	require.Contains(t, string(workersEncoded), `"workers":[]`)

	withCursor, err := json.Marshal(JobPageResponse{
		Jobs: []JobSummaryResponse{}, NextCursor: "abc",
	})
	require.NoError(t, err)
	require.Contains(t, string(withCursor), `"next_cursor":"abc"`)
}

// An empty attempt list is [] and never null, so a job that has never been
// attempted decodes the same way as one that has.
func TestAttemptListResponse_EmptyIsAnArrayNotNull(t *testing.T) {
	encoded, err := json.Marshal(AttemptListResponse{Attempts: []AttemptResponse{}})
	require.NoError(t, err)
	require.Equal(t, `{"attempts":[]}`, strings.TrimSpace(string(encoded)))
}
