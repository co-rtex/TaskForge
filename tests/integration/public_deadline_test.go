//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/jobs"
)

// Gate keys for the three public mutating routes. Distinct per route so a
// leaked gate from one subtest can never park another.
const (
	gatePublicCancelKey int64 = 7710010060
	gatePublicRetryKey  int64 = 7710010061
	gatePublicReplayKey int64 = 7710010062
)

// newAPIWithTimeout builds the real public API with a request timeout short
// enough that a parked statement exhausts it inside a test.
//
// The timeout is the API's own, applied by the same middleware production uses,
// so the deadline these tests induce is the deadline a real request has. Nothing
// about the failure is simulated: PostgreSQL really is holding the write, and
// pgx really does abort it when the request's context expires.
func newAPIWithTimeout(t *testing.T, timeout time.Duration) *httptest.Server {
	t.Helper()
	srv := api.NewServer(
		jobs.NewStore(testPool),
		api.Config{MaxRequestBytes: 256 * 1024, DevScope: testScope, RequestTimeout: timeout},
		discardLogger(),
	)
	s := httptest.NewServer(srv.Handler())
	t.Cleanup(s.Close)
	return s
}

// publicPost issues one public mutating request and returns its status and
// decoded error envelope.
func publicPost(t *testing.T, base, path, key string) (int, api.ErrorBody, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, base+path, strings.NewReader(""))
	require.NoError(t, err)
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	require.NoError(t, err)

	var envelope api.ErrorBody
	_ = json.Unmarshal(body, &envelope)
	return response.StatusCode, envelope, body
}

// TestPublicDeadline_MutatorsAnswer503RatherThanAnUnhelpful500 is the whole
// point of classifying the public surface.
//
// A deadline that elapses inside one of these operations leaves its durable
// outcome genuinely unknown: it can land while acquiring a lock, while executing
// a statement, or during COMMIT, and a COMMIT cut short is ambiguous. Answering
// 500 internal_error tells the caller a bug happened and gives no guidance at
// all — an operator cancelling a runaway job learns nothing about whether to try
// again, and the cautious reading (do not retry) is the one that leaves the job
// running.
func TestPublicDeadline_MutatorsAnswer503RatherThanAnUnhelpful500(t *testing.T) {
	t.Run("cancel", func(t *testing.T) {
		reset(t)
		base := newAPIWithTimeout(t, 400*time.Millisecond)
		job := createJob(t, "public-deadline-cancel", "demo.echo", 50, nil)

		// Parks the cancellation at its own UPDATE, after it has taken the queue
		// and job row locks. The WHEN clause names both terminal shapes a
		// cancellation can write, because a queued job goes straight to
		// CANCELED while a leased one goes to CANCEL_REQUESTED.
		release := gateOnAdvisoryLockWhen(t, gatePublicCancelKey,
			"taskforge_test_gate_public_cancel", "BEFORE UPDATE", "jobs",
			"NEW.status IN ('CANCELED', 'CANCEL_REQUESTED')")
		defer release()

		status, envelope, raw := publicPost(t, base.URL, "/v1/jobs/"+job.String()+"/cancel", "")
		release()

		require.Equalf(t, http.StatusServiceUnavailable, status,
			"an ambiguous cancellation must be a 503, not %s", raw)
		require.Equal(t, api.CodeServiceUnavailable, envelope.Error.Code)
		requireDeadlineGuidance(t, envelope.Error.Message)
		require.Contains(t, envelope.Error.Message, "scope and job id",
			"cancellation must tell the caller its identity is the job id itself")
	})

	for name, route := range map[string]string{
		"retry":  "/v1/jobs/%s/retry",
		"replay": "/v1/dlq/%s/replay",
	} {
		t.Run(name, func(t *testing.T) {
			reset(t)
			base := newAPIWithTimeout(t, 400*time.Millisecond)
			control := controlStore()
			session := registerWorker(t, control,
				workerRegistration("public-deadline-"+name, 1, nil, []string{"demo.echo"}))
			fence := deadLetterJob(t, control, session, "public-deadline-"+name)

			// Installed only after the DLQ entry exists, because a replay's
			// write is an INSERT of the replacement job and an unconditional
			// gate would otherwise have parked the original submission too.
			key := gatePublicRetryKey
			if name == "replay" {
				key = gatePublicReplayKey
			}
			release := gateOnAdvisoryLock(t, key,
				"taskforge_test_gate_public_"+name, "BEFORE INSERT", "jobs")
			defer release()

			path := strings.Replace(route, "%s", fence.JobID.String(), 1)
			status, envelope, raw := publicPost(t, base.URL, path, uuid.NewString())
			release()

			require.Equalf(t, http.StatusServiceUnavailable, status,
				"an ambiguous %s must be a 503, not %s", name, raw)
			require.Equal(t, api.CodeServiceUnavailable, envelope.Error.Code)
			requireDeadlineGuidance(t, envelope.Error.Message)
			require.Contains(t, envelope.Error.Message, "Idempotency-Key",
				name+" must tell the caller which identity makes the retry safe")
			require.Contains(t, envelope.Error.Message, "forbidden",
				"a fresh key after an ambiguous response must be forbidden, not discouraged")
		})
	}
}

// requireDeadlineGuidance pins the promises the sanitized body may and may not
// make. A message that claimed nothing was committed would be a lie whenever the
// deadline landed during COMMIT.
func requireDeadlineGuidance(t *testing.T, message string) {
	t.Helper()
	lower := strings.ToLower(message)
	require.Contains(t, lower, "deadline")
	require.Contains(t, lower, "durable outcome was known")
	for _, forbidden := range []string{
		"nothing was committed", "no changes were made", "rolled back", "was not committed",
	} {
		require.NotContainsf(t, lower, forbidden,
			"a deadline during COMMIT is ambiguous, so %q is a promise the API cannot keep", forbidden)
	}
}

// TestPublicDeadline_ClassifiesOnlyTheOperationError is the control that keeps
// the 503 from swallowing real bugs.
//
// The classifier inspects the returned error and never ctx.Err(), so an
// unrelated failure that merely finishes after a deadline elapsed keeps its own
// identity. If it consulted the context instead, every slow constraint violation
// on a loaded database would become a 503 inviting the caller to retry
// something that will never succeed.
func TestPublicDeadline_ClassifiesOnlyTheOperationError(t *testing.T) {
	reset(t)
	base := newAPIWithTimeout(t, 5*time.Second)
	ctx := context.Background()

	// A terminal job is not cancelable. The dead-lettered job is built first,
	// because a claim takes the oldest eligible job in the queue and a live job
	// created before it would be the one that got claimed.
	control := controlStore()
	session := registerWorker(t, control,
		workerRegistration("public-deadline-control", 1, nil, []string{"demo.echo"}))
	fence := deadLetterJob(t, control, session, "public-deadline-control-dlq")
	status, envelope, _ := publicPost(t, base.URL, "/v1/jobs/"+fence.JobID.String()+"/cancel", "")
	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, api.CodeNotCancelable, envelope.Error.Code)

	// And a live job is not replayable. That refusal must survive as its own
	// stable 409 rather than being reclassified.
	job := createJob(t, "public-deadline-control", "demo.echo", 50, nil)
	status, envelope, _ = publicPost(t, base.URL, "/v1/jobs/"+job.String()+"/retry", uuid.NewString())
	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, api.CodeNotDeadLettered, envelope.Error.Code)

	// The store agrees with the wire: neither refusal carries the deadline
	// sentinel, so neither could ever be rendered as a 503.
	_, err := jobStore().Replay(ctx, testScope, job, "control-key-aaaaaaaaaaaa")
	require.ErrorIs(t, err, jobs.ErrNotDeadLettered)
	require.NotErrorIs(t, err, jobs.ErrDeadlineExceeded)
	_, err = jobStore().RequestCancel(ctx, testScope, fence.JobID)
	require.ErrorIs(t, err, jobs.ErrJobNotCancelable)
	require.NotErrorIs(t, err, jobs.ErrDeadlineExceeded)
}

// TestPublicDeadline_CommittedButUnknownResponseIsSafeToRepeat is the promise
// the 503 body actually makes, tested end to end.
//
// The dangerous case is not the request that failed; it is the one that
// COMMITTED and whose response the caller never saw. The endpoint guidance is
// only worth printing if following it is safe, so each route commits for real,
// the response is discarded as if the connection had dropped, and the identical
// request is repeated. Exactly one durable effect must exist afterwards.
func TestPublicDeadline_CommittedButUnknownResponseIsSafeToRepeat(t *testing.T) {
	t.Run("cancel", func(t *testing.T) {
		reset(t)
		base := newAPIWithTimeout(t, 5*time.Second)
		job := createJob(t, "public-unknown-cancel", "demo.echo", 50, nil)

		// The first request commits; its response is thrown away unread.
		status, _, _ := publicPost(t, base.URL, "/v1/jobs/"+job.String()+"/cancel", "")
		require.Equal(t, http.StatusOK, status)
		committedAt := readCancelRequestedAt(t, job)
		require.NotNil(t, committedAt)

		// The guidance says to repeat the identical request for the same job.
		status, _, raw := publicPost(t, base.URL, "/v1/jobs/"+job.String()+"/cancel", "")
		require.Equal(t, http.StatusOK, status)
		var repeat api.CancelResponse
		require.NoError(t, json.Unmarshal(raw, &repeat))
		require.True(t, repeat.AlreadyRequested,
			"the repeat must report the decision it found, not a second cancellation")
		require.Equal(t, "CANCELED", repeat.Status)
		require.WithinDuration(t, *committedAt, repeat.CancelRequestedAt, 0,
			"a repeat must not restamp the instant cancellation won")
		require.Equal(t, 0, countRows(t, "job_attempts"),
			"cancelling a queued job creates no attempt however many times it is repeated")
	})

	for name, route := range map[string]string{
		"retry":  "/v1/jobs/%s/retry",
		"replay": "/v1/dlq/%s/replay",
	} {
		t.Run(name, func(t *testing.T) {
			reset(t)
			base := newAPIWithTimeout(t, 5*time.Second)
			control := controlStore()
			session := registerWorker(t, control,
				workerRegistration("public-unknown-"+name, 1, nil, []string{"demo.echo"}))
			fence := deadLetterJob(t, control, session, "public-unknown-"+name)
			path := strings.Replace(route, "%s", fence.JobID.String(), 1)
			key := uuid.NewString()

			status, _, raw := publicPost(t, base.URL, path, key)
			require.Equal(t, http.StatusCreated, status)
			var first api.ReplayResponse
			require.NoError(t, json.Unmarshal(raw, &first))

			// The identical path with the identical key, exactly as the 503
			// guidance instructs.
			status, _, raw = publicPost(t, base.URL, path, key)
			require.Equal(t, http.StatusOK, status)
			var repeat api.ReplayResponse
			require.NoError(t, json.Unmarshal(raw, &repeat))
			require.True(t, repeat.Replayed)
			require.Equal(t, first.Replacement.ID, repeat.Replacement.ID,
				"a repeat must return the replacement that already exists")
			require.Equal(t, 1, countReplacements(t, fence.JobID),
				"one replay identity produces exactly one replacement job")

			// The other route is the same operation in a different vocabulary,
			// so the same key there must also resolve to the same replacement
			// rather than to a second one.
			other := "/v1/jobs/" + fence.JobID.String() + "/retry"
			if name == "retry" {
				other = "/v1/dlq/" + fence.JobID.String() + "/replay"
			}
			status, _, raw = publicPost(t, base.URL, other, key)
			require.Equal(t, http.StatusOK, status)
			require.NoError(t, json.Unmarshal(raw, &repeat))
			require.Equal(t, first.Replacement.ID, repeat.Replacement.ID,
				"/retry and /replay share one identity namespace")
			require.Equal(t, 1, countReplacements(t, fence.JobID))

			// A fresh key is a different replay identity, which is exactly why
			// the guidance forbids one after an ambiguous response: it does not
			// fail, it silently creates a second replacement job.
			status, _, raw = publicPost(t, base.URL, path, uuid.NewString())
			require.Equal(t, http.StatusCreated, status)
			var second api.ReplayResponse
			require.NoError(t, json.Unmarshal(raw, &second))
			require.NotEqual(t, first.Replacement.ID, second.Replacement.ID)
			require.Equal(t, 2, countReplacements(t, fence.JobID),
				"a fresh key after an ambiguous response is what duplicates work")
		})
	}
}

func countReplacements(t *testing.T, originalJobID uuid.UUID) int {
	t.Helper()
	var count int
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM jobs WHERE replayed_from_job_id = $1`, originalJobID).Scan(&count))
	return count
}
