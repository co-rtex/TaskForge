//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// deadLetterJob drives one job all the way to DEAD_LETTERED through the real
// failure path, so the DLQ entry under test is one the production code wrote.
func deadLetterJob(t *testing.T, store *workers.Store, session workers.Session, key string) workers.Fence {
	t.Helper()
	ctx := context.Background()
	createJobWithOptions(t, key, "default", "demo.echo", 50, nil, 1, 300, nil)
	claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claim.Disposition)
	fence := assignmentFence(claim.Assignment)
	startAttempt(t, store, fence)

	result, err := store.Fail(ctx, testScope, failureReport(fence,
		lifecycle.ClassPermanent, "invalid_payload", "the payload names no known account"))
	require.NoError(t, err)
	require.Equal(t, "DEAD_LETTERED", result.JobStatus)
	return fence
}

// TestDLQ_ListingReturnsBoundedOperatorMetadata proves the list carries what an
// operator needs and, just as importantly, not the job payload.
func TestDLQ_ListingReturnsBoundedOperatorMetadata(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("dlq-list", 1, nil, []string{"demo.echo"}))
	fence := deadLetterJob(t, store, session, "dlq-list")

	page, err := jobStore().ListDLQ(ctx, testScope, "", 0)
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	require.Empty(t, page.NextCursor, "one entry is not a full page")

	entry := page.Entries[0]
	require.Equal(t, fence.JobID, entry.JobID)
	require.Equal(t, "default", entry.Queue)
	require.Equal(t, "demo.echo", entry.JobType)
	require.Equal(t, 50, entry.Priority)
	require.Equal(t, 1, entry.MaxAttempts)
	require.Equal(t, lifecycle.ReasonPermanentFailure, entry.Reason)
	require.NotZero(t, entry.CreatedAt)
	require.Zero(t, entry.ReplayCount)

	// The terminal attempt's own metadata is joined in, so an operator can see
	// why without a second request.
	require.NotNil(t, entry.TerminalAttemptID)
	require.Equal(t, fence.AttemptID, *entry.TerminalAttemptID)
	require.Equal(t, 1, *entry.AttemptNumber)
	require.Equal(t, "FAILED", *entry.AttemptStatus)
	require.Equal(t, "PERMANENT", *entry.FailureClass)
	require.Equal(t, "invalid_payload", *entry.ErrorCode)
	require.Equal(t, "the payload names no known account", *entry.ErrorMessage)
}

// TestDLQ_ListingIsScopeFiltered keeps one tenant's failures out of another's.
func TestDLQ_ListingIsScopeFiltered(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("dlq-scope", 1, nil, []string{"demo.echo"}))
	deadLetterJob(t, store, session, "dlq-scope")

	page, err := jobStore().ListDLQ(ctx, testScope, "", 0)
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)

	other, err := jobStore().ListDLQ(ctx, "someone-else", "", 0)
	require.NoError(t, err)
	require.Empty(t, other.Entries)
	require.Empty(t, other.NextCursor)
}

// TestDLQ_KeysetPaginationHasNoDuplicatesOrOmissions is why pagination is keyset
// rather than OFFSET.
//
// Every entry here shares one created_at, which is not contrived: two reconciler
// replicas finishing two exhausted attempts in the same instant produce exactly
// this. Ordering by timestamp alone is not a total order, so OFFSET over a table
// that is still growing would skip and repeat rows.
func TestDLQ_KeysetPaginationHasNoDuplicatesOrOmissions(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("dlq-page", 4, nil, []string{"demo.echo"}))

	const total = 7
	expected := make(map[uuid.UUID]struct{}, total)
	for i := 0; i < total; i++ {
		fence := deadLetterJob(t, store, session, fmt.Sprintf("dlq-page-%02d", i))
		expected[fence.JobID] = struct{}{}
	}
	// Collapse every timestamp onto one instant so the id is the only tiebreak.
	_, err := testPool.Exec(ctx,
		`UPDATE dlq_entries SET created_at = TIMESTAMPTZ '2026-09-01 12:00:00+00'`)
	require.NoError(t, err)

	seen := make(map[uuid.UUID]int, total)
	var order []uuid.UUID
	cursor := ""
	for pages := 0; ; pages++ {
		require.Less(t, pages, 10, "pagination did not terminate")
		page, err := jobStore().ListDLQ(ctx, testScope, cursor, 2)
		require.NoError(t, err)
		for _, entry := range page.Entries {
			seen[entry.JobID]++
			order = append(order, entry.ID)
		}
		if page.NextCursor == "" {
			break
		}
		require.LessOrEqual(t, len(page.Entries), 2)
		cursor = page.NextCursor
	}

	require.Len(t, seen, total, "every entry must appear")
	for jobID := range expected {
		require.Equalf(t, 1, seen[jobID], "job %s appeared %d times", jobID, seen[jobID])
	}

	// Descending by id, which is the documented total order when timestamps tie.
	for i := 1; i < len(order); i++ {
		require.Greater(t, order[i-1].String(), order[i].String(),
			"entries must be strictly descending so a page boundary is unambiguous")
	}
}

func TestDLQ_RejectsAnInvalidCursorAndBoundsThePage(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("dlq-cursor", 2, nil, []string{"demo.echo"}))
	deadLetterJob(t, store, session, "dlq-cursor-one")
	deadLetterJob(t, store, session, "dlq-cursor-two")

	for _, cursor := range []string{"not-base64!", "///", "bm90LWEtY3Vyc29y", "MjAyNi0wOS0wMQ"} {
		_, err := jobStore().ListDLQ(ctx, testScope, cursor, 0)
		require.ErrorIsf(t, err, jobs.ErrInvalidCursor, "cursor %q must be rejected", cursor)
	}

	// An oversized limit is clamped rather than honored.
	page, err := jobStore().ListDLQ(ctx, testScope, "", jobs.MaxDLQPageSize+500)
	require.NoError(t, err)
	require.Len(t, page.Entries, 2)

	// An empty page is deterministic, not an error.
	empty, err := jobStore().ListDLQ(ctx, "nobody", "", 0)
	require.NoError(t, err)
	require.Empty(t, empty.Entries)
	require.Empty(t, empty.NextCursor)
}

// TestReplay_CreatesADistinctEligibleJobAndLeavesTheOriginalUntouched is the
// core of the replay contract: a terminal job stays terminal, and its history
// stays exactly as it was.
func TestReplay_CreatesADistinctEligibleJobAndLeavesTheOriginalUntouched(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("replay-core", 1, nil, []string{"demo.echo"}))
	fence := deadLetterJob(t, store, session, "replay-core")

	originalJob := readJob(t, fence.JobID)
	originalAttempt := readAttemptOutcome(t, fence.AttemptID)
	originalLeases := leaseHistory(t, fence.JobID)
	var originalEntryID uuid.UUID
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT id FROM dlq_entries WHERE job_id = $1`, fence.JobID).Scan(&originalEntryID))
	outboxBefore := pendingOutboxIDs(t)

	result, err := jobStore().Replay(ctx, testScope, fence.JobID, "replay-key-1")
	require.NoError(t, err)
	require.False(t, result.Replayed)
	require.Equal(t, fence.JobID, result.OriginalJobID)

	replacement := result.Replacement
	require.NotEqual(t, fence.JobID, replacement.ID, "a replay is a new job, not a resurrection")
	require.Equal(t, jobs.StatusQueued, replacement.Status)
	require.Nil(t, replacement.ScheduledAt,
		"a replay is an operator saying run this now, so it carries no schedule")
	require.NotNil(t, replacement.ReplayedFromJobID)
	require.Equal(t, fence.JobID, *replacement.ReplayedFromJobID)
	require.False(t, replacement.AvailableAt.After(serverNow(t)),
		"a replacement is immediately eligible")

	// The definition is copied exactly; nothing else is.
	require.Equal(t, originalJob.status, "DEAD_LETTERED")
	require.Equal(t, "default", replacement.Queue)
	require.Equal(t, "demo.echo", replacement.Type)
	require.Equal(t, 50, replacement.Priority)
	require.Equal(t, 1, replacement.MaxAttempts)
	require.Equal(t, 300, replacement.TimeoutSeconds)

	// The original is byte-for-byte what it was.
	require.Equal(t, originalJob, readJob(t, fence.JobID))
	require.Equal(t, originalAttempt, readAttemptOutcome(t, fence.AttemptID))
	require.Equal(t, originalLeases, leaseHistory(t, fence.JobID))
	var entryAfter uuid.UUID
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT id FROM dlq_entries WHERE job_id = $1`, fence.JobID).Scan(&entryAfter))
	require.Equal(t, originalEntryID, entryAfter)
	require.Equal(t, 1, countRows(t, "dlq_entries"), "replay creates no second entry")

	// The replacement gets a fresh budget and a fresh generation, advertised in
	// the same transaction.
	require.Equal(t, 1, readJob(t, replacement.ID).generation)
	require.Empty(t, attemptHistory(t, replacement.ID), "a fresh attempt budget")
	added := newPendingOutbox(t, outboxBefore)
	require.Len(t, added, 1)
	events := eventsForJob(t, replacement.ID)
	require.Len(t, events, 1)
	require.Equal(t, 1, events[0].Generation)

	// And it is genuinely claimable.
	claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claim.Disposition)
	require.Equal(t, replacement.ID, claim.Assignment.JobID)
	require.Equal(t, 1, claim.Assignment.AttemptNumber)

	// The DLQ listing now shows the linkage.
	page, err := jobStore().ListDLQ(ctx, testScope, "", 0)
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	require.Equal(t, 1, page.Entries[0].ReplayCount)
}

// TestReplay_IsIdempotentUnderItsIdentity covers duplicate and ambiguous
// requests, which are the same thing from the server's side.
func TestReplay_IsIdempotentUnderItsIdentity(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("replay-idempotent", 1, nil, []string{"demo.echo"}))
	fence := deadLetterJob(t, store, session, "replay-idempotent")

	first, err := jobStore().Replay(ctx, testScope, fence.JobID, "same-key")
	require.NoError(t, err)
	require.False(t, first.Replayed)

	for i := 0; i < 3; i++ {
		repeat, err := jobStore().Replay(ctx, testScope, fence.JobID, "same-key")
		require.NoError(t, err)
		require.True(t, repeat.Replayed)
		require.Equal(t, first.Replacement.ID, repeat.Replacement.ID,
			"an ambiguous retry must return the committed replacement")
	}
	require.Equal(t, 2, countRows(t, "jobs"), "exactly one replacement exists")
	require.Equal(t, 1, countRows(t, "dlq_replays"))

	// A different key is deliberately a different intent: an operator replaying
	// the same failure twice on purpose gets two jobs.
	second, err := jobStore().Replay(ctx, testScope, fence.JobID, "another-key")
	require.NoError(t, err)
	require.False(t, second.Replayed)
	require.NotEqual(t, first.Replacement.ID, second.Replacement.ID)
	require.Equal(t, 3, countRows(t, "jobs"))
	require.Equal(t, 2, countRows(t, "dlq_replays"))

	page, err := jobStore().ListDLQ(ctx, testScope, "", 0)
	require.NoError(t, err)
	require.Equal(t, 2, page.Entries[0].ReplayCount)
}

// TestReplay_ConcurrentIdenticalRequestsCreateExactlyOneReplacement is the
// database-enforced half of idempotency, on separate PostgreSQL connections so
// the serialization is PostgreSQL's rather than the test's.
func TestReplay_ConcurrentIdenticalRequestsCreateExactlyOneReplacement(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("replay-concurrent", 1, nil, []string{"demo.echo"}))
	fence := deadLetterJob(t, store, session, "replay-concurrent")

	const callers = 6
	var wg sync.WaitGroup
	results := make(chan jobs.ReplayResult, callers)
	errs := make(chan error, callers)
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := jobStore().Replay(ctx, testScope, fence.JobID, "concurrent-key")
			results <- result
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	replacements := map[uuid.UUID]struct{}{}
	for result := range results {
		require.NotNil(t, result.Replacement)
		replacements[result.Replacement.ID] = struct{}{}
	}
	require.Len(t, replacements, 1,
		"every concurrent caller must be told about the same replacement")

	require.Equal(t, 2, countRows(t, "jobs"), "a loser must leave no orphan job behind")
	require.Equal(t, 1, countRows(t, "dlq_replays"))
	require.Equal(t, 2, countPendingOutbox(t),
		"the original's submission event plus exactly one replay event")
}

// TestReplay_RefusesAJobThatIsNotDeadLettered proves the precondition is real
// and is revalidated under the lock.
func TestReplay_RefusesAJobThatIsNotDeadLettered(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("replay-refuse", 1, nil, []string{"demo.echo"}))

	queued := createJob(t, "replay-queued", "demo.echo", 50, nil)
	_, err := jobStore().Replay(ctx, testScope, queued, "key")
	require.ErrorIs(t, err, jobs.ErrNotDeadLettered)

	fence := claimedAndRunning(t, store, session, "replay-running")
	_, err = jobStore().Replay(ctx, testScope, fence.JobID, "key")
	require.ErrorIs(t, err, jobs.ErrNotDeadLettered)

	require.NoError(t, store.Succeed(ctx, testScope, fence))
	_, err = jobStore().Replay(ctx, testScope, fence.JobID, "key")
	require.ErrorIs(t, err, jobs.ErrNotDeadLettered,
		"a succeeded job has nothing to replay")

	_, err = jobStore().Replay(ctx, testScope, uuid.New(), "key")
	require.ErrorIs(t, err, jobs.ErrJobNotFound)

	require.Equal(t, 2, countRows(t, "jobs"), "no refused replay may create a job")
	require.Equal(t, 0, countRows(t, "dlq_replays"))
}

// TestReplay_IsScopedToTheCaller keeps replay from crossing a tenant boundary.
func TestReplay_IsScopedToTheCaller(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("replay-scope", 1, nil, []string{"demo.echo"}))
	fence := deadLetterJob(t, store, session, "replay-scope")

	_, err := jobStore().Replay(ctx, "someone-else", fence.JobID, "key")
	require.ErrorIs(t, err, jobs.ErrJobNotFound)
	require.Equal(t, 1, countRows(t, "jobs"))
}

// TestReplay_FaultBeforeCommitLeavesNothingBehind proves the replacement job,
// the identity record, and the notification really are one transaction.
func TestReplay_FaultBeforeCommitLeavesNothingBehind(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("replay-fault", 1, nil, []string{"demo.echo"}))
	fence := deadLetterJob(t, store, session, "replay-fault")
	jobsBefore := countRows(t, "jobs")

	_, err := testPool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION taskforge_test_fail_replay_event() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected replay failure'; END $$`)
	require.NoError(t, err)
	_, err = testPool.Exec(ctx, `
		CREATE TRIGGER taskforge_test_fail_replay_event
		BEFORE INSERT ON outbox_events FOR EACH ROW
		EXECUTE FUNCTION taskforge_test_fail_replay_event()`)
	require.NoError(t, err)
	dropTrigger := func() {
		_, _ = testPool.Exec(context.Background(),
			`DROP TRIGGER IF EXISTS taskforge_test_fail_replay_event ON outbox_events`)
		_, _ = testPool.Exec(context.Background(),
			`DROP FUNCTION IF EXISTS taskforge_test_fail_replay_event()`)
	}
	t.Cleanup(dropTrigger)

	_, err = jobStore().Replay(ctx, testScope, fence.JobID, "fault-key")
	require.Error(t, err)
	require.Equal(t, jobsBefore, countRows(t, "jobs"),
		"a rolled-back replay must leave no orphan replacement job")
	require.Equal(t, 0, countRows(t, "dlq_replays"))

	// Retrying after the fault clears simply works, under the same identity.
	dropTrigger()
	result, err := jobStore().Replay(ctx, testScope, fence.JobID, "fault-key")
	require.NoError(t, err)
	require.False(t, result.Replayed)
	require.Equal(t, jobsBefore+1, countRows(t, "jobs"))
}

// TestReplay_RetryAndReplayRoutesShareOneIdentityNamespace is the property that
// makes two routes for one operation safe. Implementing them separately is how
// they would eventually answer differently for the same request.
func TestReplay_RetryAndReplayRoutesShareOneIdentityNamespace(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("replay-routes", 1, nil, []string{"demo.echo"}))
	fence := deadLetterJob(t, store, session, "replay-routes")
	server := newAPI(t)

	post := func(t *testing.T, path, key string) (*http.Response, api.ReplayResponse) {
		t.Helper()
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+path, strings.NewReader(""))
		require.NoError(t, err)
		request.Header.Set("Idempotency-Key", key)
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		t.Cleanup(func() { _ = response.Body.Close() })
		var body api.ReplayResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
		return response, body
	}

	// Through /v1/dlq/{id}/replay first.
	response, first := post(t, "/v1/dlq/"+fence.JobID.String()+"/replay", "shared-key")
	require.Equal(t, http.StatusCreated, response.StatusCode)
	require.False(t, first.Replayed)
	require.Equal(t, fence.JobID.String(), first.OriginalJobID)

	// The SAME identity through /v1/jobs/{id}/retry must return the same job.
	response, second := post(t, "/v1/jobs/"+fence.JobID.String()+"/retry", "shared-key")
	require.Equal(t, http.StatusOK, response.StatusCode,
		"a replay through the other route is a replay, not a creation")
	require.True(t, second.Replayed)
	require.Equal(t, first.Replacement.ID, second.Replacement.ID)

	// A different identity through either route is a different intent.
	response, third := post(t, "/v1/jobs/"+fence.JobID.String()+"/retry", "other-key")
	require.Equal(t, http.StatusCreated, response.StatusCode)
	require.NotEqual(t, first.Replacement.ID, third.Replacement.ID)

	require.Equal(t, 3, countRows(t, "jobs"))
	require.Equal(t, 2, countRows(t, "dlq_replays"))
}

// TestDLQ_HTTPSurfaceIsBoundedAndStable exercises the public endpoints the way a
// client does, including the two failure shapes an operator will actually hit.
func TestDLQ_HTTPSurfaceIsBoundedAndStable(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("dlq-http", 2, nil, []string{"demo.echo"}))
	deadLetterJob(t, store, session, "dlq-http-one")
	deadLetterJob(t, store, session, "dlq-http-two")
	server := newAPI(t)

	get := func(t *testing.T, path string) (*http.Response, []byte) {
		t.Helper()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+path, nil)
		require.NoError(t, err)
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		t.Cleanup(func() { _ = response.Body.Close() })
		body := make([]byte, 0)
		buf := make([]byte, 4096)
		for {
			n, err := response.Body.Read(buf)
			body = append(body, buf[:n]...)
			if err != nil {
				break
			}
		}
		return response, body
	}

	t.Run("a page is returned with no payloads in it", func(t *testing.T) {
		response, body := get(t, "/v1/dlq")
		require.Equal(t, http.StatusOK, response.StatusCode)
		var page api.DLQPageResponse
		require.NoError(t, json.Unmarshal(body, &page))
		require.Len(t, page.Entries, 2)
		require.NotContains(t, string(body), "dlq-http-one",
			"the payload must never appear in a list response")
		require.NotContains(t, string(body), testScope,
			"the internal scope must never be returned to a client")
	})

	t.Run("pagination round-trips through the opaque cursor", func(t *testing.T) {
		response, body := get(t, "/v1/dlq?limit=1")
		require.Equal(t, http.StatusOK, response.StatusCode)
		var first api.DLQPageResponse
		require.NoError(t, json.Unmarshal(body, &first))
		require.Len(t, first.Entries, 1)
		require.NotEmpty(t, first.NextCursor)

		response, body = get(t, "/v1/dlq?limit=1&cursor="+first.NextCursor)
		require.Equal(t, http.StatusOK, response.StatusCode)
		var second api.DLQPageResponse
		require.NoError(t, json.Unmarshal(body, &second))
		require.Len(t, second.Entries, 1)
		require.NotEqual(t, first.Entries[0].ID, second.Entries[0].ID)
		require.Empty(t, second.NextCursor)
	})

	t.Run("an invalid limit or cursor is a stable field-level error", func(t *testing.T) {
		for path, code := range map[string]string{
			"/v1/dlq?limit=0":          api.CodeValidationFailed,
			"/v1/dlq?limit=100000":     api.CodeValidationFailed,
			"/v1/dlq?limit=abc":        api.CodeValidationFailed,
			"/v1/dlq?cursor=not-valid": api.CodeInvalidCursor,
		} {
			response, body := get(t, path)
			require.Equalf(t, http.StatusUnprocessableEntity, response.StatusCode, "path %s", path)
			var envelope api.ErrorBody
			require.NoError(t, json.Unmarshal(body, &envelope))
			require.Equalf(t, code, envelope.Error.Code, "path %s", path)
			require.NotEmpty(t, envelope.Error.Details)
		}
	})

	t.Run("replay requires an idempotency key", func(t *testing.T) {
		page, err := jobStore().ListDLQ(ctx, testScope, "", 0)
		require.NoError(t, err)
		request, err := http.NewRequestWithContext(ctx, http.MethodPost,
			server.URL+"/v1/dlq/"+page.Entries[0].JobID.String()+"/replay", nil)
		require.NoError(t, err)
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equal(t, http.StatusUnprocessableEntity, response.StatusCode)
	})

	t.Run("replaying a job that is not dead-lettered is a stable conflict", func(t *testing.T) {
		queued := createJob(t, "dlq-http-queued", "demo.echo", 50, nil)
		request, err := http.NewRequestWithContext(ctx, http.MethodPost,
			server.URL+"/v1/jobs/"+queued.String()+"/retry", nil)
		require.NoError(t, err)
		request.Header.Set("Idempotency-Key", "k")
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equal(t, http.StatusConflict, response.StatusCode)

		var envelope api.ErrorBody
		require.NoError(t, json.NewDecoder(response.Body).Decode(&envelope))
		require.Equal(t, api.CodeNotDeadLettered, envelope.Error.Code)
	})
}

// TestCancel_HTTPSurfaceReportsBothShapes exercises the public cancel endpoint,
// whose two success shapes a client genuinely has to tell apart.
func TestCancel_HTTPSurfaceReportsBothShapes(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	// Concurrency 3 because the CANCEL_REQUESTED subtest deliberately leaves its
	// lease active — that is the state under test — and the later subtests still
	// need a free slot.
	session := registerWorker(t, store,
		workerRegistration("cancel-http", 3, nil, []string{"demo.echo"}))
	server := newAPI(t)

	cancel := func(t *testing.T, jobID uuid.UUID) (*http.Response, api.CancelResponse) {
		t.Helper()
		request, err := http.NewRequestWithContext(ctx, http.MethodPost,
			server.URL+"/v1/jobs/"+jobID.String()+"/cancel", nil)
		require.NoError(t, err)
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		t.Cleanup(func() { _ = response.Body.Close() })
		var body api.CancelResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
		return response, body
	}

	t.Run("a queued job is canceled outright", func(t *testing.T) {
		jobID := createJob(t, "cancel-http-queued", "demo.echo", 50, nil)
		response, body := cancel(t, jobID)
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, "CANCELED", body.Status)
		require.False(t, body.AlreadyRequested)
		require.NotZero(t, body.CancelRequestedAt)

		_, repeat := cancel(t, jobID)
		require.True(t, repeat.AlreadyRequested)
	})

	t.Run("a running job is asked to stop", func(t *testing.T) {
		fence := claimedAndRunning(t, store, session, "cancel-http-running")
		response, body := cancel(t, fence.JobID)
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, "CANCEL_REQUESTED", body.Status,
			"a client must be able to tell a finished cancellation from a requested one")

		// And GET reflects it, including the timestamp.
		request, err := http.NewRequestWithContext(ctx, http.MethodGet,
			server.URL+"/v1/jobs/"+fence.JobID.String(), nil)
		require.NoError(t, err)
		read, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		defer read.Body.Close()
		var job api.JobResponse
		require.NoError(t, json.NewDecoder(read.Body).Decode(&job))
		require.Equal(t, "CANCEL_REQUESTED", job.Status)
		require.NotNil(t, job.CancelRequestedAt)
		require.Nil(t, job.ScheduledAt)
		require.Nil(t, job.ReplayedFromJobID)
		require.NotZero(t, job.AvailableAt)
	})

	t.Run("a succeeded job is a stable conflict", func(t *testing.T) {
		fence := claimedAndRunning(t, store, session, "cancel-http-succeeded")
		require.NoError(t, store.Succeed(ctx, testScope, fence))
		response, _ := cancel(t, fence.JobID)
		require.Equal(t, http.StatusConflict, response.StatusCode)
	})

	t.Run("an unknown job is not found", func(t *testing.T) {
		response, _ := cancel(t, uuid.New())
		require.Equal(t, http.StatusNotFound, response.StatusCode)
	})
}

// TestSubmit_DelayedJobIsVisibleThroughTheReadAPI closes the public loop for
// delayed submission.
func TestSubmit_DelayedJobIsVisibleThroughTheReadAPI(t *testing.T) {
	reset(t)
	server := newAPI(t)
	scheduled := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)

	body := fmt.Sprintf(
		`{"queue":"default","job_type":"demo.echo","payload":{"a":1},"scheduled_at":%q}`,
		scheduled.Format(time.RFC3339))
	response, created := submit(t, server.URL, "delayed-http", body)
	require.Equal(t, http.StatusCreated, response.StatusCode)
	require.Equal(t, "PENDING", created.Status)
	require.NotNil(t, created.ScheduledAt)
	require.True(t, scheduled.Equal(*created.ScheduledAt))
	require.True(t, scheduled.Equal(created.AvailableAt),
		"eligibility starts at the requested schedule")

	// The same instant in a different offset is the same request, so it replays.
	otherOffset := scheduled.In(time.FixedZone("west", -5*3600))
	replayBody := fmt.Sprintf(
		`{"queue":"default","job_type":"demo.echo","payload":{"a":1},"scheduled_at":%q}`,
		otherOffset.Format(time.RFC3339))
	response, replayed := submit(t, server.URL, "delayed-http", replayBody)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, created.ID, replayed.ID)

	// A different instant under the same key is a conflict.
	conflictBody := fmt.Sprintf(
		`{"queue":"default","job_type":"demo.echo","payload":{"a":1},"scheduled_at":%q}`,
		scheduled.Add(time.Minute).Format(time.RFC3339))
	response, _ = submit(t, server.URL, "delayed-http", conflictBody)
	require.Equal(t, http.StatusConflict, response.StatusCode)
}
