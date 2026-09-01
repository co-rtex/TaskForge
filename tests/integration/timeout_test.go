//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// Gate keys for the timeout contention barriers. Fixed and distinct for the
// same reason the M2 and M3 gates are: a failed run leaves a diagnosable lock.
const (
	gateTimeoutBeforeSuccessKey int64 = 7710010020
	gateSuccessBeforeTimeoutKey int64 = 7710010021
)

// The WHEN clause that tells a timeout's attempt UPDATE from a success's.
const (
	whenTimingOut     = "NEW.status = 'TIMED_OUT'"
	whenSucceeding    = "NEW.status = 'SUCCEEDED'"
	fragmentAttemptTx = "UPDATE job_attempts"
)

// claimedRunningWithTimeout claims and starts one job whose per-attempt
// execution budget is timeoutSeconds, and returns its fence.
func claimedRunningWithTimeout(
	t *testing.T,
	store *workers.Store,
	session workers.Session,
	key string,
	maxAttempts, timeoutSeconds int,
) (workers.Fence, workers.StartResult) {
	t.Helper()
	createJobWithOptions(t, key, "default", "demo.echo", 50, nil, maxAttempts, timeoutSeconds, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claim.Disposition)
	fence := assignmentFence(claim.Assignment)
	return fence, startAttempt(t, store, fence)
}

// expireAttemptDeadline moves an attempt's persisted execution deadline into
// the past without touching its lease, which is what a genuinely slow handler
// looks like: authority is fine, the budget is gone.
//
// The whole timeline moves together — created, started, then timed out, all in
// the past — because that is the only shape the schema accepts, and because a
// row that says it started before it was created would not be a slow attempt,
// it would be an impossible one.
func expireAttemptDeadline(t *testing.T, attemptID uuid.UUID) {
	t.Helper()
	tag, err := testPool.Exec(context.Background(), `
		UPDATE job_attempts
		SET created_at = clock_timestamp() - interval '3 minutes',
		    started_at = clock_timestamp() - interval '2 minutes',
		    timeout_at = clock_timestamp() - interval '1 minute'
		WHERE id = $1`, attemptID)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
}

// setAttemptDeadline puts an attempt's deadline a short distance in the future
// and returns it, so a test can arrange a request that begins before the
// deadline and finishes after it.
func setAttemptDeadline(t *testing.T, attemptID uuid.UUID, in time.Duration) time.Time {
	t.Helper()
	var timeoutAt time.Time
	require.NoError(t, testPool.QueryRow(context.Background(), `
		UPDATE job_attempts
		SET created_at = clock_timestamp() - interval '2 seconds',
		    started_at = clock_timestamp() - interval '1 second',
		    timeout_at = clock_timestamp() + make_interval(secs => $2::double precision)
		WHERE id = $1
		RETURNING timeout_at`, attemptID, in.Seconds()).Scan(&timeoutAt))
	return timeoutAt
}

// TestStart_StampsAPersistedDeadlineMeasuredByPostgreSQL is the foundation the
// rest of the timeout story rests on.
func TestStart_StampsAPersistedDeadlineMeasuredByPostgreSQL(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("start-deadline", 1, nil, []string{"demo.echo"}))
	fence, result := claimedRunningWithTimeout(t, store, session, "start-deadline", 3, 45)

	require.Equal(t, fence.AttemptID, result.AttemptID)
	require.False(t, result.Replayed)
	require.Equal(t, 45*time.Second, result.TimeoutAt.Sub(result.StartedAt),
		"the deadline is the job's timeout_seconds measured from execution start")
	require.InDelta(t, float64(45*time.Second), float64(result.Remaining), float64(time.Second),
		"remaining is measured by PostgreSQL, not recomputed from timeout_seconds")

	row := readAttemptOutcome(t, fence.AttemptID)
	require.NotNil(t, row.timeoutAt)
	require.True(t, result.TimeoutAt.Equal(*row.timeoutAt),
		"the returned deadline must be the persisted one")
}

// TestStart_AmbiguousRetryReturnsTheOriginalDeadline is the single most
// important property of the start transition. A worker whose Start committed
// but whose response was lost must retry, and the retry must NOT restart the
// clock: recomputing the deadline every time a response was lost is the one way
// a timeout could never fire.
func TestStart_AmbiguousRetryReturnsTheOriginalDeadline(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("start-replay", 1, nil, []string{"demo.echo"}))
	fence, first := claimedRunningWithTimeout(t, store, session, "start-replay", 3, 30)

	// Let real time pass, so a recomputed deadline would be visibly different.
	waitForServerTime(t, first.StartedAt.Add(250*time.Millisecond))

	for i := 0; i < 3; i++ {
		replay := startAttempt(t, store, fence)
		require.True(t, replay.Replayed)
		require.True(t, first.StartedAt.Equal(replay.StartedAt),
			"a replay must return the original start time")
		require.True(t, first.TimeoutAt.Equal(replay.TimeoutAt),
			"a replay must never restart the execution budget")
		require.Less(t, replay.Remaining, first.Remaining,
			"remaining must shrink with real time, measured against the original deadline")
		require.Positive(t, replay.Remaining)
	}

	row := readAttemptOutcome(t, fence.AttemptID)
	require.True(t, first.TimeoutAt.Equal(*row.timeoutAt))
}

// TestTimeout_IsNotResetByLeaseRenewal is the boundary between the two budgets.
// Renewal extends lease authority; it must never extend the job's execution
// budget, or a job with a cooperative-but-slow handler could run forever.
func TestTimeout_IsNotResetByLeaseRenewal(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("renew-vs-timeout", 1, nil, []string{"demo.echo"}))
	fence, start := claimedRunningWithTimeout(t, store, session, "renew-vs-timeout", 3, 60)

	before := readAttemptOutcome(t, fence.AttemptID)
	expectedVersion := 0
	for i := 0; i < 3; i++ {
		renewal, err := store.RenewLease(ctx, testScope, renewalRequest(fence, expectedVersion))
		require.NoError(t, err)
		expectedVersion = renewal.RenewalVersion

		after := readAttemptOutcome(t, fence.AttemptID)
		require.True(t, before.timeoutAt.Equal(*after.timeoutAt),
			"renewal %d moved the attempt deadline", i+1)
	}
	require.Equal(t, 3, expectedVersion, "the lease really was renewed three times")

	// And once the deadline passes, renewal itself is refused: extending
	// authority that can never be used would only delay reconciliation while the
	// handler kept burning resources.
	expireAttemptDeadline(t, fence.AttemptID)
	_, err := store.RenewLease(ctx, testScope, renewalRequest(fence, expectedVersion))
	require.ErrorIs(t, err, workers.ErrAttemptTimedOut)

	_, _, _, version := leaseRow(t, fence.LeaseID)
	require.Equal(t, expectedVersion, version, "a rejected renewal must not advance the generation")
	require.True(t, start.TimeoutAt.Equal(*readAttemptOutcome(t, fence.AttemptID).timeoutAt) ||
		readAttemptOutcome(t, fence.AttemptID).timeoutAt.Before(start.TimeoutAt),
		"the deadline only ever moves backwards in this test, never forwards")
}

// TestTimeout_DueAttemptBecomesTimedOutAndEntersRetryWait is the ordinary
// timeout path: the lease is still perfectly valid, and what ran out is the
// attempt's own budget.
func TestTimeout_DueAttemptBecomesTimedOutAndEntersRetryWait(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStoreWithJitter(lifecycle.NewSeededJitter(11))
	session := registerWorker(t, store,
		workerRegistration("timeout-retry", 1, nil, []string{"demo.echo"}))
	fence, _ := claimedRunningWithTimeout(t, store, session, "timeout-retry", 3, 60)
	outboxBefore := pendingOutboxIDs(t)
	expireAttemptDeadline(t, fence.AttemptID)

	// The lease is deliberately untouched and still live.
	status, expiresAt, _, _ := leaseRow(t, fence.LeaseID)
	require.Equal(t, "ACTIVE", status)
	require.True(t, expiresAt.After(serverNow(t)))

	stats, err := store.ReconcileDueTimeouts(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.TimedOutAttempts)
	require.Equal(t, 1, stats.RetryWaitingJobs)
	require.Zero(t, stats.DeadLetteredJobs)

	row := readAttemptOutcome(t, fence.AttemptID)
	require.Equal(t, "TIMED_OUT", row.status, "not FAILED, and not ABANDONED")
	require.Equal(t, "TIMED_OUT", *row.failureClass)
	require.Equal(t, lifecycle.CodeTimeout, *row.errorCode)
	require.NotNil(t, row.finishedAt)
	require.NotNil(t, row.retryAt)
	require.Nil(t, row.outcomeID,
		"nobody requested this outcome, so there is no client identity to retain")

	state := readState(t, fence)
	require.Equal(t, "RETRY_WAIT", state.job)
	require.Equal(t, "RELEASED", state.lease,
		"the timeout was detected while the lease was still live, so authority is handed back")
	require.Equal(t, 0, countActiveLeases(t))

	job := readJob(t, fence.JobID)
	require.True(t, job.availableAt.Equal(*row.retryAt))
	require.Empty(t, newPendingOutbox(t, outboxBefore),
		"a RETRY_WAIT job is not advertised until the scheduler promotes it")
}

// TestTimeout_ExhaustedBudgetDeadLettersThroughTheSharedHelper proves a timeout
// at the end of the budget produces exactly one DLQ entry, in the same table
// every other terminal path writes to.
func TestTimeout_ExhaustedBudgetDeadLettersThroughTheSharedHelper(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("timeout-exhaust", 1, nil, []string{"demo.echo"}))
	fence, _ := claimedRunningWithTimeout(t, store, session, "timeout-exhaust", 1, 60)
	expireAttemptDeadline(t, fence.AttemptID)

	stats, err := store.ReconcileDueTimeouts(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.TimedOutAttempts)
	require.Equal(t, 1, stats.DeadLetteredJobs)

	require.Equal(t, "DEAD_LETTERED", readJob(t, fence.JobID).status)
	require.Equal(t, []string{"ATTEMPTS_EXHAUSTED"}, dlqRows(t, fence.JobID))
	require.Equal(t, "TIMED_OUT", readAttemptOutcome(t, fence.AttemptID).status,
		"the final attempt keeps its truthful status")

	// The DLQ entry points at the attempt that actually ended the job.
	var terminalAttempt uuid.UUID
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT terminal_attempt_id FROM dlq_entries WHERE job_id = $1`,
		fence.JobID).Scan(&terminalAttempt))
	require.Equal(t, fence.AttemptID, terminalAttempt)
}

// TestTimeout_ReconciliationIsIdempotent covers repeated and concurrent passes.
func TestTimeout_ReconciliationIsIdempotent(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("timeout-idempotent", 1, nil, []string{"demo.echo"}))
	fence, _ := claimedRunningWithTimeout(t, store, session, "timeout-idempotent", 3, 60)
	expireAttemptDeadline(t, fence.AttemptID)

	first, err := store.ReconcileDueTimeouts(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, first.TimedOutAttempts)
	finished := readAttemptOutcome(t, fence.AttemptID)

	for i := 0; i < 3; i++ {
		again, err := store.ReconcileDueTimeouts(ctx, 10)
		require.NoError(t, err)
		require.Zero(t, again.TimedOutAttempts, "a repeated pass must find nothing to do")
	}
	require.Equal(t, finished, readAttemptOutcome(t, fence.AttemptID))
	require.Equal(t, 1, countRows(t, "job_attempts"))
}

// TestTimeout_ConcurrentReconcilersProduceOneOutcome is the N-replica case, on
// separate PostgreSQL connections so the serialization is the database's rather
// than the test's.
func TestTimeout_ConcurrentReconcilersProduceOneOutcome(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("timeout-replicas", 4, nil, []string{"demo.echo"}))

	const jobCount = 4
	fences := make([]workers.Fence, 0, jobCount)
	for i := 0; i < jobCount; i++ {
		fence, _ := claimedRunningWithTimeout(t, store, session,
			"timeout-replica-"+uuid.NewString()[:8], 3, 60)
		expireAttemptDeadline(t, fence.AttemptID)
		fences = append(fences, fence)
	}

	const replicas = 4
	results := make(chan workers.ReconcileStats, replicas)
	errs := make(chan error, replicas)
	start := make(chan struct{})
	for i := 0; i < replicas; i++ {
		go func() {
			<-start
			stats, err := store.ReconcileDueTimeouts(ctx, 50)
			results <- stats
			errs <- err
		}()
	}
	close(start)

	total := 0
	for i := 0; i < replicas; i++ {
		require.NoError(t, <-errs)
		total += (<-results).TimedOutAttempts
	}
	require.Equal(t, jobCount, total,
		"every attempt is timed out exactly once across all replicas")

	for _, fence := range fences {
		require.Equal(t, "TIMED_OUT", readAttemptOutcome(t, fence.AttemptID).status)
		require.Equal(t, "RETRY_WAIT", readJob(t, fence.JobID).status)
	}
	require.Equal(t, 0, countActiveLeases(t))
	require.Equal(t, jobCount, countRows(t, "job_attempts"))
}

// TestTimeout_ExpiredLeaseScanRecognizesAnAlreadyDueDeadline is the precedence
// rule that keeps a genuine timeout from being recorded as an abandonment.
//
// Getting this wrong is not cosmetic. ABANDONED requeues with no backoff and
// records no failure detail, so a job whose handler reliably takes too long
// would loop through its entire attempt budget at full speed, and its history
// would say it was interrupted rather than that it ran out of time.
func TestTimeout_ExpiredLeaseScanRecognizesAnAlreadyDueDeadline(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("timeout-vs-abandon", 1, nil, []string{"demo.echo"}))
	fence, _ := claimedRunningWithTimeout(t, store, session, "timeout-vs-abandon", 3, 60)

	// Both are true at once: the deadline passed AND the lease lapsed. This is
	// what a worker that stalled past its budget and then stopped renewing
	// actually leaves behind.
	expireAttemptDeadline(t, fence.AttemptID)
	expireLease(t, fence.LeaseID)

	// Only the expired-lease scan runs, so the precedence must live inside it.
	stats, err := store.ReconcileExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.ExpiredLeases)
	require.Equal(t, 1, stats.TimedOutAttempts, "the deadline had already passed")
	require.Zero(t, stats.RequeuedJobs, "abandonment's immediate requeue must not apply")
	require.Equal(t, 1, stats.RetryWaitingJobs)

	row := readAttemptOutcome(t, fence.AttemptID)
	require.Equal(t, "TIMED_OUT", row.status)
	require.Equal(t, "TIMED_OUT", *row.failureClass)
	require.NotNil(t, row.retryAt)

	state := readState(t, fence)
	require.Equal(t, "RETRY_WAIT", state.job, "a timeout backs off; an abandonment would not")
	require.Equal(t, "EXPIRED", state.lease,
		"the lease was taken away rather than handed back, and history says so")
	require.Equal(t, 0, countActiveLeases(t))
}

// TestTimeout_LeaseExpiryWithNoDueDeadlineIsStillAbandonment is the control for
// the test above: without a due deadline, ADR-0009's path is unchanged.
func TestTimeout_LeaseExpiryWithNoDueDeadlineIsStillAbandonment(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("abandon-control", 1, nil, []string{"demo.echo"}))
	fence, _ := claimedRunningWithTimeout(t, store, session, "abandon-control", 3, 3600)
	expireLease(t, fence.LeaseID)

	stats, err := store.ReconcileExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.ExpiredLeases)
	require.Zero(t, stats.TimedOutAttempts)
	require.Equal(t, 1, stats.RequeuedJobs)

	require.Equal(t, "ABANDONED", readAttemptOutcome(t, fence.AttemptID).status)
	require.Equal(t, "QUEUED", readJob(t, fence.JobID).status)
}

// TestTimeout_SuccessBeforeTheDeadlineWinsAndStaysTerminal is the ordinary case
// the timeout machinery must not disturb.
func TestTimeout_SuccessBeforeTheDeadlineWinsAndStaysTerminal(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("success-before", 1, nil, []string{"demo.echo"}))
	fence, _ := claimedRunningWithTimeout(t, store, session, "success-before", 3, 3600)

	require.NoError(t, store.Succeed(ctx, testScope, fence))
	require.Equal(t, "SUCCEEDED", readJob(t, fence.JobID).status)

	// Even if the deadline is later moved into the past, a committed success is
	// terminal and the scan must not touch it.
	expireAttemptDeadline(t, fence.AttemptID)

	stats, err := store.ReconcileDueTimeouts(ctx, 10)
	require.NoError(t, err)
	require.Zero(t, stats.TimedOutAttempts, "the scan filters on RUNNING attempts")
	require.Equal(t, "SUCCEEDED", readAttemptOutcome(t, fence.AttemptID).status)
	require.Equal(t, "SUCCEEDED", readJob(t, fence.JobID).status)
}

// TestTimeout_SuccessWaitingAcrossTheDeadlineIsRejected is the case a stale
// clock reading would get wrong. The success request passes its own precondition
// check, then waits on a lock, and by the time it holds authority the deadline
// has passed. It must be rejected against the FRESH post-lock sample.
func TestTimeout_SuccessWaitingAcrossTheDeadlineIsRejected(t *testing.T) {
	reset(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("success-across", 1, nil, []string{"demo.echo"}))
	fence, _ := claimedRunningWithTimeout(t, store, session, "success-across", 3, 3600)

	// A deadline just far enough ahead that the success request starts before it
	// and finishes after it.
	timeoutAt := setAttemptDeadline(t, fence.AttemptID, 750*time.Millisecond)

	queueLock, err := testPool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = queueLock.Rollback(context.Background()) })
	_, err = queueLock.Exec(ctx, `SELECT name FROM queues WHERE name = 'default' FOR UPDATE`)
	require.NoError(t, err)

	resultCh := make(chan error, 1)
	go func() { resultCh <- store.Succeed(ctx, testScope, fence) }()
	waitForDatabaseLock(t, "SELECT name FROM queues WHERE name")
	waitForServerTime(t, timeoutAt.Add(50*time.Millisecond))
	require.NoError(t, queueLock.Commit(ctx))

	require.ErrorIs(t, <-resultCh, workers.ErrAttemptTimedOut,
		"a success that waited across the deadline must not commit on a stale clock reading")

	state := readState(t, fence)
	require.Equal(t, "RUNNING", state.job, "the rejected success must mutate nothing")
	require.Equal(t, "RUNNING", state.attempt)
	require.Equal(t, "ACTIVE", state.lease)
	require.Equal(t, 1, countActiveLeases(t))
}

// TestTimeout_FailureAtOrAfterTheDeadlineCannotBecomeFailed closes the other
// misclassification route. A handler that returns an error just after its
// budget expired must not be able to record an ordinary FAILED attempt, because
// the truthful outcome is TIMED_OUT and only reconciliation may write it.
func TestTimeout_FailureAtOrAfterTheDeadlineCannotBecomeFailed(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("fail-after-deadline", 1, nil, []string{"demo.echo"}))
	fence, _ := claimedRunningWithTimeout(t, store, session, "fail-after-deadline", 3, 60)
	expireAttemptDeadline(t, fence.AttemptID)

	_, err := store.Fail(ctx, testScope,
		failureReport(fence, lifecycle.ClassRetryable, "transient", "too late"))
	require.ErrorIs(t, err, workers.ErrAttemptTimedOut)

	row := readAttemptOutcome(t, fence.AttemptID)
	require.Equal(t, "RUNNING", row.status, "the rejected report must mutate nothing")
	require.Nil(t, row.failureClass)
	require.Nil(t, row.outcomeID)

	// A permanent classification cannot sneak past it either: a handler must not
	// be able to dead-letter a job it has already lost the right to speak for.
	_, err = store.Fail(ctx, testScope,
		failureReport(fence, lifecycle.ClassPermanent, "invalid_payload", ""))
	require.ErrorIs(t, err, workers.ErrAttemptTimedOut)
	require.Empty(t, dlqRows(t, fence.JobID))

	// Reconciliation then records the truthful outcome.
	stats, err := store.ReconcileDueTimeouts(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.TimedOutAttempts)
	require.Equal(t, "TIMED_OUT", readAttemptOutcome(t, fence.AttemptID).status)
}

// TestTimeout_AnUncooperativeHandlerCannotCommitAfterwards is the durable half
// of the cooperative-cancellation limitation.
//
// Go cannot forcibly terminate a handler goroutine, so a handler that ignores
// its context keeps running. What TaskForge guarantees instead is that nothing
// it produces afterwards can be committed — proven here by letting a "still
// running" attempt attempt every outcome it could possibly report once its
// timeout has been reconciled.
func TestTimeout_AnUncooperativeHandlerCannotCommitAfterwards(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("uncooperative", 1, nil, []string{"demo.echo"}))
	fence, _ := claimedRunningWithTimeout(t, store, session, "uncooperative", 3, 60)
	expireAttemptDeadline(t, fence.AttemptID)

	stats, err := store.ReconcileDueTimeouts(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.TimedOutAttempts)
	afterTimeout := readAttemptOutcome(t, fence.AttemptID)

	// Everything the handler's worker could still try, long after its attempt
	// was closed. Each must be refused, and none may mutate anything.
	require.Error(t, store.Succeed(ctx, testScope, fence))
	_, err = store.Fail(ctx, testScope,
		failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
	require.Error(t, err)
	_, err = store.AcknowledgeCancellation(ctx, testScope,
		workers.CancelAcknowledgment{Fence: fence, OutcomeRequestID: uuid.New()})
	require.Error(t, err)
	_, err = store.RenewLease(ctx, testScope, renewalRequest(fence, 0))
	require.Error(t, err)
	_, err = store.Start(ctx, testScope, fence)
	require.Error(t, err)

	require.Equal(t, afterTimeout, readAttemptOutcome(t, fence.AttemptID),
		"an uncooperative handler must not be able to move a single stored field")
	require.Equal(t, "RETRY_WAIT", readJob(t, fence.JobID).status)
	require.Equal(t, 1, countRows(t, "job_attempts"))
}

// TestContention_TimeoutVersusSuccess arranges the race deliberately in both
// orderings. Neither is a scheduling accident: an advisory-lock gate on the
// statement each operation uniquely executes decides which one holds authority
// first, so "this one committed first" is a fact about the test rather than a
// hope about the scheduler.
func TestContention_TimeoutVersusSuccess(t *testing.T) {
	t.Run("timeout first: the later success is rejected and mutates nothing", func(t *testing.T) {
		reset(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("timeout-then-success", 1, nil, []string{"demo.echo"}))
		fence, _ := claimedRunningWithTimeout(t, store, session, "timeout-then-success", 3, 60)
		expireAttemptDeadline(t, fence.AttemptID)

		release := gateOnAdvisoryLockWhen(t, gateTimeoutBeforeSuccessKey,
			"taskforge_test_gate_timeout_first", "BEFORE UPDATE", "job_attempts", whenTimingOut)

		timeoutResult := make(chan workers.ReconcileStats, 1)
		timeoutErr := make(chan error, 1)
		go func() {
			stats, err := store.ReconcileDueTimeouts(ctx, 10)
			timeoutResult <- stats
			timeoutErr <- err
		}()
		// Parked at its attempt UPDATE, holding every authority row.
		waitForDatabaseLock(t, fragmentAttemptTx)

		successErr := make(chan error, 1)
		go func() { successErr <- store.Succeed(ctx, testScope, fence) }()
		waitForDatabaseLock(t, fragmentQueueLock)

		release()
		require.NoError(t, <-timeoutErr)
		require.Equal(t, 1, (<-timeoutResult).TimedOutAttempts,
			"the timeout held authority first and must commit")
		// Both "your lease is gone" and "your budget ran out" are true once the
		// timeout commits, and the second is what the worker actually needs.
		require.ErrorIs(t, <-successErr, workers.ErrAttemptTimedOut)

		state := readState(t, fence)
		require.Equal(t, "RETRY_WAIT", state.job)
		require.Equal(t, "TIMED_OUT", state.attempt)
		require.Equal(t, "RELEASED", state.lease)
		require.Equal(t, 0, countActiveLeases(t))
	})

	t.Run("success first: the later timeout finds nothing to do", func(t *testing.T) {
		reset(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("success-then-timeout", 1, nil, []string{"demo.echo"}))
		fence, _ := claimedRunningWithTimeout(t, store, session, "success-then-timeout", 3, 60)

		// The deadline must still be in the FUTURE when the success takes
		// authority — a success that had already outlived its budget is a
		// different case, covered by
		// TestTimeout_SuccessWaitingAcrossTheDeadlineIsRejected — and it must be
		// in the PAST by the time the reconciler scans, or there would be no
		// candidate to skip. Crossing the boundary between the two is the only
		// arrangement in which both paths are the real ones.
		timeoutAt := setAttemptDeadline(t, fence.AttemptID, 750*time.Millisecond)

		release := gateOnAdvisoryLockWhen(t, gateSuccessBeforeTimeoutKey,
			"taskforge_test_gate_success_first", "BEFORE UPDATE", "job_attempts", whenSucceeding)

		successErr := make(chan error, 1)
		go func() { successErr <- store.Succeed(ctx, testScope, fence) }()
		// Parked at its attempt UPDATE, holding every authority row, with its
		// deadline check already passed against a pre-expiry sample.
		waitForDatabaseLock(t, fragmentAttemptTx)
		waitForServerTime(t, timeoutAt.Add(50*time.Millisecond))

		timeoutResult := make(chan workers.ReconcileStats, 1)
		timeoutErr := make(chan error, 1)
		go func() {
			stats, err := store.ReconcileDueTimeouts(ctx, 10)
			timeoutResult <- stats
			timeoutErr <- err
		}()
		waitForDatabaseLock(t, fragmentQueueLock)

		release()
		require.NoError(t, <-successErr, "the success held authority first and must commit")
		require.NoError(t, <-timeoutErr)
		stats := <-timeoutResult
		require.Zero(t, stats.TimedOutAttempts, "a completed attempt must not be timed out")
		require.Equal(t, 1, stats.Skipped, "the stale candidate is revalidated and skipped")

		state := readState(t, fence)
		require.Equal(t, "SUCCEEDED", state.job, "a terminal job never returns to a non-terminal state")
		require.Equal(t, "SUCCEEDED", state.attempt)
		require.Equal(t, "COMPLETED", state.lease)
		require.Empty(t, dlqRows(t, fence.JobID))
	})
}
