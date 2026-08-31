//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/reconciler"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// Gate keys for the reconciliation barriers. Fixed and distinct for the same
// reason the M2 gates are: a failed run leaves a diagnosable lock.
const (
	reconcileGateKey int64 = 7710010003
	renewGateKey     int64 = 7710010004
)

func newReconciler(t *testing.T, store *workers.Store, staleAfter time.Duration) *reconciler.Reconciler {
	t.Helper()
	return reconciler.New(store, reconciler.Config{
		StaleAfter: staleAfter, PollInterval: 50 * time.Millisecond, BatchSize: 50,
	}, discardLogger())
}

// expireLease moves a lease's server-owned window into the past without touching
// anything else, which is exactly what a dead worker's lease looks like.
func expireLease(t *testing.T, leaseID uuid.UUID) {
	t.Helper()
	tag, err := testPool.Exec(context.Background(), `
		UPDATE leases
		SET acquired_at = clock_timestamp() - interval '2 minutes',
		    renewed_at = clock_timestamp() - interval '2 minutes',
		    expires_at = clock_timestamp() - interval '1 minute'
		WHERE id = $1`, leaseID)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
}

// pendingOutboxIDs snapshots the pending notifications so a test can assert what
// reconciliation added. A submission leaves its own pending event behind in these
// tests — no publisher runs — so a bare count would conflate the two.
func pendingOutboxIDs(t *testing.T) map[uuid.UUID]struct{} {
	t.Helper()
	rows, err := testPool.Query(context.Background(),
		`SELECT id FROM outbox_events WHERE status = 'PENDING'`)
	require.NoError(t, err)
	defer rows.Close()
	ids := map[uuid.UUID]struct{}{}
	for rows.Next() {
		var id uuid.UUID
		require.NoError(t, rows.Scan(&id))
		ids[id] = struct{}{}
	}
	require.NoError(t, rows.Err())
	return ids
}

// newPendingOutbox returns the pending events created since the snapshot.
func newPendingOutbox(t *testing.T, before map[uuid.UUID]struct{}) []uuid.UUID {
	t.Helper()
	var added []uuid.UUID
	for id := range pendingOutboxIDs(t) {
		if _, existed := before[id]; !existed {
			added = append(added, id)
		}
	}
	return added
}

type jobState struct {
	job     string
	attempt string
	lease   string
}

func readState(t *testing.T, fence workers.Fence) jobState {
	t.Helper()
	var state jobState
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT j.status, a.status, l.status
		FROM jobs j
		JOIN job_attempts a ON a.id = $2
		JOIN leases l ON l.id = $3
		WHERE j.id = $1`, fence.JobID, fence.AttemptID, fence.LeaseID,
	).Scan(&state.job, &state.attempt, &state.lease))
	return state
}

func attemptHistory(t *testing.T, jobID uuid.UUID) []string {
	t.Helper()
	rows, err := testPool.Query(context.Background(),
		`SELECT status FROM job_attempts WHERE job_id = $1 ORDER BY attempt_number`, jobID)
	require.NoError(t, err)
	defer rows.Close()
	var history []string
	for rows.Next() {
		var status string
		require.NoError(t, rows.Scan(&status))
		history = append(history, status)
	}
	require.NoError(t, rows.Err())
	return history
}

func leaseHistory(t *testing.T, jobID uuid.UUID) []string {
	t.Helper()
	rows, err := testPool.Query(context.Background(),
		`SELECT l.status FROM leases l JOIN job_attempts a ON a.id = l.attempt_id
		 WHERE l.job_id = $1 ORDER BY a.attempt_number`, jobID)
	require.NoError(t, err)
	defer rows.Close()
	var history []string
	for rows.Next() {
		var status string
		require.NoError(t, rows.Scan(&status))
		history = append(history, status)
	}
	require.NoError(t, rows.Err())
	return history
}

// claimedAndRunning gets a job all the way to RUNNING so a test can start from
// the state a crashed worker would actually leave behind.
func claimedAndRunning(t *testing.T, store *workers.Store, session workers.Session, key string) workers.Fence {
	t.Helper()
	createJob(t, key, "demo.echo", 50, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claim.Disposition)
	fence := assignmentFence(claim.Assignment)
	require.NoError(t, store.Start(context.Background(), testScope, fence))
	return fence
}

// TestReconcile_ExpiredLeaseIsRecoveredEvenWhenItsSessionIsHealthy is the case
// the milestone would strand if reconciliation required both conditions. The
// worker deliberately leaves a lease active after a handler error while its
// process keeps heartbeating.
func TestReconcile_ExpiredLeaseIsRecoveredEvenWhenItsSessionIsHealthy(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("reconcile-healthy-worker", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "reconcile-healthy")
	expireLease(t, fence.LeaseID)

	// The session is demonstrably still current and heartbeating.
	heartbeat(t, store, session)
	require.Equal(t, "HEALTHY", sessionStatus(t, session.ID))
	require.Equal(t, 1, countActiveLeases(t))

	before := serverNow(t)
	outboxBefore := pendingOutboxIDs(t)
	result, err := newReconciler(t, store, time.Minute).RunOnce(context.Background())
	require.NoError(t, err)
	require.Zero(t, result.StaleSessions, "a heartbeating session must not be fenced")
	require.Equal(t, 1, result.ExpiredLeases)
	require.Equal(t, 1, result.RequeuedJobs)

	state := readState(t, fence)
	require.Equal(t, "QUEUED", state.job)
	require.Equal(t, "ABANDONED", state.attempt)
	require.Equal(t, "EXPIRED", state.lease)
	require.Equal(t, 0, countActiveLeases(t), "expiry must release the capacity the lease held")
	require.Equal(t, "HEALTHY", sessionStatus(t, session.ID))

	// Timestamps come from PostgreSQL, in the same transaction.
	var releasedAt, finishedAt, availableAt time.Time
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT l.released_at, a.finished_at, j.available_at
		FROM leases l
		JOIN job_attempts a ON a.id = l.attempt_id
		JOIN jobs j ON j.id = l.job_id
		WHERE l.id = $1`, fence.LeaseID).Scan(&releasedAt, &finishedAt, &availableAt))
	require.False(t, releasedAt.Before(before))
	require.True(t, finishedAt.Equal(releasedAt), "one server sample stamps the whole repair")
	require.True(t, availableAt.Equal(releasedAt))

	// Exactly one fresh recovery notification, and never the consumed claim id.
	recovery := newPendingOutbox(t, outboxBefore)
	require.Len(t, recovery, 1)
	var consumedClaimID uuid.UUID
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT claim_request_id FROM leases WHERE id = $1`, fence.LeaseID).Scan(&consumedClaimID))
	require.NotEqual(t, consumedClaimID, recovery[0],
		"the original event id was globally consumed; recovery needs a fresh one")
	var eventType string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT event_type FROM outbox_events WHERE id = $1`, recovery[0]).Scan(&eventType))
	require.Equal(t, "work.available", eventType)
}

// An attempt that never started is abandoned exactly like one that did. The
// attempt timeline constraint permits ABANDONED with no start time.
func TestReconcile_AbandonsAnAttemptThatNeverStarted(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("reconcile-leased-worker", 1, nil, []string{"demo.echo"}))
	createJob(t, "reconcile-leased", "demo.echo", 50, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	fence := assignmentFence(claim.Assignment)
	require.Equal(t, "LEASED", readState(t, fence).attempt)

	expireLease(t, fence.LeaseID)
	result, err := newReconciler(t, store, time.Minute).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.ExpiredLeases)

	state := readState(t, fence)
	require.Equal(t, "ABANDONED", state.attempt)
	require.Equal(t, "EXPIRED", state.lease)
	require.Equal(t, "QUEUED", state.job)

	var startedAt *time.Time
	var finishedAt time.Time
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT started_at, finished_at FROM job_attempts WHERE id = $1`,
		fence.AttemptID).Scan(&startedAt, &finishedAt))
	require.Nil(t, startedAt, "an attempt that never ran has no start time")
	require.NotNil(t, finishedAt)
}

// TestReconcile_ReleasesQueueAndLogicalWorkerCapacityAtomically proves both
// ledgers recover, including the logical-worker one that a replaced boot cannot
// evade.
func TestReconcile_ReleasesQueueAndLogicalWorkerCapacityAtomically(t *testing.T) {
	reset(t)
	store := controlStore()
	registration := workerRegistration("reconcile-capacity-worker", 1, nil, []string{"demo.echo"})
	original := registerWorker(t, store, registration)
	fence := claimedAndRunning(t, store, original, "reconcile-capacity")

	// A replacement boot inherits nothing: the old lease still reserves the one
	// slot this logical worker has.
	replacement := registerReplacement(t, store, registration)
	createJob(t, "reconcile-capacity-two", "demo.echo", 50, nil)
	blocked, err := store.Claim(context.Background(), testScope, claimRequest(replacement, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.CapacityExhausted, blocked.Disposition)

	expireLease(t, fence.LeaseID)
	result, err := newReconciler(t, store, time.Minute).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.ExpiredLeases)
	require.Equal(t, 0, countActiveLeases(t))

	// With the stale lease closed, the replacement can claim again.
	unblocked, err := store.Claim(context.Background(), testScope, claimRequest(replacement, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, unblocked.Disposition,
		"releasing the expired lease must return the logical worker's capacity")
}

// A replacement worker claims attempt 2 while attempt 1's abandoned history is
// preserved. Attempt numbers stay unique and monotonic.
func TestReconcile_ReplacementClaimsAttemptTwoAndPreservesHistory(t *testing.T) {
	reset(t)
	store := controlStore()
	first := registerWorker(t, store,
		workerRegistration("reconcile-attempt-one", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, first, "reconcile-attempts")
	expireLease(t, fence.LeaseID)

	_, err := newReconciler(t, store, time.Minute).RunOnce(context.Background())
	require.NoError(t, err)

	second := registerWorker(t, store,
		workerRegistration("reconcile-attempt-two", 1, nil, []string{"demo.echo"}))
	claim, err := store.Claim(context.Background(), testScope, claimRequest(second, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claim.Disposition)
	require.Equal(t, 2, claim.Assignment.AttemptNumber)

	replacementFence := assignmentFence(claim.Assignment)
	require.NoError(t, store.Start(context.Background(), testScope, replacementFence))
	require.NoError(t, store.Succeed(context.Background(), testScope, replacementFence))

	require.Equal(t, []string{"ABANDONED", "SUCCEEDED"}, attemptHistory(t, fence.JobID))
	require.Equal(t, []string{"EXPIRED", "COMPLETED"}, leaseHistory(t, fence.JobID))
	require.Equal(t, 0, countActiveLeases(t))

	// Every late mutation from the dead worker is still rejected.
	require.ErrorIs(t, store.Succeed(context.Background(), testScope, fence), workers.ErrLeaseExpired)
	_, err = store.RenewLease(context.Background(), testScope, renewalRequest(fence, 0))
	require.ErrorIs(t, err, workers.ErrLeaseExpired)
}

// TestReconcile_ExhaustedAttemptBudgetDeadLettersInsteadOfStranding is the
// narrow terminal consequence M3 owns. Requeueing here would produce a QUEUED
// job the claim predicate can never take, and leaving it RUNNING would strand it
// forever. See docs/adr/0009.
func TestReconcile_ExhaustedAttemptBudgetDeadLettersInsteadOfStranding(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("reconcile-budget-worker", 1, nil, []string{"demo.echo"}))
	jobID := createJob(t, "reconcile-budget", "demo.echo", 50, nil)
	_, err := testPool.Exec(context.Background(),
		`UPDATE jobs SET max_attempts = 1 WHERE id = $1`, jobID)
	require.NoError(t, err)

	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	fence := assignmentFence(claim.Assignment)
	require.NoError(t, store.Start(context.Background(), testScope, fence))
	expireLease(t, fence.LeaseID)
	outboxBefore := pendingOutboxIDs(t)

	result, err := newReconciler(t, store, time.Minute).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.ExpiredLeases)
	require.Equal(t, 1, result.DeadLetteredJobs)
	require.Zero(t, result.RequeuedJobs)

	state := readState(t, fence)
	require.Equal(t, "DEAD_LETTERED", state.job)
	require.Equal(t, "ABANDONED", state.attempt)
	require.Equal(t, "EXPIRED", state.lease)
	require.Equal(t, 0, countActiveLeases(t))
	require.Empty(t, newPendingOutbox(t, outboxBefore),
		"a job that can never be claimed again must not be advertised as available")
}

// Reconciliation is idempotent. Running it again over settled state changes
// nothing and, critically, produces no second recovery notification.
func TestReconcile_RepeatedRunsAreANoOp(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("reconcile-idempotent-worker", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "reconcile-idempotent")
	expireLease(t, fence.LeaseID)
	outboxBefore := pendingOutboxIDs(t)

	engine := newReconciler(t, store, time.Minute)
	first, err := engine.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, first.ExpiredLeases)
	settled := readState(t, fence)

	for i := 0; i < 3; i++ {
		again, err := engine.RunOnce(context.Background())
		require.NoError(t, err)
		require.Zero(t, again.ExpiredLeases)
		require.Zero(t, again.RequeuedJobs)
		require.Zero(t, again.DeadLetteredJobs)
	}
	require.Equal(t, settled, readState(t, fence))
	require.Len(t, newPendingOutbox(t, outboxBefore), 1, "exactly one recovery notification, ever")
	require.Equal(t, 1, countRows(t, "job_attempts"))
}

// Several reconcilers on separate connections must produce exactly one
// abandonment and one notification, which is the N-replica safety claim.
func TestReconcile_ConcurrentReconcilersProduceOneRepair(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("reconcile-concurrent-worker", 4, nil, []string{"demo.echo"}))

	const jobs = 4
	fences := make([]workers.Fence, 0, jobs)
	for i := 0; i < jobs; i++ {
		fences = append(fences, claimedAndRunning(t, store, session, fmt.Sprintf("reconcile-concurrent-%d", i)))
	}
	for _, fence := range fences {
		expireLease(t, fence.LeaseID)
	}
	require.Equal(t, jobs, countActiveLeases(t))
	outboxBefore := pendingOutboxIDs(t)

	const replicas = 4
	var wg sync.WaitGroup
	results := make(chan reconciler.Result, replicas)
	errs := make(chan error, replicas)
	start := make(chan struct{})
	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := newReconciler(t, store, time.Minute).RunOnce(context.Background())
			results <- result
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err, "concurrent reconcilers must not error, only skip")
	}
	repaired := 0
	for result := range results {
		repaired += result.ExpiredLeases
	}
	require.Equal(t, jobs, repaired, "each expired lease is repaired exactly once, by exactly one replica")

	require.Equal(t, 0, countActiveLeases(t))
	require.Equal(t, jobs, countRows(t, "job_attempts"))
	require.Len(t, newPendingOutbox(t, outboxBefore), jobs,
		"one recovery notification per recovered job, and no duplicates")
	for _, fence := range fences {
		state := readState(t, fence)
		require.Equal(t, "QUEUED", state.job)
		require.Equal(t, "ABANDONED", state.attempt)
		require.Equal(t, "EXPIRED", state.lease)
	}
}

// TestReconcile_FaultBeforeCommitRollsBackEveryChange proves the whole repair is
// one transaction. A trigger fails the recovery notification insert, which is
// the last write, so everything before it must vanish too.
func TestReconcile_FaultBeforeCommitRollsBackEveryChange(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("reconcile-fault-worker", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "reconcile-fault")
	expireLease(t, fence.LeaseID)
	before := readState(t, fence)
	outboxBefore := pendingOutboxIDs(t)

	_, err := testPool.Exec(context.Background(), `
		CREATE OR REPLACE FUNCTION taskforge_test_fail_recovery_event() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected recovery failure'; END $$`)
	require.NoError(t, err)
	_, err = testPool.Exec(context.Background(), `
		CREATE TRIGGER taskforge_test_fail_recovery_event
		BEFORE INSERT ON outbox_events FOR EACH ROW
		EXECUTE FUNCTION taskforge_test_fail_recovery_event()`)
	require.NoError(t, err)
	dropTrigger := func() {
		_, _ = testPool.Exec(context.Background(),
			`DROP TRIGGER IF EXISTS taskforge_test_fail_recovery_event ON outbox_events`)
		_, _ = testPool.Exec(context.Background(),
			`DROP FUNCTION IF EXISTS taskforge_test_fail_recovery_event()`)
	}
	t.Cleanup(dropTrigger)

	_, err = store.ReconcileExpiredLeases(context.Background(), 10)
	require.Error(t, err)

	require.Equal(t, before, readState(t, fence),
		"a fault before commit must leave the old state exactly as it was")
	require.Equal(t, 1, countActiveLeases(t), "no capacity may be released by a rolled-back repair")
	require.Empty(t, newPendingOutbox(t, outboxBefore))

	// Rerunning after the fault clears repairs it. This is the "ambiguous commit,
	// then rerun" recovery path: reconciliation is safe to simply try again.
	dropTrigger()
	result, err := newReconciler(t, store, time.Minute).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.ExpiredLeases)
	require.Equal(t, 1, result.RequeuedJobs)
	require.Equal(t, "ABANDONED", readState(t, fence).attempt)
	require.Len(t, newPendingOutbox(t, outboxBefore), 1)
}

// --- contention: every pair, in both lock orderings ------------------------
//
// Each of these arranges the race deliberately with an advisory-lock gate rather
// than leaving it to the scheduler, so "this operation acquired authority first"
// is a fact about the test, not a hope.

// TestContention_RenewalVersusSuccess proves the two operations a live worker
// can issue concurrently serialize into exactly one valid outcome.
func TestContention_RenewalVersusSuccess(t *testing.T) {
	t.Run("renewal first: it holds the lease row, success waits, both commit", func(t *testing.T) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("renew-then-succeed", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "renew-then-succeed")

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		// Park the renewal at its own UPDATE, after lockFence took queue, session,
		// job, attempt, and lease.
		release := gateOnAdvisoryLock(t, renewGateKey,
			"taskforge_test_gate_renew", "BEFORE UPDATE", "leases")

		renewErr := make(chan error, 1)
		go func() {
			_, err := store.RenewLease(ctx, testScope, renewalRequest(fence, 0))
			renewErr <- err
		}()
		waitForDatabaseLock(t, "UPDATE leases")

		succeedErr := make(chan error, 1)
		go func() { succeedErr <- store.Succeed(ctx, testScope, fence) }()
		waitForDatabaseLock(t, "SELECT name FROM queues WHERE name")

		release()
		require.NoError(t, <-renewErr, "the renewal held authority first and must commit")
		require.NoError(t, <-succeedErr, "success after a renewal is still valid")

		state := readState(t, fence)
		require.Equal(t, "SUCCEEDED", state.job)
		require.Equal(t, "SUCCEEDED", state.attempt)
		require.Equal(t, "COMPLETED", state.lease)
		require.Equal(t, 0, countActiveLeases(t))
	})

	t.Run("success first: the later renewal is rejected and mutates nothing", func(t *testing.T) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("succeed-then-renew", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "succeed-then-renew")

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		queueLock, err := testPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = queueLock.Rollback(context.Background()) })
		_, err = queueLock.Exec(ctx, `SELECT name FROM queues WHERE name = 'default' FOR UPDATE`)
		require.NoError(t, err)

		// Park the renewal on the queue lock, then let success commit ahead of it.
		renewErr := make(chan error, 1)
		go func() {
			_, err := store.RenewLease(ctx, testScope, renewalRequest(fence, 0))
			renewErr <- err
		}()
		waitForDatabaseLock(t, "SELECT name FROM queues WHERE name")

		require.NoError(t, queueLock.Commit(ctx))

		// Whichever of the two acquires the released queue lock first, the durable
		// end state must be a single successful outcome with no second extension.
		require.NoError(t, store.Succeed(context.Background(), testScope, fence))
		if err := <-renewErr; err != nil {
			require.True(t,
				errorIsAny(err, workers.ErrLeaseExpired, workers.ErrStateConflict),
				"a renewal that lost to success must be a stable rejection, got %v", err)
		}

		state := readState(t, fence)
		require.Equal(t, "SUCCEEDED", state.job)
		require.Equal(t, "SUCCEEDED", state.attempt)
		require.Equal(t, "COMPLETED", state.lease)

		// After success, renewal is definitively closed.
		_, err = store.RenewLease(context.Background(), testScope, renewalRequest(fence, 0))
		require.ErrorIs(t, err, workers.ErrLeaseExpired)
	})
}

// TestContention_RenewalVersusReconciliation is the race M3 must never get
// wrong: a renewal that commits first moves the expiry forward, and a candidate
// scan that already selected that lease must revalidate and skip it.
func TestContention_RenewalVersusReconciliation(t *testing.T) {
	t.Run("renewal first: reconciliation revalidates and skips the lease", func(t *testing.T) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("renew-then-reconcile", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "renew-then-reconcile")
		expireLease(t, fence.LeaseID)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		queueLock, err := testPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = queueLock.Rollback(context.Background()) })
		_, err = queueLock.Exec(ctx, `SELECT name FROM queues WHERE name = 'default' FOR UPDATE`)
		require.NoError(t, err)

		// The reconciler has already chosen this lease as a candidate and is now
		// parked before it can take authority. Its snapshot is deliberately stale.
		reconcileResult := make(chan workers.ReconcileStats, 1)
		reconcileErr := make(chan error, 1)
		go func() {
			stats, err := store.ReconcileExpiredLeases(ctx, 10)
			reconcileResult <- stats
			reconcileErr <- err
		}()
		waitForDatabaseLock(t, "SELECT name FROM queues WHERE name")

		// Move the expiry forward out of band, exactly as a committed renewal
		// would. The renewal path itself is proven elsewhere; what is under test
		// here is that reconciliation resamples and yields.
		_, err = testPool.Exec(ctx, `
			UPDATE leases SET renewed_at = clock_timestamp(),
			                  expires_at = clock_timestamp() + interval '5 minutes',
			                  renewal_version = renewal_version + 1,
			                  last_renewal_request_id = $2
			WHERE id = $1`, fence.LeaseID, uuid.New())
		require.NoError(t, err)
		require.NoError(t, queueLock.Commit(ctx))

		require.NoError(t, <-reconcileErr)
		stats := <-reconcileResult
		require.Zero(t, stats.ExpiredLeases, "a renewed lease must not be expired")
		require.Equal(t, 1, stats.Skipped, "the stale candidate must be revalidated and skipped")

		state := readState(t, fence)
		require.Equal(t, "RUNNING", state.job)
		require.Equal(t, "RUNNING", state.attempt)
		require.Equal(t, "ACTIVE", state.lease)
		require.Equal(t, 1, countActiveLeases(t))
	})

	t.Run("reconciliation first: the later renewal is rejected and mutates nothing", func(t *testing.T) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("reconcile-then-renew", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "reconcile-then-renew")
		expireLease(t, fence.LeaseID)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		queueLock, err := testPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = queueLock.Rollback(context.Background()) })
		_, err = queueLock.Exec(ctx, `SELECT name FROM queues WHERE name = 'default' FOR UPDATE`)
		require.NoError(t, err)

		// Park the renewal, let reconciliation commit ahead of it.
		renewErr := make(chan error, 1)
		go func() {
			_, err := store.RenewLease(ctx, testScope, renewalRequest(fence, 0))
			renewErr <- err
		}()
		waitForDatabaseLock(t, "SELECT name FROM queues WHERE name")
		require.NoError(t, queueLock.Commit(ctx))

		stats, err := store.ReconcileExpiredLeases(context.Background(), 10)
		require.NoError(t, err)
		require.Equal(t, 1, stats.ExpiredLeases)

		if err := <-renewErr; err != nil {
			require.True(t, errorIsAny(err, workers.ErrLeaseExpired, workers.ErrStateConflict),
				"a renewal that lost to reconciliation must be a stable rejection, got %v", err)
		}

		state := readState(t, fence)
		require.Equal(t, "QUEUED", state.job)
		require.Equal(t, "ABANDONED", state.attempt)
		require.Equal(t, "EXPIRED", state.lease)
		_, _, _, version := leaseRow(t, fence.LeaseID)
		require.Equal(t, 0, version, "a rejected renewal must not have advanced the generation")
	})
}

// TestContention_SuccessVersusReconciliation is the case where a slow worker
// finishes at the same moment the reconciler decides it is gone. Exactly one of
// the two outcomes may be durable.
func TestContention_SuccessVersusReconciliation(t *testing.T) {
	t.Run("success first: reconciliation finds nothing to repair", func(t *testing.T) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("succeed-then-reconcile", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "succeed-then-reconcile")

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		queueLock, err := testPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = queueLock.Rollback(context.Background()) })
		_, err = queueLock.Exec(ctx, `SELECT name FROM queues WHERE name = 'default' FOR UPDATE`)
		require.NoError(t, err)

		// The reconciler selected this lease before success committed. Expire it so
		// the candidate scan genuinely picks it up.
		expireLease(t, fence.LeaseID)
		reconcileResult := make(chan workers.ReconcileStats, 1)
		reconcileErr := make(chan error, 1)
		go func() {
			stats, err := store.ReconcileExpiredLeases(ctx, 10)
			reconcileResult <- stats
			reconcileErr <- err
		}()
		waitForDatabaseLock(t, "SELECT name FROM queues WHERE name")

		// Commit the success out of band, which is what a worker that beat the
		// reconciler by a hair would have done.
		_, err = testPool.Exec(ctx,
			`UPDATE jobs SET status = 'SUCCEEDED', updated_at = clock_timestamp() WHERE id = $1`, fence.JobID)
		require.NoError(t, err)
		_, err = testPool.Exec(ctx,
			`UPDATE job_attempts SET status = 'SUCCEEDED', finished_at = clock_timestamp() WHERE id = $1`, fence.AttemptID)
		require.NoError(t, err)
		_, err = testPool.Exec(ctx,
			`UPDATE leases SET status = 'COMPLETED', released_at = clock_timestamp() WHERE id = $1`, fence.LeaseID)
		require.NoError(t, err)
		require.NoError(t, queueLock.Commit(ctx))

		require.NoError(t, <-reconcileErr)
		stats := <-reconcileResult
		require.Zero(t, stats.ExpiredLeases, "a completed lease is not reconciled")
		require.Equal(t, 1, stats.Skipped)

		state := readState(t, fence)
		require.Equal(t, "SUCCEEDED", state.job, "terminal success is never overwritten")
		require.Equal(t, "SUCCEEDED", state.attempt)
		require.Equal(t, "COMPLETED", state.lease)
	})

	t.Run("reconciliation first: the late success is rejected and mutates nothing", func(t *testing.T) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("reconcile-then-succeed", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "reconcile-then-succeed")
		expireLease(t, fence.LeaseID)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		queueLock, err := testPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = queueLock.Rollback(context.Background()) })
		_, err = queueLock.Exec(ctx, `SELECT name FROM queues WHERE name = 'default' FOR UPDATE`)
		require.NoError(t, err)

		succeedErr := make(chan error, 1)
		go func() { succeedErr <- store.Succeed(ctx, testScope, fence) }()
		waitForDatabaseLock(t, "SELECT name FROM queues WHERE name")
		require.NoError(t, queueLock.Commit(ctx))

		stats, err := store.ReconcileExpiredLeases(context.Background(), 10)
		require.NoError(t, err)
		require.Equal(t, 1, stats.ExpiredLeases)

		if err := <-succeedErr; err != nil {
			require.True(t, errorIsAny(err, workers.ErrLeaseExpired, workers.ErrStateConflict),
				"a success that lost to reconciliation must be a stable rejection, got %v", err)
		}

		state := readState(t, fence)
		require.Equal(t, "QUEUED", state.job)
		require.Equal(t, "ABANDONED", state.attempt)
		require.Equal(t, "EXPIRED", state.lease)
		require.Equal(t, 0, countActiveLeases(t))
	})
}

// TestReconcile_NoDeadlockUnderEveryConcurrentOperation runs claim, heartbeat,
// renewal, success, registration replacement, and reconciliation against one
// another on separate connections. A lock-order inversion would surface here as
// a PostgreSQL deadlock (SQLSTATE 40P01), which is a hard failure, not a retry.
func TestReconcile_NoDeadlockUnderEveryConcurrentOperation(t *testing.T) {
	reset(t)
	store := controlStore()

	const workerCount = 4
	sessions := make([]workers.Session, 0, workerCount)
	registrations := make([]workers.Registration, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		registration := workerRegistration(fmt.Sprintf("deadlock-worker-%d", i), 4, nil, []string{"demo.echo"})
		registrations = append(registrations, registration)
		sessions = append(sessions, registerWorker(t, store, registration))
	}
	for i := 0; i < 24; i++ {
		createJob(t, fmt.Sprintf("deadlock-%d", i), "demo.echo", 50, nil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var mu sync.Mutex
	var deadlocks []string
	record := func(op string, err error) {
		if err == nil {
			return
		}
		// Every domain rejection is expected under this much contention; a
		// deadlock is not, and neither is any other driver-level fault.
		if errorIsAny(err, workers.ErrSessionUnavailable, workers.ErrFenceRejected,
			workers.ErrLeaseExpired, workers.ErrStateConflict, workers.ErrRenewalConflict,
			workers.ErrClaimConflict, context.Canceled, context.DeadlineExceeded) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		deadlocks = append(deadlocks, op+": "+err.Error())
	}

	var wg sync.WaitGroup
	// A closed channel, not time.After: a timer channel delivers its value to
	// exactly one receiver, so every other spinner would run until ctx expired.
	stop := make(chan struct{})
	stopTimer := time.AfterFunc(3*time.Second, func() { close(stop) })
	defer stopTimer.Stop()
	spin := func(op string, body func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case <-ctx.Done():
					return
				default:
				}
				record(op, body())
			}
		}()
	}

	for i := range sessions {
		session := sessions[i]
		registration := registrations[i]
		spin("claim", func() error {
			claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
			if err != nil || claim.Assignment == nil {
				return err
			}
			fence := assignmentFence(claim.Assignment)
			record("start", store.Start(ctx, testScope, fence))
			if _, err := store.RenewLease(ctx, testScope, renewalRequest(fence, 0)); err != nil {
				record("renew", err)
			}
			return store.Succeed(ctx, testScope, fence)
		})
		spin("heartbeat", func() error {
			_, err := store.Heartbeat(ctx, testScope,
				workers.HeartbeatRequest{WorkerID: session.WorkerID, SessionID: session.ID})
			return err
		})
		if i == workerCount-1 {
			// One logical worker keeps replacing its own boot, which is the
			// operation that contends with everything else on the session row.
			spin("register", func() error {
				replacement := registration
				replacement.SessionID = uuid.New()
				_, err := store.Register(ctx, testScope, replacement)
				return err
			})
		}
	}
	// Continuously age active leases into the past so the reconciler always has
	// real work. acquired_at and renewed_at move with expires_at because the
	// lease timeline constraints require expires_at > both of them.
	spin("expire", func() error {
		_, err := testPool.Exec(ctx, `
			UPDATE leases
			SET acquired_at = clock_timestamp() - interval '2 minutes',
			    renewed_at = clock_timestamp() - interval '2 minutes',
			    expires_at = clock_timestamp() - interval '1 minute'
			WHERE status = 'ACTIVE'`)
		return err
	})
	for i := 0; i < 3; i++ {
		spin("reconcile-leases", func() error {
			_, err := store.ReconcileExpiredLeases(ctx, 20)
			return err
		})
		spin("mark-stale", func() error {
			_, err := store.MarkStaleSessions(ctx, time.Millisecond, 20)
			return err
		})
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Empty(t, deadlocks,
		"concurrent claim, heartbeat, renewal, success, replacement, and reconciliation must not deadlock")
}

func errorIsAny(err error, targets ...error) bool {
	for _, target := range targets {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
