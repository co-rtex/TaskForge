//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/scheduler"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// These tests are the restart half of reliability invariant 17: no correctness
// property depends solely on in-memory state. The API, the outbox publisher and
// the worker have their own restart tests; the scheduler and the reconciler are
// here. See docs/VERIFICATION_MATRIX.md.
//
// A "process" below is everything that dies with a real one: its own
// connection pool, its own engine, and its own run loop. Nothing is shared with
// the process that replaces it except PostgreSQL.

const gateReconcilerCrashKey int64 = 7710010070

// newProcessPool opens a connection pool that belongs to one simulated
// process. Closing it is that process exiting.
func newProcessPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := database.Connect(context.Background(), dsn())
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// countingScheduleStore wraps the scheduler's real durable store and counts what
// each pass did, so a test can wait for "the loop has run N passes" instead of
// sleeping for a guessed duration, and can state exactly how many promotions a
// process performed.
type countingScheduleStore struct {
	inner    scheduler.Store
	passes   atomic.Int64
	promoted atomic.Int64
}

func (c *countingScheduleStore) PromoteDueJobs(ctx context.Context, limit int) (jobs.SchedulerStats, error) {
	stats, err := c.inner.PromoteDueJobs(ctx, limit)
	c.promoted.Add(int64(stats.PromotedJobs))
	c.passes.Add(1)
	return stats, err
}

func (c *countingScheduleStore) RenotifyStrandedQueued(
	ctx context.Context, after time.Duration, limit int,
) (jobs.SchedulerStats, error) {
	return c.inner.RenotifyStrandedQueued(ctx, after, limit)
}

// schedulerProcess is one running taskforge-scheduler, minus the operating
// system: a pool, an engine, and the real Run loop.
type schedulerProcess struct {
	pool    *pgxpool.Pool
	store   *countingScheduleStore
	cancel  context.CancelFunc
	done    chan error
	stopped sync.Once
}

func startSchedulerProcess(t *testing.T) *schedulerProcess {
	t.Helper()
	pool := newProcessPool(t)
	counting := &countingScheduleStore{inner: jobs.NewStore(pool)}
	engine := scheduler.New(counting, scheduler.Config{
		PollInterval: 25 * time.Millisecond, BatchSize: 50,
		// Long, so re-notification stays out of the way: this test is about
		// promotion, and a QUEUED job is not stranded for an hour.
		RenotifyAfter: time.Hour,
	}, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	process := &schedulerProcess{pool: pool, store: counting, cancel: cancel, done: make(chan error, 1)}
	go func() { process.done <- engine.Run(ctx) }()
	t.Cleanup(func() { process.stop(t) })
	return process
}

// awaitPasses blocks until the loop has completed n more passes than it had.
func (p *schedulerProcess) awaitPasses(t *testing.T, n int64) {
	t.Helper()
	target := p.store.passes.Load() + n
	eventually(t, 30*time.Second, fmt.Sprintf("the scheduler completes %d more passes", n), func() bool {
		return p.store.passes.Load() >= target
	})
}

// stop ends the process: its loop is cancelled and its pool is closed, so
// nothing of it survives. It is safe to call twice.
func (p *schedulerProcess) stop(t *testing.T) {
	t.Helper()
	p.stopped.Do(func() {
		p.cancel()
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
			t.Error("the scheduler loop did not stop after cancellation")
		}
		p.pool.Close()
	})
}

// TestSchedulerRestart_RetryWaitSurvivesAndIsPromotedExactlyOnce is the
// scheduler's restart proof, for reliability invariants 13 and 17.
//
// A retryable failure puts a job in RETRY_WAIT with a persisted due time, and
// that wait is held by nothing but PostgreSQL. The timeline here is the one that
// would expose a scheduler that kept it anywhere else:
//
//  1. scheduler A runs while the retry is not due, and promotes nothing;
//  2. A exits;
//  3. the retry comes due while NO scheduler is running;
//  4. scheduler B starts cold and promotes the job -- once;
//  5. B exits and scheduler C starts cold: it must not promote it again.
//
// The backoff is made due by moving available_at into the past with
// PostgreSQL's own clock, exactly as the existing retry tests do, rather than
// by sleeping through it (AGENTS.md section 7).
func TestSchedulerRestart_RetryWaitSurvivesAndIsPromotedExactlyOnce(t *testing.T) {
	reset(t)
	ctx := context.Background()

	// A one-hour base delay guarantees the retry cannot come due on its own while
	// scheduler A is alive, however slow the machine running this is.
	store := storeWithRetryPolicy(lifecycle.RetryPolicy{
		Base: time.Hour, Max: 2 * time.Hour, Multiplier: 2, Jitter: 0,
	})
	session := registerWorker(t, store,
		workerRegistration("scheduler-restart", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "scheduler-restart")
	outcome, err := store.Fail(ctx, testScope,
		failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
	require.NoError(t, err)
	require.Equal(t, "RETRY_WAIT", outcome.JobStatus)

	jobID := fence.JobID
	waiting := durableSnapshot(t, jobID)
	require.Equal(t, "RETRY_WAIT", readJob(t, jobID).status)
	require.Len(t, eventsForJob(t, jobID), 1, "only the submission's notification exists while the job waits")
	failedAttempt := readAttemptOutcome(t, fence.AttemptID)
	require.NotNil(t, failedAttempt.retryAt, "the retry decision is persisted on the attempt")

	// --- 1-2: scheduler A runs while the retry is not due, then exits --------
	processA := startSchedulerProcess(t)
	processA.awaitPasses(t, 3)
	require.Zero(t, processA.store.promoted.Load(), "a retry that is not due must not be promoted")
	require.Equal(t, waiting, durableSnapshot(t, jobID), "an idle pass changes nothing")
	processA.stop(t)

	// --- 3: the retry comes due with no scheduler alive ----------------------
	makeRetryDue(t, jobID)
	require.Equal(t, "RETRY_WAIT", readJob(t, jobID).status,
		"nothing promotes a job on its own: only a running scheduler does")
	require.Len(t, eventsForJob(t, jobID), 1)

	// --- 4: scheduler B starts cold and promotes it --------------------------
	processB := startSchedulerProcess(t)
	eventually(t, 15*time.Second, "the restarted scheduler promotes the due retry", func() bool {
		return readJob(t, jobID).status == "QUEUED"
	})
	// Several further passes over the now-QUEUED job: a promotion that repeated
	// itself would show up as a second event or a second generation.
	processB.awaitPasses(t, 3)
	processB.stop(t)

	require.EqualValues(t, 1, processB.store.promoted.Load(), "the restarted scheduler promoted it exactly once")
	require.Equal(t, 2, readJob(t, jobID).generation,
		"a retried job's second eligibility transition opens its second generation")
	events := eventsForJob(t, jobID)
	require.Len(t, events, 2, "exactly one fresh notification for the promotion")
	require.Equal(t, 1, events[0].Generation)
	require.Equal(t, 2, events[1].Generation)
	require.Equal(t, "PENDING", events[1].Status)
	require.Equal(t, failedAttempt, readAttemptOutcome(t, fence.AttemptID),
		"the failed attempt's recorded retry decision is untouched by the promotion")

	// --- 5: a second restart must not promote it again -----------------------
	promotedState := durableSnapshot(t, jobID)
	processC := startSchedulerProcess(t)
	processC.awaitPasses(t, 3)
	processC.stop(t)
	require.Zero(t, processC.store.promoted.Load())
	require.Equal(t, promotedState, durableSnapshot(t, jobID), "a second restart changes nothing")

	// --- the retry then runs to completion on the control plane --------------
	claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claim.Disposition)
	require.Equal(t, 2, claim.Assignment.AttemptNumber)
	retry := assignmentFence(claim.Assignment)
	startAttempt(t, store, retry)
	require.NoError(t, store.Succeed(ctx, testScope, retry, nil))
	require.Equal(t, "SUCCEEDED", readJob(t, jobID).status)
	require.Equal(t, []string{"FAILED", "SUCCEEDED"}, attemptHistory(t, jobID))
}

// terminateParkedBackend ends the one PostgreSQL backend that is blocked
// executing a statement containing fragment, which is what the server sees when
// the process that owns the connection dies mid-transaction.
func terminateParkedBackend(t *testing.T, fragment string) {
	t.Helper()
	ctx := context.Background()
	var pids []int
	rows, err := testPool.Query(ctx, `
		SELECT pid FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
		  AND wait_event_type = 'Lock'
		  AND query LIKE '%' || $1 || '%'`, fragment)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var pid int
		require.NoError(t, rows.Scan(&pid))
		pids = append(pids, pid)
	}
	require.NoError(t, rows.Err())
	require.Len(t, pids, 1, "exactly one backend must be parked inside the repair")

	var terminated bool
	require.NoError(t, testPool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, pids[0]).Scan(&terminated))
	require.True(t, terminated)
}

// TestReconcilerRestart_MidScanCrashLeavesNoPartialRepairAndTheReplacementFinishesIt
// is the reconciler's restart proof, for reliability invariants 16 and 17.
//
// It crashes a reconciler MID-SCAN, which a barrier makes deterministic rather
// than a race. Two leases are expired. The reconciler repairs them in
// expires_at order, one transaction each: the first commits, and the second is
// parked inside its own transaction -- holding every authority row it locked --
// by a trigger that blocks on an advisory lock the test owns. The reconciler's
// connection is then terminated at the server, which is exactly what a process
// death looks like to PostgreSQL, and its pool is closed.
//
// What must be true afterwards, and is asserted before any replacement runs:
// the committed repair is durable, the interrupted one left nothing behind, and
// the interrupted lease is still ACTIVE. A cold replacement reconciler then
// repairs precisely the lease that was interrupted and does not touch the one
// that was already done.
//
// The replacement runs one pass with RunOnce rather than its timer loop. Run's
// own comment is that it "holds no state between passes", so the loop adds
// nothing a restart could lose; RunOnce is the seam every reconciler test uses.
func TestReconcilerRestart_MidScanCrashLeavesNoPartialRepairAndTheReplacementFinishesIt(t *testing.T) {
	reset(t)
	ctx := context.Background()
	control := controlStore()
	session := registerWorker(t, control,
		workerRegistration("reconciler-restart", 2, nil, []string{"demo.echo"}))
	first := claimedAndRunning(t, control, session, "reconciler-restart-1")
	second := claimedAndRunning(t, control, session, "reconciler-restart-2")

	// Expired in this order, so the scan's ORDER BY expires_at visits `first`
	// before `second`. Both timestamps come from PostgreSQL's clock.
	expireLease(t, first.LeaseID)
	expireLease(t, second.LeaseID)
	require.Equal(t, 2, countActiveLeases(t))
	outboxBefore := pendingOutboxIDs(t)

	// Park the repair of `second`, and only `second`, inside its transaction.
	release := gateOnAdvisoryLockWhen(t, gateReconcilerCrashKey,
		"taskforge_test_gate_reconciler_crash", "BEFORE UPDATE", "leases",
		fmt.Sprintf("NEW.status = 'EXPIRED' AND NEW.id = '%s'", second.LeaseID))

	// --- process A: scans, repairs `first`, parks mid-repair of `second` -----
	poolA := newProcessPool(t)
	storeA := workers.NewStore(poolA, workers.StoreConfig{
		LeaseDuration: integrationLeaseDuration, RetryPolicy: integrationRetryPolicy(),
	})
	crashed := make(chan error, 1)
	go func() {
		_, err := newReconciler(t, storeA, time.Minute).RunOnce(ctx)
		crashed <- err
	}()
	waitForDatabaseLock(t, fragmentExpire)

	// --- the crash -----------------------------------------------------------
	terminateParkedBackend(t, fragmentExpire)
	require.Error(t, <-crashed, "the interrupted pass cannot have succeeded")
	poolA.Close()

	// --- durable state after the crash, before any replacement runs ----------
	require.Equal(t, jobState{job: "QUEUED", attempt: "ABANDONED", lease: "EXPIRED"}, readState(t, first),
		"the repair that committed before the crash is durable")
	require.Equal(t, jobState{job: "RUNNING", attempt: "RUNNING", lease: "ACTIVE"}, readState(t, second),
		"the repair that was interrupted left nothing behind")
	require.Equal(t, 1, countActiveLeases(t))
	require.Len(t, newPendingOutbox(t, outboxBefore), 1, "only the committed repair wrote a recovery event")

	// --- process B: a cold replacement -------------------------------------
	release()
	poolB := newProcessPool(t)
	storeB := workers.NewStore(poolB, workers.StoreConfig{
		LeaseDuration: integrationLeaseDuration, RetryPolicy: integrationRetryPolicy(),
	})
	repaired, err := newReconciler(t, storeB, time.Minute).RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, repaired.ExpiredLeases, "the replacement repairs the interrupted lease and nothing else")
	require.Equal(t, 1, repaired.RequeuedJobs)

	for _, fence := range []workers.Fence{first, second} {
		require.Equal(t, jobState{job: "QUEUED", attempt: "ABANDONED", lease: "EXPIRED"}, readState(t, fence))
		events := eventsForJob(t, fence.JobID)
		require.Len(t, events, 2, "the submission's notification plus exactly one recovery event per job")
		require.Equal(t, 2, events[1].Generation)
		require.Equal(t, 2, readJob(t, fence.JobID).generation)
	}
	require.Equal(t, 0, countActiveLeases(t))
	require.Equal(t, 2, countRows(t, "job_attempts"), "no attempt was abandoned twice")
	require.Len(t, newPendingOutbox(t, outboxBefore), 2)

	// --- and a further restart is a no-op ------------------------------------
	settled := []string{durableSnapshot(t, first.JobID), durableSnapshot(t, second.JobID)}
	again, err := newReconciler(t, storeB, time.Minute).RunOnce(ctx)
	require.NoError(t, err)
	require.Zero(t, again.ExpiredLeases)
	require.Equal(t, settled, []string{durableSnapshot(t, first.JobID), durableSnapshot(t, second.JobID)})
}
