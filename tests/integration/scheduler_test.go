//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/scheduler"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// Gate keys for the scheduler contention barriers.
const (
	gatePromoteBeforeCancelKey int64 = 7710010040
	gateCancelBeforePromoteKey int64 = 7710010041
	gateUnobservedPromotionKey int64 = 7710010042
	gateRenotifyBeforeClaimKey int64 = 7710010043
)

const (
	whenPromoting      = "NEW.status = 'QUEUED'"
	fragmentPromoteJob = "UPDATE jobs"
)

func newScheduler(t *testing.T, renotifyAfter time.Duration) *scheduler.Scheduler {
	t.Helper()
	return scheduler.New(jobStore(), scheduler.Config{
		PollInterval: 50 * time.Millisecond, BatchSize: 50, RenotifyAfter: renotifyAfter,
	}, discardLogger())
}

// eventsForJob returns every outbox event written for a job, newest last, with
// the generation each one advertises.
func eventsForJob(t *testing.T, jobID uuid.UUID) []struct {
	ID         uuid.UUID
	Generation int
	Status     string
} {
	t.Helper()
	rows, err := testPool.Query(context.Background(), `
		SELECT id, notification_generation, status
		FROM outbox_events WHERE job_id = $1
		ORDER BY created_at, id`, jobID)
	require.NoError(t, err)
	defer rows.Close()

	var events []struct {
		ID         uuid.UUID
		Generation int
		Status     string
	}
	for rows.Next() {
		var event struct {
			ID         uuid.UUID
			Generation int
			Status     string
		}
		require.NoError(t, rows.Scan(&event.ID, &event.Generation, &event.Status))
		events = append(events, event)
	}
	require.NoError(t, rows.Err())
	return events
}

func lastNotificationAt(t *testing.T, jobID uuid.UUID) time.Time {
	t.Helper()
	var at time.Time
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT last_notification_at FROM jobs WHERE id = $1`, jobID).Scan(&at))
	return at
}

// backdateNotification makes a queued job look like one whose notification was
// created some time ago, so the bounded re-notification interval can elapse
// without the test waiting out a real minute.
func backdateNotification(t *testing.T, jobID uuid.UUID, ago time.Duration) {
	t.Helper()
	tag, err := testPool.Exec(context.Background(), `
		UPDATE jobs
		SET last_notification_at = clock_timestamp() - make_interval(secs => $2::double precision)
		WHERE id = $1`, jobID, ago.Seconds())
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
}

// TestDelayedSubmission_IsDurableUnclaimableAndUnadvertised is the whole point
// of accepting a schedule: the job is safe, but nothing can act on it yet.
func TestDelayedSubmission_IsDurableUnclaimableAndUnadvertised(t *testing.T) {
	reset(t)
	ctx := context.Background()
	future := time.Now().Add(time.Hour)
	jobID := createJobWithOptions(t, "delayed", "default", "demo.echo", 50, nil, 3, 300, &future)

	job := readJob(t, jobID)
	require.Equal(t, "PENDING", job.status)
	require.Equal(t, 0, job.generation,
		"a delayed job has had no notification, so it has no generation yet")
	require.True(t, job.availableAt.After(serverNow(t)))

	var scheduledAt time.Time
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT scheduled_at FROM jobs WHERE id = $1`, jobID).Scan(&scheduledAt))
	require.True(t, scheduledAt.Equal(job.availableAt),
		"available_at starts at the requested schedule")
	require.Equal(t, time.UTC, scheduledAt.UTC().Location())

	require.Empty(t, eventsForJob(t, jobID),
		"advertising work no worker may claim yet is a wasted broker round trip")
	require.Equal(t, 0, countRows(t, "outbox_events"))

	// Unclaimable, by the claim predicate itself rather than by convention.
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("delayed-worker", 1, nil, []string{"demo.echo"}))
	claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.QueueEmpty, claim.Disposition)
	require.Equal(t, 0, countRows(t, "job_attempts"))

	// And the scheduler leaves it alone until it is actually due.
	stats, err := jobStore().PromoteDueJobs(ctx, 10)
	require.NoError(t, err)
	require.Zero(t, stats.PromotedJobs)
	require.Equal(t, "PENDING", readJob(t, jobID).status)
}

// TestDelayedSubmission_AlreadyDueScheduleIsImmediate proves the decision is
// PostgreSQL's, not the caller's: a schedule in the past is simply an immediate
// submission and is notified now.
func TestDelayedSubmission_AlreadyDueScheduleIsImmediate(t *testing.T) {
	reset(t)
	past := time.Now().Add(-time.Hour)
	jobID := createJobWithOptions(t, "already-due", "default", "demo.echo", 50, nil, 3, 300, &past)

	job := readJob(t, jobID)
	require.Equal(t, "QUEUED", job.status)
	require.Equal(t, 1, job.generation)
	events := eventsForJob(t, jobID)
	require.Len(t, events, 1, "an immediately eligible job is advertised at submission")
	require.Equal(t, 1, events[0].Generation)

	// available_at comes from server time rather than from the stale schedule,
	// so claim ordering is not distorted by how long ago the caller asked.
	require.False(t, job.availableAt.Before(past.Add(time.Minute)))
}

// TestScheduler_PromotesDueJobsWithExactlyOneFreshEvent covers both sources
// through the one mechanism they share.
func TestScheduler_PromotesDueJobsWithExactlyOneFreshEvent(t *testing.T) {
	ctx := context.Background()

	t.Run("a delayed job", func(t *testing.T) {
		reset(t)
		soon := time.Now().Add(-time.Second)
		// Submitted with a future schedule, then made due, so the job really did
		// pass through PENDING rather than being created QUEUED.
		future := time.Now().Add(time.Hour)
		jobID := createJobWithOptions(t, "promote-delayed", "default", "demo.echo", 50, nil, 3, 300, &future)
		_, err := testPool.Exec(ctx,
			`UPDATE jobs SET available_at = $2 WHERE id = $1`, jobID, soon)
		require.NoError(t, err)

		result, err := newScheduler(t, time.Hour).RunOnce(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, result.PromotedJobs)

		job := readJob(t, jobID)
		require.Equal(t, "QUEUED", job.status)
		require.Equal(t, 1, job.generation, "promotion opens the job's first generation")

		events := eventsForJob(t, jobID)
		require.Len(t, events, 1, "exactly one fresh transactional event")
		require.Equal(t, 1, events[0].Generation)
		require.Equal(t, "PENDING", events[0].Status)
		require.True(t, lastNotificationAt(t, jobID).Equal(job.availableAt) ||
			!lastNotificationAt(t, jobID).Before(job.availableAt))

		// And it is now genuinely claimable.
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("promoted-worker", 1, nil, []string{"demo.echo"}))
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		require.Equal(t, workers.Claimed, claim.Disposition)
		require.Equal(t, jobID, claim.Assignment.JobID)
	})

	t.Run("a retry-waiting job", func(t *testing.T) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("promote-retry", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "promote-retry")
		_, err := store.Fail(ctx, testScope,
			failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
		require.NoError(t, err)
		require.Equal(t, "RETRY_WAIT", readJob(t, fence.JobID).status)

		// Not yet due: the scheduler must respect the backoff it was given.
		result, err := newScheduler(t, time.Hour).RunOnce(ctx)
		require.NoError(t, err)
		require.Zero(t, result.PromotedJobs)

		_, err = testPool.Exec(ctx,
			`UPDATE jobs SET available_at = clock_timestamp() - interval '1 second' WHERE id = $1`,
			fence.JobID)
		require.NoError(t, err)

		before := eventsForJob(t, fence.JobID)
		result, err = newScheduler(t, time.Hour).RunOnce(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, result.PromotedJobs)

		job := readJob(t, fence.JobID)
		require.Equal(t, "QUEUED", job.status)
		require.Equal(t, 2, job.generation,
			"a retried job's second eligibility transition opens a second generation")

		after := eventsForJob(t, fence.JobID)
		require.Len(t, after, len(before)+1, "exactly one fresh event")
		require.Equal(t, 2, after[len(after)-1].Generation)

		// The full lifecycle closes: a new attempt claims it and succeeds.
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		require.Equal(t, workers.Claimed, claim.Disposition)
		require.Equal(t, 2, claim.Assignment.AttemptNumber)
		replacement := assignmentFence(claim.Assignment)
		startAttempt(t, store, replacement)
		require.NoError(t, store.Succeed(ctx, testScope, replacement))

		require.Equal(t, "SUCCEEDED", readJob(t, fence.JobID).status)
		require.Equal(t, []string{"FAILED", "SUCCEEDED"}, attemptHistory(t, fence.JobID))
	})
}

// TestScheduler_ConcurrentReplicasPromoteEachTransitionOnce is the N-replica
// guarantee, on separate PostgreSQL connections.
func TestScheduler_ConcurrentReplicasPromoteEachTransitionOnce(t *testing.T) {
	reset(t)
	ctx := context.Background()

	const jobCount = 6
	ids := make([]uuid.UUID, 0, jobCount)
	for i := 0; i < jobCount; i++ {
		future := time.Now().Add(time.Hour)
		id := createJobWithOptions(t, "replica-"+uuid.NewString()[:8], "default", "demo.echo",
			50, nil, 3, 300, &future)
		ids = append(ids, id)
	}
	_, err := testPool.Exec(ctx,
		`UPDATE jobs SET available_at = clock_timestamp() - interval '1 second' WHERE status = 'PENDING'`)
	require.NoError(t, err)

	const replicas = 4
	results := make(chan jobs.SchedulerStats, replicas)
	errs := make(chan error, replicas)
	start := make(chan struct{})
	for i := 0; i < replicas; i++ {
		go func() {
			<-start
			stats, err := jobStore().PromoteDueJobs(ctx, 50)
			results <- stats
			errs <- err
		}()
	}
	close(start)

	total := 0
	for i := 0; i < replicas; i++ {
		require.NoError(t, <-errs)
		total += (<-results).PromotedJobs
	}
	require.Equal(t, jobCount, total,
		"every eligibility transition is promoted exactly once across all replicas")

	for _, id := range ids {
		job := readJob(t, id)
		require.Equal(t, "QUEUED", job.status)
		require.Equal(t, 1, job.generation)
		require.Len(t, eventsForJob(t, id), 1,
			"two replicas must not both advertise one transition")
	}
	require.Equal(t, jobCount, countPendingOutbox(t))
}

// TestScheduler_FaultBeforeCommitLeavesNoPromotionAndNoEvent proves the two
// halves really are one transaction.
func TestScheduler_FaultBeforeCommitLeavesNoPromotionAndNoEvent(t *testing.T) {
	reset(t)
	ctx := context.Background()
	future := time.Now().Add(time.Hour)
	jobID := createJobWithOptions(t, "scheduler-fault", "default", "demo.echo", 50, nil, 3, 300, &future)
	_, err := testPool.Exec(ctx,
		`UPDATE jobs SET available_at = clock_timestamp() - interval '1 second' WHERE id = $1`, jobID)
	require.NoError(t, err)
	before := readJob(t, jobID)

	_, err = testPool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION taskforge_test_fail_promotion_event() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected promotion failure'; END $$`)
	require.NoError(t, err)
	_, err = testPool.Exec(ctx, `
		CREATE TRIGGER taskforge_test_fail_promotion_event
		BEFORE INSERT ON outbox_events FOR EACH ROW
		EXECUTE FUNCTION taskforge_test_fail_promotion_event()`)
	require.NoError(t, err)
	dropTrigger := func() {
		_, _ = testPool.Exec(context.Background(),
			`DROP TRIGGER IF EXISTS taskforge_test_fail_promotion_event ON outbox_events`)
		_, _ = testPool.Exec(context.Background(),
			`DROP FUNCTION IF EXISTS taskforge_test_fail_promotion_event()`)
	}
	t.Cleanup(dropTrigger)

	_, err = jobStore().PromoteDueJobs(ctx, 10)
	require.Error(t, err)

	require.Equal(t, before, readJob(t, jobID),
		"a fault before commit must leave the job exactly as it was")
	require.Equal(t, "PENDING", readJob(t, jobID).status)
	require.Empty(t, eventsForJob(t, jobID))

	// Retrying after the fault clears simply works, which is what makes a failed
	// pass a non-event.
	dropTrigger()
	result, err := newScheduler(t, time.Hour).RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.PromotedJobs)
	require.Equal(t, "QUEUED", readJob(t, jobID).status)
	require.Len(t, eventsForJob(t, jobID), 1)
}

// TestScheduler_RerunAfterAnUnobservedCommitIsSafe covers the ambiguous commit:
// a promotion that genuinely committed, whose result the process never learned.
//
// The barrier makes the ordering a fact rather than a race: the second pass's
// candidate scan provably runs while the first is parked mid-transaction, so its
// snapshot is stale by construction. The first pass's result is then discarded,
// exactly as a process that died after COMMIT would have discarded it.
func TestScheduler_RerunAfterAnUnobservedCommitIsSafe(t *testing.T) {
	reset(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	future := time.Now().Add(time.Hour)
	jobID := createJobWithOptions(t, "scheduler-ambiguous", "default", "demo.echo", 50, nil, 3, 300, &future)
	_, err := testPool.Exec(ctx,
		`UPDATE jobs SET available_at = clock_timestamp() - interval '1 second' WHERE id = $1`, jobID)
	require.NoError(t, err)

	release := gateOnAdvisoryLockWhen(t, gateUnobservedPromotionKey,
		"taskforge_test_gate_unobserved_promotion", "BEFORE UPDATE", "jobs", whenPromoting)

	firstErr := make(chan error, 1)
	go func() {
		// The result is intentionally dropped: this models a process that commits
		// and then dies before it can record or report what it did.
		_, err := jobStore().PromoteDueJobs(ctx, 10)
		firstErr <- err
	}()
	waitForDatabaseLock(t, fragmentPromoteJob)

	secondResult := make(chan jobs.SchedulerStats, 1)
	secondErr := make(chan error, 1)
	go func() {
		stats, err := jobStore().PromoteDueJobs(ctx, 10)
		secondResult <- stats
		secondErr <- err
	}()
	waitForDatabaseLock(t, fragmentQueueLock)

	release()
	require.NoError(t, <-firstErr, "the first pass held authority and must commit")
	require.NoError(t, <-secondErr, "a rerun over committed work is a no-op, not an error")

	stats := <-secondResult
	require.Zero(t, stats.PromotedJobs,
		"the locked status and generation show the promotion already happened")
	require.Equal(t, 1, stats.Skipped)

	require.Equal(t, "QUEUED", readJob(t, jobID).status)
	require.Equal(t, 1, readJob(t, jobID).generation)
	require.Len(t, eventsForJob(t, jobID), 1, "exactly one notification survives")
}

// TestContention_PromotionVersusCancellation arranges the race in both
// orderings. Both operations lock queue then job, so they serialize on the job
// row, and only valid serial outcomes are possible.
func TestContention_PromotionVersusCancellation(t *testing.T) {
	t.Run("promotion first: the job is queued, then canceled", func(t *testing.T) {
		reset(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		future := time.Now().Add(time.Hour)
		jobID := createJobWithOptions(t, "promote-then-cancel", "default", "demo.echo", 50, nil, 3, 300, &future)
		_, err := testPool.Exec(ctx,
			`UPDATE jobs SET available_at = clock_timestamp() - interval '1 second' WHERE id = $1`, jobID)
		require.NoError(t, err)

		release := gateOnAdvisoryLockWhen(t, gatePromoteBeforeCancelKey,
			"taskforge_test_gate_promote_first", "BEFORE UPDATE", "jobs", whenPromoting)

		promoteResult := make(chan jobs.SchedulerStats, 1)
		promoteErr := make(chan error, 1)
		go func() {
			stats, err := jobStore().PromoteDueJobs(ctx, 10)
			promoteResult <- stats
			promoteErr <- err
		}()
		waitForDatabaseLock(t, fragmentPromoteJob)

		cancelErr := make(chan error, 1)
		go func() {
			_, err := jobStore().RequestCancel(ctx, testScope, jobID)
			cancelErr <- err
		}()
		waitForDatabaseLock(t, fragmentQueueLock)

		release()
		require.NoError(t, <-promoteErr)
		require.Equal(t, 1, (<-promoteResult).PromotedJobs,
			"promotion held authority first and must commit")
		require.NoError(t, <-cancelErr, "cancelling a QUEUED job is valid")

		require.Equal(t, "CANCELED", readJob(t, jobID).status)
		require.Equal(t, 0, countRows(t, "job_attempts"))
		// The promotion's event survives and is harmless: the claim predicate
		// simply finds no QUEUED job.
		require.Len(t, eventsForJob(t, jobID), 1)
	})

	t.Run("cancellation first: the promotion finds nothing to promote", func(t *testing.T) {
		reset(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		future := time.Now().Add(time.Hour)
		jobID := createJobWithOptions(t, "cancel-then-promote", "default", "demo.echo", 50, nil, 3, 300, &future)
		_, err := testPool.Exec(ctx,
			`UPDATE jobs SET available_at = clock_timestamp() - interval '1 second' WHERE id = $1`, jobID)
		require.NoError(t, err)

		release := gateOnAdvisoryLockWhen(t, gateCancelBeforePromoteKey,
			"taskforge_test_gate_cancel_before_promote", "BEFORE UPDATE", "jobs",
			"NEW.status = 'CANCELED'")

		cancelErr := make(chan error, 1)
		go func() {
			_, err := jobStore().RequestCancel(ctx, testScope, jobID)
			cancelErr <- err
		}()
		waitForDatabaseLock(t, fragmentPromoteJob)

		promoteResult := make(chan jobs.SchedulerStats, 1)
		promoteErr := make(chan error, 1)
		go func() {
			stats, err := jobStore().PromoteDueJobs(ctx, 10)
			promoteResult <- stats
			promoteErr <- err
		}()
		waitForDatabaseLock(t, fragmentQueueLock)

		release()
		require.NoError(t, <-cancelErr, "cancellation held authority first and must commit")
		require.NoError(t, <-promoteErr)
		require.Zero(t, (<-promoteResult).PromotedJobs,
			"a canceled job is no longer promotable, and the decision is re-made under the lock")

		require.Equal(t, "CANCELED", readJob(t, jobID).status)
		require.Empty(t, eventsForJob(t, jobID),
			"a canceled job must never be advertised")
	})
}

// TestScheduler_RenotifiesAQueuedJobWhoseNotificationWasLost is the repair the
// broker's advisory nature makes necessary.
//
// Correctness never depends on delivery, but REACHABILITY does: a QUEUED job
// whose only notification vanished sits claimable and unclaimed, because nothing
// wakes a worker to claim it. The notification here is deliberately consumed and
// discarded without claiming, which is exactly what a lost delivery looks like
// from PostgreSQL's side.
func TestScheduler_RenotifiesAQueuedJobWhoseNotificationWasLost(t *testing.T) {
	reset(t)
	ctx := context.Background()
	jobID := createJob(t, "stranded", "demo.echo", 50, nil)

	// The publisher delivered the only event and it was lost in transit.
	original := eventsForJob(t, jobID)
	require.Len(t, original, 1)
	_, err := testPool.Exec(ctx,
		`UPDATE outbox_events SET status = 'PUBLISHED', published_at = clock_timestamp() WHERE id = $1`,
		original[0].ID)
	require.NoError(t, err)

	// Not yet: re-notification repairs a lost notification, not a slow one.
	result, err := newScheduler(t, time.Minute).RunOnce(ctx)
	require.NoError(t, err)
	require.Zero(t, result.Renotified)

	backdateNotification(t, jobID, 2*time.Minute)
	result, err = newScheduler(t, time.Minute).RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Renotified)

	events := eventsForJob(t, jobID)
	require.Len(t, events, 2)
	require.NotEqual(t, events[0].ID, events[1].ID, "a replacement gets a new event id")
	require.Equal(t, events[0].Generation, events[1].Generation,
		"it advertises the SAME eligibility transition, not a new one")
	require.Equal(t, "PENDING", events[1].Status)
	require.Equal(t, 1, readJob(t, jobID).generation,
		"re-notification must not open a new generation")

	// Rate-limited again immediately, so N replicas cannot multiply events.
	for i := 0; i < 3; i++ {
		again, err := newScheduler(t, time.Minute).RunOnce(ctx)
		require.NoError(t, err)
		require.Zero(t, again.Renotified)
	}
	require.Len(t, eventsForJob(t, jobID), 2)

	// And the work really is reachable again.
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("stranded-worker", 1, nil, []string{"demo.echo"}))
	claim, err := store.Claim(ctx, testScope,
		workers.ClaimRequest{
			WorkerID: session.WorkerID, SessionID: session.ID,
			ClaimRequestID: events[1].ID, Queue: "default",
		})
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claim.Disposition)
	require.Equal(t, jobID, claim.Assignment.JobID)
}

// TestScheduler_DoesNotRenotifyWhileACurrentEventIsStillPending keeps a slow
// publisher from turning into a growing pile of duplicates.
func TestScheduler_DoesNotRenotifyWhileACurrentEventIsStillPending(t *testing.T) {
	reset(t)
	ctx := context.Background()
	jobID := createJob(t, "pending-event", "demo.echo", 50, nil)
	backdateNotification(t, jobID, 2*time.Minute)

	// The submission event is still PENDING: the publisher simply has not got to
	// it yet.
	require.Equal(t, 1, countPendingOutbox(t))

	for i := 0; i < 3; i++ {
		result, err := newScheduler(t, time.Minute).RunOnce(ctx)
		require.NoError(t, err)
		require.Zero(t, result.Renotified)
		require.Equal(t, 1, result.Skipped)
	}
	require.Len(t, eventsForJob(t, jobID), 1)
	_ = ctx
}

// TestScheduler_AStalePendingEventDoesNotBlockANewTransition is the case the
// notification generation exists for.
//
// The publish-before-mark window can leave a PENDING event behind from an
// attempt that is already over. Checking only "is there a pending event for this
// job" would let that stale event suppress the notification a NEW eligibility
// transition requires — the job would be freshly queued and permanently
// unadvertised.
func TestScheduler_AStalePendingEventDoesNotBlockANewTransition(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("stale-generation", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "stale-generation")

	// Generation 1's event is still PENDING, left behind by the publisher.
	generationOne := eventsForJob(t, fence.JobID)
	require.Len(t, generationOne, 1)
	require.Equal(t, 1, generationOne[0].Generation)
	require.Equal(t, "PENDING", generationOne[0].Status)

	// The attempt is abandoned, which opens generation 2 and writes its event.
	expireLease(t, fence.LeaseID)
	_, err := store.ReconcileExpiredLeases(ctx, 10)
	require.NoError(t, err)

	events := eventsForJob(t, fence.JobID)
	require.Len(t, events, 2,
		"a stale generation-1 event must not suppress generation 2's fresh event")
	require.Equal(t, 1, events[0].Generation)
	require.Equal(t, 2, events[1].Generation)
	require.Equal(t, 2, readJob(t, fence.JobID).generation)

	// Now generation 2's event is lost too. Re-notification must fire, and it
	// must not be fooled into thinking generation 1's still-pending event covers
	// the current transition.
	_, err = testPool.Exec(ctx, `
		UPDATE outbox_events SET status = 'PUBLISHED', published_at = clock_timestamp()
		WHERE id = $1`, events[1].ID)
	require.NoError(t, err)
	backdateNotification(t, fence.JobID, 2*time.Minute)

	result, err := newScheduler(t, time.Minute).RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Renotified,
		"a pending event from an earlier generation answers a different question")

	after := eventsForJob(t, fence.JobID)
	require.Len(t, after, 3)
	require.Equal(t, 2, after[2].Generation)
}

// TestScheduler_RenotificationIsBatchLimited proves the bound is real.
func TestScheduler_RenotificationIsBatchLimited(t *testing.T) {
	reset(t)
	ctx := context.Background()

	const jobCount = 5
	for i := 0; i < jobCount; i++ {
		id := createJob(t, "batch-"+uuid.NewString()[:8], "demo.echo", 50, nil)
		backdateNotification(t, id, 2*time.Minute)
	}
	// Every submission event is delivered and lost.
	_, err := testPool.Exec(ctx,
		`UPDATE outbox_events SET status = 'PUBLISHED', published_at = clock_timestamp()`)
	require.NoError(t, err)

	stats, err := jobStore().RenotifyStrandedQueued(ctx, time.Minute, 2)
	require.NoError(t, err)
	require.Equal(t, 2, stats.Renotified, "one pass may not exceed its batch size")
	require.Equal(t, 2, countPendingOutbox(t))

	// The next pass picks up where it left off, in deterministic order.
	stats, err = jobStore().RenotifyStrandedQueued(ctx, time.Minute, 10)
	require.NoError(t, err)
	require.Equal(t, jobCount-2, stats.Renotified)
	require.Equal(t, jobCount, countPendingOutbox(t))
}

// TestScheduler_RenotificationRacingAClaimCannotCorruptState is the case where
// a repair and real progress overlap. The claim wins the job; the re-notification
// must not resurrect it, create a second lease, or leave anything inconsistent.
func TestScheduler_RenotificationRacingAClaimCannotCorruptState(t *testing.T) {
	reset(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("renotify-vs-claim", 1, nil, []string{"demo.echo"}))
	jobID := createJob(t, "renotify-vs-claim", "demo.echo", 50, nil)

	original := eventsForJob(t, jobID)
	require.Len(t, original, 1)
	_, err := testPool.Exec(ctx,
		`UPDATE outbox_events SET status = 'PUBLISHED', published_at = clock_timestamp() WHERE id = $1`,
		original[0].ID)
	require.NoError(t, err)
	backdateNotification(t, jobID, 2*time.Minute)

	// The claim holds the queue row first; the re-notification blocks behind it.
	release := gateOnAdvisoryLockWhen(t, gateRenotifyBeforeClaimKey,
		"taskforge_test_gate_renotify_vs_claim", "BEFORE UPDATE", "jobs", "NEW.status = 'LEASED'")

	claimResult := make(chan workers.ClaimResult, 1)
	claimErr := make(chan error, 1)
	go func() {
		result, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		claimResult <- result
		claimErr <- err
	}()
	waitForDatabaseLock(t, fragmentPromoteJob)

	renotifyResult := make(chan jobs.SchedulerStats, 1)
	renotifyErr := make(chan error, 1)
	go func() {
		stats, err := jobStore().RenotifyStrandedQueued(ctx, time.Minute, 10)
		renotifyResult <- stats
		renotifyErr <- err
	}()
	waitForDatabaseLock(t, fragmentQueueLock)

	release()
	require.NoError(t, <-claimErr)
	claim := <-claimResult
	require.Equal(t, workers.Claimed, claim.Disposition, "the claim held authority first")
	require.NoError(t, <-renotifyErr)
	require.Zero(t, (<-renotifyResult).Renotified,
		"a job that is no longer QUEUED is no longer stranded")

	require.Equal(t, "LEASED", readJob(t, jobID).status)
	require.Equal(t, 1, countActiveLeases(t), "a re-notification can never create a second lease")
	require.Len(t, eventsForJob(t, jobID), 1)
}
