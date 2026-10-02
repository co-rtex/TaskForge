//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// durableSnapshot reads back every durable row that belongs to one job -- the
// job itself, its attempts, its leases, its result, its dead-letter entry, and
// its outbox events -- as one string.
//
// It exists so a test can prove "this refused operation changed nothing" by
// comparing before with after, rather than by re-asserting the handful of
// columns its author happened to think of. Each row is serialized whole by
// PostgreSQL, so a column added later is covered without touching the test.
func durableSnapshot(t *testing.T, jobID uuid.UUID) string {
	t.Helper()
	var snapshot string
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT json_build_object(
			'job',      (SELECT row_to_json(j) FROM jobs j WHERE j.id = $1),
			'attempts', (SELECT coalesce(json_agg(row_to_json(a) ORDER BY a.attempt_number), '[]'::json)
			             FROM job_attempts a WHERE a.job_id = $1),
			'leases',   (SELECT coalesce(json_agg(row_to_json(l) ORDER BY l.acquired_at, l.id), '[]'::json)
			             FROM leases l WHERE l.job_id = $1),
			'results',  (SELECT coalesce(json_agg(row_to_json(r)), '[]'::json)
			             FROM results r WHERE r.job_id = $1),
			'dlq',      (SELECT coalesce(json_agg(row_to_json(d) ORDER BY d.id), '[]'::json)
			             FROM dlq_entries d WHERE d.job_id = $1),
			'outbox',   (SELECT coalesce(json_agg(row_to_json(e) ORDER BY e.created_at, e.id), '[]'::json)
			             FROM outbox_events e WHERE e.job_id = $1)
		)::text`, jobID).Scan(&snapshot))
	return snapshot
}

// TestInvariant_TerminalJobsStayTerminalUnderEveryMutator proves reliability
// invariant 2: a terminal job never returns to a non-terminal state.
//
// The other tests that touch this -- a cancel against a succeeded job, a
// reconciliation after a success -- each try ONE operation against ONE terminal
// status. Nothing tried the scheduler against any of them, which is the path a
// careless predicate would turn into a resurrection. So this puts one job in
// each terminal status, makes each look as eligible as it can be to every
// mutator the system has (a due available_at, a long-stale notification, a
// deadline in the past, a lease window in the past), and then runs all of them:
//
//   - the scheduler, promoting and re-notifying as eagerly as it can;
//   - the reconciler, over timeouts, expired leases, and stale sessions;
//   - a capable worker's claim;
//   - public cancellation;
//   - every call a zombie worker could still make with the terminal attempt's
//     fence: start, renewal, failure, cancellation acknowledgment, success;
//   - exact replays of the outcomes that committed, which are recognised and
//     must change nothing;
//   - an operator replay of the dead-lettered job, which creates a NEW job and
//     leaves the original alone.
//
// Afterwards every durable row belonging to the three jobs must be byte-for-byte
// what it was, not merely still carry a terminal status.
func TestInvariant_TerminalJobsStayTerminalUnderEveryMutator(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("terminal-sweep", 4, nil, []string{"demo.echo"}))

	// One job in each terminal status, each reached by its own real path.
	succeeded := claimedAndRunning(t, store, session, "sweep-succeeded")
	require.NoError(t, store.Succeed(ctx, testScope, succeeded, nil))

	deadLettered := claimedAndRunning(t, store, session, "sweep-dead-lettered")
	deadReport := failureReport(deadLettered, lifecycle.ClassPermanent, "invalid_payload", "")
	_, err := store.Fail(ctx, testScope, deadReport)
	require.NoError(t, err)

	canceled := claimedAndRunning(t, store, session, "sweep-canceled")
	_, err = jobStore().RequestCancel(ctx, testScope, canceled.JobID)
	require.NoError(t, err)
	cancelReport := cancelAck(canceled)
	_, err = store.AcknowledgeCancellation(ctx, testScope, cancelReport)
	require.NoError(t, err)

	terminal := map[string]workers.Fence{
		"SUCCEEDED": succeeded, "DEAD_LETTERED": deadLettered, "CANCELED": canceled,
	}
	for status, fence := range terminal {
		require.Equal(t, status, readJob(t, fence.JobID).status)
	}

	// Make every one of them look eligible for everything, using PostgreSQL's
	// own clock, so the only thing standing between a terminal job and a mutator
	// is the mutator's own status predicate.
	before := map[string]string{}
	for status, fence := range terminal {
		_, err := testPool.Exec(ctx, `
			UPDATE jobs
			SET available_at = clock_timestamp() - interval '2 hours',
			    last_notification_at = clock_timestamp() - interval '2 hours'
			WHERE id = $1`, fence.JobID)
		require.NoError(t, err)
		expireAttemptDeadline(t, fence.AttemptID)
		expireLease(t, fence.LeaseID)
		before[status] = durableSnapshot(t, fence.JobID)
	}

	// The scheduler, as eager as it can be: re-notify anything a millisecond old.
	for pass := 0; pass < 2; pass++ {
		result, err := newScheduler(t, time.Millisecond).RunOnce(ctx)
		require.NoError(t, err)
		require.Zero(t, result.PromotedJobs, "a terminal job is never promoted")
		require.Zero(t, result.Renotified, "a terminal job is never re-advertised")
	}

	// The reconciler's three scans.
	repaired, err := newReconciler(t, store, time.Minute).RunOnce(ctx)
	require.NoError(t, err)
	require.False(t, repaired.Changed(), "there is nothing for reconciliation to repair")

	// A capable worker with a free slot finds no work.
	claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.NotEqual(t, workers.Claimed, claim.Disposition)

	// Public cancellation: refused for the two that cannot be canceled, and an
	// idempotent acknowledgment for the one that already was.
	for _, status := range []string{"SUCCEEDED", "DEAD_LETTERED"} {
		_, err := jobStore().RequestCancel(ctx, testScope, terminal[status].JobID)
		require.ErrorIs(t, err, jobs.ErrJobNotCancelable, status)
	}
	again, err := jobStore().RequestCancel(ctx, testScope, canceled.JobID)
	require.NoError(t, err)
	require.True(t, again.AlreadyRequested)

	// Everything a zombie worker could still send with a terminal fence.
	for status, fence := range terminal {
		require.Error(t, startError(store, fence), status+": start")
		_, err := store.RenewLease(ctx, testScope, renewalRequest(fence, 0))
		require.Error(t, err, status+": renew")
		_, err = store.Fail(ctx, testScope, failureReport(fence, lifecycle.ClassRetryable, "late", ""))
		require.Error(t, err, status+": a new failure")
		_, err = store.AcknowledgeCancellation(ctx, testScope, cancelAck(fence))
		require.Error(t, err, status+": a new cancellation acknowledgment")
		if status != "SUCCEEDED" {
			require.Error(t, store.Succeed(ctx, testScope, fence, nil), status+": success")
		}
	}

	// Exact replays of what committed are recognised, and recognising is not
	// mutating.
	require.NoError(t, store.Succeed(ctx, testScope, succeeded, nil))
	replayedFailure, err := store.Fail(ctx, testScope, deadReport)
	require.NoError(t, err)
	require.True(t, replayedFailure.Replayed)
	replayedCancel, err := store.AcknowledgeCancellation(ctx, testScope, cancelReport)
	require.NoError(t, err)
	require.True(t, replayedCancel.Replayed)

	// An operator replay builds a new job. The original is not resurrected.
	replacement, err := jobStore().Replay(ctx, testScope, deadLettered.JobID, "sweep-replay")
	require.NoError(t, err)
	require.NotEqual(t, deadLettered.JobID, replacement.Replacement.ID)

	for status, fence := range terminal {
		require.Equal(t, status, readJob(t, fence.JobID).status, "still terminal")
		require.Equal(t, before[status], durableSnapshot(t, fence.JobID),
			status+": every durable row of a terminal job survives every mutator unchanged")
	}
}

// TestInvariant_AttemptNumbersIncreaseWithoutGapsAndAreUniquePerJob proves
// reliability invariant 5.
//
// The existing tests see an attempt number only as a value Claim returned, and
// only ever 1 and 2. This drives one job through four real attempts and reads
// the numbers back from the table in the order the attempts were created, and
// then asks the database itself to refuse a duplicate, because uniqueness is a
// property the schema must hold whatever the application does (AGENTS.md
// section 6).
func TestInvariant_AttemptNumbersIncreaseWithoutGapsAndAreUniquePerJob(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("attempt-numbers", 1, nil, []string{"demo.echo"}))
	jobID := createJobWithBudget(t, "attempt-numbers", 4, 300)

	for attempt := 1; attempt <= 4; attempt++ {
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		require.Equal(t, workers.Claimed, claim.Disposition)
		fence := assignmentFence(claim.Assignment)
		startAttempt(t, store, fence)
		if attempt == 4 {
			require.NoError(t, store.Succeed(ctx, testScope, fence, nil))
			break
		}
		_, err = store.Fail(ctx, testScope, failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
		require.NoError(t, err)
		makeRetryDue(t, jobID)
		promoted, err := newScheduler(t, time.Hour).RunOnce(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, promoted.PromotedJobs)
	}

	rows, err := testPool.Query(ctx,
		`SELECT attempt_number FROM job_attempts WHERE job_id = $1 ORDER BY created_at, id`, jobID)
	require.NoError(t, err)
	defer rows.Close()
	var numbers []int
	for rows.Next() {
		var number int
		require.NoError(t, rows.Scan(&number))
		numbers = append(numbers, number)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int{1, 2, 3, 4}, numbers,
		"in creation order the numbers rise by exactly one: no repeat, no gap, no reordering")

	// The database refuses a duplicate on its own.
	var workerID, sessionID uuid.UUID
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT worker_id, worker_session_id FROM job_attempts WHERE job_id = $1 AND attempt_number = 1`, jobID,
	).Scan(&workerID, &sessionID))
	_, err = testPool.Exec(ctx, `
		INSERT INTO job_attempts (id, job_id, scope, queue, attempt_number, worker_id, worker_session_id, status)
		VALUES (gen_random_uuid(), $1, $2, 'default', 3, $3, $4, 'LEASED')`,
		jobID, testScope, workerID, sessionID)
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "the duplicate must be refused by PostgreSQL, got %v", err)
	require.Equal(t, "23505", pgErr.Code)
	require.Equal(t, "job_attempts_job_id_attempt_number_key", pgErr.ConstraintName)
	require.Equal(t, 4, countRows(t, "job_attempts"), "the refused row was not stored")
}

// TestInvariant_ReleasingCapacityNeverAdmitsPastTheLimit proves reliability
// invariant 15.
//
// Capacity here is not a counter: it is the number of ACTIVE leases, measured
// against a worker's concurrency limit and a queue's global limit. There is
// nothing to decrement, so a literal negative is impossible, and "cannot become
// negative" has to mean what it means for any ledger -- a release must be worth
// exactly one slot, however many times it is reported, retried, or repaired.
//
// So for every way a lease can be released (success, failure, cooperative
// cancellation, abandonment by an expired lease, and timeout), this fills a
// limit, releases ONE lease, REPLAYS the release, and then asks the control
// plane to admit work. Exactly one more claim may succeed. A release that were
// counted twice would admit a second one.
//
// The worker's limit and the queue's are exercised separately, each with the
// other set far out of the way. Filled together they would mask each other: a
// defect in one ledger would be hidden by the other still saying "full".
func TestInvariant_ReleasingCapacityNeverAdmitsPastTheLimit(t *testing.T) {
	const limit = 2
	ledgers := []struct {
		name                     string
		workerConcurrency, queue int
	}{
		{"worker limit", limit, 100},
		{"queue limit", 10, limit},
	}

	releases := map[string]func(t *testing.T, store *workers.Store, fence workers.Fence){
		"success": func(t *testing.T, store *workers.Store, fence workers.Fence) {
			for i := 0; i < 3; i++ {
				require.NoError(t, store.Succeed(context.Background(), testScope, fence, nil))
			}
		},
		"failure": func(t *testing.T, store *workers.Store, fence workers.Fence) {
			report := failureReport(fence, lifecycle.ClassRetryable, "transient", "")
			for i := 0; i < 3; i++ {
				_, err := store.Fail(context.Background(), testScope, report)
				require.NoError(t, err)
			}
		},
		"cooperative cancellation": func(t *testing.T, store *workers.Store, fence workers.Fence) {
			_, err := jobStore().RequestCancel(context.Background(), testScope, fence.JobID)
			require.NoError(t, err)
			ack := cancelAck(fence)
			for i := 0; i < 3; i++ {
				_, err := store.AcknowledgeCancellation(context.Background(), testScope, ack)
				require.NoError(t, err)
			}
		},
		"abandonment": func(t *testing.T, store *workers.Store, fence workers.Fence) {
			expireLease(t, fence.LeaseID)
			for i := 0; i < 3; i++ {
				_, err := store.ReconcileExpiredLeases(context.Background(), 10)
				require.NoError(t, err)
			}
		},
		"timeout": func(t *testing.T, store *workers.Store, fence workers.Fence) {
			expireAttemptDeadline(t, fence.AttemptID)
			for i := 0; i < 3; i++ {
				_, err := store.ReconcileDueTimeouts(context.Background(), 10)
				require.NoError(t, err)
			}
		},
	}

	for _, ledger := range ledgers {
		for name, release := range releases {
			t.Run(ledger.name+"/"+name, func(t *testing.T) {
				reset(t)
				ctx := context.Background()
				_, err := testPool.Exec(ctx, fmt.Sprintf(
					`UPDATE queues SET max_concurrency = %d WHERE name = 'default'`, ledger.queue))
				require.NoError(t, err)
				store := controlStore()
				session := registerWorker(t, store, workerRegistration(
					"capacity-"+uuid.NewString()[:8], ledger.workerConcurrency, nil, []string{"demo.echo"}))

				// The limit under test is now full.
				held := []workers.Fence{
					claimedAndRunning(t, store, session, "capacity-held-1"),
					claimedAndRunning(t, store, session, "capacity-held-2"),
				}
				createJob(t, "capacity-waiting-1", "demo.echo", 50, nil)
				full, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
				require.NoError(t, err)
				require.Equal(t, workers.CapacityExhausted, full.Disposition)
				require.Equal(t, limit, countActiveLeases(t))

				// One release, reported repeatedly, is worth exactly one slot.
				release(t, store, held[0])
				require.Equal(t, limit-1, countActiveLeases(t))

				admitted, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
				require.NoError(t, err)
				require.Equal(t, workers.Claimed, admitted.Disposition, "the freed slot is usable")
				require.Equal(t, limit, countActiveLeases(t))

				createJob(t, "capacity-waiting-2", "demo.echo", 50, nil)
				refused, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
				require.NoError(t, err)
				require.Equal(t, workers.CapacityExhausted, refused.Disposition,
					"a release reported more than once must not have freed a second slot")
				require.Equal(t, limit, countActiveLeases(t))
			})
		}
	}
}
