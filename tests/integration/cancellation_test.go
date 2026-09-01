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
	"github.com/co-rtex/TaskForge/internal/workers"
)

// Gate keys for the cancellation contention barriers.
const (
	gateCancelBeforeSuccessKey int64 = 7710010030
	gateSuccessBeforeCancelKey int64 = 7710010031
)

// The reconciler, the scheduler, and cancellation all UPDATE jobs, so the
// barriers below pair this fragment with a WHEN clause naming the status only
// one of them writes.
const fragmentCancelJob = "UPDATE jobs"

func jobStore() *jobs.Store { return jobs.NewStore(testPool) }

func cancelAck(fence workers.Fence) workers.CancelAcknowledgment {
	return workers.CancelAcknowledgment{Fence: fence, OutcomeRequestID: uuid.New()}
}

func readCancelRequestedAt(t *testing.T, jobID uuid.UUID) *time.Time {
	t.Helper()
	var at *time.Time
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT cancel_requested_at FROM jobs WHERE id = $1`, jobID).Scan(&at))
	return at
}

// TestCancel_BeforeAClaimIsTerminalAndCreatesNoAttempt covers the three states
// in which no attempt exists and no lease could commit, so cancellation is
// simply the end of the job.
func TestCancel_BeforeAClaimIsTerminalAndCreatesNoAttempt(t *testing.T) {
	ctx := context.Background()

	t.Run("QUEUED", func(t *testing.T) {
		reset(t)
		jobID := createJob(t, "cancel-queued", "demo.echo", 50, nil)
		outboxBefore := pendingOutboxIDs(t)

		result, err := jobStore().RequestCancel(ctx, testScope, jobID)
		require.NoError(t, err)
		require.Equal(t, jobs.StatusCanceled, result.Status)
		require.False(t, result.AlreadyRequested)

		require.Equal(t, "CANCELED", readJob(t, jobID).status)
		require.NotNil(t, readCancelRequestedAt(t, jobID))
		require.Equal(t, 0, countRows(t, "job_attempts"),
			"cancelling before a claim must create no attempt at all")
		require.Empty(t, newPendingOutbox(t, outboxBefore))

		// The advisory notification submission already published stays harmless:
		// the claim predicate simply finds no QUEUED job.
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("cancel-queued-worker", 1, nil, []string{"demo.echo"}))
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		require.Equal(t, workers.QueueEmpty, claim.Disposition)
		require.Equal(t, 0, countRows(t, "job_attempts"))
	})

	t.Run("PENDING", func(t *testing.T) {
		reset(t)
		future := time.Now().Add(time.Hour)
		jobID := createJobWithOptions(t, "cancel-pending", "default", "demo.echo", 50, nil, 3, 300, &future)
		require.Equal(t, "PENDING", readJob(t, jobID).status)

		result, err := jobStore().RequestCancel(ctx, testScope, jobID)
		require.NoError(t, err)
		require.Equal(t, jobs.StatusCanceled, result.Status)
		require.Equal(t, "CANCELED", readJob(t, jobID).status)
		require.Equal(t, 0, countRows(t, "job_attempts"))

		// A canceled job must never be promoted, however due it becomes.
		_, err = testPool.Exec(ctx,
			`UPDATE jobs SET available_at = clock_timestamp() - interval '1 minute' WHERE id = $1`, jobID)
		require.NoError(t, err)
		stats, err := jobStore().PromoteDueJobs(ctx, 10)
		require.NoError(t, err)
		require.Zero(t, stats.PromotedJobs)
		require.Equal(t, "CANCELED", readJob(t, jobID).status)
	})

	t.Run("RETRY_WAIT", func(t *testing.T) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("cancel-retry-wait", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "cancel-retry-wait")
		_, err := store.Fail(ctx, testScope,
			failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
		require.NoError(t, err)
		require.Equal(t, "RETRY_WAIT", readJob(t, fence.JobID).status)

		result, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
		require.NoError(t, err)
		require.Equal(t, jobs.StatusCanceled, result.Status)

		require.Equal(t, "CANCELED", readJob(t, fence.JobID).status)
		require.Equal(t, []string{"FAILED"}, attemptHistory(t, fence.JobID),
			"the attempt that already failed keeps its own history")
		require.Equal(t, 1, countRows(t, "job_attempts"),
			"cancelling a waiting job creates no new attempt")
	})
}

// TestCancel_WhileLeasedOrRunningRequestsRatherThanTerminates is the other half
// of the matrix: an attempt still holds authority that could commit a success,
// so the job must not be declared terminal while that is true.
func TestCancel_WhileLeasedOrRunningRequestsRatherThanTerminates(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		start bool
	}{
		{"LEASED", false},
		{"RUNNING", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset(t)
			store := controlStore()
			session := registerWorker(t, store,
				workerRegistration("cancel-executing", 1, nil, []string{"demo.echo"}))
			createJob(t, "cancel-executing", "demo.echo", 50, nil)
			claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
			require.NoError(t, err)
			fence := assignmentFence(claim.Assignment)
			if tc.start {
				startAttempt(t, store, fence)
			}

			result, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
			require.NoError(t, err)
			require.Equal(t, jobs.StatusCancelRequested, result.Status)
			require.NotZero(t, result.CancelRequestedAt)

			state := readState(t, fence)
			require.Equal(t, "CANCEL_REQUESTED", state.job)
			require.Equal(t, tc.name, state.attempt, "attempt history is not erased yet")
			require.Equal(t, "ACTIVE", state.lease, "the lease is still the capacity ledger")
			require.Equal(t, 1, countActiveLeases(t))

			// Every operation that could otherwise commit an outcome is now
			// refused. This is what makes CANCEL_REQUESTED a decision rather than
			// a hint.
			require.ErrorIs(t, store.Succeed(ctx, testScope, fence), workers.ErrStateConflict)
			_, err = store.Fail(ctx, testScope,
				failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
			require.ErrorIs(t, err, workers.ErrStateConflict)
			_, err = store.RenewLease(ctx, testScope, renewalRequest(fence, 0))
			require.ErrorIs(t, err, workers.ErrStateConflict)
			require.ErrorIs(t, startError(store, fence), workers.ErrStateConflict)

			require.Equal(t, state, readState(t, fence), "every rejection must mutate nothing")
		})
	}
}

// TestCancel_IsIdempotentUnderScopeAndJobIdAlone proves the operation needs no
// request identity: cancelling twice is one decision observed twice.
func TestCancel_IsIdempotentUnderScopeAndJobIdAlone(t *testing.T) {
	reset(t)
	ctx := context.Background()
	jobID := createJob(t, "cancel-idempotent", "demo.echo", 50, nil)

	first, err := jobStore().RequestCancel(ctx, testScope, jobID)
	require.NoError(t, err)
	require.False(t, first.AlreadyRequested)
	stamped := readCancelRequestedAt(t, jobID)
	require.NotNil(t, stamped)

	for i := 0; i < 3; i++ {
		repeat, err := jobStore().RequestCancel(ctx, testScope, jobID)
		require.NoError(t, err)
		require.True(t, repeat.AlreadyRequested)
		require.Equal(t, jobs.StatusCanceled, repeat.Status)
		require.True(t, first.CancelRequestedAt.Equal(repeat.CancelRequestedAt),
			"a repeat must report the instant cancellation actually won")
	}
	require.True(t, stamped.Equal(*readCancelRequestedAt(t, jobID)),
		"a repeat must not restamp the job")
}

// TestCancel_TerminalJobsAreAStableConflict proves cancellation cannot rewrite
// history. A terminal job never returns to a non-terminal state, and it never
// changes which terminal state it is in.
func TestCancel_TerminalJobsAreAStableConflict(t *testing.T) {
	ctx := context.Background()

	t.Run("SUCCEEDED", func(t *testing.T) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("cancel-succeeded", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "cancel-succeeded")
		require.NoError(t, store.Succeed(ctx, testScope, fence))

		_, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
		require.ErrorIs(t, err, jobs.ErrJobNotCancelable)
		require.Equal(t, "SUCCEEDED", readJob(t, fence.JobID).status)
		require.Nil(t, readCancelRequestedAt(t, fence.JobID))
	})

	t.Run("DEAD_LETTERED", func(t *testing.T) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("cancel-dead", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "cancel-dead")
		_, err := store.Fail(ctx, testScope,
			failureReport(fence, lifecycle.ClassPermanent, "invalid_payload", ""))
		require.NoError(t, err)

		_, err = jobStore().RequestCancel(ctx, testScope, fence.JobID)
		require.ErrorIs(t, err, jobs.ErrJobNotCancelable)
		require.Equal(t, "DEAD_LETTERED", readJob(t, fence.JobID).status)
	})

	t.Run("another tenant's job is simply not found", func(t *testing.T) {
		reset(t)
		jobID := createJob(t, "cancel-scoped", "demo.echo", 50, nil)
		_, err := jobStore().RequestCancel(ctx, "someone-else", jobID)
		require.ErrorIs(t, err, jobs.ErrJobNotFound)
		require.Equal(t, "QUEUED", readJob(t, jobID).status)
	})
}

// TestCancel_CooperativeAcknowledgmentFinalizesTheAttempt is the path a healthy
// worker takes: it learns of the cancellation, stops, and hands authority back.
func TestCancel_CooperativeAcknowledgmentFinalizesTheAttempt(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("cancel-ack", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "cancel-ack")
	outboxBefore := pendingOutboxIDs(t)

	_, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
	require.NoError(t, err)

	ack := cancelAck(fence)
	result, err := store.AcknowledgeCancellation(ctx, testScope, ack)
	require.NoError(t, err)
	require.Equal(t, "CANCELED", result.JobStatus)
	require.Equal(t, workers.AttemptCanceled, result.AttemptStatus)
	require.False(t, result.Replayed)

	state := readState(t, fence)
	require.Equal(t, "CANCELED", state.job)
	require.Equal(t, "CANCELED", state.attempt)
	require.Equal(t, "RELEASED", state.lease,
		"a cooperative acknowledgment hands authority back rather than losing it")
	require.Equal(t, 0, countActiveLeases(t))

	row := readAttemptOutcome(t, fence.AttemptID)
	require.Equal(t, "CANCELED", *row.failureClass)
	require.Equal(t, lifecycle.CodeCanceled, *row.errorCode)
	require.NotNil(t, row.finishedAt)
	require.Nil(t, row.retryAt, "cancellation never produces a retry")
	require.NotNil(t, row.outcomeID)
	require.Equal(t, ack.OutcomeRequestID, *row.outcomeID)

	require.Empty(t, dlqRows(t, fence.JobID), "cancellation never creates a dead-letter entry")
	require.Empty(t, newPendingOutbox(t, outboxBefore))
}

// TestCancel_AcknowledgmentIsIdempotentUnderItsOutcomeIdentity is the ambiguity
// contract for the acknowledgment, matching the one failure reporting has.
func TestCancel_AcknowledgmentIsIdempotentUnderItsOutcomeIdentity(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("cancel-ack-replay", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "cancel-ack-replay")
	_, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
	require.NoError(t, err)

	ack := cancelAck(fence)
	_, err = store.AcknowledgeCancellation(ctx, testScope, ack)
	require.NoError(t, err)
	committed := readAttemptOutcome(t, fence.AttemptID)

	for i := 0; i < 3; i++ {
		replay, err := store.AcknowledgeCancellation(ctx, testScope, ack)
		require.NoError(t, err)
		require.True(t, replay.Replayed)
		require.Equal(t, "CANCELED", replay.JobStatus)
	}
	require.Equal(t, committed, readAttemptOutcome(t, fence.AttemptID),
		"a replay must not move a single stored field")

	// A different identity for the same, already-finalized attempt is refused
	// rather than silently accepted: the attempt is no longer executing.
	_, err = store.AcknowledgeCancellation(ctx, testScope, cancelAck(fence))
	require.Error(t, err)
	require.Equal(t, committed, readAttemptOutcome(t, fence.AttemptID))
}

// TestCancel_AttemptCanceledBeforeItEverStarted is the case migration 0009
// revised the timeline constraint for.
func TestCancel_AttemptCanceledBeforeItEverStarted(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("cancel-unstarted", 1, nil, []string{"demo.echo"}))
	createJob(t, "cancel-unstarted", "demo.echo", 50, nil)
	claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	fence := assignmentFence(claim.Assignment)

	_, err = jobStore().RequestCancel(ctx, testScope, fence.JobID)
	require.NoError(t, err)
	_, err = store.AcknowledgeCancellation(ctx, testScope, cancelAck(fence))
	require.NoError(t, err)

	row := readAttemptOutcome(t, fence.AttemptID)
	require.Equal(t, "CANCELED", row.status)
	require.NotNil(t, row.finishedAt)

	var startedAt *time.Time
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT started_at FROM job_attempts WHERE id = $1`, fence.AttemptID).Scan(&startedAt))
	require.Nil(t, startedAt,
		"an attempt canceled between claim and start truthfully never started")
	require.Equal(t, "CANCELED", readState(t, fence).job)
}

// TestCancel_ReconciliationFinalizesWhenNoWorkerAcknowledges is the fallback for
// a worker that is gone or uncooperative. Nothing is stranded: renewal is
// already refused for a CANCEL_REQUESTED job, so the lease simply lapses.
func TestCancel_ReconciliationFinalizesWhenNoWorkerAcknowledges(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("cancel-fallback", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "cancel-fallback")
	outboxBefore := pendingOutboxIDs(t)

	_, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
	require.NoError(t, err)
	expireLease(t, fence.LeaseID)

	stats, err := store.ReconcileExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.ExpiredLeases)
	require.Equal(t, 1, stats.CanceledAttempts)
	require.Zero(t, stats.RequeuedJobs, "a canceled job is not recovered")
	require.Zero(t, stats.DeadLetteredJobs)

	state := readState(t, fence)
	require.Equal(t, "CANCELED", state.job)
	require.Equal(t, "CANCELED", state.attempt)
	require.Equal(t, "EXPIRED", state.lease,
		"authority was taken away rather than handed back, and history says so")
	require.Equal(t, 0, countActiveLeases(t))

	row := readAttemptOutcome(t, fence.AttemptID)
	require.Equal(t, "CANCELED", *row.failureClass)
	require.Nil(t, row.outcomeID, "nobody requested this, so there is no identity to retain")
	require.Empty(t, dlqRows(t, fence.JobID))
	require.Empty(t, newPendingOutbox(t, outboxBefore),
		"a canceled job must never be advertised again")

	// Repeated passes change nothing.
	for i := 0; i < 2; i++ {
		again, err := store.ReconcileExpiredLeases(ctx, 10)
		require.NoError(t, err)
		require.Zero(t, again.CanceledAttempts)
	}
	require.Equal(t, state, readState(t, fence))
}

// TestCancel_TakesPrecedenceOverADueTimeout proves the precedence order inside
// the expired-lease scan. Once cancellation has durably won, calling the same
// lapse a timeout would both lose the operator's decision and wrongly consume
// attempt budget.
func TestCancel_TakesPrecedenceOverADueTimeout(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("cancel-vs-timeout", 1, nil, []string{"demo.echo"}))
	fence, _ := claimedRunningWithTimeout(t, store, session, "cancel-vs-timeout", 3, 60)

	_, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
	require.NoError(t, err)
	expireAttemptDeadline(t, fence.AttemptID)
	expireLease(t, fence.LeaseID)

	// The dedicated timeout scan must not touch it either: it filters on a
	// RUNNING job, and this one is CANCEL_REQUESTED.
	timeouts, err := store.ReconcileDueTimeouts(ctx, 10)
	require.NoError(t, err)
	require.Zero(t, timeouts.TimedOutAttempts)

	stats, err := store.ReconcileExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.CanceledAttempts)
	require.Zero(t, stats.TimedOutAttempts)

	state := readState(t, fence)
	require.Equal(t, "CANCELED", state.job)
	require.Equal(t, "CANCELED", state.attempt)
	row := readAttemptOutcome(t, fence.AttemptID)
	require.Equal(t, "CANCELED", row.status, "the attempt must not be recorded as timed out")
	require.Equal(t, "CANCELED", *row.failureClass)
	require.Nil(t, row.retryAt, "cancellation consumes no attempt budget and schedules no retry")
	require.Empty(t, dlqRows(t, fence.JobID))
}

// TestCancel_AnUncooperativeHandlerCannotCommitAfterwards is the durable half of
// the cooperative-cancellation limitation, for the cancellation cause.
func TestCancel_AnUncooperativeHandlerCannotCommitAfterwards(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("cancel-uncooperative", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "cancel-uncooperative")

	_, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
	require.NoError(t, err)
	expireLease(t, fence.LeaseID)
	_, err = store.ReconcileExpiredLeases(ctx, 10)
	require.NoError(t, err)
	finalized := readAttemptOutcome(t, fence.AttemptID)

	// Everything the still-running handler's worker could try afterwards.
	require.Error(t, store.Succeed(ctx, testScope, fence))
	_, err = store.Fail(ctx, testScope,
		failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
	require.Error(t, err)
	_, err = store.AcknowledgeCancellation(ctx, testScope, cancelAck(fence))
	require.Error(t, err)
	_, err = store.RenewLease(ctx, testScope, renewalRequest(fence, 0))
	require.Error(t, err)

	require.Equal(t, finalized, readAttemptOutcome(t, fence.AttemptID))
	require.Equal(t, "CANCELED", readJob(t, fence.JobID).status)
}

// TestCancel_DirectivesReachOnlyTheSessionExecutingTheAttempt covers the
// delivery mechanism itself: cancellation rides the heartbeat, which runs
// unconditionally, so it reaches a worker that is executing and one that is
// idle alike, with no broker delivery involved.
func TestCancel_DirectivesReachOnlyTheSessionExecutingTheAttempt(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	owner := registerWorker(t, store,
		workerRegistration("directive-owner", 1, nil, []string{"demo.echo"}))
	bystander := registerWorker(t, store,
		workerRegistration("directive-bystander", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, owner, "directive")

	// Nothing to deliver before a cancellation exists.
	require.Empty(t, heartbeat(t, store, owner).Cancellations)

	_, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
	require.NoError(t, err)

	result := heartbeat(t, store, owner)
	require.Len(t, result.Cancellations, 1)
	directive := result.Cancellations[0]
	require.Equal(t, fence.JobID, directive.JobID)
	require.Equal(t, fence.AttemptID, directive.AttemptID)
	require.Equal(t, fence.LeaseID, directive.LeaseID,
		"the directive names the lease the worker is expected to hold")
	require.NotZero(t, directive.CancelRequestedAt)

	require.Empty(t, heartbeat(t, store, bystander).Cancellations,
		"a directive must never be handed to a session that is not executing the attempt")

	// It keeps being delivered until the attempt is finalized, so a worker that
	// missed one tick is not left waiting for a redelivery that never comes.
	require.Len(t, heartbeat(t, store, owner).Cancellations, 1)

	_, err = store.AcknowledgeCancellation(ctx, testScope, cancelAck(fence))
	require.NoError(t, err)
	require.Empty(t, heartbeat(t, store, owner).Cancellations,
		"a finalized attempt has nothing left to cancel")
}

// TestCancel_DirectiveStopsOnceAuthorityIsGone proves a directive is not handed
// to a worker whose lease has already lapsed. Telling it to stop cooperatively
// would be pointless: reconciliation owns that case.
func TestCancel_DirectiveStopsOnceAuthorityIsGone(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("directive-lapsed", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "directive-lapsed")
	_, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
	require.NoError(t, err)
	require.Len(t, heartbeat(t, store, session).Cancellations, 1)

	expireLease(t, fence.LeaseID)
	_, err = store.ReconcileExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, heartbeat(t, store, session).Cancellations)
}

// TestContention_CancelVersusSuccess arranges the race deliberately in both
// orderings, with exactly one terminal winner each time.
func TestContention_CancelVersusSuccess(t *testing.T) {
	t.Run("cancel first: the later success is rejected and mutates nothing", func(t *testing.T) {
		reset(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("cancel-then-success", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "cancel-then-success")

		// The gate fires on the cancellation's own job UPDATE. Success also
		// updates jobs, so the WHEN clause names the status only cancellation
		// writes from this state.
		release := gateOnAdvisoryLockWhen(t, gateCancelBeforeSuccessKey,
			"taskforge_test_gate_cancel_first", "BEFORE UPDATE", "jobs",
			"NEW.status = 'CANCEL_REQUESTED'")

		cancelErr := make(chan error, 1)
		go func() {
			_, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
			cancelErr <- err
		}()
		// Parked at its job UPDATE, holding the queue and job rows.
		waitForDatabaseLock(t, fragmentCancelJob)

		successErr := make(chan error, 1)
		go func() { successErr <- store.Succeed(ctx, testScope, fence) }()
		waitForDatabaseLock(t, fragmentQueueLock)

		release()
		require.NoError(t, <-cancelErr, "cancellation held authority first and must commit")
		require.ErrorIs(t, <-successErr, workers.ErrStateConflict,
			"a success arriving after cancellation won must be refused")

		state := readState(t, fence)
		require.Equal(t, "CANCEL_REQUESTED", state.job)
		require.Equal(t, "RUNNING", state.attempt)
		require.Equal(t, "ACTIVE", state.lease)
		require.Equal(t, 1, countActiveLeases(t))
	})

	t.Run("success first: the later cancel is a stable conflict", func(t *testing.T) {
		reset(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("success-then-cancel", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "success-then-cancel")

		release := gateOnAdvisoryLockWhen(t, gateSuccessBeforeCancelKey,
			"taskforge_test_gate_success_before_cancel", "BEFORE UPDATE", "jobs",
			"NEW.status = 'SUCCEEDED'")

		successErr := make(chan error, 1)
		go func() { successErr <- store.Succeed(ctx, testScope, fence) }()
		waitForDatabaseLock(t, fragmentCancelJob)

		cancelErr := make(chan error, 1)
		go func() {
			_, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
			cancelErr <- err
		}()
		waitForDatabaseLock(t, fragmentQueueLock)

		release()
		require.NoError(t, <-successErr, "the success held authority first and must commit")
		require.ErrorIs(t, <-cancelErr, jobs.ErrJobNotCancelable)

		state := readState(t, fence)
		require.Equal(t, "SUCCEEDED", state.job, "a terminal job never becomes canceled")
		require.Equal(t, "SUCCEEDED", state.attempt)
		require.Equal(t, "COMPLETED", state.lease)
		require.Nil(t, readCancelRequestedAt(t, fence.JobID),
			"a refused cancellation must leave no stamp behind")
	})
}
