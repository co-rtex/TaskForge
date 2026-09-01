//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// Gate keys for the remaining M4 contention barriers.
const (
	gateFailBeforeRenewKey   int64 = 7710010050
	gateRenewBeforeFailKey   int64 = 7710010051
	gateCancelBeforeRenewKey int64 = 7710010052
	gateRenewBeforeCancelKey int64 = 7710010053
	gateCancelBeforeStartKey int64 = 7710010054
	gateStartBeforeCancelKey int64 = 7710010055
	gateRenotifyVsReconcile  int64 = 7710010056
)

const (
	whenFailing          = "NEW.status = 'FAILED'"
	whenStarting         = "NEW.status = 'RUNNING'"
	whenRequestingCancel = "NEW.status = 'CANCEL_REQUESTED'"
)

// TestContention_FailureVersusRenewal is the race a worker creates against
// itself: the renewal loop and the terminal report are separate calls, and the
// worker stops renewal before reporting precisely so they do not overlap. This
// proves the control plane resolves them correctly even if they do.
func TestContention_FailureVersusRenewal(t *testing.T) {
	t.Run("failure first: the later renewal is rejected and mutates nothing", func(t *testing.T) {
		reset(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("fail-then-renew", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "fail-then-renew")

		release := gateOnAdvisoryLockWhen(t, gateFailBeforeRenewKey,
			"taskforge_test_gate_fail_first", "BEFORE UPDATE", "job_attempts", whenFailing)

		failErr := make(chan error, 1)
		go func() {
			_, err := store.Fail(ctx, testScope,
				failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
			failErr <- err
		}()
		// Parked at its attempt UPDATE, holding every authority row.
		waitForDatabaseLock(t, fragmentAttemptTx)

		renewErr := make(chan error, 1)
		go func() {
			_, err := store.RenewLease(ctx, testScope, renewalRequest(fence, 0))
			renewErr <- err
		}()
		waitForDatabaseLock(t, fragmentQueueLock)

		release()
		require.NoError(t, <-failErr, "the failure held authority first and must commit")
		require.ErrorIs(t, <-renewErr, workers.ErrLeaseExpired,
			"a released lease is no longer authority to extend")

		state := readState(t, fence)
		require.Equal(t, "RETRY_WAIT", state.job)
		require.Equal(t, "FAILED", state.attempt)
		require.Equal(t, "RELEASED", state.lease)
		_, _, _, version := leaseRow(t, fence.LeaseID)
		require.Equal(t, 0, version, "a rejected renewal must not advance the generation")
		require.Equal(t, 0, countActiveLeases(t))
	})

	t.Run("renewal first: both commit, in that order", func(t *testing.T) {
		reset(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("renew-then-fail", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "renew-then-fail")

		release := gateOnAdvisoryLockWhen(t, gateRenewBeforeFailKey,
			"taskforge_test_gate_renew_before_fail", "BEFORE UPDATE", "leases", whenRenewing)

		renewErr := make(chan error, 1)
		go func() {
			_, err := store.RenewLease(ctx, testScope, renewalRequest(fence, 0))
			renewErr <- err
		}()
		waitForDatabaseLock(t, fragmentRenewing)

		failErr := make(chan error, 1)
		go func() {
			_, err := store.Fail(ctx, testScope,
				failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
			failErr <- err
		}()
		waitForDatabaseLock(t, fragmentQueueLock)

		release()
		require.NoError(t, <-renewErr, "the renewal held authority first and must commit")
		require.NoError(t, <-failErr,
			"a freshly renewed lease is still authority, so the failure commits after it")

		state := readState(t, fence)
		require.Equal(t, "RETRY_WAIT", state.job)
		require.Equal(t, "FAILED", state.attempt)
		require.Equal(t, "RELEASED", state.lease)
		_, _, _, version := leaseRow(t, fence.LeaseID)
		require.Equal(t, 1, version, "the renewal really did commit before the failure")
		require.Equal(t, 0, countActiveLeases(t))
		require.Equal(t, 1, countRows(t, "job_attempts"),
			"the two operations together consume exactly one attempt")
	})
}

// TestContention_CancellationVersusRenewal proves the two decisions serialize on
// the job row, and that once cancellation wins, renewal stops being able to
// extend the authority reconciliation is waiting to reclaim.
func TestContention_CancellationVersusRenewal(t *testing.T) {
	t.Run("cancellation first: the later renewal is rejected", func(t *testing.T) {
		reset(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("cancel-then-renew", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "cancel-then-renew")

		release := gateOnAdvisoryLockWhen(t, gateCancelBeforeRenewKey,
			"taskforge_test_gate_cancel_before_renew", "BEFORE UPDATE", "jobs",
			whenRequestingCancel)

		cancelErr := make(chan error, 1)
		go func() {
			_, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
			cancelErr <- err
		}()
		waitForDatabaseLock(t, fragmentCancelJob)

		renewErr := make(chan error, 1)
		go func() {
			_, err := store.RenewLease(ctx, testScope, renewalRequest(fence, 0))
			renewErr <- err
		}()
		waitForDatabaseLock(t, fragmentQueueLock)

		release()
		require.NoError(t, <-cancelErr, "cancellation held authority first and must commit")
		require.ErrorIs(t, <-renewErr, workers.ErrStateConflict,
			"refusing renewal is what makes the lease lapse so reconciliation can finalize")

		state := readState(t, fence)
		require.Equal(t, "CANCEL_REQUESTED", state.job)
		require.Equal(t, "RUNNING", state.attempt)
		require.Equal(t, "ACTIVE", state.lease)
		_, _, _, version := leaseRow(t, fence.LeaseID)
		require.Equal(t, 0, version)
	})

	t.Run("renewal first: both commit, and cancellation still wins the job", func(t *testing.T) {
		reset(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("renew-then-cancel", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "renew-then-cancel")

		release := gateOnAdvisoryLockWhen(t, gateRenewBeforeCancelKey,
			"taskforge_test_gate_renew_before_cancel", "BEFORE UPDATE", "leases", whenRenewing)

		renewErr := make(chan error, 1)
		go func() {
			_, err := store.RenewLease(ctx, testScope, renewalRequest(fence, 0))
			renewErr <- err
		}()
		waitForDatabaseLock(t, fragmentRenewing)

		cancelErr := make(chan error, 1)
		go func() {
			_, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
			cancelErr <- err
		}()
		waitForDatabaseLock(t, fragmentQueueLock)

		release()
		require.NoError(t, <-renewErr, "the renewal held authority first and must commit")
		require.NoError(t, <-cancelErr, "cancelling a running job is valid whatever its lease window is")

		state := readState(t, fence)
		require.Equal(t, "CANCEL_REQUESTED", state.job)
		_, _, _, version := leaseRow(t, fence.LeaseID)
		require.Equal(t, 1, version, "the renewal really did commit first")

		// A longer lease window changes nothing about the outcome: the next
		// renewal is refused, so the window simply runs out.
		_, err := store.RenewLease(ctx, testScope, renewalRequest(fence, 1))
		require.ErrorIs(t, err, workers.ErrStateConflict)
	})
}

// TestContention_CancellationVersusStart covers the narrow window in which a job
// is claimed but its attempt has not begun.
func TestContention_CancellationVersusStart(t *testing.T) {
	claimOnly := func(t *testing.T, store *workers.Store, session workers.Session, key string) workers.Fence {
		t.Helper()
		createJob(t, key, "demo.echo", 50, nil)
		claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		require.Equal(t, workers.Claimed, claim.Disposition)
		return assignmentFence(claim.Assignment)
	}

	t.Run("cancellation first: the attempt never starts", func(t *testing.T) {
		reset(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("cancel-then-start", 1, nil, []string{"demo.echo"}))
		fence := claimOnly(t, store, session, "cancel-then-start")

		release := gateOnAdvisoryLockWhen(t, gateCancelBeforeStartKey,
			"taskforge_test_gate_cancel_before_start", "BEFORE UPDATE", "jobs",
			whenRequestingCancel)

		cancelErr := make(chan error, 1)
		go func() {
			_, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
			cancelErr <- err
		}()
		waitForDatabaseLock(t, fragmentCancelJob)

		startErr := make(chan error, 1)
		go func() {
			_, err := store.Start(ctx, testScope, fence)
			startErr <- err
		}()
		waitForDatabaseLock(t, fragmentQueueLock)

		release()
		require.NoError(t, <-cancelErr)

		// The refusal is typed. A worker that loses this race has to
		// acknowledge the cancellation rather than drop the attempt, and it can
		// only tell the two apart if the control plane names the reason.
		err := <-startErr
		require.ErrorIs(t, err, workers.ErrCancellationRequested)
		require.NotErrorIs(t, err, workers.ErrStateConflict,
			"a cancel-first Start must not be reported as an unrelated state conflict")

		state := readState(t, fence)
		require.Equal(t, "CANCEL_REQUESTED", state.job)
		require.Equal(t, "LEASED", state.attempt, "the attempt never started")
		require.Nil(t, readAttemptOutcome(t, fence.AttemptID).timeoutAt,
			"an attempt that never started has no execution deadline")

		// Acknowledging it produces the CANCELED attempt with no start time that
		// migration 0009 revised the timeline constraint to allow.
		_, err = store.AcknowledgeCancellation(ctx, testScope, cancelAck(fence))
		require.NoError(t, err)
		require.Equal(t, "CANCELED", readState(t, fence).attempt)
	})

	t.Run("start first: cancellation still lands on the running attempt", func(t *testing.T) {
		reset(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("start-then-cancel", 1, nil, []string{"demo.echo"}))
		fence := claimOnly(t, store, session, "start-then-cancel")

		release := gateOnAdvisoryLockWhen(t, gateStartBeforeCancelKey,
			"taskforge_test_gate_start_before_cancel", "BEFORE UPDATE", "job_attempts", whenStarting)

		startErr := make(chan error, 1)
		go func() {
			_, err := store.Start(ctx, testScope, fence)
			startErr <- err
		}()
		waitForDatabaseLock(t, fragmentAttemptTx)

		cancelErr := make(chan error, 1)
		go func() {
			_, err := jobStore().RequestCancel(ctx, testScope, fence.JobID)
			cancelErr <- err
		}()
		waitForDatabaseLock(t, fragmentQueueLock)

		release()
		require.NoError(t, <-startErr, "the start held authority first and must commit")
		require.NoError(t, <-cancelErr)

		state := readState(t, fence)
		require.Equal(t, "CANCEL_REQUESTED", state.job)
		require.Equal(t, "RUNNING", state.attempt)
		require.NotNil(t, readAttemptOutcome(t, fence.AttemptID).timeoutAt,
			"a started attempt has its deadline stamped, even though it is about to be canceled")
	})
}

// TestContention_RenotificationVersusReconciliation covers the last pair that
// can overlap on one job: a scheduler repairing reachability while a reconciler
// is recovering the same job's abandoned attempt.
func TestContention_RenotificationVersusReconciliation(t *testing.T) {
	reset(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("renotify-vs-reconcile", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "renotify-vs-reconcile")
	expireLease(t, fence.LeaseID)

	// Reconciliation parks mid-transaction, holding the queue and job rows. Its
	// requeue will open generation 2.
	release := gateOnAdvisoryLockWhen(t, gateRenotifyVsReconcile,
		"taskforge_test_gate_renotify_vs_reconcile", "BEFORE UPDATE", "job_attempts",
		"NEW.status = 'ABANDONED'")

	reconcileErr := make(chan error, 1)
	go func() {
		_, err := store.ReconcileExpiredLeases(ctx, 10)
		reconcileErr <- err
	}()
	waitForDatabaseLock(t, fragmentAttemptTx)

	// The scheduler's re-notification scan runs now. The job is not QUEUED yet,
	// so there is nothing for it to repair, and it must not invent anything.
	renotifyErr := make(chan error, 1)
	renotifyResult := make(chan jobs.SchedulerStats, 1)
	go func() {
		stats, err := jobStore().RenotifyStrandedQueued(ctx, time.Millisecond, 10)
		renotifyResult <- stats
		renotifyErr <- err
	}()

	release()
	require.NoError(t, <-reconcileErr)
	require.NoError(t, <-renotifyErr)
	require.Zero(t, (<-renotifyResult).Renotified,
		"a job that is not QUEUED is not stranded, whatever its notification timestamp says")

	state := readState(t, fence)
	require.Equal(t, "QUEUED", state.job)
	require.Equal(t, "ABANDONED", state.attempt)

	job := readJob(t, fence.JobID)
	require.Equal(t, 2, job.generation)
	events := eventsForJob(t, fence.JobID)
	require.Len(t, events, 2, "exactly one recovery notification, and no extra repair")
	require.Equal(t, 1, events[0].Generation)
	require.Equal(t, 2, events[1].Generation)
	require.Equal(t, 0, countActiveLeases(t))
}
