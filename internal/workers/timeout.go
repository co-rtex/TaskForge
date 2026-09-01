package workers

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/co-rtex/TaskForge/internal/lifecycle"
)

// ReconcileDueTimeouts records TIMED_OUT for every running attempt whose
// persisted execution deadline has passed.
//
// There is deliberately no worker-authoritative timeout endpoint. A worker
// cancels its handler at a conservative local deadline so cooperative code gets
// a chance to stop, but only a PostgreSQL transaction may record the outcome:
// a worker that has stalled, been paused, or lost its clock is exactly the
// worker least able to judge whether it timed out, and an uncooperative handler
// that keeps running must still be fenced out of committing anything.
//
// Every scan here is candidate discovery. The decision is re-made under the full
// queue -> worker session -> job -> attempt -> lease lock order against a fresh
// post-lock clock_timestamp(), so a success that committed while this pass was
// waiting wins, and a second reconciler finds nothing left to do.
func (s *Store) ReconcileDueTimeouts(ctx context.Context, limit int) (_ ReconcileStats, err error) {
	defer func() { err = classifyDatabaseError(err) }()

	if limit < 1 {
		return ReconcileStats{}, fmt.Errorf("scan limit must be positive")
	}

	// Matches job_attempts_due_timeout_idx. No lock is taken: acquiring an
	// attempt lock here and then reaching for queue and session locks per
	// candidate would invert the established order.
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM job_attempts
		WHERE status = 'RUNNING' AND timeout_at <= clock_timestamp()
		ORDER BY timeout_at, id
		LIMIT $1`, limit)
	if err != nil {
		return ReconcileStats{}, fmt.Errorf("scan due attempt timeouts: %w", err)
	}
	candidates, err := collectIDs(rows)
	if err != nil {
		return ReconcileStats{}, err
	}

	var stats ReconcileStats
	for _, attemptID := range candidates {
		if err := s.reconcileDueTimeout(ctx, attemptID, &stats); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func (s *Store) reconcileDueTimeout(ctx context.Context, attemptID uuid.UUID, stats *ReconcileStats) error {
	// Immutable routing columns only. They are written once at claim and never
	// change, so reading them without a lock is a routing hint rather than an
	// authority decision; every mutable field is re-read under the locks below.
	var fence Fence
	var scope string
	err := s.pool.QueryRow(ctx, `
		SELECT a.job_id, a.id, l.id, a.worker_id, a.worker_session_id, a.scope
		FROM job_attempts a
		JOIN leases l ON l.attempt_id = a.id
		WHERE a.id = $1`, attemptID,
	).Scan(&fence.JobID, &fence.AttemptID, &fence.LeaseID,
		&fence.WorkerID, &fence.SessionID, &scope)
	if errors.Is(err, pgx.ErrNoRows) {
		stats.Skipped++
		return nil
	}
	if err != nil {
		return fmt.Errorf("route attempt timeout: %w", err)
	}

	tx, state, err := s.lockFenceForReconciliation(ctx, scope, fence)
	if err != nil {
		if errors.Is(err, ErrFenceRejected) {
			stats.Skipped++
			return nil
		}
		return err
	}
	defer rollback(ctx, tx)

	// Everything below is revalidated against the post-lock sample.
	//
	// The lease must still be ACTIVE: if it lapsed first, the expired-lease scan
	// owns this attempt and applies the same timeout precedence there. The job
	// must still be RUNNING: a job that moved to CANCEL_REQUESTED while this pass
	// waited has already been decided, and cancellation never becomes a timeout.
	if !state.leaseUsable() || !state.timedOut() ||
		state.jobStatus != "RUNNING" || state.attemptStatus != AttemptRunning {
		stats.Skipped++
		return nil
	}

	if err := s.finalizeTimeout(ctx, tx, state, fence, stats); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit attempt timeout: %w", err)
	}
	stats.TimedOutAttempts++
	return nil
}

// finalizeTimeout applies the TIMED_OUT outcome inside a transaction that
// already holds every authority row.
//
// It is shared by the due-timeout scan and by the expired-lease scan, because a
// lease that lapsed around a deadline that had already passed is a timeout, not
// an abandonment, and the two scans must not disagree about that.
//
// leaseStatus differs between them for the same reason it does elsewhere:
// RELEASED when the timeout was detected while the lease was still live, EXPIRED
// when the lease had already lapsed.
func (s *Store) finalizeTimeout(
	ctx context.Context,
	tx pgx.Tx,
	state fenceState,
	fence Fence,
	stats *ReconcileStats,
) error {
	decision, err := s.retryPolicy.Decide(
		lifecycle.ClassTimedOut, state.attemptNumber, state.attemptNumber, state.maxAttempts, s.jitter)
	if err != nil {
		return fmt.Errorf("decide timeout retry: %w", err)
	}
	leaseStatus := LeaseReleased
	if state.leaseStatus == LeaseActive && !state.serverNow.Before(state.expiresAt) {
		leaseStatus = LeaseExpired
	}

	result, err := s.finalizeAttempt(ctx, tx, state, fence, attemptOutcome{
		status:       AttemptTimedOut,
		class:        lifecycle.ClassTimedOut,
		errorCode:    lifecycle.CodeTimeout,
		errorMessage: lifecycle.MessageTimeout,
		// No outcome identity: this decision was not requested by anybody, so
		// there is no client identity to retain. Idempotency comes from the
		// attempt no longer being RUNNING once this commits.
		outcomeRequestID: nil,
		leaseStatus:      leaseStatus,
	}, decision)
	if err != nil {
		return err
	}
	if result.JobStatus == "DEAD_LETTERED" {
		stats.DeadLetteredJobs++
	} else {
		stats.RetryWaitingJobs++
	}
	return nil
}

// lockFenceForReconciliation acquires the same authority rows in the same order
// as lockFence, but WITHOUT requiring the worker session to still be healthy.
//
// That difference is the whole point. Reconciliation exists to repair state
// whose worker is gone, so demanding a healthy session would refuse to repair
// exactly the cases it was built for. The session row is still locked, so
// reconciliation serializes against registration, heartbeat, claim, and every
// fenced transition on that row.
func (s *Store) lockFenceForReconciliation(ctx context.Context, scope string, fence Fence) (pgx.Tx, fenceState, error) {
	return s.lockAuthorityRows(ctx, scope, fence, false)
}
