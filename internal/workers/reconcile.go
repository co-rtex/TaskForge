package workers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/co-rtex/TaskForge/internal/lifecycle"
)

// MarkStaleSessions marks every current process session that has missed the
// staleness threshold as UNHEALTHY, using PostgreSQL receipt time.
//
// UNHEALTHY, not OFFLINE: OFFLINE means "this boot's authority ended because it
// was replaced or it shut down", and keeping the two distinct is what lets an
// operator tell a crash from a restart. Either status leaves the current set, so
// the fenced process can neither heartbeat nor claim nor commit an outcome, and
// its logical worker is free to register a new boot.
//
// Marking a session stale deliberately does NOT touch its leases. Those are
// reconciled by ReconcileExpiredLeases on their own server-owned expiry, because
// a lease can outlive its session's heartbeat and a session can keep heartbeating
// while one of its leases expires.
//
// Locking: one session row per transaction, a prefix of the established
// queue -> worker session -> job -> attempt -> lease order.
//
// Safe with N replicas: the candidate scan carries no authority, and every
// decision is revalidated under the row lock against a fresh clock sample.
func (s *Store) MarkStaleSessions(ctx context.Context, staleAfter time.Duration, limit int) (_ int, err error) {
	defer func() { err = classifyDatabaseError(err) }()

	if staleAfter <= 0 {
		return 0, fmt.Errorf("stale threshold must be positive")
	}
	if limit < 1 {
		return 0, fmt.Errorf("scan limit must be positive")
	}

	// Candidate discovery only. These ids are re-read and revalidated under locks
	// below; nothing here is treated as a decision. Matches
	// worker_sessions_current_heartbeat_idx.
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM worker_sessions
		WHERE status IN ('STARTING', 'HEALTHY', 'DRAINING')
		  AND last_heartbeat_at < clock_timestamp() - make_interval(secs => $1::double precision)
		ORDER BY last_heartbeat_at, id
		LIMIT $2`, staleAfter.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("scan stale worker sessions: %w", err)
	}
	candidates, err := collectIDs(rows)
	if err != nil {
		return 0, err
	}

	marked := 0
	for _, id := range candidates {
		changed, err := s.markSessionStale(ctx, id, staleAfter)
		if err != nil {
			return marked, err
		}
		if changed {
			marked++
		}
	}
	return marked, nil
}

func (s *Store) markSessionStale(ctx context.Context, id uuid.UUID, staleAfter time.Duration) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin stale session transaction: %w", err)
	}
	defer rollback(ctx, tx)

	var status SessionStatus
	var lastHeartbeatAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT status, last_heartbeat_at FROM worker_sessions
		WHERE id = $1
		FOR UPDATE`, id).Scan(&status, &lastHeartbeatAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock stale session candidate: %w", err)
	}

	// Resampled after the lock: a heartbeat that committed while this transaction
	// waited must be able to save its session, and transaction-start now() could
	// not see it.
	var serverNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&serverNow); err != nil {
		return false, fmt.Errorf("read stale session decision time: %w", err)
	}
	if !isCurrentSessionStatus(status) || !lastHeartbeatAt.Before(serverNow.Add(-staleAfter)) {
		return false, nil
	}

	tag, err := tx.Exec(ctx, `
		UPDATE worker_sessions
		SET status = 'UNHEALTHY', ended_at = $2
		WHERE id = $1 AND status IN ('STARTING', 'HEALTHY', 'DRAINING')`, id, serverNow)
	if err != nil {
		return false, fmt.Errorf("mark worker session unhealthy: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit stale session: %w", err)
	}
	return true, nil
}

func isCurrentSessionStatus(status SessionStatus) bool {
	return status == SessionStarting || status == SessionHealthy || status == SessionDraining
}

// ReconcileExpiredLeases turns every expired active lease into durable recovery.
//
// One transaction per lease does all of it: the lease becomes EXPIRED, its
// attempt becomes ABANDONED, capacity is released purely by the lease no longer
// being ACTIVE, and the job either returns to QUEUED with a brand-new
// work.available outbox event or, if that abandonment consumed its total attempt
// budget, becomes DEAD_LETTERED. Committing atomically is what makes a crash
// before commit leave the old state intact and a rerun repair it.
//
// An expired lease is reconciled even when its session still looks healthy. The
// worker deliberately leaves a lease active after a handler error while its
// process keeps heartbeating, so requiring both a stale session and an expired
// lease would strand exactly the case this milestone exists to recover.
//
// Idempotency: the second reconciler to reach a lease finds it no longer ACTIVE
// under the row lock and skips it, so there is never a second abandonment, a
// second capacity release, or a duplicate recovery event.
//
// The recovery event gets a fresh id on purpose. The original outbox event id is
// the globally unique claim identity and has already been consumed
// (docs/adr/0007-globally-idempotent-notification-claims.md); reusing it would
// make the replacement claim collide with the attempt that was just abandoned.
func (s *Store) ReconcileExpiredLeases(ctx context.Context, limit int) (_ ReconcileStats, err error) {
	defer func() { err = classifyDatabaseError(err) }()

	if limit < 1 {
		return ReconcileStats{}, fmt.Errorf("scan limit must be positive")
	}

	// Candidate discovery only, and deliberately not under a lease lock: taking a
	// lease lock here and then acquiring queue and session locks per candidate
	// would invert the established order. Matches leases_active_expiry_idx.
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM leases
		WHERE status = 'ACTIVE' AND expires_at <= clock_timestamp()
		ORDER BY expires_at, id
		LIMIT $1`, limit)
	if err != nil {
		return ReconcileStats{}, fmt.Errorf("scan expired leases: %w", err)
	}
	candidates, err := collectIDs(rows)
	if err != nil {
		return ReconcileStats{}, err
	}

	var stats ReconcileStats
	for _, id := range candidates {
		if err := s.reconcileExpiredLease(ctx, id, &stats); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func (s *Store) reconcileExpiredLease(ctx context.Context, leaseID uuid.UUID, stats *ReconcileStats) error {
	// Immutable routing columns only, read without a lock on purpose: taking a
	// lease lock here and then reaching for queue and session locks would invert
	// the established order. Every mutable field is re-read under the locks.
	var fence Fence
	var scope string
	err := s.pool.QueryRow(ctx, `
		SELECT job_id, attempt_id, id, worker_id, worker_session_id, scope
		FROM leases WHERE id = $1`, leaseID,
	).Scan(&fence.JobID, &fence.AttemptID, &fence.LeaseID,
		&fence.WorkerID, &fence.SessionID, &scope)
	if errors.Is(err, pgx.ErrNoRows) {
		stats.Skipped++
		return nil
	}
	if err != nil {
		return fmt.Errorf("route lease reconciliation: %w", err)
	}

	// The session row is locked but its status is NOT a precondition. An expired
	// lease is recoverable whether or not its session is still heartbeating; the
	// lock exists so reconciliation serializes against registration, heartbeat,
	// claim, and fenced transitions on the same row.
	tx, state, err := s.lockFenceForReconciliation(ctx, scope, fence)
	if err != nil {
		if errors.Is(err, ErrFenceRejected) {
			stats.Skipped++
			return nil
		}
		return err
	}
	defer rollback(ctx, tx)

	// Revalidated against the post-lock sample. A renewal that committed while
	// this transaction waited moved the expiry forward, and transaction-start
	// now() would not have seen it.
	if state.leaseStatus != LeaseActive || state.serverNow.Before(state.expiresAt) {
		stats.Skipped++
		return nil
	}
	if !isExecutingAttemptStatus(state.attemptStatus) {
		// A committed outcome or another reconciler already resolved this attempt.
		stats.Skipped++
		return nil
	}

	// Precedence, and the order matters.
	//
	// 1. CANCEL_REQUESTED first. Cancellation already won durably; the only thing
	//    left is to finalize it, and calling that an abandonment would both lose
	//    the operator's decision and wrongly consume attempt budget.
	// 2. A due persisted deadline next. A lease that lapsed around a deadline
	//    that had already passed is a TIMEOUT, not an abandonment. Getting this
	//    wrong is not cosmetic: ABANDONED would requeue with no backoff and
	//    record no failure detail, so a genuinely timing-out job would loop
	//    through its whole budget at full speed.
	// 3. Otherwise M3's abandonment path, unchanged (ADR-0009).
	switch {
	case state.jobStatus == "CANCEL_REQUESTED":
		// No outcome identity: nobody requested this, and reconciliation's
		// idempotency comes from the attempt no longer being LEASED or RUNNING.
		if _, err := s.finalizeCancellation(ctx, tx, state, fence, nil, LeaseExpired); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit cancellation finalization: %w", err)
		}
		stats.ExpiredLeases++
		stats.CanceledAttempts++
		return nil

	case !isExecutingJobStatus(state.jobStatus):
		stats.Skipped++
		return nil

	case state.timedOut():
		if err := s.finalizeTimeout(ctx, tx, state, fence, stats); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit expired-lease timeout: %w", err)
		}
		stats.ExpiredLeases++
		stats.TimedOutAttempts++
		return nil
	}

	// ADR-0009, unchanged by M4: an abandoned attempt consumes the attempt
	// budget, and recovery while budget remains is IMMEDIATE requeue — no
	// backoff, no jitter, no RETRY_WAIT, no failure classification of the job.
	// The work was interrupted, not judged. Only the budget arithmetic is shared
	// with retry, through lifecycle.Decide, which returns a zero delay for this
	// class precisely so the two cannot drift.
	decision, err := s.retryPolicy.Decide(
		lifecycle.ClassAbandoned, state.attemptNumber, state.attemptNumber, state.maxAttempts, s.jitter)
	if err != nil {
		return fmt.Errorf("decide abandonment recovery: %w", err)
	}
	result, err := s.finalizeAttempt(ctx, tx, state, fence, attemptOutcome{
		status:       AttemptAbandoned,
		class:        lifecycle.ClassAbandoned,
		errorCode:    lifecycle.CodeAbandoned,
		errorMessage: lifecycle.MessageAbandoned,
		leaseStatus:  LeaseExpired,
	}, decision)
	if err != nil {
		return err
	}

	// Counted only after the durable commit, so a reported repair is always a
	// repair that actually happened (AGENTS.md section 9).
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit lease reconciliation: %w", err)
	}
	stats.ExpiredLeases++
	if result.JobStatus == "QUEUED" {
		stats.RequeuedJobs++
	} else {
		stats.DeadLetteredJobs++
	}
	return nil
}

func collectIDs(rows pgx.Rows) ([]uuid.UUID, error) {
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan reconciliation candidate: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reconciliation candidates: %w", err)
	}
	return ids, nil
}
