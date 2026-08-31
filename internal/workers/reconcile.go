package workers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/co-rtex/TaskForge/internal/outbox"
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

// leaseBinding is a lease's immutable routing columns. They are set once at
// claim and never change, so reading them without a lock is a routing hint, not
// an authority decision; every mutable field is re-read under locks below.
type leaseBinding struct {
	jobID     uuid.UUID
	attemptID uuid.UUID
	scope     string
	queue     string
	workerID  uuid.UUID
	sessionID uuid.UUID
}

func (s *Store) reconcileExpiredLease(ctx context.Context, leaseID uuid.UUID, stats *ReconcileStats) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin lease reconciliation: %w", err)
	}
	defer rollback(ctx, tx)

	var binding leaseBinding
	err = tx.QueryRow(ctx, `
		SELECT job_id, attempt_id, scope, queue, worker_id, worker_session_id
		FROM leases WHERE id = $1`, leaseID,
	).Scan(&binding.jobID, &binding.attemptID, &binding.scope, &binding.queue,
		&binding.workerID, &binding.sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		stats.Skipped++
		return nil
	}
	if err != nil {
		return fmt.Errorf("route lease reconciliation: %w", err)
	}

	// Established order: queue -> worker session -> job -> attempt -> lease.
	var queue string
	if err := tx.QueryRow(ctx,
		`SELECT name FROM queues WHERE name = $1 FOR UPDATE`, binding.queue).Scan(&queue); err != nil {
		return fmt.Errorf("lock queue for lease reconciliation: %w", err)
	}
	// The session row is locked but its status is NOT a precondition. An expired
	// lease is recoverable whether or not its session is still heartbeating; the
	// lock exists so reconciliation serializes against registration, heartbeat,
	// claim, and fenced transitions on the same row.
	var sessionStatus SessionStatus
	err = tx.QueryRow(ctx, `
		SELECT status FROM worker_sessions
		WHERE id = $1 AND worker_id = $2 AND scope = $3
		FOR UPDATE`, binding.sessionID, binding.workerID, binding.scope).Scan(&sessionStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		stats.Skipped++
		return nil
	}
	if err != nil {
		return fmt.Errorf("lock worker session for lease reconciliation: %w", err)
	}

	var (
		jobStatus     string
		maxAttempts   int
		attemptStatus AttemptStatus
		leaseStatus   LeaseStatus
		expiresAt     time.Time
	)
	err = tx.QueryRow(ctx, `
		SELECT j.status, j.max_attempts, a.status, l.status, l.expires_at
		FROM jobs j
		JOIN job_attempts a ON a.job_id = j.id
		JOIN leases l ON l.attempt_id = a.id
		WHERE j.id = $1 AND a.id = $2 AND l.id = $3 AND j.scope = $4
		FOR UPDATE OF j, a, l`,
		binding.jobID, binding.attemptID, leaseID, binding.scope,
	).Scan(&jobStatus, &maxAttempts, &attemptStatus, &leaseStatus, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		stats.Skipped++
		return nil
	}
	if err != nil {
		return fmt.Errorf("lock lease reconciliation state: %w", err)
	}

	// Resampled after every authority lock. A renewal that committed while this
	// transaction waited moved the expiry forward, and transaction-start now()
	// would not see it — this is the sample that makes the skip below correct.
	var serverNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&serverNow); err != nil {
		return fmt.Errorf("read lease reconciliation time: %w", err)
	}
	if leaseStatus != LeaseActive || serverNow.Before(expiresAt) {
		stats.Skipped++
		return nil
	}
	// A committed success or another reconciler already resolved this attempt.
	if !isExecutingJobStatus(jobStatus) || !isExecutingAttemptStatus(attemptStatus) {
		stats.Skipped++
		return nil
	}

	// Capacity is released solely by the lease ceasing to be ACTIVE. There is no
	// counter to decrement, so it cannot drift or go negative.
	if tag, err := tx.Exec(ctx, `
		UPDATE leases SET status = 'EXPIRED', released_at = $2
		WHERE id = $1 AND status = 'ACTIVE'`, leaseID, serverNow); err != nil {
		return fmt.Errorf("expire lease: %w", err)
	} else if tag.RowsAffected() != 1 {
		stats.Skipped++
		return nil
	}
	// Works from LEASED (never started) and from RUNNING alike: the attempt
	// timeline constraint requires only a finish time for ABANDONED.
	if tag, err := tx.Exec(ctx, `
		UPDATE job_attempts SET status = 'ABANDONED', finished_at = $2
		WHERE id = $1 AND status IN ('LEASED', 'RUNNING')`, binding.attemptID, serverNow); err != nil {
		return fmt.Errorf("abandon attempt: %w", err)
	} else if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: attempt changed during reconciliation", ErrStateConflict)
	}

	// max_attempts counts total attempts including the first, and an abandoned
	// attempt is an attempt. Counting after the abandonment commits in this same
	// transaction is what keeps this consistent with the claim predicate, which
	// refuses a job whose attempts already reach max_attempts.
	var attemptsUsed int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM job_attempts WHERE job_id = $1`, binding.jobID).Scan(&attemptsUsed); err != nil {
		return fmt.Errorf("count job attempts: %w", err)
	}

	requeued := attemptsUsed < maxAttempts
	if requeued {
		if tag, err := tx.Exec(ctx, `
			UPDATE jobs SET status = 'QUEUED', available_at = $2, updated_at = $2
			WHERE id = $1 AND status IN ('LEASED', 'RUNNING')`, binding.jobID, serverNow); err != nil {
			return fmt.Errorf("requeue recovered job: %w", err)
		} else if tag.RowsAffected() != 1 {
			return fmt.Errorf("%w: job changed during reconciliation", ErrStateConflict)
		}
		// Written in this transaction, so a recovered job is never durable without
		// the notification that wakes a replacement worker.
		if _, err := outbox.InsertTx(ctx, tx, outbox.EventWorkAvailable, outbox.WorkAvailableSchemaVersion,
			outbox.WorkAvailableData{Queue: binding.queue, JobID: binding.jobID.String()}); err != nil {
			return fmt.Errorf("record recovery notification: %w", err)
		}
	} else {
		// The abandonment consumed the total budget. Leaving the job LEASED or
		// RUNNING would strand it permanently, and requeueing it would produce a
		// QUEUED job the claim predicate can never take. This is the minimal
		// terminal consequence; failure classes, retry backoff, dlq_entries,
		// listing, and replay are M4.
		// See docs/adr/0009-abandoned-attempts-consume-the-attempt-budget.md.
		if tag, err := tx.Exec(ctx, `
			UPDATE jobs SET status = 'DEAD_LETTERED', updated_at = $2
			WHERE id = $1 AND status IN ('LEASED', 'RUNNING')`, binding.jobID, serverNow); err != nil {
			return fmt.Errorf("dead-letter exhausted job: %w", err)
		} else if tag.RowsAffected() != 1 {
			return fmt.Errorf("%w: job changed during reconciliation", ErrStateConflict)
		}
	}

	// Counted only after the durable commit, so a reported repair is always a
	// repair that actually happened (AGENTS.md section 9).
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit lease reconciliation: %w", err)
	}
	stats.ExpiredLeases++
	if requeued {
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
