package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/co-rtex/TaskForge/internal/outbox"
)

// SchedulerStats counts what one scheduler pass durably changed.
type SchedulerStats struct {
	// PromotedJobs counts jobs that newly became QUEUED, each with exactly one
	// fresh transactional notification.
	PromotedJobs int
	// Renotified counts stranded QUEUED jobs that received a replacement
	// notification because their previous one was never delivered or was lost.
	Renotified int
	// Skipped counts candidates that no longer qualified once their authority
	// rows were locked and PostgreSQL time was resampled — another replica got
	// there first, a cancellation won, or a pending event still exists.
	Skipped int
}

// Add accumulates one pass's counts into another's.
func (s *SchedulerStats) Add(other SchedulerStats) {
	s.PromotedJobs += other.PromotedJobs
	s.Renotified += other.Renotified
	s.Skipped += other.Skipped
}

// PromoteDueJobs moves due PENDING and RETRY_WAIT jobs to QUEUED and writes
// their notifications in the same transaction.
//
// Both sources go through one mechanism deliberately. A delayed submission and a
// retry-waiting job differ only in how available_at got its value; from the
// scheduler's side they are the same question — "is this job's durable
// eligibility time in the past" — and answering it twice in two places is how
// the two would eventually disagree.
//
// Safe with N replicas by construction. The scan carries no authority; the
// decision is re-made under queue -> job locks against a fresh post-lock
// clock_timestamp(), and the UPDATE names both the expected status and the
// expected notification generation, so the replica that arrives second finds a
// row its predicate no longer matches and promotes nothing.
func (s *Store) PromoteDueJobs(ctx context.Context, limit int) (SchedulerStats, error) {
	if limit < 1 {
		return SchedulerStats{}, fmt.Errorf("scan limit must be positive")
	}

	// Candidate discovery only, and deliberately unlocked: holding a job lock
	// while reaching for the queue lock would invert the established order.
	// Matches jobs_due_promotion_idx.
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM jobs
		WHERE status IN ('PENDING', 'RETRY_WAIT')
		  AND available_at <= clock_timestamp()
		ORDER BY available_at, id
		LIMIT $1`, limit)
	if err != nil {
		return SchedulerStats{}, fmt.Errorf("scan due jobs: %w", err)
	}
	candidates, err := collectJobIDs(rows)
	if err != nil {
		return SchedulerStats{}, err
	}

	var stats SchedulerStats
	for _, id := range candidates {
		promoted, err := s.promoteDueJob(ctx, id)
		if err != nil {
			return stats, err
		}
		if promoted {
			stats.PromotedJobs++
		} else {
			stats.Skipped++
		}
	}
	return stats, nil
}

func (s *Store) promoteDueJob(ctx context.Context, jobID uuid.UUID) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin job promotion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var queue string
	err = tx.QueryRow(ctx, `SELECT queue FROM jobs WHERE id = $1`, jobID).Scan(&queue)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("route job promotion: %w", err)
	}
	if err := tx.QueryRow(ctx,
		`SELECT name FROM queues WHERE name = $1 FOR UPDATE`, queue).Scan(&queue); err != nil {
		return false, fmt.Errorf("lock queue for promotion: %w", err)
	}

	var status Status
	var availableAt time.Time
	var generation int
	err = tx.QueryRow(ctx, `
		SELECT status, available_at, notification_generation
		FROM jobs WHERE id = $1
		FOR UPDATE`, jobID).Scan(&status, &availableAt, &generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock job for promotion: %w", err)
	}

	var serverNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&serverNow); err != nil {
		return false, fmt.Errorf("read promotion time: %w", err)
	}
	// Revalidated after the locks. A cancellation that committed while this
	// transaction waited moved the job out of the promotable set, and a
	// transaction-start now() could not have seen it.
	if status != StatusPending && status != StatusRetryWait {
		return false, nil
	}
	if serverNow.Before(availableAt) {
		return false, nil
	}

	// The expected status and generation are both in the predicate. That is what
	// makes a second replica's promotion of the same eligibility transition
	// impossible rather than merely unlikely: it would have to match a generation
	// the first replica has already moved past.
	var newGeneration int
	err = tx.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'QUEUED', updated_at = $2,
		    notification_generation = notification_generation + 1,
		    last_notification_at = $2
		WHERE id = $1 AND status = $3 AND notification_generation = $4
		RETURNING notification_generation`,
		jobID, serverNow, string(status), generation).Scan(&newGeneration)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("promote job: %w", err)
	}

	// Written in the same transaction as the promotion, so a job can never become
	// claimable without the notification that wakes a worker for it — and a
	// crash before commit leaves neither.
	if _, err := outbox.InsertWorkAvailableTx(ctx, tx, jobID, queue, newGeneration); err != nil {
		return false, fmt.Errorf("record promotion notification: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit job promotion: %w", err)
	}
	return true, nil
}

// RenotifyStrandedQueued creates a replacement notification for claimable jobs
// whose notification never reached a worker.
//
// The broker is advisory, which means correctness cannot depend on delivery —
// but reachability can still suffer: a QUEUED job whose only notification was
// lost sits claimable and unclaimed, because nothing wakes a worker to claim it.
// This is the bounded repair for that, and the bounds are the whole design:
//
//   - still QUEUED, revalidated under locks;
//   - the configured interval has elapsed since its last notification;
//   - no PENDING event exists for its CURRENT generation.
//
// That last condition is why events carry a generation. Checking only "is there
// a pending event for this job" would let a stale event left behind by the
// publish-before-mark window suppress the notification a NEW eligibility
// transition requires — the job would be freshly queued, and permanently
// unadvertised, because of an event belonging to an attempt that is already
// over. Checking the current generation asks the right question instead.
//
// The replacement gets a new event id but keeps the SAME generation: it
// advertises the same eligibility transition, not a new one. last_notification_at
// advances in the same statement, so the job is rate-limited again immediately
// and N replicas cannot multiply events for one stranded job.
func (s *Store) RenotifyStrandedQueued(ctx context.Context, after time.Duration, limit int) (SchedulerStats, error) {
	if after <= 0 {
		return SchedulerStats{}, fmt.Errorf("re-notification interval must be positive")
	}
	if limit < 1 {
		return SchedulerStats{}, fmt.Errorf("scan limit must be positive")
	}

	// Matches jobs_stranded_queued_idx.
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM jobs
		WHERE status = 'QUEUED'
		  AND last_notification_at < clock_timestamp() - make_interval(secs => $1::double precision)
		ORDER BY last_notification_at, id
		LIMIT $2`, after.Seconds(), limit)
	if err != nil {
		return SchedulerStats{}, fmt.Errorf("scan stranded queued jobs: %w", err)
	}
	candidates, err := collectJobIDs(rows)
	if err != nil {
		return SchedulerStats{}, err
	}

	var stats SchedulerStats
	for _, id := range candidates {
		renotified, err := s.renotifyStrandedJob(ctx, id, after)
		if err != nil {
			return stats, err
		}
		if renotified {
			stats.Renotified++
		} else {
			stats.Skipped++
		}
	}
	return stats, nil
}

func (s *Store) renotifyStrandedJob(ctx context.Context, jobID uuid.UUID, after time.Duration) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin re-notification: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var queue string
	err = tx.QueryRow(ctx, `SELECT queue FROM jobs WHERE id = $1`, jobID).Scan(&queue)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("route re-notification: %w", err)
	}
	if err := tx.QueryRow(ctx,
		`SELECT name FROM queues WHERE name = $1 FOR UPDATE`, queue).Scan(&queue); err != nil {
		return false, fmt.Errorf("lock queue for re-notification: %w", err)
	}

	var status Status
	var generation int
	var lastNotificationAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT status, notification_generation, last_notification_at
		FROM jobs WHERE id = $1
		FOR UPDATE`, jobID).Scan(&status, &generation, &lastNotificationAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock job for re-notification: %w", err)
	}

	var serverNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&serverNow); err != nil {
		return false, fmt.Errorf("read re-notification time: %w", err)
	}
	// A job that was claimed while this transaction waited is no longer stranded,
	// and re-notifying it would advertise work that is already being executed.
	if status != StatusQueued || generation < 1 || lastNotificationAt == nil {
		return false, nil
	}
	if !lastNotificationAt.Before(serverNow.Add(-after)) {
		return false, nil
	}

	pending, err := outbox.HasPendingWorkAvailableTx(ctx, tx, jobID, generation)
	if err != nil {
		return false, err
	}
	if pending {
		// The publisher simply has not got to it yet. Adding another event would
		// turn a slow publisher into a growing pile of duplicates.
		return false, nil
	}

	tag, err := tx.Exec(ctx, `
		UPDATE jobs SET last_notification_at = $2
		WHERE id = $1 AND status = 'QUEUED' AND notification_generation = $3`,
		jobID, serverNow, generation)
	if err != nil {
		return false, fmt.Errorf("stamp re-notification: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return false, nil
	}
	// Same generation, new event id. It advertises the same eligibility
	// transition; the id differs because the old one may already have been
	// consumed as a claim identity (ADR-0007).
	if _, err := outbox.InsertWorkAvailableTx(ctx, tx, jobID, queue, generation); err != nil {
		return false, fmt.Errorf("record re-notification: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit re-notification: %w", err)
	}
	return true, nil
}

func collectJobIDs(rows pgx.Rows) ([]uuid.UUID, error) {
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan scheduler candidate: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scheduler candidates: %w", err)
	}
	return ids, nil
}
