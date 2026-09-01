package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrJobNotCancelable reports that the job reached a terminal outcome before
// cancellation arrived. It is a stable conflict rather than a silent success:
// telling a caller "canceled" about a job that already succeeded would be a lie
// with consequences, since terminal states never change.
var ErrJobNotCancelable = errors.New("job is already terminal and cannot be canceled")

// CancelResult is the committed public cancellation decision.
type CancelResult struct {
	JobID uuid.UUID
	// Status after the transition: CANCELED when cancellation was terminal
	// immediately, or CANCEL_REQUESTED while an attempt still holds authority
	// that has to be withdrawn cooperatively or by reconciliation.
	Status Status
	// CancelRequestedAt is the PostgreSQL instant cancellation won, sampled after
	// the authority locks.
	CancelRequestedAt time.Time
	// AlreadyRequested is true when this job was already cancelling or canceled.
	AlreadyRequested bool
}

// RequestCancel durably cancels one job, or begins withdrawing authority from
// the attempt that is executing it.
//
// Identity is scope plus job id, and nothing else. A cancellation request id
// would add no safety: cancelling twice is not two cancellations, it is the same
// decision observed twice, and the job's own state is what makes the second call
// a no-op. That is why this operation is idempotent without carrying an identity
// the way failure reporting must.
//
// Two shapes, decided under the lock:
//
//   - PENDING, QUEUED, or RETRY_WAIT — no attempt exists and no lease can
//     commit, so the job goes straight to terminal CANCELED and NO attempt is
//     created. Any advisory notification already on the broker stays harmless:
//     the claim predicate will simply find no QUEUED job.
//   - LEASED or RUNNING — an attempt holds authority that could still commit a
//     success, so the job goes to CANCEL_REQUESTED instead. Attempt and lease
//     history are untouched at this point. What changes immediately is that
//     start, success, failure, and renewal all stop committing, because none of
//     them accepts CANCEL_REQUESTED as an executing state. The attempt is
//     finalized either by the worker acknowledging cooperatively or by
//     reconciliation once the lease lapses.
//
// Locking is queue -> job, a subsequence of the established
// queue -> worker session -> job -> attempt -> lease order. Taking the queue
// row first is what keeps this from deadlocking against a fenced transition that
// already holds it, and the job row is the one every competing decision
// contends on, which is what makes cancel-versus-success resolve to exactly one
// winner.
func (s *Store) RequestCancel(ctx context.Context, scope string, jobID uuid.UUID) (_ CancelResult, err error) {
	defer func() { err = classifyDatabaseError(err) }()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return CancelResult{}, fmt.Errorf("begin cancellation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	// Immutable routing hint only: a job's queue is fixed at submission. The
	// authoritative status is re-read under the locks below.
	var queue string
	err = tx.QueryRow(ctx,
		`SELECT queue FROM jobs WHERE id = $1 AND scope = $2`, jobID, scope).Scan(&queue)
	if errors.Is(err, pgx.ErrNoRows) {
		return CancelResult{}, ErrJobNotFound
	}
	if err != nil {
		return CancelResult{}, fmt.Errorf("route cancellation: %w", err)
	}
	if err := tx.QueryRow(ctx,
		`SELECT name FROM queues WHERE name = $1 FOR UPDATE`, queue).Scan(&queue); err != nil {
		return CancelResult{}, fmt.Errorf("lock queue for cancellation: %w", err)
	}

	var status Status
	var cancelRequestedAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT status, cancel_requested_at FROM jobs
		WHERE id = $1 AND scope = $2
		FOR UPDATE`, jobID, scope).Scan(&status, &cancelRequestedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return CancelResult{}, ErrJobNotFound
	}
	if err != nil {
		return CancelResult{}, fmt.Errorf("lock job for cancellation: %w", err)
	}

	// Sampled only after both locks, so a cancellation that waited behind a
	// success, a promotion, or another cancellation stamps the instant it
	// actually won rather than the one it hoped for.
	var serverNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&serverNow); err != nil {
		return CancelResult{}, fmt.Errorf("read cancellation time: %w", err)
	}

	if status == StatusCancelRequested || status == StatusCanceled {
		result := CancelResult{JobID: jobID, Status: status, AlreadyRequested: true}
		if cancelRequestedAt != nil {
			result.CancelRequestedAt = *cancelRequestedAt
		}
		if err := tx.Commit(ctx); err != nil {
			return CancelResult{}, fmt.Errorf("commit duplicate cancellation: %w", err)
		}
		return result, nil
	}

	var target Status
	switch status {
	case StatusPending, StatusQueued, StatusRetryWait:
		target = StatusCanceled
	case StatusLeased, StatusRunning:
		target = StatusCancelRequested
	default:
		// SUCCEEDED or DEAD_LETTERED. A terminal job never returns to a
		// non-terminal state, and it never changes which terminal state it is in.
		return CancelResult{}, ErrJobNotCancelable
	}

	tag, err := tx.Exec(ctx, `
		UPDATE jobs
		SET status = $3, cancel_requested_at = $4, updated_at = $4
		WHERE id = $1 AND scope = $2 AND status = $5`,
		jobID, scope, string(target), serverNow, string(status))
	if err != nil {
		return CancelResult{}, fmt.Errorf("cancel job: %w", err)
	}
	if tag.RowsAffected() != 1 {
		// The row was locked and revalidated above, so this can only mean another
		// transaction changed it between the two statements in a way the
		// predicate rejects. Report the same stable conflict rather than a raw
		// failure.
		return CancelResult{}, ErrJobNotCancelable
	}

	if err := tx.Commit(ctx); err != nil {
		return CancelResult{}, fmt.Errorf("commit cancellation: %w", err)
	}
	return CancelResult{JobID: jobID, Status: target, CancelRequestedAt: serverNow}, nil
}
