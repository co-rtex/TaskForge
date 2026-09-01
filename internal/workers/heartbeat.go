package workers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Heartbeat records that one process session is still alive, using PostgreSQL
// receipt time.
//
// The request carries no timestamp on purpose: reliability invariant 18 says
// worker-supplied time is never authoritative for staleness, so the only clock
// that can move last_heartbeat_at is the database's own, sampled after the
// session row is locked.
//
// Locking: this takes the worker-session row and nothing else, which is a prefix
// of the established queue -> worker session -> job -> attempt -> lease order, so
// it cannot deadlock against claim, renewal, a fenced transition, or
// reconciliation. Registration's fencing UPDATE contends on the same row, so a
// heartbeat that waits across a process replacement observes the replacement
// rather than reviving what it replaced.
//
// Idempotency: repeating a heartbeat is harmless. It may advance the timestamp
// again — that is the point of a heartbeat — but it can never create a session
// and never returns a fenced one to health. A caller that saw an ambiguous
// response may safely send the identical request again.
func (s *Store) Heartbeat(ctx context.Context, scope string, req HeartbeatRequest) (_ HeartbeatResult, err error) {
	defer func() { err = classifyDatabaseError(err) }()

	if err := ValidateHeartbeat(req); err != nil {
		return HeartbeatResult{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return HeartbeatResult{}, fmt.Errorf("begin worker heartbeat: %w", err)
	}
	defer rollback(ctx, tx)

	var status SessionStatus
	err = tx.QueryRow(ctx, `
		SELECT status FROM worker_sessions
		WHERE id = $1 AND worker_id = $2 AND scope = $3
		FOR UPDATE`, req.SessionID, req.WorkerID, scope).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return HeartbeatResult{}, ErrSessionUnavailable
	}
	if err != nil {
		return HeartbeatResult{}, fmt.Errorf("lock worker session for heartbeat: %w", err)
	}
	// A replaced, unhealthy, or ended session stays where it is. Reviving it here
	// would let a process that already lost authority reclaim it just by being
	// slow, and would break the one-current-session-per-worker invariant.
	if status != SessionHealthy {
		return HeartbeatResult{}, ErrSessionUnavailable
	}

	// Sampled only after the row lock, so a heartbeat that waited behind a
	// replacement or a reconciliation cannot stamp a time it observed earlier.
	var receivedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&receivedAt); err != nil {
		return HeartbeatResult{}, fmt.Errorf("read heartbeat receipt time: %w", err)
	}

	result := HeartbeatResult{SessionID: req.SessionID, Status: SessionHealthy}
	// GREATEST keeps the stored time monotonic. Two concurrent heartbeats for one
	// session serialize on the row lock, and the later sample wins regardless of
	// which transaction commits second.
	err = tx.QueryRow(ctx, `
		UPDATE worker_sessions
		SET last_heartbeat_at = GREATEST(last_heartbeat_at, $2)
		WHERE id = $1 AND status = 'HEALTHY'
		RETURNING last_heartbeat_at`, req.SessionID, receivedAt).Scan(&result.LastHeartbeatAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return HeartbeatResult{}, ErrSessionUnavailable
	}
	if err != nil {
		return HeartbeatResult{}, fmt.Errorf("record worker heartbeat: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return HeartbeatResult{}, fmt.Errorf("commit worker heartbeat: %w", err)
	}
	return result, nil
}

// RenewLease extends one lease's authority window without resurrecting anything.
//
// The operation is fenced by all five existing identifiers and additionally by a
// client-generated renewal identity plus the generation the caller expects to be
// current. That extra pair is what makes renewal safe to retry:
//
//   - an exact replay of the just-committed renewal returns the stored result and
//     moves nothing, so an ambiguous response cannot extend authority twice — but
//     only while the lease is still ACTIVE, unexpired, and backed by an executing
//     job and attempt. Current authority is validated before a replay is even
//     recognized;
//   - a delayed older generation, or a second distinct request for the same
//     generation, performs no mutation and returns ErrRenewalConflict;
//   - reusing a renewal identity that is currently recorded on a different lease
//     is a domain conflict, not a leaked uniqueness error. Only each lease's live
//     identity is retained, so an identity a lease has already superseded is
//     reusable by design; see ADR-0008's scope note.
//
// Renewal extends lease authority only. It never touches the job's overall
// timeout budget, and it never revives an expired, completed, released, or
// reconciled lease: PostgreSQL time is resampled after every authority lock, so
// a renewal that waited across the expiry boundary is rejected.
func (s *Store) RenewLease(ctx context.Context, scope string, req RenewalRequest) (_ RenewalResult, err error) {
	defer func() { err = classifyDatabaseError(err) }()

	if err := ValidateRenewal(req); err != nil {
		return RenewalResult{}, err
	}
	if s.leaseDuration <= 0 {
		return RenewalResult{}, fmt.Errorf("lease duration must be positive")
	}

	// lockFence takes queue -> worker session -> job -> attempt -> lease in the
	// established order and samples clock_timestamp() only afterwards.
	tx, state, err := s.lockFence(ctx, scope, req.Fence)
	if err != nil {
		return RenewalResult{}, err
	}
	defer rollback(ctx, tx)

	// Current authority is validated FIRST, before any replay is recognized.
	//
	// Order matters here and used to be wrong. Recognizing a replay before these
	// checks let a lease that had been renewed once and then completed, expired,
	// or reconciled answer a replayed renewal with 200 and a positive remaining
	// window — authority for a lease that no longer exists. ADR-0008 says renewal
	// never resurrects an expired, completed, released, or reconciled lease, and
	// that has to bind the replay path exactly as it binds a first attempt.
	if state.leaseStatus != LeaseActive || !state.serverNow.Before(state.expiresAt) {
		return RenewalResult{}, ErrLeaseExpired
	}
	// Renewal authorizes continued execution, so the attempt must still be the
	// one executing. Both LEASED and RUNNING are accepted because a renewal may
	// legitimately race the start transition.
	if !isExecutingJobStatus(state.jobStatus) || !isExecutingAttemptStatus(state.attemptStatus) {
		return RenewalResult{}, ErrStateConflict
	}

	// An exact replay of the renewal that produced the current generation, on a
	// lease that is still live. The stored window is returned unchanged; remaining
	// is measured from the fresh server sample, so a replay never reports more
	// time than actually remains.
	if state.lastRenewalRequestID != nil && *state.lastRenewalRequestID == req.RenewalRequestID {
		if state.renewalVersion != req.ExpectedVersion+1 {
			return RenewalResult{}, ErrRenewalConflict
		}
		result := RenewalResult{
			LeaseID:        req.Fence.LeaseID,
			RenewalVersion: state.renewalVersion,
			ExpiresAt:      state.expiresAt,
			Remaining:      remainingUntil(state.expiresAt, state.serverNow),
			Replayed:       true,
		}
		if err := tx.Commit(ctx); err != nil {
			return RenewalResult{}, fmt.Errorf("commit replayed lease renewal: %w", err)
		}
		return result, nil
	}
	// Reusing a renewal identity that is currently recorded on a different lease
	// is a caller error with a deterministic answer. Checking it under the same
	// locks turns what would otherwise surface as a raw unique-violation into a
	// stable domain conflict.
	//
	// Only the current generation's identity is retained per lease, so this
	// rejects reuse of a live identity, not of one a lease has already superseded.
	// See ADR-0008's scope note for why the narrower guarantee is sufficient.
	var identityOwner uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT id FROM leases WHERE last_renewal_request_id = $1`, req.RenewalRequestID).Scan(&identityOwner)
	switch {
	case err == nil && identityOwner != req.Fence.LeaseID:
		return RenewalResult{}, ErrRenewalConflict
	case err != nil && !errors.Is(err, pgx.ErrNoRows):
		return RenewalResult{}, fmt.Errorf("read renewal identity owner: %w", err)
	}

	if state.renewalVersion != req.ExpectedVersion {
		return RenewalResult{}, ErrRenewalConflict
	}

	result := RenewalResult{LeaseID: req.Fence.LeaseID}
	err = tx.QueryRow(ctx, `
		UPDATE leases
		SET renewed_at = $2,
		    expires_at = $3,
		    renewal_version = renewal_version + 1,
		    last_renewal_request_id = $4
		WHERE id = $1 AND status = 'ACTIVE' AND renewal_version = $5
		RETURNING renewal_version, expires_at`,
		req.Fence.LeaseID, state.serverNow, state.serverNow.Add(s.leaseDuration),
		req.RenewalRequestID, req.ExpectedVersion,
	).Scan(&result.RenewalVersion, &result.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// The row was locked and revalidated above, so this can only mean another
		// transaction changed it between the two statements in a way the predicate
		// rejects. Report the same stable conflict rather than a raw failure.
		return RenewalResult{}, ErrRenewalConflict
	}
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "23505" {
			return RenewalResult{}, ErrRenewalConflict
		}
		return RenewalResult{}, fmt.Errorf("renew lease: %w", err)
	}
	result.Remaining = remainingUntil(result.ExpiresAt, state.serverNow)

	if err := tx.Commit(ctx); err != nil {
		return RenewalResult{}, fmt.Errorf("commit lease renewal: %w", err)
	}
	return result, nil
}

// isExecutingJobStatus reports the job states in which an attempt may still be
// executing. It is deliberately a closed list rather than a "not terminal" test.
func isExecutingJobStatus(status string) bool {
	return status == "LEASED" || status == "RUNNING"
}

func isExecutingAttemptStatus(status AttemptStatus) bool {
	return status == AttemptLeased || status == AttemptRunning
}

func remainingUntil(expiresAt, serverNow time.Time) time.Duration {
	remaining := expiresAt.Sub(serverNow)
	if remaining < 0 {
		return 0
	}
	return remaining
}
