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

	// Cancellation rides the heartbeat rather than a work notification, and it is
	// read in the same transaction that just proved this session is current. That
	// pairing matters: a directive handed to a session the control plane has
	// already fenced would be advice to a process whose outcome can no longer
	// commit anyway.
	//
	// Reading it here needs no additional lock. The directives are advisory — the
	// durable decision committed when the job reached CANCEL_REQUESTED — so a
	// directive that is one transaction stale simply arrives on the next tick,
	// and one that arrives for an attempt already finalized is ignored by the
	// worker's registry.
	result.Cancellations, err = cancellationDirectives(ctx, tx, req.SessionID, maxCancellationDirectives)
	if err != nil {
		return HeartbeatResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return HeartbeatResult{}, fmt.Errorf("commit worker heartbeat: %w", err)
	}
	return result, nil
}

// maxCancellationDirectives bounds one heartbeat response.
//
// A worker's concurrency limit is at most 256, so it can hold at most 256
// cancellable attempts and this cap can never truncate a real backlog. It exists
// so the response stays bounded even if a future change makes that untrue.
const maxCancellationDirectives = 256

// cancellationDirectives lists the attempts this session may still be executing
// whose jobs have been canceled.
//
// The lease join is not decoration. An attempt whose lease is no longer ACTIVE
// has already lost authority, so telling its worker to cancel cooperatively
// would be pointless — reconciliation owns that case and finalizes it without
// the worker's help.
func cancellationDirectives(
	ctx context.Context,
	tx pgx.Tx,
	sessionID uuid.UUID,
	limit int,
) ([]CancellationDirective, error) {
	// Matches job_attempts_session_executing_idx.
	rows, err := tx.Query(ctx, `
		SELECT a.job_id, a.id, l.id, j.cancel_requested_at
		FROM job_attempts a
		JOIN jobs j ON j.id = a.job_id
		JOIN leases l ON l.attempt_id = a.id
		WHERE a.worker_session_id = $1
		  AND a.status IN ('LEASED', 'RUNNING')
		  AND j.status = 'CANCEL_REQUESTED'
		  AND l.status = 'ACTIVE'
		ORDER BY j.cancel_requested_at, a.id
		LIMIT $2`, sessionID, limit)
	if err != nil {
		return nil, fmt.Errorf("read cancellation directives: %w", err)
	}
	defer rows.Close()

	var directives []CancellationDirective
	for rows.Next() {
		var directive CancellationDirective
		if err := rows.Scan(&directive.JobID, &directive.AttemptID,
			&directive.LeaseID, &directive.CancelRequestedAt); err != nil {
			return nil, fmt.Errorf("scan cancellation directive: %w", err)
		}
		directives = append(directives, directive)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cancellation directives: %w", err)
	}
	return directives, nil
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
	if !state.leaseUsable() {
		return RenewalResult{}, ErrLeaseExpired
	}
	// Renewal extends lease authority, and lease authority only exists to let
	// work that is still allowed to run keep running. Once the attempt's
	// persisted execution deadline has passed, nothing this attempt does can
	// commit, so extending its window would only delay the reconciler's timeout
	// while the handler kept burning resources under an authority that can never
	// be used.
	//
	// This is also where the M3 invariant is enforced from the other side:
	// renewal never resets timeout_at, so a deadline that has arrived stays
	// arrived no matter how many renewals succeeded before it.
	if state.timedOut() {
		return RenewalResult{}, ErrAttemptTimedOut
	}
	// Renewal authorizes continued execution, so the attempt must still be the
	// one executing. Both LEASED and RUNNING are accepted because a renewal may
	// legitimately race the start transition. CANCEL_REQUESTED is deliberately
	// not executing: once cancellation has durably won, refusing renewal is what
	// makes the lease lapse so reconciliation can finalize an uncooperative
	// worker's attempt.
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
