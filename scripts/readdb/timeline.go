package readdb

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// This file holds the query `go run ./scripts/bench recovery-probe` splits a
// recovery with (M8D3). It is not a benchmark measurement: the recorded runs never
// call it, and the recovery figure ADR-0020 defines is still Recoveries'. It reads
// every instant between a kill and the replacement's claim that PostgreSQL holds,
// so that a slow recovery can be put in a named segment instead of guessed at.

// RecoveryHop is one attempt a killed session abandoned, and every PostgreSQL
// instant on the way to the attempt that replaced it. A pointer is nil when the
// row it comes from does not exist yet (or never will: a job whose budget ran
// out has no replacement).
type RecoveryHop struct {
	JobID            uuid.UUID
	AbandonedAttempt int

	// The abandoned attempt's lease. ReleasedAt is when the reconciler expired it,
	// in the same transaction that abandoned the attempt and requeued the job.
	LeaseAcquiredAt time.Time
	LeaseRenewedAt  time.Time
	LeaseExpiresAt  time.Time
	LeaseReleasedAt *time.Time

	// The recovery outbox event: the work.available event the abandonment's
	// transaction wrote for this job. See RecoveryTimeline for how it is found.
	EventID          *uuid.UUID
	EventCreatedAt   *time.Time
	EventPublishedAt *time.Time
	EventAttempts    *int
	EventLastError   *string
	// EventClaimedJobID is the job whose claim the recovery event's notification
	// produced (the lease whose claim_request_id is the event), if one did. A claim
	// takes the queue's oldest eligible job, not the job a notification names
	// (ADR-0003), so it need not be JobID.
	EventClaimedJobID *uuid.UUID

	// The replacement: the job's next attempt, and the claim that created it.
	ReplacementCreatedAt *time.Time
	ReplacementSessionID *uuid.UUID
	// ReplacementEventID is the replacement lease's claim_request_id: the outbox
	// event whose notification produced the replacement claim, with its job and
	// its two instants. It is EventID when the recovery event's own notification
	// claimed this job.
	ReplacementEventID          *uuid.UUID
	ReplacementEventJobID       *uuid.UUID
	ReplacementEventCreatedAt   *time.Time
	ReplacementEventPublishedAt *time.Time
}

// RecoveryTimeline returns, for each attempt the session abandoned, its lease,
// its recovery outbox event and its replacement, ordered as Recoveries orders
// them.
//
// How the recovery event is found, without guessing. The schema does not link an
// outbox event to the attempt whose abandonment wrote it; it links it to the job
// (outbox_events.job_id). The abandonment is one transaction
// (internal/workers/reconcile.go, ReconcileExpiredLeases): the reconciler's scan
// sees the lease expired (expires_at <= clock_timestamp()) and only then begins
// the transaction, so the event's created_at, which is that transaction's now(),
// is at or after the lease's expires_at; and the lease's released_at is a
// clock_timestamp() sampled inside the same transaction after its row locks, so
// it is at or after the event's created_at. The recovery event is therefore the
// work.available event for the job with expires_at <= created_at <= released_at.
// The job's earlier events (its submission, an earlier recovery) were created
// before this lease was acquired, and a later one after it was released, so
// neither can fall in the window. The two instants are NOT equal, which is why
// the rule is a window and not an equality: released_at is sampled after the
// locks and created_at is the transaction's start.
//
// If more than one event falls in a lease's window the function returns an
// error rather than pick one. While the lease is still active, or the attempt was
// resolved some other way, no event is matched and the pointers are nil.
func RecoveryTimeline(ctx context.Context, q Querier, scope string, sessionID uuid.UUID) ([]RecoveryHop, error) {
	rows, err := q.Query(ctx, `
		SELECT ab.job_id, ab.attempt_number,
		       l.acquired_at, l.renewed_at, l.expires_at, l.released_at,
		       ev.id, ev.created_at, ev.published_at, ev.attempts, ev.last_error,
		       COALESCE(ev.matches, 0), el.job_id,
		       rep.created_at, rep.worker_session_id,
		       rl.claim_request_id, re.job_id, re.created_at, re.published_at
		FROM job_attempts ab
		JOIN leases l ON l.attempt_id = ab.id
		LEFT JOIN LATERAL (
		    SELECT e.id, e.created_at, e.published_at, e.attempts, e.last_error,
		           count(*) OVER () AS matches
		    FROM outbox_events e
		    WHERE e.job_id = ab.job_id
		      AND e.event_type = 'work.available'
		      AND e.created_at >= l.expires_at
		      AND e.created_at <= l.released_at
		    ORDER BY e.created_at, e.id
		    LIMIT 1
		) ev ON true
		LEFT JOIN leases el ON el.claim_request_id = ev.id
		LEFT JOIN job_attempts rep
		       ON rep.job_id = ab.job_id AND rep.attempt_number = ab.attempt_number + 1
		LEFT JOIN leases rl ON rl.attempt_id = rep.id
		LEFT JOIN outbox_events re ON re.id = rl.claim_request_id
		WHERE ab.scope = $1 AND ab.worker_session_id = $2 AND ab.status = 'ABANDONED'
		ORDER BY ab.job_id, ab.attempt_number`, scope, sessionID)
	if err != nil {
		return nil, fmt.Errorf("read recovery timelines: %w", err)
	}
	defer rows.Close()
	var out []RecoveryHop
	for rows.Next() {
		var h RecoveryHop
		var matches int
		if err := rows.Scan(&h.JobID, &h.AbandonedAttempt,
			&h.LeaseAcquiredAt, &h.LeaseRenewedAt, &h.LeaseExpiresAt, &h.LeaseReleasedAt,
			&h.EventID, &h.EventCreatedAt, &h.EventPublishedAt, &h.EventAttempts, &h.EventLastError,
			&matches, &h.EventClaimedJobID,
			&h.ReplacementCreatedAt, &h.ReplacementSessionID,
			&h.ReplacementEventID, &h.ReplacementEventJobID,
			&h.ReplacementEventCreatedAt, &h.ReplacementEventPublishedAt); err != nil {
			return nil, fmt.Errorf("scan a recovery timeline: %w", err)
		}
		if matches > 1 {
			return nil, fmt.Errorf("job %s attempt %d: %d work.available events fall inside its lease's expiry-to-release window; refusing to choose one",
				h.JobID, h.AbandonedAttempt, matches)
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// Segments is a recovery split at the instants PostgreSQL records, named as
// docs/CURRENT_STATE.md's M8D3 section names them:
//
//	S1  lease expires_at - kill                     the lease running out
//	S2  lease released_at - lease expires_at        the reconciler
//	S3  event created_at - lease released_at        inside one transaction (<= 0)
//	S4  event published_at - event created_at       the outbox publisher
//	S5  replacement's claim - event published_at    broker delivery and the claim
//
// They telescope, so their sum is the replacement's claim minus the kill: the
// recovery figure ADR-0020 defines.
type Segments struct{ S1, S2, S3, S4, S5 time.Duration }

// Sum is S1 + S2 + S3 + S4 + S5.
func (s Segments) Sum() time.Duration { return s.S1 + s.S2 + s.S3 + s.S4 + s.S5 }

// Segments splits the hop's recovery from the given kill. The second result is
// false while any instant it needs is missing: the lease not yet released, the
// event not yet published, or no replacement yet.
func (h RecoveryHop) Segments(kill time.Time) (Segments, bool) {
	if h.LeaseReleasedAt == nil || h.EventCreatedAt == nil || h.EventPublishedAt == nil || h.ReplacementCreatedAt == nil {
		return Segments{}, false
	}
	return Segments{
		S1: h.LeaseExpiresAt.Sub(kill),
		S2: h.LeaseReleasedAt.Sub(h.LeaseExpiresAt),
		S3: h.EventCreatedAt.Sub(*h.LeaseReleasedAt),
		S4: h.EventPublishedAt.Sub(*h.EventCreatedAt),
		S5: h.ReplacementCreatedAt.Sub(*h.EventPublishedAt),
	}, true
}
