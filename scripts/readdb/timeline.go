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
// published_at is stamped by the publisher's own transaction after SendMessage
// returns, so it is an upper bound on the send: a worker can receive and claim
// before the mark commits, and S5 can then be a few milliseconds below zero.
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

// HeldBySession is the number of attempts a session holds a slot with right now:
// LEASED or RUNNING. Read just after a kill, it is how many attempts the kill will
// abandon, since a dead process can no longer finish any of them; zero means the
// kill missed.
func HeldBySession(ctx context.Context, q Querier, scope string, sessionID uuid.UUID) (int, error) {
	var n int
	err := q.QueryRow(ctx, `
		SELECT count(*) FROM job_attempts
		WHERE scope = $1 AND worker_session_id = $2 AND status IN ('LEASED', 'RUNNING')`,
		scope, sessionID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count the attempts session %s holds: %w", sessionID, err)
	}
	return n, nil
}

// Foreign is work outside one scope that a run's own services would act on.
// The reconciler, the outbox publisher and the scheduler scan the whole database,
// not one scope: an expired lease anywhere is requeued with a fresh notification,
// a pending event anywhere is published, and a QUEUED job anywhere is
// re-notified every TASKFORGE_SCHEDULER_RENOTIFY_AFTER. Each lands in the run's
// own broker queue, so any of it would put messages there that the run did not
// cause.
type Foreign struct {
	// NonTerminalJobs is jobs of other scopes in any status but SUCCEEDED,
	// DEAD_LETTERED or CANCELED.
	NonTerminalJobs int
	// PendingEvents is unpublished outbox events, of any scope but this one.
	PendingEvents int
}

// None reports whether there is nothing foreign to act on.
func (f Foreign) None() bool { return f.NonTerminalJobs == 0 && f.PendingEvents == 0 }

// ForeignActivity counts the work outside scope that the run's services would
// act on. An outbox event's scope is its job's; an event with no job is counted.
func ForeignActivity(ctx context.Context, q Querier, scope string) (Foreign, error) {
	var f Foreign
	err := q.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM jobs
		   WHERE scope <> $1 AND status NOT IN ('SUCCEEDED', 'DEAD_LETTERED', 'CANCELED')),
		  (SELECT count(*) FROM outbox_events e LEFT JOIN jobs j ON j.id = e.job_id
		   WHERE e.status = 'PENDING' AND j.scope IS DISTINCT FROM $1)`, scope).Scan(&f.NonTerminalJobs, &f.PendingEvents)
	if err != nil {
		return Foreign{}, fmt.Errorf("count work outside scope %q: %w", scope, err)
	}
	return f, nil
}

// ActiveLeaseExpiries is the expires_at of every lease of the session that is
// still ACTIVE: after a kill, when each of the dead session's attempts becomes
// recoverable. A lease the worker renewed before it died expires sooner than a
// lease length after the kill.
func ActiveLeaseExpiries(ctx context.Context, q Querier, scope string, sessionID uuid.UUID) ([]time.Time, error) {
	rows, err := q.Query(ctx, `
		SELECT expires_at FROM leases
		WHERE scope = $1 AND worker_session_id = $2 AND status = 'ACTIVE'
		ORDER BY expires_at`, scope, sessionID)
	if err != nil {
		return nil, fmt.Errorf("read the session's active leases: %w", err)
	}
	defer rows.Close()
	var out []time.Time
	for rows.Next() {
		var t time.Time
		if err := rows.Scan(&t); err != nil {
			return nil, fmt.Errorf("scan a lease expiry: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// AtCapacity is one logical worker that, at some instant inside an interval, held
// as many active leases as its concurrency limit, counting the leases of its
// earlier, dead boots: the count the claim transaction compares with the limit
// (internal/workers/store.go, "Count by logical worker, not just process
// session"), so a claim it made then was CAPACITY_EXHAUSTED.
type AtCapacity struct {
	Worker string
	// At is the first instant in the interval at which it was at its limit;
	// Active is the leases it held then, and DeadBoots how many of them belong to
	// a session other than its newest by then.
	At                time.Time
	Active, DeadBoots int
	Limit             int
}

// WorkersAtCapacityDuring reconstructs, from the leases' acquired_at and
// released_at, which of the scope's logical workers were at their concurrency
// limit at any instant in [from, to]. The limit is that of the worker's newest
// session registered by then.
//
// It takes an interval and not an instant because a claim's capacity check runs at
// an instant the logs do not record: claims queue on the queue row's lock, so the
// check can come at any point of the request, and with 50 ms jobs the leases a
// worker holds change within a request's span. The candidate instants are the
// interval's start and every lease acquisition inside it, the only instants at
// which a count can rise. It is a reconstruction from durable timestamps, not a
// record of any claim's decision.
func WorkersAtCapacityDuring(ctx context.Context, q Querier, scope string, from, to time.Time) ([]AtCapacity, error) {
	rows, err := q.Query(ctx, `
		WITH instants AS (
		    SELECT $2::timestamptz AS at
		    UNION
		    SELECT l.acquired_at FROM leases l
		    WHERE l.scope = $1 AND l.acquired_at > $2 AND l.acquired_at <= $3
		), counts AS (
		    SELECT w.name, i.at, count(l.id) AS active,
		           count(l.id) FILTER (WHERE l.worker_session_id <> cs.id) AS dead_boots,
		           cs.concurrency_limit
		    FROM instants i
		    CROSS JOIN workers w
		    JOIN LATERAL (
		        SELECT s.id, s.concurrency_limit FROM worker_sessions s
		        WHERE s.worker_id = w.id AND s.scope = w.scope AND s.registered_at <= i.at
		        ORDER BY s.registered_at DESC, s.id DESC
		        LIMIT 1
		    ) cs ON true
		    JOIN leases l ON l.worker_id = w.id AND l.scope = w.scope
		                 AND l.acquired_at <= i.at AND (l.released_at IS NULL OR l.released_at > i.at)
		    WHERE w.scope = $1
		    GROUP BY w.name, i.at, cs.concurrency_limit
		    HAVING count(l.id) >= cs.concurrency_limit
		)
		SELECT DISTINCT ON (name) name, at, active, dead_boots, concurrency_limit
		FROM counts
		ORDER BY name, at`, scope, from, to)
	if err != nil {
		return nil, fmt.Errorf("reconstruct capacity in [%s, %s]: %w", from, to, err)
	}
	defer rows.Close()
	var out []AtCapacity
	for rows.Next() {
		var c AtCapacity
		if err := rows.Scan(&c.Worker, &c.At, &c.Active, &c.DeadBoots, &c.Limit); err != nil {
			return nil, fmt.Errorf("scan a worker at capacity: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
