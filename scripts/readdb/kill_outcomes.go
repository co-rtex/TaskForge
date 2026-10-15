package readdb

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// This file holds the queries `go run ./scripts/bench recovery-probe` reads a kill's
// outcomes with (M8D4). Like timeline.go it is not a benchmark measurement: the
// recorded runs never call it, and the recovery figure ADR-0020 defines is still
// Recoveries', which counts ABANDONED attempts only. What is here says what became
// of every attempt a kill caught, whatever status it ended in, so a kill whose
// attempts ended TIMED_OUT is reported instead of falling out of the figure.

// Attempt states at a kill.
const (
	HeldLeased  = "LEASED"
	HeldRunning = "RUNNING"
	// HeldCreatedAfterKill is an attempt of the killed session claimed after the
	// kill was stamped. The stamp is read just before the signal, so a claim the
	// dying process had in flight can commit after it; that attempt was neither
	// LEASED nor RUNNING at the stamp, but the kill is what stranded it.
	HeldCreatedAfterKill = "CREATED_AFTER_KILL"
)

// The outcome buckets a held attempt is reported in. They are the probe's
// categories, not statuses of the schema. An attempt that dead-lettered its job
// is its own bucket, because what follows it (no retry at all) is not what follows
// one with budget left (a replacement). Which attempt dead-lettered a job is the
// one its DLQ entry names (dlq_entries.terminal_attempt_id), not any attempt of a
// job that is DEAD_LETTERED now: a later attempt may have done that.
const (
	OutcomeAbandoned           = "ABANDONED"
	OutcomeAbandonedDeadLetter = "DEAD_LETTERED_ON_ABANDON"
	OutcomeTimedOut            = "TIMED_OUT"
	OutcomeTimedOutDeadLetter  = "DEAD_LETTERED_ON_TIMEOUT"
	OutcomeCanceled            = "CANCELED"
	// OutcomeUnresolved is an attempt still LEASED or RUNNING when it was read.
	OutcomeUnresolved = "UNRESOLVED"
	// OutcomeOther is anything else (SUCCEEDED or FAILED: a report that was in
	// flight when the worker died and committed after the kill instant).
	OutcomeOther = "OTHER"
)

// KillOutcome is one attempt that a killed session had not finished at the kill
// instant, and what became of it.
type KillOutcome struct {
	JobID         uuid.UUID
	AttemptID     uuid.UUID
	AttemptNumber int

	// StateAtKill is HeldCreatedAfterKill if the attempt was claimed after the
	// kill instant, HeldRunning if its started_at is at or before the kill, and
	// HeldLeased otherwise (never started, or started after the kill instant by a
	// Start that was in flight when the worker died).
	StateAtKill string

	// The attempt as it is now.
	Status       string
	FailureClass *string
	ErrorCode    *string
	CreatedAt    time.Time
	StartedAt    *time.Time
	TimeoutAt    *time.Time
	FinishedAt   *time.Time
	RetryAt      *time.Time

	// The attempt's lease as it is now. ReleasedAt is the instant the attempt's
	// outcome transaction released or expired it; LeaseStatus says which (a
	// timeout found while the lease was live leaves it RELEASED, one found on the
	// expired-lease path leaves it EXPIRED).
	LeaseStatus     string
	LeaseAcquiredAt time.Time
	LeaseRenewedAt  time.Time
	LeaseExpiresAt  time.Time
	LeaseReleasedAt *time.Time
	RenewalVersion  int

	// The job as it is now, and its dead-letter reason if it has one.
	JobStatus string
	DLQReason *string

	// ReplacementClaimedAt is the created_at of the job's next attempt (attempt
	// number + 1): its claim. Nil if there is none.
	ReplacementClaimedAt *time.Time
}

// Outcome is the attempt's outcome bucket. The dead-letter buckets are chosen by
// the attempt's own DLQ entry (DLQReason), never by the job's status now.
func (o KillOutcome) Outcome() string {
	switch o.Status {
	case "ABANDONED":
		if o.DLQReason != nil {
			return OutcomeAbandonedDeadLetter
		}
		return OutcomeAbandoned
	case "TIMED_OUT":
		if o.DLQReason != nil {
			return OutcomeTimedOutDeadLetter
		}
		return OutcomeTimedOut
	case "CANCELED":
		return OutcomeCanceled
	case "LEASED", "RUNNING":
		return OutcomeUnresolved
	default:
		return OutcomeOther
	}
}

// ClaimToStart is started_at minus the lease's acquired_at: how long the attempt
// was LEASED before its Start committed. For an attempt never renewed it is also
// timeout_at minus expires_at whenever timeout_seconds equals the lease. Before
// M8D5 that was the window in which a reconciler pass abandoned it rather than
// timed it out; since ADR-0025 a lease that lapsed that much before the deadline
// is abandoned whenever the pass comes. The second result is false for an attempt
// that never started.
func (o KillOutcome) ClaimToStart() (time.Duration, bool) {
	if o.StartedAt == nil {
		return 0, false
	}
	return o.StartedAt.Sub(o.LeaseAcquiredAt), true
}

// ToReplacement is the replacement's claim minus the kill. The second result is
// false when there is no replacement. For an ABANDONED attempt it is the recovery
// figure (Recoveries reads the same two instants); for any other outcome it is not
// a recovery and must be reported as its own figure.
func (o KillOutcome) ToReplacement(kill time.Time) (time.Duration, bool) {
	if o.ReplacementClaimedAt == nil {
		return 0, false
	}
	return o.ReplacementClaimedAt.Sub(kill), true
}

// KillOutcomes returns every attempt of the killed session that was not finished
// at the kill instant: not finished, or finished after it. It assumes the session
// is dead from the kill on: a live session's later reports, renewals and claims
// would be read as the kill's outcomes. An attempt the session
// had already finished is not one the kill affected and is not returned; see
// FinishedBetween for those.
//
// There is deliberately no bound on created_at. The kill instant is stamped just
// before the signal, so a claim the dying process had in flight can commit after
// the stamp; that attempt is the kill's too, and is labelled HeldCreatedAfterKill
// rather than dropped (a dropped one would make the outcomes disagree with the
// attempts the session held once it was dead). Every instant is PostgreSQL's.
func KillOutcomes(ctx context.Context, q Querier, scope string, sessionID uuid.UUID, kill time.Time) ([]KillOutcome, error) {
	rows, err := q.Query(ctx, `
		SELECT a.job_id, a.id, a.attempt_number,
		       CASE WHEN a.created_at > $3 THEN 'CREATED_AFTER_KILL'
		            WHEN a.started_at IS NOT NULL AND a.started_at <= $3 THEN 'RUNNING'
		            ELSE 'LEASED' END,
		       a.status, a.failure_class, a.error_code,
		       a.created_at, a.started_at, a.timeout_at, a.finished_at, a.retry_at,
		       l.status, l.acquired_at, l.renewed_at, l.expires_at, l.released_at, l.renewal_version,
		       j.status, d.reason, rep.created_at
		FROM job_attempts a
		JOIN leases l ON l.attempt_id = a.id
		JOIN jobs j ON j.id = a.job_id
		LEFT JOIN dlq_entries d ON d.job_id = a.job_id AND d.terminal_attempt_id = a.id
		LEFT JOIN job_attempts rep
		       ON rep.job_id = a.job_id AND rep.attempt_number = a.attempt_number + 1
		WHERE a.scope = $1 AND a.worker_session_id = $2
		  AND (a.finished_at IS NULL OR a.finished_at > $3)
		ORDER BY a.job_id, a.attempt_number`, scope, sessionID, kill)
	if err != nil {
		return nil, fmt.Errorf("read kill outcomes: %w", err)
	}
	defer rows.Close()
	var out []KillOutcome
	for rows.Next() {
		var o KillOutcome
		if err := rows.Scan(&o.JobID, &o.AttemptID, &o.AttemptNumber, &o.StateAtKill,
			&o.Status, &o.FailureClass, &o.ErrorCode,
			&o.CreatedAt, &o.StartedAt, &o.TimeoutAt, &o.FinishedAt, &o.RetryAt,
			&o.LeaseStatus, &o.LeaseAcquiredAt, &o.LeaseRenewedAt, &o.LeaseExpiresAt, &o.LeaseReleasedAt, &o.RenewalVersion,
			&o.JobStatus, &o.DLQReason, &o.ReplacementClaimedAt); err != nil {
			return nil, fmt.Errorf("scan a kill outcome: %w", err)
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// Finished is one attempt of a session that finished inside an interval.
type Finished struct {
	JobID         uuid.UUID
	AttemptNumber int
	Status        string
	FinishedAt    time.Time
}

// FinishedBetween returns every attempt of the session that finished in [from,
// to], both ends included, whether or not the kill was aimed at it. Between a
// kill's choice of victim (from) and the kill (to) they include any attempt the
// kill was aimed at but could no longer catch, which is M8A's explanation of a kill
// that hit nothing; they are not limited to those.
func FinishedBetween(ctx context.Context, q Querier, scope string, sessionID uuid.UUID, from, to time.Time) ([]Finished, error) {
	rows, err := q.Query(ctx, `
		SELECT job_id, attempt_number, status, finished_at
		FROM job_attempts
		WHERE scope = $1 AND worker_session_id = $2
		  AND finished_at >= $3 AND finished_at <= $4
		ORDER BY finished_at, job_id`, scope, sessionID, from, to)
	if err != nil {
		return nil, fmt.Errorf("read attempts finished between %s and %s: %w", from, to, err)
	}
	defer rows.Close()
	var out []Finished
	for rows.Next() {
		var f Finished
		if err := rows.Scan(&f.JobID, &f.AttemptNumber, &f.Status, &f.FinishedAt); err != nil {
			return nil, fmt.Errorf("scan a finished attempt: %w", err)
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// RenewedAttempts is, for each of the scope's workers whose current session holds
// attempts and every one of them is RUNNING, renewed at least once and with its
// persisted deadline at least margin away, how many it holds, keyed by the
// worker's name. A worker holding nothing, or holding any attempt that does not
// qualify, is absent.
//
// It is the recovery probe's condition E victim selection: a kill after a renewal
// of every attempt the victim holds, none of them about to time out by itself.
// Requiring all of them, not one, is what makes every attempt the kill catches a
// renewed one. The current session is read the way TargetableAttempts and
// CurrentSession read it, and the deadline is judged on PostgreSQL's clock.
func RenewedAttempts(ctx context.Context, q Querier, scope string, margin time.Duration) (map[string]int, error) {
	if margin < 0 {
		return nil, fmt.Errorf("a renewed attempt's deadline margin must not be negative; got %s", margin)
	}
	rows, err := q.Query(ctx, `
		SELECT w.name, count(*)
		FROM job_attempts a
		JOIN workers w ON w.id = a.worker_id AND w.scope = a.scope
		JOIN leases l ON l.attempt_id = a.id
		WHERE a.scope = $1
		  AND a.worker_session_id = (
		      SELECT s.id
		      FROM worker_sessions s
		      WHERE s.worker_id = w.id AND s.scope = w.scope
		      ORDER BY s.registered_at DESC, s.id DESC
		      LIMIT 1)
		  AND a.status IN ('LEASED', 'RUNNING')
		GROUP BY w.name
		HAVING bool_and(a.status = 'RUNNING' AND l.status = 'ACTIVE' AND l.renewal_version >= 1
		                AND a.timeout_at >= clock_timestamp() + ($2::bigint * interval '1 millisecond'))`,
		scope, margin.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("count renewed attempts: %w", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var name string
		var n int
		if err := rows.Scan(&name, &n); err != nil {
			return nil, fmt.Errorf("scan a renewed count: %w", err)
		}
		out[name] = n
	}
	return out, rows.Err()
}
