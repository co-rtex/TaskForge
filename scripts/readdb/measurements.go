package readdb

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// This file holds every query the benchmark measures with. scripts/bench imports
// it and tests/integration/bench_queries_test.go imports it, and there is no
// second copy of any statement: a query that read the wrong column would produce
// a plausible, confident, wrong benchmark, so the test seeds exact timestamps
// and asserts the exact differences these definitions name. The definitions are
// docs/adr/0020-benchmark-methodology.md's.
//
// Every instant here is a PostgreSQL timestamp. The harness never mixes in its
// own clock, which is the single-clock rule.

// Querier is the part of *pgx.Conn and *pgxpool.Pool the queries need.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// ClockNow is PostgreSQL's clock_timestamp(): the wall-clock instant on the
// database server at the moment the statement runs. It is how a window boundary
// or a kill time is stamped, so that it is comparable with the columns below.
func ClockNow(ctx context.Context, q Querier) (time.Time, error) {
	var now time.Time
	if err := q.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read the database clock: %w", err)
	}
	return now, nil
}

// ServerVersion is PostgreSQL's own version string, for the record.
func ServerVersion(ctx context.Context, q Querier) (string, error) {
	var version string
	if err := q.QueryRow(ctx, `SHOW server_version`).Scan(&version); err != nil {
		return "", fmt.Errorf("read the server version: %w", err)
	}
	return version, nil
}

// Dispatch is one job submitted inside a window and, once it has been claimed,
// the instant its first attempt was.
type Dispatch struct {
	JobID uuid.UUID
	// SubmittedAt is jobs.created_at: the start of the submit transaction.
	SubmittedAt time.Time
	// ClaimedAt is job_attempts.created_at of attempt 1: the start of the claim
	// transaction. It is not job_attempts.started_at, which is the worker's start
	// report and arrives after the work has been handed over. Nil until claimed.
	ClaimedAt *time.Time
	// LeasedAt is leases.acquired_at of attempt 1, sampled with clock_timestamp()
	// after every lock the claim takes. It is later than ClaimedAt by whatever the
	// claim transaction spent before that point, so it is the stricter reading,
	// and it is reported beside the defined figure and never instead of it.
	LeasedAt *time.Time
}

// Latency is the dispatch latency of the definition: attempt 1's claim minus the
// job's submission. The second result is false while the job is unclaimed.
func (d Dispatch) Latency() (time.Duration, bool) {
	if d.ClaimedAt == nil {
		return 0, false
	}
	return d.ClaimedAt.Sub(d.SubmittedAt), true
}

// LeaseLatency is the supplementary reading, to the lease's issuance.
func (d Dispatch) LeaseLatency() (time.Duration, bool) {
	if d.LeasedAt == nil {
		return 0, false
	}
	return d.LeasedAt.Sub(d.SubmittedAt), true
}

// Dispatches returns every immediate job of the scope submitted in [from, to),
// with attempt 1's claim where there is one. The window is half-open, so a job
// created exactly at from is in and one created exactly at to is out.
//
// Delayed jobs (scheduled_at set) are excluded: their wait is the schedule the
// caller asked for, not dispatch. Unclaimed jobs are kept with a nil ClaimedAt
// rather than dropped, so the harness can count them; averaging only the claimed
// ones would flatter a system that was still sitting on some.
func Dispatches(ctx context.Context, q Querier, scope string, from, to time.Time) ([]Dispatch, error) {
	rows, err := q.Query(ctx, `
		SELECT j.id, j.created_at, a.created_at, l.acquired_at
		FROM jobs j
		LEFT JOIN job_attempts a ON a.job_id = j.id AND a.attempt_number = 1
		LEFT JOIN leases l ON l.attempt_id = a.id
		WHERE j.scope = $1
		  AND j.scheduled_at IS NULL
		  AND j.created_at >= $2 AND j.created_at < $3
		ORDER BY j.created_at, j.id`, scope, from, to)
	if err != nil {
		return nil, fmt.Errorf("read dispatches: %w", err)
	}
	defer rows.Close()
	var out []Dispatch
	for rows.Next() {
		var d Dispatch
		if err := rows.Scan(&d.JobID, &d.SubmittedAt, &d.ClaimedAt, &d.LeasedAt); err != nil {
			return nil, fmt.Errorf("scan a dispatch: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// SucceededFinishes returns, ascending, the finish time of every SUCCEEDED job
// of the scope: the finished_at of its SUCCEEDED attempt. A job has at most one
// SUCCEEDED attempt, so each job counts once, and an abandoned first attempt
// never contributes its own finish.
//
// That finished_at is the same instant the transaction wrote to jobs.updated_at
// when it set the job SUCCEEDED (internal/workers/store.go, Succeed), sampled
// with clock_timestamp() after the authority rows were locked.
func SucceededFinishes(ctx context.Context, q Querier, scope string) ([]time.Time, error) {
	rows, err := q.Query(ctx, `
		SELECT a.finished_at
		FROM jobs j
		JOIN job_attempts a ON a.job_id = j.id AND a.status = 'SUCCEEDED'
		WHERE j.scope = $1 AND j.status = 'SUCCEEDED'
		ORDER BY a.finished_at, j.id`, scope)
	if err != nil {
		return nil, fmt.Errorf("read finish times: %w", err)
	}
	defer rows.Close()
	var out []time.Time
	for rows.Next() {
		var finished time.Time
		if err := rows.Scan(&finished); err != nil {
			return nil, fmt.Errorf("scan a finish time: %w", err)
		}
		out = append(out, finished)
	}
	return out, rows.Err()
}

// StatusCounts is the number of the scope's jobs in each status.
func StatusCounts(ctx context.Context, q Querier, scope string) (map[string]int, error) {
	return countBy(ctx, q, `SELECT status, count(*) FROM jobs WHERE scope = $1 GROUP BY status`, scope)
}

// DLQReasons is the number of the scope's dead-lettered jobs for each reason.
func DLQReasons(ctx context.Context, q Querier, scope string) (map[string]int, error) {
	return countBy(ctx, q, `SELECT reason, count(*) FROM dlq_entries WHERE scope = $1 GROUP BY reason`, scope)
}

// Occupancy is the number of attempts each of the scope's workers holds a slot
// with right now: attempts that are LEASED or RUNNING, keyed by the worker's
// name. A worker holding nothing is absent.
func Occupancy(ctx context.Context, q Querier, scope string) (map[string]int, error) {
	return countBy(ctx, q, `
		SELECT w.name, count(*)
		FROM job_attempts a
		JOIN workers w ON w.id = a.worker_id AND w.scope = a.scope
		WHERE a.scope = $1 AND a.status IN ('LEASED', 'RUNNING')
		GROUP BY w.name`, scope)
}

// TargetableAttempts is the number of attempts each of the scope's workers holds
// that a kill could still catch mid-work, keyed by the worker's name. A worker with
// none is absent.
//
// An attempt is targetable if it is LEASED, because its sleep has not begun, or it
// is RUNNING and started no longer ago than duration-margin, so that at least margin
// of its duration is left. Both are read on PostgreSQL's clock, `clock_timestamp()`,
// the one every other instant in the harness is read from, and not this machine's:
// started_at is PostgreSQL's now() when the worker reported the start.
//
// It exists for the smoke. Occupancy counts an attempt that is 50 ms from
// finishing exactly like one that has just begun, so a kill aimed at the worker
// holding it can find nothing left to abandon by the time the signal is sent.
func TargetableAttempts(ctx context.Context, q Querier, scope string, duration, margin time.Duration) (map[string]int, error) {
	return targetableAttempts(ctx, q, scope, duration, margin, nil)
}

// TargetableAttemptsAsOf is TargetableAttempts judged at a given instant instead
// of PostgreSQL's clock, so a test can place an attempt exactly on, just inside and
// just outside the boundary. Nothing outside tests calls it.
func TargetableAttemptsAsOf(ctx context.Context, q Querier, scope string, duration, margin time.Duration, asOf time.Time) (map[string]int, error) {
	return targetableAttempts(ctx, q, scope, duration, margin, &asOf)
}

func targetableAttempts(ctx context.Context, q Querier, scope string, duration, margin time.Duration, asOf *time.Time) (map[string]int, error) {
	if duration <= 0 || margin < 0 || margin > duration {
		return nil, fmt.Errorf("a targetable attempt needs 0 <= margin <= duration; got duration %s, margin %s", duration, margin)
	}
	rows, err := q.Query(ctx, `
		SELECT w.name, count(*)
		FROM job_attempts a
		JOIN workers w ON w.id = a.worker_id AND w.scope = a.scope
		WHERE a.scope = $1
		  AND (a.status = 'LEASED'
		       OR (a.status = 'RUNNING'
		           AND a.started_at >= COALESCE($4::timestamptz, clock_timestamp())
		                               - (($2::bigint - $3::bigint) * interval '1 millisecond')))
		GROUP BY w.name`,
		scope, duration.Milliseconds(), margin.Milliseconds(), asOf)
	if err != nil {
		return nil, fmt.Errorf("count targetable attempts: %w", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var name string
		var n int
		if err := rows.Scan(&name, &n); err != nil {
			return nil, fmt.Errorf("scan a targetable count: %w", err)
		}
		out[name] = n
	}
	return out, rows.Err()
}

func countBy(ctx context.Context, q Querier, sql, scope string) (map[string]int, error) {
	rows, err := q.Query(ctx, sql, scope)
	if err != nil {
		return nil, fmt.Errorf("count rows: %w", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var key string
		var n int
		if err := rows.Scan(&key, &n); err != nil {
			return nil, fmt.Errorf("scan a count: %w", err)
		}
		out[key] = n
	}
	return out, rows.Err()
}

// CurrentSession is the newest process session of the scope's logical worker
// with that name: the boot that is running now, and so the one a SIGKILL ends.
// A worker restarted under the same name has one session per boot, and the
// older ones remain as history.
func CurrentSession(ctx context.Context, q Querier, scope, workerName string) (uuid.UUID, error) {
	var id uuid.UUID
	err := q.QueryRow(ctx, `
		SELECT s.id
		FROM worker_sessions s
		JOIN workers w ON w.id = s.worker_id AND w.scope = s.scope
		WHERE w.scope = $1 AND w.name = $2
		ORDER BY s.registered_at DESC, s.id DESC
		LIMIT 1`, scope, workerName).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("find the current session of worker %q: %w", workerName, err)
	}
	return id, nil
}

// Recovery is one attempt that a worker kill abandoned, and the attempt that
// replaced it.
type Recovery struct {
	JobID            uuid.UUID
	AbandonedAttempt int
	// ReplacementClaimedAt is job_attempts.created_at of the next attempt of the
	// same job: its claim, not its start report. Nil if no replacement exists,
	// because the attempt budget ran out or the job has not been re-claimed yet.
	ReplacementClaimedAt *time.Time
}

// After is the recovery time of the definition: the replacement's claim minus
// the instant of the kill. The second result is false when there is no
// replacement, which the caller must report rather than leave out.
func (r Recovery) After(kill time.Time) (time.Duration, bool) {
	if r.ReplacementClaimedAt == nil {
		return 0, false
	}
	return r.ReplacementClaimedAt.Sub(kill), true
}

// Recoveries returns the attempts bound to the killed session that ended
// ABANDONED, each with its replacement. An attempt of that session that finished
// before the kill is not affected and is not returned.
func Recoveries(ctx context.Context, q Querier, scope string, sessionID uuid.UUID) ([]Recovery, error) {
	rows, err := q.Query(ctx, `
		SELECT ab.job_id, ab.attempt_number, rep.created_at
		FROM job_attempts ab
		LEFT JOIN job_attempts rep
		       ON rep.job_id = ab.job_id AND rep.attempt_number = ab.attempt_number + 1
		WHERE ab.scope = $1 AND ab.worker_session_id = $2 AND ab.status = 'ABANDONED'
		ORDER BY ab.job_id, ab.attempt_number`, scope, sessionID)
	if err != nil {
		return nil, fmt.Errorf("read recoveries: %w", err)
	}
	defer rows.Close()
	var out []Recovery
	for rows.Next() {
		var r Recovery
		if err := rows.Scan(&r.JobID, &r.AbandonedAttempt, &r.ReplacementClaimedAt); err != nil {
			return nil, fmt.Errorf("scan a recovery: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
