package workers

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrInvalidCursor means the pagination cursor was not one this API issued.
//
// Deliberately its own error rather than a reuse of jobs.ErrInvalidCursor:
// internal/workers does not import internal/jobs, and the HTTP layer maps both
// onto the same public CodeInvalidCursor, which is where that equivalence
// belongs.
var ErrInvalidCursor = errors.New("invalid pagination cursor")

// Listing bounds for GET /v1/workers, matching the job listing's.
const (
	DefaultPageSize = 25
	MaxPageSize     = 100
)

// WorkerSummary is one row of GET /v1/workers: a logical worker joined to its
// most recent process session.
//
// Session, lease, and fencing identifiers are deliberately absent. The
// worker-control surface trusts a session id as authority on every call after
// registration (ADR-0014), so publishing one here would hand an authenticated
// public caller an identifier that surface treats as a credential. WorkerID is
// safe and is included: it names a logical worker, is required to read attempt
// history coherently, and authorizes nothing.
type WorkerSummary struct {
	ID          uuid.UUID
	Name        string
	Status      SessionStatus
	WorkerGroup string
	Hostname    string
	// ConcurrencyLimit is this process's declared bounded pool size.
	ConcurrencyLimit  int
	Capabilities      []string
	SupportedJobTypes []string
	RegisteredAt      time.Time
	LastHeartbeatAt   time.Time
	// EndedAt is set once a session is no longer current -- OFFLINE because a
	// replacement boot took over, or UNHEALTHY because reconciliation found it
	// stale.
	EndedAt *time.Time
	// HeartbeatAgeSeconds is measured by PostgreSQL, never by a client clock.
	// It is floored at zero: a heartbeat stamped microseconds in the future by
	// a concurrent transaction is a clock artifact, not a negative age.
	HeartbeatAgeSeconds float64
	// ActiveLeases is how many leases this logical worker currently holds.
	// Capacity is derived from active leases, so this is the live occupancy of
	// ConcurrencyLimit.
	ActiveLeases int
}

// WorkerPage is one bounded page of workers, by name.
type WorkerPage struct {
	Workers []WorkerSummary
	// NextCursor is empty when this is the last page.
	NextCursor string
}

// EncodeWorkerCursor renders a keyset position as an opaque token.
//
// The position is a worker name, which is UNIQUE (scope, name) in the schema
// and therefore already a total order within one scope -- no id tiebreak is
// needed, unlike the jobs and DLQ cursors whose timestamps can tie.
func EncodeWorkerCursor(name string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(name))
}

func decodeWorkerCursor(cursor string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", ErrInvalidCursor
	}
	name := string(raw)
	// Validated against the same pattern registration enforces. A cursor is a
	// position, not an authorization, but it still reaches a SQL comparison and
	// is echoed nowhere -- bounding it here keeps a hand-built cursor from
	// carrying an arbitrary-length or control-character string into the query.
	if !workerNamePattern.MatchString(name) {
		return "", ErrInvalidCursor
	}
	return name, nil
}

// ListWorkers returns one bounded, scope-filtered page of logical workers with
// their most recent session, ordered by name.
//
// The session it reports is the LATEST one, whatever its status -- not the
// "current" one. That distinction is the whole design of this query.
// worker_sessions_one_current_per_worker_idx covers only
// status IN ('STARTING','HEALTHY','DRAINING'); reconciliation moves a stale
// session to UNHEALTHY and a replacement registration moves the prior one to
// OFFLINE. A listing restricted to current sessions would therefore omit
// exactly the workers an operator opened the page to find: the one that just
// crashed and the one that was replaced. The LATERAL below is ordered by
// (registered_at DESC, id DESC) with no status predicate, served by
// worker_sessions_latest_per_worker_idx.
//
// The inner join to that LATERAL is safe -- it can never drop a worker --
// because registration writes the workers row and its first worker_sessions
// row in one transaction, so a worker with no session at all is not a state
// this schema can reach.
func (s *Store) ListWorkers(ctx context.Context, scope, cursor string, limit int) (WorkerPage, error) {
	if limit <= 0 {
		limit = DefaultPageSize
	}
	if limit > MaxPageSize {
		limit = MaxPageSize
	}

	var after *string
	if cursor != "" {
		name, err := decodeWorkerCursor(cursor)
		if err != nil {
			return WorkerPage{}, err
		}
		after = &name
	}

	// heartbeat age and the active-lease count are both computed by
	// PostgreSQL. Deriving the age in Go would compare the API process's wall
	// clock against a server timestamp, which AGENTS.md section 6 forbids for
	// anything about staleness; counting leases in a second round trip would
	// report a number from a different instant than the session it decorates.
	//
	// GREATEST(0, ...) floors the age: clock_timestamp() is sampled during this
	// statement while a concurrent heartbeat may have stamped a marginally
	// later instant, and "-0.001 seconds old" is a nonsense an operator should
	// never have to interpret.
	rows, err := s.pool.Query(ctx, `
		SELECT w.id, w.name, s.status, s.worker_group, s.hostname,
		       s.concurrency_limit, s.capabilities, s.supported_job_types,
		       s.registered_at, s.last_heartbeat_at, s.ended_at,
		       GREATEST(0, EXTRACT(EPOCH FROM (clock_timestamp() - s.last_heartbeat_at))),
		       (SELECT count(*) FROM leases l
		         WHERE l.worker_id = w.id AND l.status = 'ACTIVE')
		FROM workers w
		CROSS JOIN LATERAL (
			SELECT status, worker_group, hostname, concurrency_limit,
			       capabilities, supported_job_types, registered_at,
			       last_heartbeat_at, ended_at
			FROM worker_sessions ws
			WHERE ws.worker_id = w.id
			ORDER BY ws.registered_at DESC, ws.id DESC
			LIMIT 1
		) s
		WHERE w.scope = $1
		  AND ($2::text IS NULL OR w.name > $2)
		ORDER BY w.name ASC
		LIMIT $3`, scope, after, limit+1)
	if err != nil {
		return WorkerPage{}, fmt.Errorf("list workers: %w", err)
	}
	defer rows.Close()

	page := WorkerPage{Workers: make([]WorkerSummary, 0, limit)}
	for rows.Next() {
		var worker WorkerSummary
		if err := rows.Scan(&worker.ID, &worker.Name, &worker.Status,
			&worker.WorkerGroup, &worker.Hostname, &worker.ConcurrencyLimit,
			&worker.Capabilities, &worker.SupportedJobTypes,
			&worker.RegisteredAt, &worker.LastHeartbeatAt, &worker.EndedAt,
			&worker.HeartbeatAgeSeconds, &worker.ActiveLeases); err != nil {
			return WorkerPage{}, fmt.Errorf("scan worker summary: %w", err)
		}
		page.Workers = append(page.Workers, worker)
	}
	if err := rows.Err(); err != nil {
		return WorkerPage{}, fmt.Errorf("iterate workers: %w", err)
	}

	if len(page.Workers) > limit {
		last := page.Workers[limit-1]
		page.Workers = page.Workers[:limit]
		page.NextCursor = EncodeWorkerCursor(last.Name)
	}
	return page, nil
}
