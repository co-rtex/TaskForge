package jobs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Listing bounds shared by the operator read endpoints added in M6A.
//
// These are deliberately their own constants rather than a reuse of
// DefaultDLQPageSize / MaxDLQPageSize. The two happen to agree today, and a
// future decision to page the DLQ differently from the job list must not be
// blocked by, or silently change, the other endpoint.
const (
	DefaultPageSize = 25
	MaxPageSize     = 100
)

// ErrInvalidJobFilter reports a status or queue filter this API cannot honor.
var ErrInvalidJobFilter = errors.New("invalid job listing filter")

// JobSummary is one row of GET /v1/jobs.
//
// It carries every JobResponse field except the payload, for the reason
// DLQEntry already states: a payload is unbounded user data, and a list
// endpoint that returned payloads would let one request pull an arbitrary
// amount of it. A single job, payload included, stays readable through
// GET /v1/jobs/{job_id}.
type JobSummary struct {
	ID                   uuid.UUID
	Queue                string
	Type                 string
	Status               Status
	Priority             int
	MaxAttempts          int
	TimeoutSeconds       int
	RequiredCapabilities []string
	ScheduledAt          *time.Time
	AvailableAt          time.Time
	CancelRequestedAt    *time.Time
	ReplayedFromJobID    *uuid.UUID
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// JobPage is one bounded page of jobs, newest first.
type JobPage struct {
	Jobs []JobSummary
	// NextCursor is empty when this is the last page. A full page is not
	// proof that more exist; the absence of a cursor is.
	NextCursor string
}

// JobFilter narrows a listing. A zero value lists everything in scope.
type JobFilter struct {
	// Status, when non-empty, must be one of AllStatuses().
	Status Status
	// Queue, when non-empty, must match the queue-name pattern. A queue that
	// does not exist is not an error: it simply matches nothing.
	Queue string
}

// Validate reports a filter this API will not honor.
//
// A queue that does not exist is deliberately NOT an error here. "No jobs in
// queue X" and "queue X was removed" are the same answer to an operator
// paging a list, and turning the second into a 422 would make the listing
// endpoint disagree with itself as reference data changes underneath it.
// Submission still rejects an unknown queue, because writing to one is a
// different question from reading from one.
func (f JobFilter) Validate() error {
	if f.Status != "" && !f.Status.Valid() {
		return fmt.Errorf("%w: status %q is not a known job status", ErrInvalidJobFilter, f.Status)
	}
	if f.Queue != "" && !queueNamePattern.MatchString(f.Queue) {
		return fmt.Errorf("%w: queue %q is not a valid queue name", ErrInvalidJobFilter, f.Queue)
	}
	return nil
}

// EncodeJobCursor renders a keyset position as an opaque token.
//
// Identical in shape and reasoning to EncodeDLQCursor: opaque but not secret,
// a position rather than an authorization, encoded so clients cannot build one
// by hand and freeze the ordering columns into the public contract.
func EncodeJobCursor(createdAt time.Time, id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(createdAt.UTC().Format(time.RFC3339Nano) + "|" + id.String()))
}

func decodeJobCursor(cursor string) (time.Time, uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	stamp, rest, ok := strings.Cut(string(raw), "|")
	if !ok {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	createdAt, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	id, err := uuid.Parse(rest)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	return createdAt, id, nil
}

// boundPage clamps a caller-supplied page size into the documented range.
func boundPage(limit int) int {
	if limit <= 0 {
		return DefaultPageSize
	}
	if limit > MaxPageSize {
		return MaxPageSize
	}
	return limit
}

// ListJobs returns one bounded, scope-filtered page of jobs, newest first.
//
// Pagination is keyset on (created_at DESC, id DESC) for the reason ListDLQ
// states at length: two jobs can be submitted in the same instant, so
// timestamps alone are not a total order, and OFFSET over a growing table
// skips and repeats rows.
//
// One property this ordering does NOT provide is worth stating rather than
// implying. jobs.created_at defaults to now(), which in PostgreSQL is
// TRANSACTION START time, so a submission that began before a page was read
// but committed after it can carry a created_at already behind the cursor.
// Such a job is not returned by that walk and IS returned by a fresh listing.
// What keyset ordering guarantees is that no job present throughout a walk is
// duplicated or skipped; it does not guarantee that a walk sees a job that
// became visible mid-walk. TestReadAPI_ConcurrentInsertsNeverDuplicateOrReorder
// pins exactly that distinction.
func (s *Store) ListJobs(ctx context.Context, scope string, filter JobFilter, cursor string, limit int) (JobPage, error) {
	if err := filter.Validate(); err != nil {
		return JobPage{}, err
	}
	limit = boundPage(limit)

	var cursorAt *time.Time
	var cursorID *uuid.UUID
	if cursor != "" {
		at, id, err := decodeJobCursor(cursor)
		if err != nil {
			return JobPage{}, err
		}
		cursorAt, cursorID = &at, &id
	}

	var status *string
	if filter.Status != "" {
		value := filter.Status.String()
		status = &value
	}
	var queue *string
	if filter.Queue != "" {
		value := filter.Queue
		queue = &value
	}

	// Matches jobs_scope_keyset_idx when status is NULL, and
	// jobs_scope_status_keyset_idx when it is not. The extra row is requested
	// so the presence of a next page is a fact rather than a guess.
	//
	// payload is deliberately absent from the projection. It is not merely
	// dropped after the scan: it is never read out of PostgreSQL at all, so a
	// page of large payloads costs nothing on the wire between the API and the
	// database either.
	rows, err := s.pool.Query(ctx, `
		SELECT id, queue, job_type, status, priority, max_attempts,
		       timeout_seconds, required_capabilities, scheduled_at,
		       available_at, cancel_requested_at, replayed_from_job_id,
		       created_at, updated_at
		FROM jobs
		WHERE scope = $1
		  AND ($2::text IS NULL OR status = $2)
		  AND ($3::text IS NULL OR queue = $3)
		  AND ($4::timestamptz IS NULL OR (created_at, id) < ($4, $5))
		ORDER BY created_at DESC, id DESC
		LIMIT $6`, scope, status, queue, cursorAt, cursorID, limit+1)
	if err != nil {
		return JobPage{}, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	page := JobPage{Jobs: make([]JobSummary, 0, limit)}
	for rows.Next() {
		var job JobSummary
		if err := rows.Scan(&job.ID, &job.Queue, &job.Type, &job.Status,
			&job.Priority, &job.MaxAttempts, &job.TimeoutSeconds,
			&job.RequiredCapabilities, &job.ScheduledAt, &job.AvailableAt,
			&job.CancelRequestedAt, &job.ReplayedFromJobID,
			&job.CreatedAt, &job.UpdatedAt); err != nil {
			return JobPage{}, fmt.Errorf("scan job summary: %w", err)
		}
		page.Jobs = append(page.Jobs, job)
	}
	if err := rows.Err(); err != nil {
		return JobPage{}, fmt.Errorf("iterate jobs: %w", err)
	}

	if len(page.Jobs) > limit {
		last := page.Jobs[limit-1]
		page.Jobs = page.Jobs[:limit]
		page.NextCursor = EncodeJobCursor(last.CreatedAt, last.ID)
	}
	return page, nil
}

// Attempt is one row of a job's attempt timeline.
//
// It deliberately carries no worker_session_id, lease id, or
// outcome_request_id. Those are the control plane's fencing identifiers:
// every internal worker-control route except registration is unauthenticated
// and trusts the session identity a request already carries (ADR-0014), so
// publishing one on an authenticated public read would hand a caller an
// identifier that surface treats as authority.
type Attempt struct {
	ID            uuid.UUID
	AttemptNumber int
	Status        string
	WorkerID      uuid.UUID
	WorkerName    string
	CreatedAt     time.Time
	StartedAt     *time.Time
	FinishedAt    *time.Time
	TimeoutAt     *time.Time
	FailureClass  *string
	ErrorCode     *string
	ErrorMessage  *string
	RetryDelayMS  *int64
	RetryAt       *time.Time
}

// ListAttempts returns one job's full attempt timeline, oldest attempt first.
//
// It is deliberately unpaginated. An abandoned attempt still consumes the
// attempt budget (ADR-0009) and jobs.max_attempts is CHECK-constrained to at
// most 100, so a job's timeline is bounded by the schema itself rather than by
// a page size. Served by job_attempts' UNIQUE (job_id, attempt_number) from
// migration 0002; no index was added for it.
//
// The existence check and the attempt read are ONE statement, not two. Asking
// "does this job exist in my scope?" and then "what are its attempts?"
// separately would race: between the two, nothing prevents a different answer,
// and the two-query version would have to guess which one to believe. A LEFT
// JOIN answers both from one snapshot -- no rows at all means no such job in
// this scope, while one row with a NULL attempt id means a real job that has
// never been attempted.
func (s *Store) ListAttempts(ctx context.Context, scope string, jobID uuid.UUID) ([]Attempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT a.id, a.attempt_number, a.status, a.worker_id, w.name,
		       a.created_at, a.started_at, a.finished_at, a.timeout_at,
		       a.failure_class, a.error_code, a.error_message,
		       a.retry_delay_ms, a.retry_at
		FROM jobs j
		LEFT JOIN job_attempts a ON a.job_id = j.id AND a.scope = j.scope
		LEFT JOIN workers w ON w.id = a.worker_id AND w.scope = a.scope
		WHERE j.id = $1 AND j.scope = $2
		ORDER BY a.attempt_number ASC`, jobID, scope)
	if err != nil {
		return nil, fmt.Errorf("list job attempts: %w", err)
	}
	defer rows.Close()

	attempts := make([]Attempt, 0, 4)
	found := false
	for rows.Next() {
		found = true
		var (
			attempt    Attempt
			id         *uuid.UUID
			number     *int
			status     *string
			workerID   *uuid.UUID
			workerName *string
			createdAt  *time.Time
		)
		if err := rows.Scan(&id, &number, &status, &workerID, &workerName,
			&createdAt, &attempt.StartedAt, &attempt.FinishedAt, &attempt.TimeoutAt,
			&attempt.FailureClass, &attempt.ErrorCode, &attempt.ErrorMessage,
			&attempt.RetryDelayMS, &attempt.RetryAt); err != nil {
			return nil, fmt.Errorf("scan job attempt: %w", err)
		}
		// The LEFT JOIN's one all-NULL row: a real job with no attempts yet.
		if id == nil {
			continue
		}
		attempt.ID = *id
		attempt.AttemptNumber = *number
		attempt.Status = *status
		attempt.WorkerID = *workerID
		if workerName != nil {
			attempt.WorkerName = *workerName
		}
		attempt.CreatedAt = *createdAt
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate job attempts: %w", err)
	}
	if !found {
		return nil, ErrJobNotFound
	}
	return attempts, nil
}

// QueueDepth counts one queue's non-terminal work, per status.
//
// Every key in NonTerminalStatuses() is always present, zero-filled, so a
// client never has to distinguish "no jobs in this status" from "this server
// did not report this status".
type QueueDepth map[Status]int

// Queue is one row of GET /v1/queues.
type Queue struct {
	Name string
	// WorkerGroup routes this queue's work to a worker group.
	WorkerGroup string
	// MaxConcurrency is a QUEUE-WIDE limit shared by every scope, unlike
	// Depth, which counts only the authenticated caller's own jobs. The two
	// numbers are therefore not comparable, and the HTTP layer says so.
	MaxConcurrency int
	Depth          QueueDepth
}

// NonTerminalStatuses lists every status a job can still leave.
//
// It is derived from Status.Terminal() rather than written out, so a status
// added to AllStatuses() cannot be silently omitted here. The partial index
// jobs_scope_queue_depth_idx hardcodes the same set in SQL -- it must, because
// a partial index predicate cannot be parameterized -- and
// TestReadAPI_QueueDepthStatusesMatchTheIndexPredicate asserts the two agree.
func NonTerminalStatuses() []Status {
	statuses := make([]Status, 0, len(AllStatuses()))
	for _, status := range AllStatuses() {
		if !status.Terminal() {
			statuses = append(statuses, status)
		}
	}
	return statuses
}

// ListQueues returns every queue with this scope's non-terminal depth.
//
// It is deliberately unpaginated: no API creates a queue, so the table holds
// exactly the rows an operator provisioned by migration or by direct SQL. That
// bound is a property of the current system rather than a guarantee, and it is
// recorded as a limitation in docs/CURRENT_STATE.md rather than defended by a
// page size that has nothing to bound.
//
// Depth counts only non-terminal work. Lifetime totals are deliberately absent:
// counting every job a queue has ever held is a scan of unbounded history, and
// the question it answers belongs to metrics rather than to a live listing.
//
// The non-terminal status list is embedded in the SQL as literals rather than
// passed as a bind parameter, so the planner can prove the predicate matches
// jobs_scope_queue_depth_idx's own. A parameterized `status = ANY($2)` is not
// provably a subset of the index predicate at plan time, and would silently
// fall back to a sequential scan of every job ever submitted.
func (s *Store) ListQueues(ctx context.Context, scope string) ([]Queue, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT q.name, q.worker_group, q.max_concurrency, d.status, d.count
		FROM queues q
		LEFT JOIN (
			SELECT queue, status, count(*) AS count
			FROM jobs
			WHERE scope = $1
			  AND status IN ('PENDING', 'QUEUED', 'LEASED', 'RUNNING',
			                 'RETRY_WAIT', 'CANCEL_REQUESTED')
			GROUP BY queue, status
		) d ON d.queue = q.name
		ORDER BY q.name ASC`, scope)
	if err != nil {
		return nil, fmt.Errorf("list queues: %w", err)
	}
	defer rows.Close()

	ordered := make([]string, 0, 8)
	byName := make(map[string]*Queue, 8)
	for rows.Next() {
		var (
			name        string
			workerGroup string
			concurrency int
			status      *string
			count       *int
		)
		if err := rows.Scan(&name, &workerGroup, &concurrency, &status, &count); err != nil {
			return nil, fmt.Errorf("scan queue: %w", err)
		}
		queue, seen := byName[name]
		if !seen {
			depth := make(QueueDepth, len(NonTerminalStatuses()))
			for _, nonTerminal := range NonTerminalStatuses() {
				depth[nonTerminal] = 0
			}
			queue = &Queue{
				Name: name, WorkerGroup: workerGroup,
				MaxConcurrency: concurrency, Depth: depth,
			}
			byName[name] = queue
			ordered = append(ordered, name)
		}
		if status != nil && count != nil {
			queue.Depth[Status(*status)] = *count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queues: %w", err)
	}

	queues := make([]Queue, 0, len(ordered))
	for _, name := range ordered {
		queues = append(queues, *byName[name])
	}
	return queues, nil
}
