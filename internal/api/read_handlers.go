package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// WorkerReads serves GET /v1/workers. See internal/workers.Store.
//
// It is a separate interface from WorkerControl, which gates the eight fenced
// worker-control routes, because this one is a public authenticated read and
// those are internal plumbing. A deployment always wires both from the same
// *workers.Store; keeping the interfaces apart is what stops a public read
// from quietly acquiring the ability to call a fenced transition.
type WorkerReads interface {
	ListWorkers(ctx context.Context, scope, cursor string, limit int) (workers.WorkerPage, error)
}

// WithWorkerReads supplies GET /v1/workers its dependency.
//
// Registered unconditionally, exactly like WithResults and for the same
// reason: PROJECT_SPEC.md section 4 item 7 ("inspect worker capacity and
// health") makes this a core public route, not opt-in admin plumbing. A server
// built without calling this answers the same 401 every other public route
// does for an unauthenticated caller and a sanitized 500 for an authenticated
// one -- never a 404 that would make a real route look like it does not exist.
func (s *Server) WithWorkerReads(reads WorkerReads) *Server {
	s.workerReads = reads
	return s
}

// ---------------------------------------------------------------------------
// GET /v1/jobs
// ---------------------------------------------------------------------------

// JobSummaryResponse is one row of a job listing.
//
// It is JobResponse minus payload, and the omission is the point. A list
// endpoint that returned payloads would let one request pull an unbounded
// amount of user data -- the same rule DLQEntryResponse already states. The
// payload is never even read out of PostgreSQL for this route, so it is absent
// from the wire in both directions. A single job, payload included, stays
// readable through GET /v1/jobs/{job_id}.
type JobSummaryResponse struct {
	ID                   string     `json:"id"`
	Queue                string     `json:"queue"`
	JobType              string     `json:"job_type"`
	Status               string     `json:"status"`
	Priority             int        `json:"priority"`
	MaxAttempts          int        `json:"max_attempts"`
	TimeoutSeconds       int        `json:"timeout_seconds"`
	RequiredCapabilities []string   `json:"required_capabilities"`
	ScheduledAt          *time.Time `json:"scheduled_at"`
	AvailableAt          time.Time  `json:"available_at"`
	CancelRequestedAt    *time.Time `json:"cancel_requested_at"`
	ReplayedFromJobID    *string    `json:"replayed_from_job_id"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

// JobPageResponse is one bounded page of jobs, newest first.
type JobPageResponse struct {
	Jobs []JobSummaryResponse `json:"jobs"`
	// NextCursor is absent on the last page. A full page is not proof that
	// more exist; the presence of a cursor is.
	NextCursor string `json:"next_cursor,omitempty"`
}

func toJobSummaryResponse(job jobs.JobSummary) JobSummaryResponse {
	caps := job.RequiredCapabilities
	if caps == nil {
		caps = []string{} // an empty JSON array, never null
	}
	response := JobSummaryResponse{
		ID:                   job.ID.String(),
		Queue:                job.Queue,
		JobType:              job.Type,
		Status:               job.Status.String(),
		Priority:             job.Priority,
		MaxAttempts:          job.MaxAttempts,
		TimeoutSeconds:       job.TimeoutSeconds,
		RequiredCapabilities: caps,
		AvailableAt:          job.AvailableAt.UTC(),
		CreatedAt:            job.CreatedAt.UTC(),
		UpdatedAt:            job.UpdatedAt.UTC(),
	}
	if job.ScheduledAt != nil {
		utc := job.ScheduledAt.UTC()
		response.ScheduledAt = &utc
	}
	if job.CancelRequestedAt != nil {
		utc := job.CancelRequestedAt.UTC()
		response.CancelRequestedAt = &utc
	}
	if job.ReplayedFromJobID != nil {
		id := job.ReplayedFromJobID.String()
		response.ReplayedFromJobID = &id
	}
	return response
}

// handleListJobs serves GET /v1/jobs.
//
// Responses:
//
//	200 one bounded page
//	422 the limit, status, queue, or cursor was invalid
//	401 no valid API key was presented
func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	scope, ok := s.scopeOrUnauthorized(w, r)
	if !ok {
		return
	}

	query := r.URL.Query()
	limit, ok := s.pageLimit(w, r, query.Get("limit"))
	if !ok {
		return
	}

	filter := jobs.JobFilter{
		Status: jobs.Status(query.Get("status")),
		Queue:  query.Get("queue"),
	}

	page, err := s.jobs.ListJobs(r.Context(), scope, filter, query.Get("cursor"), limit)
	switch {
	case err == nil:
		summaries := make([]JobSummaryResponse, 0, len(page.Jobs))
		for _, job := range page.Jobs {
			summaries = append(summaries, toJobSummaryResponse(job))
		}
		writeJSON(w, s.log, http.StatusOK, JobPageResponse{
			Jobs: summaries, NextCursor: page.NextCursor,
		})
	case errors.Is(err, jobs.ErrInvalidCursor):
		s.writeFieldError(w, r, CodeInvalidCursor, "cursor",
			"must be a cursor returned by a previous page of this endpoint")
	case errors.Is(err, jobs.ErrInvalidJobFilter):
		// The field is named from what actually failed, not guessed: a caller
		// who sent both a bad status and a good queue must be told which one
		// this API refused.
		field, message := "status", "must be one of the documented job statuses"
		if filter.Status.Valid() || filter.Status == "" {
			field, message = "queue", "must be a valid queue name"
		}
		s.writeFieldError(w, r, CodeValidationFailed, field, message)
	default:
		s.internalError(w, r, "list jobs", err)
	}
}

// pageLimit parses and bounds a ?limit= parameter, writing the 422 itself when
// it is out of range. Shared by every listing route so one endpoint cannot
// drift to a different bound than the document states for all of them.
func (s *Server) pageLimit(w http.ResponseWriter, r *http.Request, raw string) (int, bool) {
	if raw == "" {
		return jobs.DefaultPageSize, true
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed < 1 || parsed > jobs.MaxPageSize {
		s.writeFieldError(w, r, CodeValidationFailed, "limit",
			"must be an integer between 1 and "+strconv.Itoa(jobs.MaxPageSize))
		return 0, false
	}
	return parsed, true
}

// ---------------------------------------------------------------------------
// GET /v1/jobs/{job_id}/attempts
// ---------------------------------------------------------------------------

// AttemptResponse is one entry in a job's attempt timeline.
//
// It deliberately carries no worker_session_id, no lease id, and no
// outcome_request_id. Every worker-control route except registration is
// unauthenticated and trusts the session identity the request already carries
// (ADR-0014), so a session id published on an authenticated public read would
// be an identifier that surface treats as authority. worker_id is included:
// it names a logical worker, matches what GET /v1/workers reports, and
// authorizes nothing.
type AttemptResponse struct {
	ID            string `json:"id"`
	AttemptNumber int    `json:"attempt_number"`
	Status        string `json:"status"`
	WorkerID      string `json:"worker_id"`
	WorkerName    string `json:"worker_name"`

	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
	// TimeoutAt is the persisted per-attempt execution deadline, stamped once
	// when the attempt started. Lease renewal never moves it.
	TimeoutAt *time.Time `json:"timeout_at"`

	FailureClass *string `json:"failure_class"`
	ErrorCode    *string `json:"error_code"`
	ErrorMessage *string `json:"error_message"`
	// RetryDelayMS and RetryAt are set only when this attempt's outcome put
	// the job into RETRY_WAIT. They are the committed decision, never
	// recomputed: recomputing would draw fresh jitter and report a different
	// instant on every read.
	RetryDelayMS *int64     `json:"retry_delay_ms"`
	RetryAt      *time.Time `json:"retry_at"`
}

// AttemptListResponse is one job's full attempt timeline, oldest first.
//
// There is no cursor: the timeline is bounded by jobs.max_attempts, which the
// schema CHECK-constrains to at most 100, so it is not a page of anything.
type AttemptListResponse struct {
	Attempts []AttemptResponse `json:"attempts"`
}

func toAttemptResponse(attempt jobs.Attempt) AttemptResponse {
	response := AttemptResponse{
		ID:            attempt.ID.String(),
		AttemptNumber: attempt.AttemptNumber,
		Status:        attempt.Status,
		WorkerID:      attempt.WorkerID.String(),
		WorkerName:    attempt.WorkerName,
		CreatedAt:     attempt.CreatedAt.UTC(),
		FailureClass:  attempt.FailureClass,
		ErrorCode:     attempt.ErrorCode,
		ErrorMessage:  attempt.ErrorMessage,
		RetryDelayMS:  attempt.RetryDelayMS,
	}
	response.StartedAt = utcOrNil(attempt.StartedAt)
	response.FinishedAt = utcOrNil(attempt.FinishedAt)
	response.TimeoutAt = utcOrNil(attempt.TimeoutAt)
	response.RetryAt = utcOrNil(attempt.RetryAt)
	return response
}

func utcOrNil(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	utc := t.UTC()
	return &utc
}

// handleListJobAttempts serves GET /v1/jobs/{job_id}/attempts.
//
// Responses:
//
//	200 the timeline, possibly empty for a job that has never been attempted
//	404 no such job in the caller's scope, or a malformed id
//	401 no valid API key was presented
func (s *Server) handleListJobAttempts(w http.ResponseWriter, r *http.Request) {
	scope, ok := s.scopeOrUnauthorized(w, r)
	if !ok {
		return
	}

	id, err := uuid.Parse(r.PathValue("job_id"))
	if err != nil {
		// The same anti-oracle rule handleGetJob applies: a malformed id and
		// an id that is simply not the caller's must be indistinguishable.
		writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "job not found", nil)
		return
	}

	attempts, err := s.jobs.ListAttempts(r.Context(), scope, id)
	switch {
	case err == nil:
		items := make([]AttemptResponse, 0, len(attempts))
		for _, attempt := range attempts {
			items = append(items, toAttemptResponse(attempt))
		}
		writeJSON(w, s.log, http.StatusOK, AttemptListResponse{Attempts: items})
	case errors.Is(err, jobs.ErrJobNotFound):
		writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "job not found", nil)
	default:
		s.internalError(w, r, "list job attempts", err)
	}
}

// ---------------------------------------------------------------------------
// GET /v1/workers
// ---------------------------------------------------------------------------

// WorkerResponse is one logical worker joined to its most recent session.
//
// The session reported is the latest one whatever its status, not the current
// one: a crashed worker's session is UNHEALTHY and a replaced worker's is
// OFFLINE, and both are exactly what an operator is looking for.
type WorkerResponse struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	WorkerGroup string `json:"worker_group"`
	Hostname    string `json:"hostname"`

	ConcurrencyLimit  int      `json:"concurrency_limit"`
	Capabilities      []string `json:"capabilities"`
	SupportedJobTypes []string `json:"supported_job_types"`

	RegisteredAt    time.Time  `json:"registered_at"`
	LastHeartbeatAt time.Time  `json:"last_heartbeat_at"`
	EndedAt         *time.Time `json:"ended_at"`
	// HeartbeatAgeSeconds is measured by PostgreSQL. This endpoint reports it
	// and deliberately does not judge it: the staleness threshold that decides
	// whether a worker is dead belongs to the reconciler's configuration, and
	// an API that hardcoded its own copy would disagree with the component
	// that actually acts on it.
	HeartbeatAgeSeconds float64 `json:"heartbeat_age_seconds"`
	// ActiveLeases is the live occupancy of ConcurrencyLimit.
	ActiveLeases int `json:"active_leases"`
}

// WorkerPageResponse is one bounded page of workers, by name.
type WorkerPageResponse struct {
	Workers    []WorkerResponse `json:"workers"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

func toWorkerResponse(worker workers.WorkerSummary) WorkerResponse {
	capabilities := worker.Capabilities
	if capabilities == nil {
		capabilities = []string{}
	}
	jobTypes := worker.SupportedJobTypes
	if jobTypes == nil {
		jobTypes = []string{}
	}
	return WorkerResponse{
		ID:                  worker.ID.String(),
		Name:                worker.Name,
		Status:              string(worker.Status),
		WorkerGroup:         worker.WorkerGroup,
		Hostname:            worker.Hostname,
		ConcurrencyLimit:    worker.ConcurrencyLimit,
		Capabilities:        capabilities,
		SupportedJobTypes:   jobTypes,
		RegisteredAt:        worker.RegisteredAt.UTC(),
		LastHeartbeatAt:     worker.LastHeartbeatAt.UTC(),
		EndedAt:             utcOrNil(worker.EndedAt),
		HeartbeatAgeSeconds: worker.HeartbeatAgeSeconds,
		ActiveLeases:        worker.ActiveLeases,
	}
}

// handleListWorkers serves GET /v1/workers.
//
// Responses:
//
//	200 one bounded page
//	422 the limit or cursor was invalid
//	401 no valid API key was presented
func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	scope, ok := s.scopeOrUnauthorized(w, r)
	if !ok {
		return
	}
	query := r.URL.Query()
	limit, ok := s.pageLimit(w, r, query.Get("limit"))
	if !ok {
		return
	}

	// The wiring check comes AFTER parameter validation, deliberately, and
	// after the same check on /v1/jobs by construction -- that route validates
	// before it dereferences its store too. "limit must be between 1 and 100"
	// is a fact about the request; it must not become "internal error" because
	// of how this particular process was assembled, or a caller debugging their
	// own bad query string would be sent looking for a server fault.
	if s.workerReads == nil {
		// Authenticated, but this server instance was never wired with a
		// worker read store -- an operator misconfiguration, reported the same
		// way a missing results store is, never a 404 that would make a real
		// route look like it does not exist.
		s.internalError(w, r, "list workers", errors.New("no worker read store configured"))
		return
	}

	page, err := s.workerReads.ListWorkers(r.Context(), scope, query.Get("cursor"), limit)
	switch {
	case err == nil:
		items := make([]WorkerResponse, 0, len(page.Workers))
		for _, worker := range page.Workers {
			items = append(items, toWorkerResponse(worker))
		}
		writeJSON(w, s.log, http.StatusOK, WorkerPageResponse{
			Workers: items, NextCursor: page.NextCursor,
		})
	case errors.Is(err, workers.ErrInvalidCursor):
		s.writeFieldError(w, r, CodeInvalidCursor, "cursor",
			"must be a cursor returned by a previous page of this endpoint")
	default:
		s.internalError(w, r, "list workers", err)
	}
}

// ---------------------------------------------------------------------------
// GET /v1/queues
// ---------------------------------------------------------------------------

// QueueResponse is one queue and this scope's non-terminal depth in it.
type QueueResponse struct {
	Name        string `json:"name"`
	WorkerGroup string `json:"worker_group"`
	// MaxConcurrency is a QUEUE-WIDE execution limit shared by every scope,
	// while Depth counts only the authenticated caller's own jobs. The two are
	// deliberately not comparable, and no ratio of them is reported: a global
	// in-flight figure would leak other scopes' load to this caller.
	MaxConcurrency int `json:"max_concurrency"`
	// Depth carries every non-terminal status as a key, always, zero-filled,
	// so a client never has to tell "no jobs in this status" apart from "this
	// server did not report this status". Terminal statuses are absent: their
	// counts grow without bound over a deployment's life and answer a
	// historical question rather than an operational one.
	Depth map[string]int `json:"depth"`
}

// QueueListResponse is every queue, by name.
//
// There is no cursor. No API creates a queue, so this table holds exactly the
// rows an operator provisioned; that bound is a property of the system today
// rather than a guarantee, and it is recorded as such in docs/CURRENT_STATE.md.
type QueueListResponse struct {
	Queues []QueueResponse `json:"queues"`
}

func toQueueResponse(queue jobs.Queue) QueueResponse {
	depth := make(map[string]int, len(queue.Depth))
	for status, count := range queue.Depth {
		depth[status.String()] = count
	}
	return QueueResponse{
		Name:           queue.Name,
		WorkerGroup:    queue.WorkerGroup,
		MaxConcurrency: queue.MaxConcurrency,
		Depth:          depth,
	}
}

// handleListQueues serves GET /v1/queues.
//
// Responses:
//
//	200 every queue, with this scope's non-terminal depth
//	401 no valid API key was presented
func (s *Server) handleListQueues(w http.ResponseWriter, r *http.Request) {
	scope, ok := s.scopeOrUnauthorized(w, r)
	if !ok {
		return
	}

	queues, err := s.jobs.ListQueues(r.Context(), scope)
	if err != nil {
		s.internalError(w, r, "list queues", err)
		return
	}
	items := make([]QueueResponse, 0, len(queues))
	for _, queue := range queues {
		items = append(items, toQueueResponse(queue))
	}
	writeJSON(w, s.log, http.StatusOK, QueueListResponse{Queues: items})
}
