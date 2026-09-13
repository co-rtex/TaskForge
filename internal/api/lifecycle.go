package api

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/jobs"
)

// CancelResponse is the committed public cancellation decision.
//
// It reports the resulting status rather than answering 204, because the two
// outcomes are genuinely different and a caller has to be able to tell them
// apart: CANCELED means it is over, while CANCEL_REQUESTED means an attempt
// still holds authority that is being withdrawn.
type CancelResponse struct {
	JobID             string    `json:"job_id"`
	Status            string    `json:"status"`
	CancelRequestedAt time.Time `json:"cancel_requested_at"`
	// AlreadyRequested is true when this job was already cancelling or canceled.
	// Cancellation is keyed by scope plus job id, so repeating it is idempotent.
	AlreadyRequested bool `json:"already_requested"`
}

// handleCancelJob cancels one job, or begins withdrawing authority from the
// attempt executing it.
//
// Responses:
//
//	200 the cancellation decision, including an idempotent repeat
//	404 no such job in the caller's scope
//	409 the job already reached SUCCEEDED or DEAD_LETTERED
func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("job_id"))
	if err != nil {
		// 404, not 400: matching handleGetJob means a malformed id and someone
		// else's id are indistinguishable, so ids cannot be probed.
		writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "job not found", nil)
		return
	}

	result, err := s.jobs.RequestCancel(r.Context(), s.cfg.DevScope, id)
	switch {
	case err == nil:
		s.log.Info("job cancellation requested",
			slog.String("request_id", RequestIDFrom(r.Context())),
			slog.String("job_id", result.JobID.String()),
			slog.String("status", result.Status.String()),
			slog.Bool("already_requested", result.AlreadyRequested))
		writeJSON(w, s.log, http.StatusOK, CancelResponse{
			JobID:             result.JobID.String(),
			Status:            result.Status.String(),
			CancelRequestedAt: result.CancelRequestedAt.UTC(),
			AlreadyRequested:  result.AlreadyRequested,
		})
	case errors.Is(err, jobs.ErrJobNotFound):
		writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "job not found", nil)
	case errors.Is(err, jobs.ErrJobNotCancelable):
		writeError(w, r, s.log, http.StatusConflict, CodeNotCancelable,
			"the job already reached a terminal outcome and cannot be canceled", nil)
	case isDeadlineExhausted(err):
		s.writeDeadlineAmbiguity(w, r, "cancel job",
			"repeat the identical cancellation request for the same job id; "+
				"cancellation is keyed by scope and job id alone, so a repeat "+
				"returns the current decision and never cancels twice")
	default:
		s.internalError(w, r, "cancel job", err)
	}
}

// ReplayResponse is the outcome of a replay or operator retry.
type ReplayResponse struct {
	// OriginalJobID is the terminal job, which this operation never mutates.
	OriginalJobID string `json:"original_job_id"`
	// Replacement is the new job: a distinct id, a fresh attempt budget, and
	// immediate eligibility.
	Replacement JobResponse `json:"replacement"`
	// Replayed is true when this replay identity had already created the
	// replacement, which is what makes an ambiguous response safe to retry.
	Replayed bool `json:"replayed"`
}

// handleReplayJob serves both POST /v1/jobs/{job_id}/retry and
// POST /v1/dlq/{job_id}/replay.
//
// One handler, one service, one idempotency namespace. "Retry this job" and
// "replay this DLQ entry" are the same operation described by two operator
// vocabularies, and implementing them separately is how they would eventually
// answer differently for the same request.
//
// Responses:
//
//	201 the replacement job was created
//	200 this replay identity had already created it
//	404 no such job in the caller's scope
//	409 the job is not dead-lettered, so there is nothing to replay
//	422 the Idempotency-Key header was missing or invalid
func (s *Server) handleReplayJob(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if err := jobs.ValidateIdempotencyKey(key); err != nil {
		s.writeValidationError(w, r, err)
		return
	}
	id, err := uuid.Parse(r.PathValue("job_id"))
	if err != nil {
		writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "job not found", nil)
		return
	}

	result, err := s.jobs.Replay(r.Context(), s.cfg.DevScope, id, key)
	switch {
	case err == nil:
		status := http.StatusCreated
		if result.Replayed {
			status = http.StatusOK
		}
		s.log.Info("dead-lettered job replayed",
			slog.String("request_id", RequestIDFrom(r.Context())),
			slog.String("original_job_id", result.OriginalJobID.String()),
			slog.String("replacement_job_id", result.Replacement.ID.String()),
			slog.Bool("replayed", result.Replayed))
		writeJSON(w, s.log, status, ReplayResponse{
			OriginalJobID: result.OriginalJobID.String(),
			Replacement:   toJobResponse(result.Replacement),
			Replayed:      result.Replayed,
		})
	case errors.Is(err, jobs.ErrJobNotFound):
		writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "job not found", nil)
	case errors.Is(err, jobs.ErrNotDeadLettered):
		writeError(w, r, s.log, http.StatusConflict, CodeNotDeadLettered,
			"only a dead-lettered job can be replayed", nil)
	case isDeadlineExhausted(err):
		s.writeDeadlineAmbiguity(w, r, "replay job",
			"repeat the identical request on the same path with the same "+
				"Idempotency-Key; a fresh key after an ambiguous response is "+
				"forbidden, because it is a different replay identity and would "+
				"create a second replacement job")
	default:
		s.internalError(w, r, "replay job", err)
	}
}

// DLQEntryResponse is one operator-visible dead-letter row.
//
// It deliberately carries no payload. A list endpoint that returned payloads
// would let one request pull an unbounded amount of user data; a single job is
// still readable through GET /v1/jobs/{job_id}.
type DLQEntryResponse struct {
	ID          string    `json:"id"`
	JobID       string    `json:"job_id"`
	Queue       string    `json:"queue"`
	JobType     string    `json:"job_type"`
	Priority    int       `json:"priority"`
	MaxAttempts int       `json:"max_attempts"`
	Reason      string    `json:"reason"`
	CreatedAt   time.Time `json:"created_at"`

	TerminalAttemptID *string `json:"terminal_attempt_id"`
	AttemptNumber     *int    `json:"attempt_number"`
	AttemptStatus     *string `json:"attempt_status"`
	FailureClass      *string `json:"failure_class"`
	ErrorCode         *string `json:"error_code"`
	ErrorMessage      *string `json:"error_message"`
	// ReplayCount can exceed one: different idempotency keys deliberately create
	// different replacement jobs.
	ReplayCount int `json:"replay_count"`
}

// DLQPageResponse is one bounded page, newest first.
type DLQPageResponse struct {
	Entries []DLQEntryResponse `json:"entries"`
	// NextCursor is empty on the last page. A full page is not proof that more
	// exist; the absence of a cursor is.
	NextCursor string `json:"next_cursor,omitempty"`
}

// handleListDLQ serves GET /v1/dlq.
//
// Responses:
//
//	200 one bounded page
//	422 the limit or cursor was invalid
func (s *Server) handleListDLQ(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit := jobs.DefaultDLQPageSize
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > jobs.MaxDLQPageSize {
			s.writeFieldError(w, r, CodeValidationFailed, "limit",
				"must be an integer between 1 and "+strconv.Itoa(jobs.MaxDLQPageSize))
			return
		}
		limit = parsed
	}

	page, err := s.jobs.ListDLQ(r.Context(), s.cfg.DevScope, query.Get("cursor"), limit)
	switch {
	case err == nil:
		entries := make([]DLQEntryResponse, 0, len(page.Entries))
		for _, entry := range page.Entries {
			entries = append(entries, toDLQEntryResponse(entry))
		}
		writeJSON(w, s.log, http.StatusOK, DLQPageResponse{
			Entries: entries, NextCursor: page.NextCursor,
		})
	case errors.Is(err, jobs.ErrInvalidCursor):
		s.writeFieldError(w, r, CodeInvalidCursor, "cursor",
			"must be a cursor returned by a previous page of this endpoint")
	default:
		s.internalError(w, r, "list dead-letter queue", err)
	}
}

func toDLQEntryResponse(entry jobs.DLQEntry) DLQEntryResponse {
	response := DLQEntryResponse{
		ID:            entry.ID.String(),
		JobID:         entry.JobID.String(),
		Queue:         entry.Queue,
		JobType:       entry.JobType,
		Priority:      entry.Priority,
		MaxAttempts:   entry.MaxAttempts,
		Reason:        entry.Reason.String(),
		CreatedAt:     entry.CreatedAt.UTC(),
		AttemptNumber: entry.AttemptNumber,
		AttemptStatus: entry.AttemptStatus,
		FailureClass:  entry.FailureClass,
		ErrorCode:     entry.ErrorCode,
		ErrorMessage:  entry.ErrorMessage,
		ReplayCount:   entry.ReplayCount,
	}
	if entry.TerminalAttemptID != nil {
		id := entry.TerminalAttemptID.String()
		response.TerminalAttemptID = &id
	}
	return response
}

// writeDeadlineAmbiguity renders the sanitized 503 for a public mutating route
// whose durable outcome is unknown.
//
// A deadline can elapse while acquiring a lock, while executing a statement, or
// during COMMIT, and a COMMIT that is cut short is ambiguous — so this never
// claims nothing was committed. What it can promise is that repeating the
// identical request is safe, and it says which identity makes that true for the
// endpoint the caller actually used. That is why the guidance is a parameter
// rather than one shared sentence: the worker-control surface already learned
// that a single string cannot be correct for four different operations.
func (s *Server) writeDeadlineAmbiguity(w http.ResponseWriter, r *http.Request, op, guidance string) {
	s.log.Warn("request exhausted its deadline",
		slog.String("request_id", RequestIDFrom(r.Context())),
		slog.String("op", op))
	writeError(w, r, s.log, http.StatusServiceUnavailable, CodeServiceUnavailable,
		"the request exceeded its deadline before the durable outcome was known; "+
			guidance, nil)
}

// writeFieldError renders a single-field 422 with a stable code.
func (s *Server) writeFieldError(w http.ResponseWriter, r *http.Request, code, field, message string) {
	writeError(w, r, s.log, http.StatusUnprocessableEntity, code,
		"the request was rejected by validation",
		[]jobs.FieldError{{Field: field, Message: message}})
}
