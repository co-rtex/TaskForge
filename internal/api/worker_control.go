package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// WorkerControl is the internal control-plane surface used by a DB-less worker.
type WorkerControl interface {
	Register(context.Context, string, workers.Registration) (workers.Session, error)
	Claim(context.Context, string, workers.ClaimRequest) (workers.ClaimResult, error)
	Start(context.Context, string, workers.Fence) error
	Succeed(context.Context, string, workers.Fence) error
}

type registerWorkerSessionRequest struct {
	WorkerName        string   `json:"worker_name"`
	Hostname          string   `json:"hostname"`
	WorkerGroup       string   `json:"worker_group"`
	ConcurrencyLimit  int      `json:"concurrency_limit"`
	Capabilities      []string `json:"capabilities"`
	SupportedJobTypes []string `json:"supported_job_types"`
}

// WorkerSessionResponse is the internal representation returned to a worker.
type WorkerSessionResponse struct {
	WorkerID          string    `json:"worker_id"`
	WorkerSessionID   string    `json:"worker_session_id"`
	WorkerName        string    `json:"worker_name"`
	Hostname          string    `json:"hostname"`
	WorkerGroup       string    `json:"worker_group"`
	ConcurrencyLimit  int       `json:"concurrency_limit"`
	Capabilities      []string  `json:"capabilities"`
	SupportedJobTypes []string  `json:"supported_job_types"`
	Status            string    `json:"status"`
	RegisteredAt      time.Time `json:"registered_at"`
	LastHeartbeatAt   time.Time `json:"last_heartbeat_at"`
}

func toWorkerSessionResponse(session workers.Session) WorkerSessionResponse {
	return WorkerSessionResponse{
		WorkerID:          session.WorkerID.String(),
		WorkerSessionID:   session.ID.String(),
		WorkerName:        session.Name,
		Hostname:          session.Hostname,
		WorkerGroup:       session.WorkerGroup,
		ConcurrencyLimit:  session.ConcurrencyLimit,
		Capabilities:      emptyIfNil(session.Capabilities),
		SupportedJobTypes: emptyIfNil(session.SupportedJobTypes),
		Status:            string(session.Status),
		RegisteredAt:      session.RegisteredAt.UTC(),
		LastHeartbeatAt:   session.LastHeartbeatAt.UTC(),
	}
}

func (s *Server) handleRegisterWorkerSession(w http.ResponseWriter, r *http.Request) {
	sessionID, err := uuid.Parse(r.PathValue("worker_session_id"))
	if err != nil {
		s.writeWorkerValidation(w, r, []workers.FieldError{{Field: "worker_session_id", Message: "must be a UUID"}})
		return
	}

	var request registerWorkerSessionRequest
	if !s.decodeControlJSON(w, r, &request) {
		return
	}
	session, err := s.control.Register(r.Context(), s.cfg.DevScope, workers.Registration{
		SessionID:         sessionID,
		Name:              request.WorkerName,
		Hostname:          request.Hostname,
		WorkerGroup:       request.WorkerGroup,
		ConcurrencyLimit:  request.ConcurrencyLimit,
		Capabilities:      request.Capabilities,
		SupportedJobTypes: request.SupportedJobTypes,
	})
	if err != nil {
		s.writeWorkerControlError(w, r, "register worker session", err)
		return
	}
	s.log.Info("worker session registered",
		slog.String("request_id", RequestIDFrom(r.Context())),
		slog.String("worker_id", session.WorkerID.String()),
		slog.String("worker_session_id", session.ID.String()),
		slog.String("worker_group", session.WorkerGroup))
	writeJSON(w, s.log, http.StatusOK, toWorkerSessionResponse(session))
}

type claimRequest struct {
	WorkerID        string `json:"worker_id"`
	WorkerSessionID string `json:"worker_session_id"`
	ClaimRequestID  string `json:"claim_request_id"`
	Queue           string `json:"queue"`
}

// AssignmentResponse is the committed authoritative payload plus its fence.
type AssignmentResponse struct {
	JobID                string          `json:"job_id"`
	Queue                string          `json:"queue"`
	JobType              string          `json:"job_type"`
	Payload              json.RawMessage `json:"payload"`
	Priority             int             `json:"priority"`
	TimeoutSeconds       int             `json:"timeout_seconds"`
	RequiredCapabilities []string        `json:"required_capabilities"`
	AttemptID            string          `json:"attempt_id"`
	AttemptNumber        int             `json:"attempt_number"`
	LeaseID              string          `json:"lease_id"`
	LeaseExpiresAt       time.Time       `json:"lease_expires_at"`
	LeaseRemainingMillis int64           `json:"lease_remaining_milliseconds"`
	WorkerID             string          `json:"worker_id"`
	WorkerSessionID      string          `json:"worker_session_id"`
}

// ClaimResponse carries an explicit outcome because only two outcomes are safe
// reasons to delete an advisory broker notification.
type ClaimResponse struct {
	Outcome           string              `json:"outcome"`
	SafeToAcknowledge bool                `json:"safe_to_acknowledge"`
	Replayed          bool                `json:"replayed"`
	Assignment        *AssignmentResponse `json:"assignment,omitempty"`
}

func toClaimResponse(result workers.ClaimResult) ClaimResponse {
	response := ClaimResponse{
		Outcome:           string(result.Disposition),
		SafeToAcknowledge: result.SafeToAcknowledge(),
		Replayed:          result.Replayed,
	}
	if assignment := result.Assignment; assignment != nil {
		response.Assignment = &AssignmentResponse{
			JobID:                assignment.JobID.String(),
			Queue:                assignment.Queue,
			JobType:              assignment.JobType,
			Payload:              assignment.Payload,
			Priority:             assignment.Priority,
			TimeoutSeconds:       assignment.TimeoutSeconds,
			RequiredCapabilities: emptyIfNil(assignment.RequiredCapabilities),
			AttemptID:            assignment.AttemptID.String(),
			AttemptNumber:        assignment.AttemptNumber,
			LeaseID:              assignment.LeaseID.String(),
			LeaseExpiresAt:       assignment.LeaseExpiresAt.UTC(),
			LeaseRemainingMillis: assignment.LeaseRemaining.Milliseconds(),
			WorkerID:             assignment.WorkerID.String(),
			WorkerSessionID:      assignment.SessionID.String(),
		}
	}
	return response
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	var request claimRequest
	if !s.decodeControlJSON(w, r, &request) {
		return
	}
	req, fields := parseClaimRequest(request)
	if len(fields) > 0 {
		s.writeWorkerValidation(w, r, fields)
		return
	}
	result, err := s.control.Claim(r.Context(), s.cfg.DevScope, req)
	if err != nil {
		s.writeWorkerControlError(w, r, "claim job", err)
		return
	}
	if result.Assignment != nil {
		s.log.Info("job claimed",
			slog.String("request_id", RequestIDFrom(r.Context())),
			slog.String("job_id", result.Assignment.JobID.String()),
			slog.String("attempt_id", result.Assignment.AttemptID.String()),
			slog.String("lease_id", result.Assignment.LeaseID.String()),
			slog.Bool("replayed", result.Replayed))
	}
	writeJSON(w, s.log, http.StatusOK, toClaimResponse(result))
}

func parseClaimRequest(request claimRequest) (workers.ClaimRequest, []workers.FieldError) {
	workerID, _ := uuid.Parse(request.WorkerID)
	sessionID, _ := uuid.Parse(request.WorkerSessionID)
	claimID, _ := uuid.Parse(request.ClaimRequestID)
	req := workers.ClaimRequest{
		WorkerID: workerID, SessionID: sessionID, ClaimRequestID: claimID, Queue: request.Queue,
	}
	if err := workers.ValidateClaim(req); err != nil {
		var validation *workers.ValidationError
		if errors.As(err, &validation) {
			return req, validation.Fields
		}
	}
	return req, nil
}

type fenceRequest struct {
	JobID           string `json:"job_id"`
	LeaseID         string `json:"lease_id"`
	WorkerID        string `json:"worker_id"`
	WorkerSessionID string `json:"worker_session_id"`
}

func (s *Server) handleStartAttempt(w http.ResponseWriter, r *http.Request) {
	s.handleFencedTransition(w, r, "start attempt", s.control.Start)
}

func (s *Server) handleSucceedAttempt(w http.ResponseWriter, r *http.Request) {
	s.handleFencedTransition(w, r, "succeed attempt", s.control.Succeed)
}

func (s *Server) handleFencedTransition(
	w http.ResponseWriter,
	r *http.Request,
	op string,
	transition func(context.Context, string, workers.Fence) error,
) {
	var request fenceRequest
	if !s.decodeControlJSON(w, r, &request) {
		return
	}
	fence, fields := parseFence(r.PathValue("attempt_id"), request)
	if len(fields) > 0 {
		s.writeWorkerValidation(w, r, fields)
		return
	}
	if err := transition(r.Context(), s.cfg.DevScope, fence); err != nil {
		s.writeWorkerControlError(w, r, op, err)
		return
	}
	s.log.Info(op,
		slog.String("request_id", RequestIDFrom(r.Context())),
		slog.String("job_id", fence.JobID.String()),
		slog.String("attempt_id", fence.AttemptID.String()),
		slog.String("lease_id", fence.LeaseID.String()),
		slog.String("worker_id", fence.WorkerID.String()),
		slog.String("worker_session_id", fence.SessionID.String()))
	w.WriteHeader(http.StatusNoContent)
}

func parseFence(attempt string, request fenceRequest) (workers.Fence, []workers.FieldError) {
	inputs := []struct {
		field string
		value string
		dest  *uuid.UUID
	}{
		{"attempt_id", attempt, new(uuid.UUID)},
		{"job_id", request.JobID, new(uuid.UUID)},
		{"lease_id", request.LeaseID, new(uuid.UUID)},
		{"worker_id", request.WorkerID, new(uuid.UUID)},
		{"worker_session_id", request.WorkerSessionID, new(uuid.UUID)},
	}
	var fields []workers.FieldError
	for i := range inputs {
		id, err := uuid.Parse(inputs[i].value)
		if err != nil {
			fields = append(fields, workers.FieldError{Field: inputs[i].field, Message: "must be a UUID"})
			continue
		}
		*inputs[i].dest = id
	}
	return workers.Fence{
		AttemptID: *inputs[0].dest,
		JobID:     *inputs[1].dest,
		LeaseID:   *inputs[2].dest,
		WorkerID:  *inputs[3].dest,
		SessionID: *inputs[4].dest,
	}, fields
}

func (s *Server) decodeControlJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		var maxBytes *http.MaxBytesError
		if errors.As(err, &maxBytes) {
			writeError(w, r, s.log, http.StatusRequestEntityTooLarge, CodePayloadTooLarge,
				fmt.Sprintf("request body exceeds the %d byte limit", s.cfg.MaxRequestBytes), nil)
			return false
		}
		writeError(w, r, s.log, http.StatusBadRequest, CodeMalformedJSON,
			"request body is not valid JSON: "+sanitizeDecodeError(err), nil)
		return false
	}
	if err := decoder.Decode(new(struct{})); !errors.Is(err, io.EOF) {
		writeError(w, r, s.log, http.StatusBadRequest, CodeMalformedJSON,
			"request body must contain exactly one JSON object", nil)
		return false
	}
	return true
}

func (s *Server) writeWorkerControlError(w http.ResponseWriter, r *http.Request, op string, err error) {
	var validation *workers.ValidationError
	switch {
	case errors.As(err, &validation):
		s.writeWorkerValidation(w, r, validation.Fields)
	case errors.Is(err, workers.ErrUnknownQueue):
		writeError(w, r, s.log, http.StatusUnprocessableEntity, CodeUnknownQueue, "queue does not exist", nil)
	case errors.Is(err, workers.ErrSessionConflict):
		writeError(w, r, s.log, http.StatusConflict, CodeSessionConflict,
			"worker session id was already registered with different immutable data", nil)
	case errors.Is(err, workers.ErrSessionUnavailable):
		writeError(w, r, s.log, http.StatusConflict, CodeSessionUnavailable,
			"worker session is not current and healthy", nil)
	case errors.Is(err, workers.ErrClaimConflict):
		writeError(w, r, s.log, http.StatusConflict, CodeClaimConflict,
			"claim request id was already used for a different claim", nil)
	case errors.Is(err, workers.ErrFenceRejected):
		writeError(w, r, s.log, http.StatusConflict, CodeFenceRejected,
			"job, attempt, lease, worker, and session fence did not match", nil)
	case errors.Is(err, workers.ErrLeaseExpired):
		writeError(w, r, s.log, http.StatusConflict, CodeLeaseExpired,
			"the PostgreSQL-authoritative lease has expired", nil)
	case errors.Is(err, workers.ErrStateConflict):
		writeError(w, r, s.log, http.StatusConflict, CodeStateConflict,
			"the requested state transition is no longer valid", nil)
	case isDeadlineExhausted(err):
		// A database call in this request failed because the request's own
		// deadline elapsed. The outcome is genuinely ambiguous — a deadline
		// reached during COMMIT may or may not have committed — so the response
		// promises retryability, not rollback.
		s.log.Warn("worker control request exhausted its deadline",
			slog.String("request_id", RequestIDFrom(r.Context())),
			slog.String("op", op))
		writeError(w, r, s.log, http.StatusServiceUnavailable, CodeServiceUnavailable,
			"the request exceeded its deadline before the outcome was known; "+
				"retry with the same worker session and claim request identifiers, "+
				"or read the job back to observe the durable result", nil)
	default:
		s.internalError(w, r, op, err)
	}
}

// isDeadlineExhausted reports the one condition mapped to 503, and decides it
// from the returned error alone.
//
// It never consults the request context. Asking whether the deadline has
// elapsed would misclassify every unrelated failure that merely finished after
// it — a constraint violation, a driver fault, a bug — as a retryable
// deadline, hiding real errors behind a 503. The store produces
// workers.ErrDeadlineExceeded at the failing database call itself; a directly
// wrapped context.DeadlineExceeded is accepted for any future caller that has
// not been through that translation.
func isDeadlineExhausted(err error) bool {
	return errors.Is(err, workers.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded)
}

func (s *Server) writeWorkerValidation(w http.ResponseWriter, r *http.Request, fields []workers.FieldError) {
	details := make([]jobs.FieldError, 0, len(fields))
	for _, field := range fields {
		details = append(details, jobs.FieldError{Field: field.Field, Message: field.Message})
	}
	writeError(w, r, s.log, http.StatusUnprocessableEntity, CodeValidationFailed,
		"the request was rejected by validation", details)
}

func emptyIfNil(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}
