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
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// WorkerControl is the internal control-plane surface used by a DB-less worker.
//
// There is deliberately no generic "set attempt status" operation and no
// worker-authoritative timeout: each transition is its own named method with its
// own preconditions, and TIMED_OUT is reachable only through reconciliation.
type WorkerControl interface {
	Register(context.Context, string, workers.Registration) (workers.Session, error)
	Heartbeat(context.Context, string, workers.HeartbeatRequest) (workers.HeartbeatResult, error)
	Claim(context.Context, string, workers.ClaimRequest) (workers.ClaimResult, error)
	RenewLease(context.Context, string, workers.RenewalRequest) (workers.RenewalResult, error)
	Start(context.Context, string, workers.Fence) (workers.StartResult, error)
	Succeed(context.Context, string, workers.Fence) error
	Fail(context.Context, string, workers.FailureReport) (workers.OutcomeResult, error)
	AcknowledgeCancellation(context.Context, string, workers.CancelAcknowledgment) (workers.OutcomeResult, error)
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

type heartbeatRequest struct {
	WorkerID string `json:"worker_id"`
}

// HeartbeatResponse reports the PostgreSQL time the control plane accepted.
//
// It deliberately echoes a server timestamp rather than answering 204: a worker
// must be able to confirm that its liveness actually advanced, and it must never
// substitute its own clock for the answer.
type HeartbeatResponse struct {
	WorkerSessionID string    `json:"worker_session_id"`
	Status          string    `json:"status"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
	// Cancellations are this session's outstanding cancellation directives.
	// Delivering them on the heartbeat rather than on a work notification is
	// what makes cancellation reach an idle worker and a draining one, with no
	// broker delivery involved.
	Cancellations []CancellationDirectiveResponse `json:"cancellations"`
}

// CancellationDirectiveResponse names one attempt this session should stop.
type CancellationDirectiveResponse struct {
	JobID             string    `json:"job_id"`
	AttemptID         string    `json:"attempt_id"`
	LeaseID           string    `json:"lease_id"`
	CancelRequestedAt time.Time `json:"cancel_requested_at"`
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var request heartbeatRequest
	if !s.decodeControlJSON(w, r, &request) {
		return
	}
	sessionID, sessionErr := uuid.Parse(r.PathValue("worker_session_id"))
	workerID, workerErr := uuid.Parse(request.WorkerID)
	var fields []workers.FieldError
	if sessionErr != nil {
		fields = append(fields, workers.FieldError{Field: "worker_session_id", Message: "must be a UUID"})
	}
	if workerErr != nil {
		fields = append(fields, workers.FieldError{Field: "worker_id", Message: "must be a UUID"})
	}
	if len(fields) > 0 {
		s.writeWorkerValidation(w, r, fields)
		return
	}

	result, err := s.control.Heartbeat(r.Context(), s.cfg.DevScope,
		workers.HeartbeatRequest{WorkerID: workerID, SessionID: sessionID})
	if err != nil {
		s.writeWorkerControlError(w, r, "heartbeat worker session", err)
		return
	}
	directives := make([]CancellationDirectiveResponse, 0, len(result.Cancellations))
	for _, directive := range result.Cancellations {
		directives = append(directives, CancellationDirectiveResponse{
			JobID:             directive.JobID.String(),
			AttemptID:         directive.AttemptID.String(),
			LeaseID:           directive.LeaseID.String(),
			CancelRequestedAt: directive.CancelRequestedAt.UTC(),
		})
	}
	writeJSON(w, s.log, http.StatusOK, HeartbeatResponse{
		WorkerSessionID: result.SessionID.String(),
		Status:          string(result.Status),
		LastHeartbeatAt: result.LastHeartbeatAt.UTC(),
		Cancellations:   directives,
	})
}

type renewLeaseRequest struct {
	JobID                  string `json:"job_id"`
	AttemptID              string `json:"attempt_id"`
	WorkerID               string `json:"worker_id"`
	WorkerSessionID        string `json:"worker_session_id"`
	RenewalRequestID       string `json:"renewal_request_id"`
	ExpectedRenewalVersion int    `json:"expected_renewal_version"`
}

// RenewalResponse is one committed renewal window.
//
// LeaseRemainingMillis is measured by PostgreSQL after every authority lock. A
// worker turns it into a conservative monotonic deadline instead of comparing
// its own wall clock with LeaseExpiresAt.
type RenewalResponse struct {
	LeaseID              string    `json:"lease_id"`
	RenewalVersion       int       `json:"renewal_version"`
	LeaseExpiresAt       time.Time `json:"lease_expires_at"`
	LeaseRemainingMillis int64     `json:"lease_remaining_milliseconds"`
	Replayed             bool      `json:"replayed"`
}

func (s *Server) handleRenewLease(w http.ResponseWriter, r *http.Request) {
	var request renewLeaseRequest
	if !s.decodeControlJSON(w, r, &request) {
		return
	}
	req, fields := parseRenewalRequest(r.PathValue("lease_id"), request)
	if len(fields) > 0 {
		s.writeWorkerValidation(w, r, fields)
		return
	}

	result, err := s.control.RenewLease(r.Context(), s.cfg.DevScope, req)
	if err != nil {
		s.writeWorkerControlError(w, r, "renew lease", err)
		return
	}
	s.log.Info("lease renewed",
		slog.String("request_id", RequestIDFrom(r.Context())),
		slog.String("job_id", req.Fence.JobID.String()),
		slog.String("attempt_id", req.Fence.AttemptID.String()),
		slog.String("lease_id", req.Fence.LeaseID.String()),
		slog.Int("renewal_version", result.RenewalVersion),
		slog.Bool("replayed", result.Replayed))
	writeJSON(w, s.log, http.StatusOK, RenewalResponse{
		LeaseID:              result.LeaseID.String(),
		RenewalVersion:       result.RenewalVersion,
		LeaseExpiresAt:       result.ExpiresAt.UTC(),
		LeaseRemainingMillis: result.Remaining.Milliseconds(),
		Replayed:             result.Replayed,
	})
}

func parseRenewalRequest(leaseID string, request renewLeaseRequest) (workers.RenewalRequest, []workers.FieldError) {
	inputs := []struct {
		field string
		value string
		dest  *uuid.UUID
	}{
		{"lease_id", leaseID, new(uuid.UUID)},
		{"job_id", request.JobID, new(uuid.UUID)},
		{"attempt_id", request.AttemptID, new(uuid.UUID)},
		{"worker_id", request.WorkerID, new(uuid.UUID)},
		{"worker_session_id", request.WorkerSessionID, new(uuid.UUID)},
		{"renewal_request_id", request.RenewalRequestID, new(uuid.UUID)},
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
	req := workers.RenewalRequest{
		Fence: workers.Fence{
			LeaseID:   *inputs[0].dest,
			JobID:     *inputs[1].dest,
			AttemptID: *inputs[2].dest,
			WorkerID:  *inputs[3].dest,
			SessionID: *inputs[4].dest,
		},
		RenewalRequestID: *inputs[5].dest,
		ExpectedVersion:  request.ExpectedRenewalVersion,
	}
	if request.ExpectedRenewalVersion < 0 {
		fields = append(fields, workers.FieldError{
			Field: "expected_renewal_version", Message: "must not be negative"})
	}
	return req, fields
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

// StartResponse is the committed LEASED -> RUNNING transition.
//
// It replaces M2's empty 204 because the attempt's execution deadline is durable
// state a worker must be told, not recompute. AttemptTimeoutRemainingMillis is
// measured by PostgreSQL after every authority lock, so a worker converts a
// server-measured duration into a conservative monotonic deadline rather than
// starting a fresh timer from timeout_seconds once the response lands.
type StartResponse struct {
	AttemptID                     string    `json:"attempt_id"`
	StartedAt                     time.Time `json:"started_at"`
	AttemptTimeoutAt              time.Time `json:"attempt_timeout_at"`
	AttemptTimeoutRemainingMillis int64     `json:"attempt_timeout_remaining_milliseconds"`
	// Replayed is true when the attempt was already RUNNING. The original
	// deadline is returned; a replay never restarts the timeout.
	Replayed bool `json:"replayed"`
}

func (s *Server) handleStartAttempt(w http.ResponseWriter, r *http.Request) {
	var request fenceRequest
	if !s.decodeControlJSON(w, r, &request) {
		return
	}
	fence, fields := parseFence(r.PathValue("attempt_id"), request)
	if len(fields) > 0 {
		s.writeWorkerValidation(w, r, fields)
		return
	}
	result, err := s.control.Start(r.Context(), s.cfg.DevScope, fence)
	if err != nil {
		s.writeWorkerControlError(w, r, "start attempt", err)
		return
	}
	s.log.Info("start attempt",
		append(fenceLogAttrs(fence),
			slog.Time("attempt_timeout_at", result.TimeoutAt.UTC()),
			slog.Bool("replayed", result.Replayed))...)
	writeJSON(w, s.log, http.StatusOK, StartResponse{
		AttemptID:                     result.AttemptID.String(),
		StartedAt:                     result.StartedAt.UTC(),
		AttemptTimeoutAt:              result.TimeoutAt.UTC(),
		AttemptTimeoutRemainingMillis: result.Remaining.Milliseconds(),
		Replayed:                      result.Replayed,
	})
}

func (s *Server) handleSucceedAttempt(w http.ResponseWriter, r *http.Request) {
	s.handleFencedTransition(w, r, "succeed attempt", s.control.Succeed)
}

type failAttemptRequest struct {
	JobID            string `json:"job_id"`
	LeaseID          string `json:"lease_id"`
	WorkerID         string `json:"worker_id"`
	WorkerSessionID  string `json:"worker_session_id"`
	OutcomeRequestID string `json:"outcome_request_id"`
	FailureClass     string `json:"failure_class"`
	ErrorCode        string `json:"error_code"`
	ErrorMessage     string `json:"error_message"`
}

type cancelAttemptRequest struct {
	JobID            string `json:"job_id"`
	LeaseID          string `json:"lease_id"`
	WorkerID         string `json:"worker_id"`
	WorkerSessionID  string `json:"worker_session_id"`
	OutcomeRequestID string `json:"outcome_request_id"`
}

// OutcomeResponse is the committed decision for one terminal attempt outcome.
//
// RetryAt and RetryDelayMillis are read back from what was persisted, never
// recomputed. That is the visible half of the promise that an ambiguous failure
// response never redraws jitter: a replay returns the same instant.
type OutcomeResponse struct {
	JobID            string     `json:"job_id"`
	JobStatus        string     `json:"job_status"`
	AttemptStatus    string     `json:"attempt_status"`
	RetryAt          *time.Time `json:"retry_at"`
	RetryDelayMillis *int64     `json:"retry_delay_milliseconds"`
	DeadLetterReason *string    `json:"dead_letter_reason"`
	Replayed         bool       `json:"replayed"`
}

func toOutcomeResponse(result workers.OutcomeResult) OutcomeResponse {
	response := OutcomeResponse{
		JobID:         result.JobID.String(),
		JobStatus:     result.JobStatus,
		AttemptStatus: string(result.AttemptStatus),
		Replayed:      result.Replayed,
	}
	if result.RetryAt != nil {
		utc := result.RetryAt.UTC()
		response.RetryAt = &utc
	}
	if result.RetryDelay != nil {
		millis := result.RetryDelay.Milliseconds()
		response.RetryDelayMillis = &millis
	}
	if result.DeadLetterReason != "" {
		reason := result.DeadLetterReason.String()
		response.DeadLetterReason = &reason
	}
	return response
}

// handleFailAttempt records one fenced terminal failure.
//
// The body carries a client-generated outcome_request_id, and repeating an
// ambiguous request MUST reuse it. A fresh identity would be a second failure
// report for the same attempt, which is exactly what the retained identity
// exists to make impossible.
func (s *Server) handleFailAttempt(w http.ResponseWriter, r *http.Request) {
	var request failAttemptRequest
	if !s.decodeControlJSON(w, r, &request) {
		return
	}
	fence, fields := parseFence(r.PathValue("attempt_id"), fenceRequest{
		JobID: request.JobID, LeaseID: request.LeaseID,
		WorkerID: request.WorkerID, WorkerSessionID: request.WorkerSessionID,
	})
	outcomeID, err := uuid.Parse(request.OutcomeRequestID)
	if err != nil {
		fields = append(fields, workers.FieldError{Field: "outcome_request_id", Message: "must be a UUID"})
	}
	if len(fields) > 0 {
		s.writeWorkerValidation(w, r, fields)
		return
	}

	report := workers.FailureReport{
		Fence:            fence,
		OutcomeRequestID: outcomeID,
		Class:            lifecycle.FailureClass(request.FailureClass),
		ErrorCode:        request.ErrorCode,
		ErrorMessage:     request.ErrorMessage,
	}
	result, err := s.control.Fail(r.Context(), s.cfg.DevScope, report)
	if err != nil {
		s.writeWorkerControlError(w, r, "fail attempt", err)
		return
	}
	// The error code is a stable token by construction; the safe message is
	// deliberately absent from the log line, because a bounded value is still
	// caller-supplied text and the code is what an operator groups by.
	s.log.Info("attempt failed",
		append(fenceLogAttrs(fence),
			slog.String("failure_class", request.FailureClass),
			slog.String("error_code", request.ErrorCode),
			slog.String("job_status", result.JobStatus),
			slog.Bool("replayed", result.Replayed))...)
	writeJSON(w, s.log, http.StatusOK, toOutcomeResponse(result))
}

// handleCancelAttempt records a worker's cooperative acknowledgment that it
// stopped a canceled attempt.
func (s *Server) handleCancelAttempt(w http.ResponseWriter, r *http.Request) {
	var request cancelAttemptRequest
	if !s.decodeControlJSON(w, r, &request) {
		return
	}
	fence, fields := parseFence(r.PathValue("attempt_id"), fenceRequest{
		JobID: request.JobID, LeaseID: request.LeaseID,
		WorkerID: request.WorkerID, WorkerSessionID: request.WorkerSessionID,
	})
	outcomeID, err := uuid.Parse(request.OutcomeRequestID)
	if err != nil {
		fields = append(fields, workers.FieldError{Field: "outcome_request_id", Message: "must be a UUID"})
	}
	if len(fields) > 0 {
		s.writeWorkerValidation(w, r, fields)
		return
	}

	result, err := s.control.AcknowledgeCancellation(r.Context(), s.cfg.DevScope,
		workers.CancelAcknowledgment{Fence: fence, OutcomeRequestID: outcomeID})
	if err != nil {
		s.writeWorkerControlError(w, r, "acknowledge attempt cancellation", err)
		return
	}
	s.log.Info("attempt cancellation acknowledged",
		append(fenceLogAttrs(fence), slog.Bool("replayed", result.Replayed))...)
	writeJSON(w, s.log, http.StatusOK, toOutcomeResponse(result))
}

func fenceLogAttrs(fence workers.Fence, extra ...any) []any {
	attrs := []any{
		slog.String("job_id", fence.JobID.String()),
		slog.String("attempt_id", fence.AttemptID.String()),
		slog.String("lease_id", fence.LeaseID.String()),
		slog.String("worker_id", fence.WorkerID.String()),
		slog.String("worker_session_id", fence.SessionID.String()),
	}
	return append(attrs, extra...)
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
	case errors.Is(err, workers.ErrRenewalConflict):
		writeError(w, r, s.log, http.StatusConflict, CodeRenewalConflict,
			"the renewal named a generation that is no longer current, or reused a "+
				"renewal request id for a different lease", nil)
	case errors.Is(err, workers.ErrAttemptTimedOut):
		writeError(w, r, s.log, http.StatusConflict, CodeAttemptTimedOut,
			"the attempt reached its persisted execution deadline; reconciliation "+
				"owns the TIMED_OUT outcome and no worker-reported outcome is accepted", nil)
	case errors.Is(err, workers.ErrCancellationRequested):
		writeError(w, r, s.log, http.StatusConflict, CodeCancellationRequested,
			"cancellation was requested before this attempt started; acknowledge it "+
				"through POST /internal/v1/attempts/{attempt_id}/cancel", nil)
	case errors.Is(err, workers.ErrOutcomeConflict):
		writeError(w, r, s.log, http.StatusConflict, CodeOutcomeConflict,
			"the outcome request id was already used for a different attempt, or "+
				"replayed with a different classification, code, or message", nil)
	case isDeadlineExhausted(err):
		// A database call in this request failed because the request's own
		// deadline elapsed. That can happen while acquiring a lock, while
		// executing a statement, or during COMMIT, so the outcome may be
		// ambiguous: the response promises retryability, never rollback.
		//
		// This message is shared by every worker-control operation, so it names
		// no operation-specific identifier. Per-endpoint retry guidance — which
		// identity or fence to repeat — lives in api/openapi.yaml, where it can
		// be accurate for each operation.
		s.log.Warn("worker control request exhausted its deadline",
			slog.String("request_id", RequestIDFrom(r.Context())),
			slog.String("op", op))
		writeError(w, r, s.log, http.StatusServiceUnavailable, CodeServiceUnavailable,
			"the request exceeded its deadline before the durable outcome was known; "+
				"retry the identical request, which is safe because every worker-control "+
				"operation is idempotent under the identity and fencing identifiers it "+
				"already carries", nil)
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
	return errors.Is(err, workers.ErrDeadlineExceeded) ||
		errors.Is(err, jobs.ErrDeadlineExceeded) ||
		errors.Is(err, context.DeadlineExceeded)
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
