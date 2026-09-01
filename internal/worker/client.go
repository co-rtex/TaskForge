package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// ControlPlane is the API surface a DB-less worker needs.
type ControlPlane interface {
	Register(context.Context, workers.Registration) (workers.Session, error)
	Heartbeat(context.Context, workers.HeartbeatRequest) (workers.HeartbeatResult, error)
	Claim(context.Context, workers.ClaimRequest) (workers.ClaimResult, error)
	RenewLease(context.Context, workers.RenewalRequest) (workers.RenewalResult, error)
	Start(context.Context, workers.Fence) (workers.StartResult, error)
	Succeed(context.Context, workers.Fence) error
	Fail(context.Context, workers.FailureReport) (workers.OutcomeResult, error)
	AcknowledgeCancellation(context.Context, workers.CancelAcknowledgment) (workers.OutcomeResult, error)
	Ping(context.Context) error
}

// Client calls taskforge-api's internal worker control operations.
type Client struct {
	baseURL string
	http    *http.Client
	now     func() time.Time
}

func NewClient(baseURL string, httpClient *http.Client) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: httpClient, now: time.Now}
}

func (c *Client) Register(ctx context.Context, registration workers.Registration) (workers.Session, error) {
	request := struct {
		WorkerName        string   `json:"worker_name"`
		Hostname          string   `json:"hostname"`
		WorkerGroup       string   `json:"worker_group"`
		ConcurrencyLimit  int      `json:"concurrency_limit"`
		Capabilities      []string `json:"capabilities"`
		SupportedJobTypes []string `json:"supported_job_types"`
	}{
		WorkerName: registration.Name, Hostname: registration.Hostname,
		WorkerGroup: registration.WorkerGroup, ConcurrencyLimit: registration.ConcurrencyLimit,
		Capabilities: registration.Capabilities, SupportedJobTypes: registration.SupportedJobTypes,
	}
	var response api.WorkerSessionResponse
	if err := c.doJSON(ctx, http.MethodPut,
		"/internal/v1/worker-sessions/"+registration.SessionID.String(), request, &response); err != nil {
		return workers.Session{}, err
	}
	workerID, err := uuid.Parse(response.WorkerID)
	if err != nil {
		return workers.Session{}, fmt.Errorf("control plane returned invalid worker id: %w", err)
	}
	sessionID, err := uuid.Parse(response.WorkerSessionID)
	if err != nil {
		return workers.Session{}, fmt.Errorf("control plane returned invalid session id: %w", err)
	}
	return workers.Session{
		ID: sessionID, WorkerID: workerID, Name: response.WorkerName,
		Hostname: response.Hostname, WorkerGroup: response.WorkerGroup,
		ConcurrencyLimit: response.ConcurrencyLimit, Capabilities: response.Capabilities,
		SupportedJobTypes: response.SupportedJobTypes,
		Status:            workers.SessionStatus(response.Status),
		RegisteredAt:      response.RegisteredAt,
		LastHeartbeatAt:   response.LastHeartbeatAt,
	}, nil
}

// Heartbeat reports liveness for one process session and returns the server time
// the control plane accepted. The request carries no timestamp: PostgreSQL
// receipt time is the only authority for staleness.
func (c *Client) Heartbeat(ctx context.Context, request workers.HeartbeatRequest) (workers.HeartbeatResult, error) {
	body := struct {
		WorkerID string `json:"worker_id"`
	}{request.WorkerID.String()}
	var response api.HeartbeatResponse
	if err := c.doJSON(ctx, http.MethodPost,
		"/internal/v1/worker-sessions/"+request.SessionID.String()+"/heartbeat", body, &response); err != nil {
		return workers.HeartbeatResult{}, err
	}
	sessionID, err := uuid.Parse(response.WorkerSessionID)
	if err != nil {
		return workers.HeartbeatResult{}, fmt.Errorf("control plane returned invalid session id: %w", err)
	}
	// A response about a different session, or one that reports a session that is
	// not healthy, is not a confirmation of this session's liveness.
	if sessionID != request.SessionID {
		return workers.HeartbeatResult{}, fmt.Errorf("control plane acknowledged a different worker session")
	}
	if workers.SessionStatus(response.Status) != workers.SessionHealthy {
		return workers.HeartbeatResult{}, fmt.Errorf(
			"control plane returned a heartbeat for a %s session", response.Status)
	}
	if response.LastHeartbeatAt.IsZero() {
		return workers.HeartbeatResult{}, fmt.Errorf("control plane returned no heartbeat receipt time")
	}
	directives, err := parseCancellationDirectives(response.Cancellations)
	if err != nil {
		return workers.HeartbeatResult{}, err
	}
	return workers.HeartbeatResult{
		SessionID:       sessionID,
		Status:          workers.SessionStatus(response.Status),
		LastHeartbeatAt: response.LastHeartbeatAt,
		Cancellations:   directives,
	}, nil
}

// parseCancellationDirectives rejects a directive that is not fully identified.
//
// A directive is acted on by cancelling a running handler, so a malformed one is
// not something to interpret generously: the worker would either cancel nothing
// or, worse, be unable to tell which attempt it was told about.
func parseCancellationDirectives(
	responses []api.CancellationDirectiveResponse,
) ([]workers.CancellationDirective, error) {
	if len(responses) == 0 {
		return nil, nil
	}
	directives := make([]workers.CancellationDirective, 0, len(responses))
	for _, response := range responses {
		jobID, err := uuid.Parse(response.JobID)
		if err != nil {
			return nil, fmt.Errorf("control plane returned an invalid cancellation job id: %w", err)
		}
		attemptID, err := uuid.Parse(response.AttemptID)
		if err != nil {
			return nil, fmt.Errorf("control plane returned an invalid cancellation attempt id: %w", err)
		}
		leaseID, err := uuid.Parse(response.LeaseID)
		if err != nil {
			return nil, fmt.Errorf("control plane returned an invalid cancellation lease id: %w", err)
		}
		directives = append(directives, workers.CancellationDirective{
			JobID: jobID, AttemptID: attemptID, LeaseID: leaseID,
			CancelRequestedAt: response.CancelRequestedAt,
		})
	}
	return directives, nil
}

// RenewLease extends one lease window and reports the server-measured remaining
// duration. Retrying it with the same renewal request id and expected version is
// safe: the control plane returns the committed result instead of extending the
// lease a second time.
func (c *Client) RenewLease(ctx context.Context, request workers.RenewalRequest) (workers.RenewalResult, error) {
	body := struct {
		JobID                  string `json:"job_id"`
		AttemptID              string `json:"attempt_id"`
		WorkerID               string `json:"worker_id"`
		WorkerSessionID        string `json:"worker_session_id"`
		RenewalRequestID       string `json:"renewal_request_id"`
		ExpectedRenewalVersion int    `json:"expected_renewal_version"`
	}{
		request.Fence.JobID.String(), request.Fence.AttemptID.String(),
		request.Fence.WorkerID.String(), request.Fence.SessionID.String(),
		request.RenewalRequestID.String(), request.ExpectedVersion,
	}
	var response api.RenewalResponse
	if err := c.doJSON(ctx, http.MethodPost,
		"/internal/v1/leases/"+request.Fence.LeaseID.String()+"/renew", body, &response); err != nil {
		return workers.RenewalResult{}, err
	}
	return parseRenewal(request, response)
}

// parseRenewal rejects a renewal response that could not have come from the
// request that was sent. A renewal that names another lease, moves the
// generation backwards, or reports a negative window is not something a worker
// may convert into execution authority.
func parseRenewal(request workers.RenewalRequest, response api.RenewalResponse) (workers.RenewalResult, error) {
	leaseID, err := uuid.Parse(response.LeaseID)
	if err != nil {
		return workers.RenewalResult{}, fmt.Errorf("control plane returned invalid lease id: %w", err)
	}
	if leaseID != request.Fence.LeaseID {
		return workers.RenewalResult{}, fmt.Errorf("control plane renewed a different lease")
	}
	if response.RenewalVersion != request.ExpectedVersion+1 {
		return workers.RenewalResult{}, fmt.Errorf(
			"control plane returned renewal version %d for expected version %d",
			response.RenewalVersion, request.ExpectedVersion)
	}
	if response.LeaseRemainingMillis < 0 {
		return workers.RenewalResult{}, fmt.Errorf("control plane returned a negative lease window")
	}
	if response.LeaseExpiresAt.IsZero() {
		return workers.RenewalResult{}, fmt.Errorf("control plane returned no lease expiry")
	}
	return workers.RenewalResult{
		LeaseID:        leaseID,
		RenewalVersion: response.RenewalVersion,
		ExpiresAt:      response.LeaseExpiresAt,
		Remaining:      time.Duration(response.LeaseRemainingMillis) * time.Millisecond,
		Replayed:       response.Replayed,
	}, nil
}

func (c *Client) Claim(ctx context.Context, request workers.ClaimRequest) (workers.ClaimResult, error) {
	requestStarted := c.now()
	body := struct {
		WorkerID        string `json:"worker_id"`
		WorkerSessionID string `json:"worker_session_id"`
		ClaimRequestID  string `json:"claim_request_id"`
		Queue           string `json:"queue"`
	}{request.WorkerID.String(), request.SessionID.String(), request.ClaimRequestID.String(), request.Queue}
	var response api.ClaimResponse
	if err := c.doJSON(ctx, http.MethodPost, "/internal/v1/claims", body, &response); err != nil {
		return workers.ClaimResult{}, err
	}
	if err := validateClaimResponse(response); err != nil {
		return workers.ClaimResult{}, err
	}
	result := workers.ClaimResult{
		Disposition: workers.ClaimDisposition(response.Outcome),
		Replayed:    response.Replayed,
	}
	if response.Assignment == nil {
		return result, nil
	}
	assignment, err := parseAssignment(*response.Assignment)
	if err != nil {
		return workers.ClaimResult{}, err
	}
	if assignment.WorkerID != request.WorkerID || assignment.SessionID != request.SessionID || assignment.Queue != request.Queue {
		return workers.ClaimResult{}, fmt.Errorf("control plane returned an assignment outside the requested worker, session, or queue")
	}
	assignment.ExecutionDeadline = requestStarted.Add(executionBudget(assignment.LeaseRemaining))
	result.Assignment = &assignment
	return result, nil
}

func validateClaimResponse(response api.ClaimResponse) error {
	hasAssignment := response.Assignment != nil
	switch workers.ClaimDisposition(response.Outcome) {
	case workers.Claimed:
		if !hasAssignment || !response.SafeToAcknowledge {
			return fmt.Errorf("control plane returned an inconsistent claimed response")
		}
	case workers.QueueEmpty:
		if hasAssignment || !response.SafeToAcknowledge {
			return fmt.Errorf("control plane returned an inconsistent empty-queue response")
		}
	case workers.DuplicateNotification:
		if hasAssignment || !response.SafeToAcknowledge {
			return fmt.Errorf("control plane returned an inconsistent duplicate-notification response")
		}
	case workers.NoEligibleJob, workers.CapacityExhausted:
		if hasAssignment || response.SafeToAcknowledge {
			return fmt.Errorf("control plane returned an inconsistent non-claim response")
		}
	default:
		return fmt.Errorf("control plane returned unknown claim outcome %q", response.Outcome)
	}
	return nil
}

func parseAssignment(response api.AssignmentResponse) (workers.Assignment, error) {
	if response.LeaseRemainingMillis < 0 {
		return workers.Assignment{}, fmt.Errorf("control plane returned a negative lease window")
	}
	parse := func(name, value string) (uuid.UUID, error) {
		id, err := uuid.Parse(value)
		if err != nil {
			return uuid.Nil, fmt.Errorf("control plane returned invalid %s: %w", name, err)
		}
		return id, nil
	}
	jobID, err := parse("job id", response.JobID)
	if err != nil {
		return workers.Assignment{}, err
	}
	attemptID, err := parse("attempt id", response.AttemptID)
	if err != nil {
		return workers.Assignment{}, err
	}
	leaseID, err := parse("lease id", response.LeaseID)
	if err != nil {
		return workers.Assignment{}, err
	}
	workerID, err := parse("worker id", response.WorkerID)
	if err != nil {
		return workers.Assignment{}, err
	}
	sessionID, err := parse("worker session id", response.WorkerSessionID)
	if err != nil {
		return workers.Assignment{}, err
	}
	return workers.Assignment{
		JobID: jobID, Queue: response.Queue, JobType: response.JobType, Payload: response.Payload,
		Priority: response.Priority, TimeoutSeconds: response.TimeoutSeconds,
		RequiredCapabilities: response.RequiredCapabilities,
		AttemptID:            attemptID, AttemptNumber: response.AttemptNumber,
		LeaseID: leaseID, LeaseExpiresAt: response.LeaseExpiresAt,
		LeaseRemaining: time.Duration(response.LeaseRemainingMillis) * time.Millisecond,
		WorkerID:       workerID, SessionID: sessionID,
	}, nil
}

func executionBudget(remaining time.Duration) time.Duration {
	if remaining <= 0 {
		return 0
	}
	margin := remaining / 10
	if margin > time.Second {
		margin = time.Second
	}
	return remaining - margin
}

// Start moves the attempt to RUNNING and returns its persisted execution
// deadline.
//
// The remaining budget comes from PostgreSQL, measured after every authority
// lock. The worker must derive its local deadline from that value rather than
// starting a fresh timer from timeout_seconds once this response lands, or it
// would silently grant itself the round trip back on every attempt — and on
// every ambiguous retry, the whole budget again.
func (c *Client) Start(ctx context.Context, fence workers.Fence) (workers.StartResult, error) {
	body := fenceBody(fence)
	var response api.StartResponse
	if err := c.doJSON(ctx, http.MethodPost,
		"/internal/v1/attempts/"+fence.AttemptID.String()+"/start", body, &response); err != nil {
		return workers.StartResult{}, err
	}
	return parseStartResult(fence, response)
}

func parseStartResult(fence workers.Fence, response api.StartResponse) (workers.StartResult, error) {
	attemptID, err := uuid.Parse(response.AttemptID)
	if err != nil {
		return workers.StartResult{}, fmt.Errorf("control plane returned invalid attempt id: %w", err)
	}
	// A response about a different attempt is not an answer to this request, and
	// converting it into an execution deadline would run one attempt's handler
	// against another attempt's budget.
	if attemptID != fence.AttemptID {
		return workers.StartResult{}, fmt.Errorf("control plane started a different attempt")
	}
	if response.AttemptTimeoutRemainingMillis < 0 {
		return workers.StartResult{}, fmt.Errorf("control plane returned a negative attempt timeout window")
	}
	if response.AttemptTimeoutAt.IsZero() || response.StartedAt.IsZero() {
		return workers.StartResult{}, fmt.Errorf("control plane returned no attempt execution deadline")
	}
	return workers.StartResult{
		AttemptID: attemptID,
		StartedAt: response.StartedAt,
		TimeoutAt: response.AttemptTimeoutAt,
		Remaining: time.Duration(response.AttemptTimeoutRemainingMillis) * time.Millisecond,
		Replayed:  response.Replayed,
	}, nil
}

func (c *Client) Succeed(ctx context.Context, fence workers.Fence) error {
	return c.transition(ctx, "succeed", fence)
}

// Fail reports one fenced terminal failure.
//
// Retrying an ambiguous response MUST reuse the same outcome request id, class,
// code, and message. A fresh identity would consume a second place in the
// attempt budget and draw fresh jitter for a different retry instant.
func (c *Client) Fail(ctx context.Context, report workers.FailureReport) (workers.OutcomeResult, error) {
	body := struct {
		JobID            string `json:"job_id"`
		LeaseID          string `json:"lease_id"`
		WorkerID         string `json:"worker_id"`
		WorkerSessionID  string `json:"worker_session_id"`
		OutcomeRequestID string `json:"outcome_request_id"`
		FailureClass     string `json:"failure_class"`
		ErrorCode        string `json:"error_code"`
		ErrorMessage     string `json:"error_message"`
	}{
		report.Fence.JobID.String(), report.Fence.LeaseID.String(),
		report.Fence.WorkerID.String(), report.Fence.SessionID.String(),
		report.OutcomeRequestID.String(), string(report.Class),
		report.ErrorCode, report.ErrorMessage,
	}
	var response api.OutcomeResponse
	if err := c.doJSON(ctx, http.MethodPost,
		"/internal/v1/attempts/"+report.Fence.AttemptID.String()+"/fail", body, &response); err != nil {
		return workers.OutcomeResult{}, err
	}
	return parseOutcome(report.Fence, response)
}

// AcknowledgeCancellation confirms this worker stopped a canceled attempt.
func (c *Client) AcknowledgeCancellation(ctx context.Context, ack workers.CancelAcknowledgment) (workers.OutcomeResult, error) {
	body := struct {
		JobID            string `json:"job_id"`
		LeaseID          string `json:"lease_id"`
		WorkerID         string `json:"worker_id"`
		WorkerSessionID  string `json:"worker_session_id"`
		OutcomeRequestID string `json:"outcome_request_id"`
	}{
		ack.Fence.JobID.String(), ack.Fence.LeaseID.String(),
		ack.Fence.WorkerID.String(), ack.Fence.SessionID.String(),
		ack.OutcomeRequestID.String(),
	}
	var response api.OutcomeResponse
	if err := c.doJSON(ctx, http.MethodPost,
		"/internal/v1/attempts/"+ack.Fence.AttemptID.String()+"/cancel", body, &response); err != nil {
		return workers.OutcomeResult{}, err
	}
	return parseOutcome(ack.Fence, response)
}

// parseOutcome rejects an outcome response that could not have answered the
// request that was sent.
//
// The retry decision is checked as strictly as the identifiers, because it is
// the part a worker would otherwise report onward as fact: a response naming
// another job, or claiming a retry with no instant to retry at, is not something
// to log as though it were the committed decision.
func parseOutcome(fence workers.Fence, response api.OutcomeResponse) (workers.OutcomeResult, error) {
	jobID, err := uuid.Parse(response.JobID)
	if err != nil {
		return workers.OutcomeResult{}, fmt.Errorf("control plane returned invalid job id: %w", err)
	}
	if jobID != fence.JobID {
		return workers.OutcomeResult{}, fmt.Errorf("control plane reported an outcome for a different job")
	}
	if response.JobStatus == "" || response.AttemptStatus == "" {
		return workers.OutcomeResult{}, fmt.Errorf("control plane returned no outcome status")
	}
	if (response.RetryAt == nil) != (response.RetryDelayMillis == nil) {
		return workers.OutcomeResult{}, fmt.Errorf("control plane returned an incomplete retry decision")
	}
	if response.RetryAt != nil && response.JobStatus != "RETRY_WAIT" && response.JobStatus != "QUEUED" {
		return workers.OutcomeResult{}, fmt.Errorf(
			"control plane returned a retry decision for a %s job", response.JobStatus)
	}
	result := workers.OutcomeResult{
		JobID:         jobID,
		JobStatus:     response.JobStatus,
		AttemptStatus: workers.AttemptStatus(response.AttemptStatus),
		RetryAt:       response.RetryAt,
		Replayed:      response.Replayed,
	}
	if response.RetryDelayMillis != nil {
		delay := time.Duration(*response.RetryDelayMillis) * time.Millisecond
		result.RetryDelay = &delay
	}
	if response.DeadLetterReason != nil {
		result.DeadLetterReason = lifecycle.DLQReason(*response.DeadLetterReason)
	}
	return result, nil
}

func (c *Client) transition(ctx context.Context, operation string, fence workers.Fence) error {
	return c.doJSON(ctx, http.MethodPost,
		"/internal/v1/attempts/"+fence.AttemptID.String()+"/"+operation, fenceBody(fence), nil)
}

func fenceBody(fence workers.Fence) any {
	return struct {
		JobID           string `json:"job_id"`
		LeaseID         string `json:"lease_id"`
		WorkerID        string `json:"worker_id"`
		WorkerSessionID string `json:"worker_session_id"`
	}{fence.JobID.String(), fence.LeaseID.String(), fence.WorkerID.String(), fence.SessionID.String()}
}

func (c *Client) Ping(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/readyz", nil)
	if err != nil {
		return fmt.Errorf("build control-plane readiness request: %w", err)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("control-plane readiness request: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
	if response.StatusCode != http.StatusOK {
		return &RemoteError{Status: response.StatusCode, Code: "not_ready"}
	}
	return nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, body, responseTarget any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode control request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("build control request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(api.RequestIDHeader, uuid.NewString())

	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("control request %s %s: %w", method, path, err)
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, 512*1024)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var errorBody api.ErrorBody
		_ = json.NewDecoder(limited).Decode(&errorBody)
		return &RemoteError{Status: response.StatusCode, Code: errorBody.Error.Code}
	}
	if responseTarget == nil || response.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, limited)
		return nil
	}
	if err := json.NewDecoder(limited).Decode(responseTarget); err != nil {
		return fmt.Errorf("decode control response: %w", err)
	}
	return nil
}

// RemoteError is a sanitized API failure. Only server and throttling errors are
// retryable; fencing and validation conflicts are final for that operation.
type RemoteError struct {
	Status int
	Code   string
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("control plane returned HTTP %d (%s)", e.Status, e.Code)
}

func (e *RemoteError) Retryable() bool {
	return e.Status == http.StatusTooManyRequests || e.Status >= 500
}

var _ ControlPlane = (*Client)(nil)
