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
	"github.com/co-rtex/TaskForge/internal/workers"
)

// ControlPlane is the API surface a DB-less worker needs.
type ControlPlane interface {
	Register(context.Context, workers.Registration) (workers.Session, error)
	Claim(context.Context, workers.ClaimRequest) (workers.ClaimResult, error)
	Start(context.Context, workers.Fence) error
	Succeed(context.Context, workers.Fence) error
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

func (c *Client) Start(ctx context.Context, fence workers.Fence) error {
	return c.transition(ctx, "start", fence)
}

func (c *Client) Succeed(ctx context.Context, fence workers.Fence) error {
	return c.transition(ctx, "succeed", fence)
}

func (c *Client) transition(ctx context.Context, operation string, fence workers.Fence) error {
	body := struct {
		JobID           string `json:"job_id"`
		LeaseID         string `json:"lease_id"`
		WorkerID        string `json:"worker_id"`
		WorkerSessionID string `json:"worker_session_id"`
	}{fence.JobID.String(), fence.LeaseID.String(), fence.WorkerID.String(), fence.SessionID.String()}
	return c.doJSON(ctx, http.MethodPost,
		"/internal/v1/attempts/"+fence.AttemptID.String()+"/"+operation, body, nil)
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
