package worker

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/workers"
)

func TestValidateClaimResponse_RejectsInconsistentAckContracts(t *testing.T) {
	assignment := &api.AssignmentResponse{}
	for name, response := range map[string]api.ClaimResponse{
		"claimed without assignment": {
			Outcome: string(workers.Claimed), SafeToAcknowledge: true,
		},
		"claimed but unsafe": {
			Outcome: string(workers.Claimed), SafeToAcknowledge: false, Assignment: assignment,
		},
		"empty queue with assignment": {
			Outcome: string(workers.QueueEmpty), SafeToAcknowledge: true, Assignment: assignment,
		},
		"capacity marked safe": {
			Outcome: string(workers.CapacityExhausted), SafeToAcknowledge: true,
		},
		"unknown outcome": {Outcome: "NEWER_SERVER_OUTCOME"},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, validateClaimResponse(response))
		})
	}

	for _, response := range []api.ClaimResponse{
		{Outcome: string(workers.Claimed), SafeToAcknowledge: true, Assignment: assignment},
		{Outcome: string(workers.QueueEmpty), SafeToAcknowledge: true},
		{Outcome: string(workers.DuplicateNotification), SafeToAcknowledge: true},
		{Outcome: string(workers.NoEligibleJob)},
		{Outcome: string(workers.CapacityExhausted)},
	} {
		require.NoError(t, validateClaimResponse(response))
	}
}

func TestExecutionBudget_UsesMonotonicWindowAndReservesCompletionMargin(t *testing.T) {
	require.Zero(t, executionBudget(0))
	require.Equal(t, 900*time.Millisecond, executionBudget(time.Second))
	require.Equal(t, 29*time.Second, executionBudget(30*time.Second))
	require.Equal(t, 4*time.Minute+59*time.Second, executionBudget(5*time.Minute))
}

func TestParseAssignment_RejectsNegativeLeaseWindow(t *testing.T) {
	_, err := parseAssignment(api.AssignmentResponse{LeaseRemainingMillis: -1})
	require.Error(t, err)
}

// TestParseRenewal_RejectsAResponseThatCouldNotAnswerThisRequest is the client's
// half of the renewal fence. A response naming another lease, or a generation
// that does not follow the one the caller asked to advance, cannot be turned
// into execution authority no matter how well-formed it is.
func TestParseRenewal_RejectsAResponseThatCouldNotAnswerThisRequest(t *testing.T) {
	leaseID := uuid.New()
	request := workers.RenewalRequest{
		Fence:            workers.Fence{LeaseID: leaseID},
		RenewalRequestID: uuid.New(),
		ExpectedVersion:  3,
	}
	expiresAt := time.Now().Add(30 * time.Second)
	valid := api.RenewalResponse{
		LeaseID: leaseID.String(), RenewalVersion: 4,
		LeaseExpiresAt: expiresAt, LeaseRemainingMillis: 30000,
	}

	t.Run("a consistent response parses", func(t *testing.T) {
		result, err := parseRenewal(request, valid)
		require.NoError(t, err)
		require.Equal(t, leaseID, result.LeaseID)
		require.Equal(t, 4, result.RenewalVersion)
		require.Equal(t, 30*time.Second, result.Remaining)
	})

	for name, mutate := range map[string]func(api.RenewalResponse) api.RenewalResponse{
		"another lease": func(r api.RenewalResponse) api.RenewalResponse {
			r.LeaseID = uuid.NewString()
			return r
		},
		"unparseable lease id": func(r api.RenewalResponse) api.RenewalResponse {
			r.LeaseID = "not-a-uuid"
			return r
		},
		"generation that did not advance": func(r api.RenewalResponse) api.RenewalResponse {
			r.RenewalVersion = 3
			return r
		},
		"generation that skipped ahead": func(r api.RenewalResponse) api.RenewalResponse {
			r.RenewalVersion = 5
			return r
		},
		"negative window": func(r api.RenewalResponse) api.RenewalResponse {
			r.LeaseRemainingMillis = -1
			return r
		},
		"missing expiry": func(r api.RenewalResponse) api.RenewalResponse {
			r.LeaseExpiresAt = time.Time{}
			return r
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseRenewal(request, mutate(valid))
			require.Error(t, err)
		})
	}
}

// TestIsAuthorityLost_SeparatesDefinitiveLossFromAmbiguity decides whether a
// failed renewal ends the attempt or is retried. Getting it wrong in either
// direction is a real defect: treating a 503 as loss abandons recoverable work,
// and treating a rejected fence as transient keeps executing work that can never
// commit.
func TestIsAuthorityLost_SeparatesDefinitiveLossFromAmbiguity(t *testing.T) {
	definitive := map[string]error{
		"session replaced":        workers.ErrSessionUnavailable,
		"fence rejected":          workers.ErrFenceRejected,
		"lease expired":           workers.ErrLeaseExpired,
		"renewal conflict":        workers.ErrRenewalConflict,
		"state conflict":          workers.ErrStateConflict,
		"wrapped lease expiry":    fmt.Errorf("renew lease: %w", workers.ErrLeaseExpired),
		"remote session loss":     &RemoteError{Status: 409, Code: "worker_session_unavailable"},
		"remote fence rejection":  &RemoteError{Status: 409, Code: "fence_rejected"},
		"remote lease expiry":     &RemoteError{Status: 409, Code: "lease_expired"},
		"remote renewal conflict": &RemoteError{Status: 409, Code: "renewal_conflict"},
		"remote state conflict":   &RemoteError{Status: 409, Code: "state_conflict"},
	}
	for name, err := range definitive {
		t.Run(name, func(t *testing.T) { require.True(t, isAuthorityLost(err)) })
	}

	ambiguous := map[string]error{
		"service unavailable": &RemoteError{Status: 503, Code: "service_unavailable"},
		"internal error":      &RemoteError{Status: 500, Code: "internal_error"},
		"throttled":           &RemoteError{Status: 429, Code: "too_many_requests"},
		"transport failure":   errors.New("dial tcp: connection refused"),
		"claim conflict":      workers.ErrClaimConflict,
	}
	for name, err := range ambiguous {
		t.Run(name, func(t *testing.T) { require.False(t, isAuthorityLost(err)) })
	}
}

// Only losing the session ends the whole process boot. Losing one lease ends one
// attempt and leaves the worker free to claim other work.
func TestEscalateSessionLoss_OnlyPromotesSessionLoss(t *testing.T) {
	require.ErrorIs(t, escalateSessionLoss(workers.ErrSessionUnavailable), ErrSessionLost)
	require.ErrorIs(t,
		escalateSessionLoss(&RemoteError{Status: 409, Code: "worker_session_unavailable"}), ErrSessionLost)
	// This repository already classifies a rejected fence as session loss:
	// lockFence answers ErrFenceRejected both for a stale tuple and for a session
	// that is no longer current, and the runner has treated it as fatal since M2.
	require.ErrorIs(t, escalateSessionLoss(workers.ErrFenceRejected), ErrSessionLost)
	require.NoError(t, escalateSessionLoss(workers.ErrLeaseExpired))
	require.NoError(t, escalateSessionLoss(workers.ErrRenewalConflict))
}

// TestParseStartResult_RejectsAResponseThatCouldNotAnswerThisRequest is the
// strictness the attempt deadline demands.
//
// The returned window becomes the handler's execution budget, so a response
// naming another attempt, or carrying no deadline at all, must not be converted
// into authority to run for that long.
func TestParseStartResult_RejectsAResponseThatCouldNotAnswerThisRequest(t *testing.T) {
	fence := workers.Fence{
		JobID: uuid.New(), AttemptID: uuid.New(), LeaseID: uuid.New(),
		WorkerID: uuid.New(), SessionID: uuid.New(),
	}
	now := time.Now().UTC()
	valid := api.StartResponse{
		AttemptID:                     fence.AttemptID.String(),
		StartedAt:                     now,
		AttemptTimeoutAt:              now.Add(30 * time.Second),
		AttemptTimeoutRemainingMillis: 30_000,
	}

	for name, mutate := range map[string]func(api.StartResponse) api.StartResponse{
		"another attempt": func(r api.StartResponse) api.StartResponse {
			r.AttemptID = uuid.NewString()
			return r
		},
		"unparseable attempt id": func(r api.StartResponse) api.StartResponse {
			r.AttemptID = "not-a-uuid"
			return r
		},
		"negative remaining window": func(r api.StartResponse) api.StartResponse {
			r.AttemptTimeoutRemainingMillis = -1
			return r
		},
		"no deadline": func(r api.StartResponse) api.StartResponse {
			r.AttemptTimeoutAt = time.Time{}
			return r
		},
		"no start time": func(r api.StartResponse) api.StartResponse {
			r.StartedAt = time.Time{}
			return r
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseStartResult(fence, mutate(valid))
			require.Error(t, err)
		})
	}

	result, err := parseStartResult(fence, valid)
	require.NoError(t, err)
	require.Equal(t, fence.AttemptID, result.AttemptID)
	require.Equal(t, 30*time.Second, result.Remaining)
	require.True(t, valid.AttemptTimeoutAt.Equal(result.TimeoutAt))

	// A replay carries the original deadline, and the client passes that through
	// rather than treating it as a fresh budget.
	replayed := valid
	replayed.Replayed = true
	replayed.AttemptTimeoutRemainingMillis = 500
	result, err = parseStartResult(fence, replayed)
	require.NoError(t, err)
	require.True(t, result.Replayed)
	require.Equal(t, 500*time.Millisecond, result.Remaining)
}

// TestParseOutcome_RejectsAnIncoherentDecision keeps a worker from logging a
// retry decision the control plane did not actually make.
func TestParseOutcome_RejectsAnIncoherentDecision(t *testing.T) {
	fence := workers.Fence{
		JobID: uuid.New(), AttemptID: uuid.New(), LeaseID: uuid.New(),
		WorkerID: uuid.New(), SessionID: uuid.New(),
	}
	retryAt := time.Now().UTC().Add(time.Minute)
	delay := int64(60_000)

	for name, response := range map[string]api.OutcomeResponse{
		"another job": {
			JobID: uuid.NewString(), JobStatus: "RETRY_WAIT", AttemptStatus: "FAILED",
		},
		"unparseable job id": {
			JobID: "not-a-uuid", JobStatus: "RETRY_WAIT", AttemptStatus: "FAILED",
		},
		"no job status": {
			JobID: fence.JobID.String(), AttemptStatus: "FAILED",
		},
		"no attempt status": {
			JobID: fence.JobID.String(), JobStatus: "RETRY_WAIT",
		},
		"retry instant without a delay": {
			JobID: fence.JobID.String(), JobStatus: "RETRY_WAIT",
			AttemptStatus: "FAILED", RetryAt: &retryAt,
		},
		"delay without a retry instant": {
			JobID: fence.JobID.String(), JobStatus: "RETRY_WAIT",
			AttemptStatus: "FAILED", RetryDelayMillis: &delay,
		},
		"retry decision on a dead-lettered job": {
			JobID: fence.JobID.String(), JobStatus: "DEAD_LETTERED",
			AttemptStatus: "FAILED", RetryAt: &retryAt, RetryDelayMillis: &delay,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseOutcome(fence, response)
			require.Error(t, err)
		})
	}

	result, err := parseOutcome(fence, api.OutcomeResponse{
		JobID: fence.JobID.String(), JobStatus: "RETRY_WAIT", AttemptStatus: "FAILED",
		RetryAt: &retryAt, RetryDelayMillis: &delay,
	})
	require.NoError(t, err)
	require.Equal(t, fence.JobID, result.JobID)
	require.Equal(t, workers.AttemptFailed, result.AttemptStatus)
	require.Equal(t, time.Minute, *result.RetryDelay)

	// A terminal outcome with no retry is equally valid.
	reason := "PERMANENT_FAILURE"
	result, err = parseOutcome(fence, api.OutcomeResponse{
		JobID: fence.JobID.String(), JobStatus: "DEAD_LETTERED",
		AttemptStatus: "FAILED", DeadLetterReason: &reason,
	})
	require.NoError(t, err)
	require.Nil(t, result.RetryAt)
	require.Equal(t, lifecycle.ReasonPermanentFailure, result.DeadLetterReason)
}

// TestParseCancellationDirectives_RejectsAMalformedDirective is why a directive
// is not interpreted generously: it is acted on by cancelling a running handler,
// so a worker that cannot tell which attempt it names must not guess.
func TestParseCancellationDirectives_RejectsAMalformedDirective(t *testing.T) {
	valid := api.CancellationDirectiveResponse{
		JobID: uuid.NewString(), AttemptID: uuid.NewString(),
		LeaseID: uuid.NewString(), CancelRequestedAt: time.Now().UTC(),
	}

	for name, mutate := range map[string]func(api.CancellationDirectiveResponse) api.CancellationDirectiveResponse{
		"bad job id": func(d api.CancellationDirectiveResponse) api.CancellationDirectiveResponse {
			d.JobID = "nope"
			return d
		},
		"bad attempt id": func(d api.CancellationDirectiveResponse) api.CancellationDirectiveResponse {
			d.AttemptID = ""
			return d
		},
		"bad lease id": func(d api.CancellationDirectiveResponse) api.CancellationDirectiveResponse {
			d.LeaseID = "nope"
			return d
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseCancellationDirectives(
				[]api.CancellationDirectiveResponse{valid, mutate(valid)})
			require.Error(t, err)
		})
	}

	// The ordinary case: none is the norm, and a well-formed one round-trips.
	empty, err := parseCancellationDirectives(nil)
	require.NoError(t, err)
	require.Empty(t, empty)

	directives, err := parseCancellationDirectives([]api.CancellationDirectiveResponse{valid})
	require.NoError(t, err)
	require.Len(t, directives, 1)
	require.Equal(t, valid.AttemptID, directives[0].AttemptID.String())
}
