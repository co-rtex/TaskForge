package worker

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
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
