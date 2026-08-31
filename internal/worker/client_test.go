package worker

import (
	"testing"
	"time"

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
