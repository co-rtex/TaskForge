package workers

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const thirtySeconds = 30 * time.Second

func timeAt(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	require.NoError(t, err)
	return parsed
}

func fieldNames(t *testing.T, err error) []string {
	t.Helper()
	var validation *ValidationError
	require.ErrorAs(t, err, &validation)
	names := make([]string, 0, len(validation.Fields))
	for _, field := range validation.Fields {
		names = append(names, field.Field)
	}
	return names
}

func TestValidateHeartbeat_ReportsEveryMissingIdentifier(t *testing.T) {
	require.NoError(t, ValidateHeartbeat(HeartbeatRequest{
		WorkerID: uuid.New(), SessionID: uuid.New(),
	}))
	require.ElementsMatch(t,
		[]string{"worker_id", "worker_session_id"},
		fieldNames(t, ValidateHeartbeat(HeartbeatRequest{})))
}

// A renewal request has three independent things to get wrong — the fence, the
// renewal identity, and the generation — so a caller must learn about all of
// them at once rather than one failed request at a time.
func TestValidateRenewal_ReportsFenceIdentityAndGenerationTogether(t *testing.T) {
	complete := RenewalRequest{
		Fence: Fence{
			JobID: uuid.New(), AttemptID: uuid.New(), LeaseID: uuid.New(),
			WorkerID: uuid.New(), SessionID: uuid.New(),
		},
		RenewalRequestID: uuid.New(),
		ExpectedVersion:  0,
	}
	require.NoError(t, ValidateRenewal(complete))

	require.ElementsMatch(t, []string{
		"job_id", "attempt_id", "lease_id", "worker_id", "worker_session_id",
		"renewal_request_id", "expected_renewal_version",
	}, fieldNames(t, ValidateRenewal(RenewalRequest{ExpectedVersion: -1})))
}

func TestExecutingStatuses_AreAClosedList(t *testing.T) {
	// Renewal and reconciliation both gate on "is this attempt still executing".
	// A terminal or not-yet-started state must never satisfy that.
	for _, status := range []string{"LEASED", "RUNNING"} {
		require.True(t, isExecutingJobStatus(status), status)
	}
	for _, status := range []string{
		"PENDING", "QUEUED", "RETRY_WAIT", "CANCEL_REQUESTED",
		"SUCCEEDED", "CANCELED", "DEAD_LETTERED", "",
	} {
		require.False(t, isExecutingJobStatus(status), status)
	}

	for _, status := range []AttemptStatus{AttemptLeased, AttemptRunning} {
		require.True(t, isExecutingAttemptStatus(status), string(status))
	}
	for _, status := range []AttemptStatus{
		AttemptSucceeded, AttemptFailed, AttemptTimedOut, AttemptCanceled, AttemptAbandoned, "",
	} {
		require.False(t, isExecutingAttemptStatus(status), string(status))
	}
}

// A replay reports the window that actually remains, never the window that was
// granted. Clamping at zero keeps a caller from turning a lapsed lease into a
// negative — and therefore instantly-expired but nonsensical — local deadline.
func TestRemainingUntil_IsMeasuredFromTheServerSampleAndNeverNegative(t *testing.T) {
	now := timeAt(t, "2026-08-31T12:00:00Z")
	require.Equal(t, thirtySeconds, remainingUntil(now.Add(thirtySeconds), now))
	require.Zero(t, remainingUntil(now.Add(-thirtySeconds), now))
	require.Zero(t, remainingUntil(now, now))
}
