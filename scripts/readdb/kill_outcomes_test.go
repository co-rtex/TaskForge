package readdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The queries are tested against real PostgreSQL in
// tests/integration/kill_outcomes_test.go; these are the pure methods.

func TestKillOutcome_OutcomeBuckets(t *testing.T) {
	for _, c := range []struct{ status, job, want string }{
		{"ABANDONED", "QUEUED", OutcomeAbandoned},
		{"ABANDONED", "DEAD_LETTERED", OutcomeAbandoned},
		{"TIMED_OUT", "RETRY_WAIT", OutcomeTimedOut},
		{"TIMED_OUT", "QUEUED", OutcomeTimedOut},
		{"TIMED_OUT", "SUCCEEDED", OutcomeTimedOut},
		{"TIMED_OUT", "DEAD_LETTERED", OutcomeTimedOutDeadLetter},
		{"CANCELED", "CANCELED", OutcomeCanceled},
		{"LEASED", "LEASED", OutcomeUnresolved},
		{"RUNNING", "RUNNING", OutcomeUnresolved},
		{"SUCCEEDED", "SUCCEEDED", OutcomeOther},
		{"FAILED", "RETRY_WAIT", OutcomeOther},
	} {
		require.Equal(t, c.want, KillOutcome{Status: c.status, JobStatus: c.job}.Outcome(), "%s / job %s", c.status, c.job)
	}
}

func TestKillOutcome_ClaimToStartAndToReplacement(t *testing.T) {
	acquired := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	started := acquired.Add(17 * time.Millisecond)
	replaced := acquired.Add(34 * time.Second)
	o := KillOutcome{LeaseAcquiredAt: acquired, StartedAt: &started, ReplacementClaimedAt: &replaced}

	got, ok := o.ClaimToStart()
	require.True(t, ok)
	require.Equal(t, 17*time.Millisecond, got)
	got, ok = o.ToReplacement(acquired.Add(time.Second))
	require.True(t, ok)
	require.Equal(t, 33*time.Second, got)

	_, ok = KillOutcome{LeaseAcquiredAt: acquired}.ClaimToStart()
	require.False(t, ok, "never started")
	_, ok = KillOutcome{}.ToReplacement(acquired)
	require.False(t, ok, "no replacement")
}
