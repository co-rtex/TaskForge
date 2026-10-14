package readdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The queries are tested against real PostgreSQL in
// tests/integration/kill_outcomes_test.go; these are the pure methods.

func TestKillOutcome_OutcomeBuckets(t *testing.T) {
	// The dead-letter buckets are chosen by the attempt's own DLQ entry (DLQReason,
	// joined on dlq_entries.terminal_attempt_id), never by the job's status now: a
	// job a LATER attempt dead-lettered says nothing about how this attempt ended.
	exhausted := "ATTEMPTS_EXHAUSTED"
	for _, c := range []struct {
		status, job string
		dlq         *string
		want        string
	}{
		{"ABANDONED", "QUEUED", nil, OutcomeAbandoned},
		{"ABANDONED", "DEAD_LETTERED", nil, OutcomeAbandoned}, // a later attempt dead-lettered the job
		{"ABANDONED", "DEAD_LETTERED", &exhausted, OutcomeAbandonedDeadLetter},
		{"TIMED_OUT", "RETRY_WAIT", nil, OutcomeTimedOut},
		{"TIMED_OUT", "QUEUED", nil, OutcomeTimedOut},
		{"TIMED_OUT", "SUCCEEDED", nil, OutcomeTimedOut},
		{"TIMED_OUT", "DEAD_LETTERED", nil, OutcomeTimedOut}, // a later attempt dead-lettered the job
		{"TIMED_OUT", "DEAD_LETTERED", &exhausted, OutcomeTimedOutDeadLetter},
		{"CANCELED", "CANCELED", nil, OutcomeCanceled},
		{"LEASED", "LEASED", nil, OutcomeUnresolved},
		{"RUNNING", "RUNNING", nil, OutcomeUnresolved},
		{"SUCCEEDED", "SUCCEEDED", nil, OutcomeOther},
		{"FAILED", "RETRY_WAIT", nil, OutcomeOther},
	} {
		t.Run(fmt.Sprintf("%s, job %s, own DLQ entry %v", c.status, c.job, c.dlq != nil), func(t *testing.T) {
			require.Equal(t, c.want, KillOutcome{Status: c.status, JobStatus: c.job, DLQReason: c.dlq}.Outcome())
		})
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
