package main

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// steadyInputs builds a synthetic run whose answers are known exactly: jobs are
// created every 60ms for 30s of warm-up and 300s of steady window, each is
// claimed `claim` after its creation and finished `work` after that.
func steadyInputs(claim, work time.Duration) throughputInputs {
	const interval = 60 * time.Millisecond
	warmup, steady := 30*time.Second, 300*time.Second
	window := Window{Start: t0.Add(warmup), End: t0.Add(warmup + steady)}

	var dispatches []readdb.Dispatch
	var finishes []time.Time
	for created := t0.Add(warmup); created.Before(window.End); created = created.Add(interval) {
		claimed := created.Add(claim)
		dispatches = append(dispatches, readdb.Dispatch{JobID: uuid.New(), SubmittedAt: created, ClaimedAt: &claimed})
		finishes = append(finishes, claimed.Add(work))
	}
	// The warm-up jobs finish before the window opens or just inside it, and are
	// part of the finishes the query returns.
	for created := t0; created.Before(window.Start); created = created.Add(interval) {
		finishes = append(finishes, created.Add(claim+work))
	}
	return throughputInputs{
		profile: "shipped", workers: 12, concurrency: 4, targetRate: 1000,
		warmup: warmup, drain: 4 * time.Second, window: window,
		submitted: len(dispatches) + int(warmup/interval), dispatches: dispatches, finishes: finishes,
		statuses:        map[string]int{"SUCCEEDED": len(finishes)},
		monotonicWindow: steady,
	}
}

func TestAnalyzeThroughput_AKnownSteadyRunGivesTheKnownAnswers(t *testing.T) {
	got := analyzeThroughput(steadyInputs(10*time.Millisecond, 55*time.Millisecond))

	require.Equal(t, 5000, got.JobsInWindow)
	require.InDelta(t, 1000.0, got.OfferedPerMin, 1e-9, "5,000 jobs created in a 5 minute window")
	require.InDelta(t, 1000.0, got.ThroughputPerMin, 1e-9, "every job finished 65ms after creation, so the window sees 5,000 finishes")
	require.Equal(t, Summary{N: 5000, P50: ms(10), P95: ms(10), P99: ms(10), Max: ms(10)}, got.Dispatch)
	require.Zero(t, got.Unclaimed)
	require.Zero(t, got.NegativeLatency)
	require.True(t, got.Valid, "%v", got.Problems)
	require.Equal(t, 30*time.Second, got.Warmup)
	require.Equal(t, 300*time.Second, got.Steady)
	require.Equal(t, 4*time.Second, got.Drain)
}

func TestAnalyzeThroughput_JobsFinishingInTheDrainDoNotCountAndLowerTheRate(t *testing.T) {
	in := steadyInputs(10*time.Millisecond, 55*time.Millisecond)
	// The window's own jobs come first in the slice (indices 0-4999). Move 100 of
	// them that finished inside the window to after its end.
	for i := 4800; i < 4900; i++ {
		in.finishes[i] = in.window.End.Add(time.Duration(i) * time.Millisecond)
	}
	got := analyzeThroughput(in)

	require.InDelta(t, 980.0, got.ThroughputPerMin, 1e-9, "100 fewer finishes than 5,000, over 5 minutes")
	require.InDelta(t, 1000.0, got.OfferedPerMin, 1e-9, "the offered rate is about creation, and did not change")
}

func TestAnalyzeThroughput_AFinishOnTheBoundariesFollowsTheHalfOpenRule(t *testing.T) {
	in := steadyInputs(0, 0)
	in.finishes = []time.Time{in.window.Start, in.window.End, in.window.Start.Add(-time.Nanosecond), in.window.End.Add(-time.Nanosecond)}
	got := analyzeThroughput(in)

	require.Equal(t, 2, got.CompletedInWindow, "Start and one tick before End are in; End and one tick before Start are out")
}

func TestAnalyzeThroughput_JobsNobodyClaimedAreCountedNotAveragedAway(t *testing.T) {
	in := steadyInputs(10*time.Millisecond, 55*time.Millisecond)
	in.dispatches[10].ClaimedAt = nil
	in.dispatches[11].ClaimedAt = nil
	got := analyzeThroughput(in)

	require.Equal(t, 2, got.Unclaimed)
	require.Equal(t, 4998, got.Dispatch.N)
	require.Equal(t, 5000, got.JobsInWindow)
}

func TestAnalyzeThroughput_AClaimBeforeItsSubmissionIsAProblemNotALatency(t *testing.T) {
	in := steadyInputs(10*time.Millisecond, 55*time.Millisecond)
	before := in.dispatches[3].SubmittedAt.Add(-time.Millisecond)
	in.dispatches[3].ClaimedAt = &before
	got := analyzeThroughput(in)

	require.Equal(t, 1, got.NegativeLatency)
	require.False(t, got.Valid)
	require.Contains(t, got.Problems[0], "before")
}

// The single-clock rule's guard. If this machine slept, or a timer misfired, the
// window measured in PostgreSQL time and the window measured by this process's
// monotonic clock disagree, and the run says so instead of reporting a rate over
// the wrong span.
func TestAnalyzeThroughput_AWindowTheTwoClocksDisagreeAboutIsInvalid(t *testing.T) {
	in := steadyInputs(10*time.Millisecond, 55*time.Millisecond)
	in.monotonicWindow = 300*time.Second - 3*time.Second
	got := analyzeThroughput(in)

	require.False(t, got.Valid)
	require.Equal(t, 3*time.Second, got.ClockDivergence)
	require.Contains(t, got.Problems[0], "slept")

	in.monotonicWindow = 300*time.Second - 1500*time.Millisecond
	require.True(t, analyzeThroughput(in).Valid, "1.5s is inside the tolerance")
}

func TestAnalyzeThroughput_NoJobsInTheWindowIsInvalidNotAZeroRate(t *testing.T) {
	in := steadyInputs(0, 0)
	in.dispatches = nil
	got := analyzeThroughput(in)
	require.False(t, got.Valid)
	require.Zero(t, got.Dispatch.N)
}

func TestAnalyzeThroughput_TheLeaseReadingIsReportedBesideTheDefinedOne(t *testing.T) {
	in := steadyInputs(10*time.Millisecond, 55*time.Millisecond)
	for i := range in.dispatches {
		leased := in.dispatches[i].SubmittedAt.Add(35 * time.Millisecond)
		in.dispatches[i].LeasedAt = &leased
	}
	got := analyzeThroughput(in)

	require.Equal(t, ms(10), got.Dispatch.P95)
	require.Equal(t, ms(35), got.DispatchLease.P95, "the supplementary figure reads the lease issuance")
	require.Equal(t, 5000, got.DispatchLease.N)
}

// --- recovery -------------------------------------------------------------------

func claimedAt(offset time.Duration) *time.Time { v := t0.Add(offset); return &v }

func TestRecoveryTimes_AreEachReplacementsClaimMinusTheKill(t *testing.T) {
	kill := t0.Add(10 * time.Second)
	got := recoveryTimes(kill, []readdb.Recovery{
		{JobID: uuid.New(), AbandonedAttempt: 1, ReplacementClaimedAt: claimedAt(41 * time.Second)},
		{JobID: uuid.New(), AbandonedAttempt: 1, ReplacementClaimedAt: claimedAt(36 * time.Second)},
		{JobID: uuid.New(), AbandonedAttempt: 2, ReplacementClaimedAt: nil},
	})

	require.ElementsMatch(t, []time.Duration{31 * time.Second, 26 * time.Second}, got.Durations)
	require.Equal(t, 3, got.Affected)
	require.Equal(t, 1, got.Unrecovered, "an attempt with no replacement is reported, not dropped")
	require.Empty(t, got.Problems)
}

func TestRecoveryTimes_AReplacementBeforeTheKillIsAProblem(t *testing.T) {
	kill := t0.Add(10 * time.Second)
	got := recoveryTimes(kill, []readdb.Recovery{
		{JobID: uuid.New(), AbandonedAttempt: 1, ReplacementClaimedAt: claimedAt(9 * time.Second)},
	})
	require.NotEmpty(t, got.Problems)
	require.Contains(t, got.Problems[0], "before the kill")
}

func TestRecoveryTimes_NothingAbandonedIsNothingMeasured(t *testing.T) {
	got := recoveryTimes(t0, nil)
	require.Zero(t, got.Affected)
	require.Empty(t, got.Durations)
}

func TestAnalyzeThroughput_AProblemTheRunnerObservedMakesTheRunInvalid(t *testing.T) {
	in := steadyInputs(10*time.Millisecond, 55*time.Millisecond)
	in.observed = []string{"worker w04 exited on its own during the run"}
	got := analyzeThroughput(in)

	require.False(t, got.Valid)
	require.Contains(t, got.Problems, "worker w04 exited on its own during the run")
}
