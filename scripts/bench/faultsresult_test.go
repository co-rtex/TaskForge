package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/scripts/readdb"
)

func killAt(index int, offset time.Duration, worker string, held int, abandoned ...readdb.Recovery) killObservation {
	return killObservation{
		Index: index, ScheduledOffset: offset, KilledAt: t0.Add(offset), Worker: worker,
		Session: uuid.New(), HeldAtSelection: held, ChoseOccupied: held > 0, Abandoned: abandoned,
	}
}

func replacedAt(offset time.Duration) readdb.Recovery {
	return readdb.Recovery{JobID: uuid.New(), AbandonedAttempt: 1, ReplacementClaimedAt: claimedAt(offset)}
}

func faultInputs(kills ...killObservation) faultsInputs {
	return faultsInputs{
		profile: "shipped", seed: 42, workers: 12, concurrency: 4, targetRate: 1000,
		jobsTarget: 10000, submitted: 10000, jobsInDatabase: 10000,
		kills: kills, scheduledKills: len(kills),
		submissionDuration: 10 * time.Minute, deadline: 5 * time.Minute, endedBy: "all jobs terminal",
		statuses: map[string]int{"SUCCEEDED": 10000},
	}
}

func TestAnalyzeFaults_RecoveryIsAggregatedAcrossKillsAndKeepsEveryAttempt(t *testing.T) {
	in := faultInputs(
		killAt(0, 20*time.Second, "w03", 2, replacedAt(20*time.Second+31*time.Second), replacedAt(20*time.Second+33*time.Second)),
		killAt(1, 45*time.Second, "w07", 0), // an idle worker: killed, and nothing was affected
		killAt(2, 70*time.Second, "w01", 1, replacedAt(70*time.Second+32*time.Second)),
		killAt(3, 95*time.Second, "w02", 1, readdb.Recovery{JobID: uuid.New(), AbandonedAttempt: 1}),
	)
	got := analyzeFaults(in)

	require.Equal(t, 4, got.KillsDone)
	require.Equal(t, 4, got.Affected, "2 + 0 + 1 + 1 abandoned attempts")
	require.Equal(t, 1, got.Unrecovered)
	require.Equal(t, Summary{N: 3, P50: 32 * time.Second, P95: 33 * time.Second, P99: 33 * time.Second, Max: 33 * time.Second}, got.Recovery)

	require.Len(t, got.Kills, 4)
	require.Equal(t, 2, got.Kills[0].Affected)
	require.Equal(t, []float64{31, 33}, sorted(got.Kills[0].RecoverySeconds))
	require.Zero(t, got.Kills[1].Affected, "an idle worker's kill is in the timeline with nothing affected")
	require.Equal(t, 1, got.Kills[3].Unrecovered)
}

func sorted(v []float64) []float64 {
	out := append([]float64(nil), v...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func TestAnalyzeFaults_CompletionIsSucceededOverTheJobsThatExist(t *testing.T) {
	in := faultInputs(killAt(0, 20*time.Second, "w01", 1))
	in.statuses = map[string]int{"SUCCEEDED": 9985, "DEAD_LETTERED": 3, "RUNNING": 12}
	in.dlq = map[string]int{"ATTEMPTS_EXHAUSTED": 3}
	got := analyzeFaults(in)

	require.Equal(t, 9985, got.Succeeded)
	require.InDelta(t, 99.85, got.CompletionPercent, 1e-9)
	require.Equal(t, map[string]int{"SUCCEEDED": 9985, "DEAD_LETTERED": 3, "RUNNING": 12}, got.FinalStatuses, "every other final status is reported")
	require.Equal(t, map[string]int{"ATTEMPTS_EXHAUSTED": 3}, got.DLQReasons)
}

func TestAnalyzeFaults_ARunThatDegradedIsInvalidAndSaysWhy(t *testing.T) {
	// Fewer kills than were scheduled.
	in := faultInputs(killAt(0, 20*time.Second, "w01", 1))
	in.scheduledKills = 24
	got := analyzeFaults(in)
	require.False(t, got.Valid)
	require.Contains(t, got.Problems[0], "1 of the 24")

	// A worker that could not be restarted.
	in = faultInputs(killAt(0, 20*time.Second, "w01", 1))
	in.kills[0].RestartError = "worker-w01 exited before it was ready"
	got = analyzeFaults(in)
	require.False(t, got.Valid)
	require.Contains(t, got.Problems[0], "restart")

	// Rows that do not match what the API accepted.
	in = faultInputs(killAt(0, 20*time.Second, "w01", 1))
	in.jobsInDatabase = 9990
	got = analyzeFaults(in)
	require.False(t, got.Valid)
	require.Contains(t, got.Problems[0], "9990")

	// A replacement claimed before its kill.
	in = faultInputs(killAt(0, 20*time.Second, "w01", 1, replacedAt(19*time.Second)))
	got = analyzeFaults(in)
	require.False(t, got.Valid)
	require.Contains(t, got.Problems[0], "before the kill")
}

func TestAnalyzeFaults_AHealthyRunIsValid(t *testing.T) {
	got := analyzeFaults(faultInputs(killAt(0, 20*time.Second, "w01", 1, replacedAt(52*time.Second))))
	require.True(t, got.Valid, "%v", got.Problems)
	require.Empty(t, got.Problems)
}

func TestAnalyzeFaults_AProblemTheRunnerObservedMakesTheRunInvalid(t *testing.T) {
	in := faultInputs(killAt(0, 20*time.Second, "w01", 1, replacedAt(52*time.Second)))
	in.observed = []string{"worker w09 exited on its own during the run"}
	got := analyzeFaults(in)

	require.False(t, got.Valid)
	require.Contains(t, got.Problems, "worker w09 exited on its own during the run")
}

// A kill that was not made is not a kill. With aimed kills (only the smoke) a kill
// can find no attempt with time left and is skipped; it must not be counted as one
// made, must not contribute a victim, a session or a recovery, and must make the
// run invalid with a problem that says why.
func TestAnalyzeFaults_AKillThatWasNotMadeIsNotAKill(t *testing.T) {
	skipped := killObservation{Index: 0, ScheduledOffset: time.Second, NotMade: "no worker held an attempt with at least 1.5s of its 3s left within 2s"}
	res := analyzeFaults(faultInputs(skipped))

	require.Equal(t, 1, res.KillsScheduled)
	require.Equal(t, 0, res.KillsDone, "nothing was killed")
	require.Equal(t, 1, res.KillsUntargeted)
	require.Empty(t, res.Kills, "a skipped kill has no victim, session or time to record")
	require.Zero(t, res.Affected)
	require.False(t, res.Valid, "a run that did not make its kill is not a valid fault run")
	require.Contains(t, strings.Join(res.Problems, "; "), "kill 0 was not made: no worker held an attempt with at least 1.5s")

	// And it is not mistaken for a restart failure or a recovery problem.
	for _, problem := range res.Problems {
		require.NotContains(t, problem, "could not be restarted")
		require.NotContains(t, problem, "never replaced")
	}
}

// The new field must not reach the summary a recorded run writes. A recorded run
// can never have an unmade kill, so its JSON must be exactly what it was.
func TestAnalyzeFaults_UntargetedKillsAreNotInTheSummaryJSON(t *testing.T) {
	res := analyzeFaults(faultInputs(killAt(0, 20*time.Second, "w03", 1, replacedAt(51*time.Second))))
	require.Zero(t, res.KillsUntargeted)

	encoded, err := json.Marshal(res)
	require.NoError(t, err)
	require.NotContains(t, strings.ToLower(string(encoded)), "untargeted")
	require.NotContains(t, string(encoded), "NotMade")
	require.Contains(t, string(encoded), `"kills_done":1`)
}

// targetableCandidates is the filter between what the database says and who may be
// killed: a worker is a candidate only if it holds a targetable attempt AND is
// running now.
func TestTargetableCandidates_AreTheLiveWorkersHoldingATargetableAttempt(t *testing.T) {
	held := map[string]int{"w01": 2, "w02": 0, "w03": 1, "w09": 4}
	live := []string{"w01", "w02", "w03", "w04"}

	require.ElementsMatch(t, []string{"w01", "w03"}, targetableCandidates(held, live),
		"w02 holds none, w04 is absent, and w09 holds some but is not running")
	require.Empty(t, targetableCandidates(map[string]int{}, live))
	require.Empty(t, targetableCandidates(held, nil))

	// With the same draw the same state picks the same worker: the candidates are
	// sorted by ChooseVictim, so the order they were filtered in cannot matter.
	a, err := ChooseVictim(7, targetableCandidates(held, []string{"w03", "w01"}))
	require.NoError(t, err)
	b, err := ChooseVictim(7, targetableCandidates(held, []string{"w01", "w03"}))
	require.NoError(t, err)
	require.Equal(t, a, b)
}
