package main

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// goodSmoke is the pair of results a healthy smoke run produces.
func goodSmoke() (ThroughputResult, FaultsResult) {
	th := ThroughputResult{
		Submitted: 120, JobsInWindow: 80, CompletedInWindow: 79, Dispatch: Summary{N: 80, P50: ms(8), P95: ms(20), Max: ms(30)},
		FinalStatuses: map[string]int{"SUCCEEDED": 120}, Valid: true,
	}
	f := FaultsResult{
		JobsSubmitted: 60, JobsInDatabase: 60, KillsDone: 1, Succeeded: 60,
		Kills:    []KillRecord{{Index: 0, KilledAt: t0, Session: "5b1c3a1e-0000-4000-8000-000000000001", Affected: 2, RecoverySeconds: []float64{11.2, 12.9}}},
		Affected: 2, Recovery: Summary{N: 2, P50: 11 * time.Second, P95: 13 * time.Second, Max: 13 * time.Second},
		FinalStatuses: map[string]int{"SUCCEEDED": 60}, Valid: true,
	}
	return th, f
}

func failed(checks []check) []string {
	var out []string
	for _, c := range checks {
		if !c.ok {
			out = append(out, c.name)
		}
	}
	return out
}

func TestSmokeChecks_AHealthyRunPassesEveryCheck(t *testing.T) {
	th, f := goodSmoke()
	checks := smokeChecks(th, nil, f, nil)

	require.Empty(t, failed(checks))
	require.GreaterOrEqual(t, len(checks), 10, "it must actually check things")
}

// Each flaw below is one the harness could plausibly have, and each must fail
// the check that exists for it, by name. A smoke that cannot fail proves nothing
// about the harness, and one that fails the wrong check proves little more.
func TestSmokeChecks_EachKindOfBrokenMeasurementFailsItsOwnCheck(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*ThroughputResult, *FaultsResult)
		want   string
	}{
		"fewer than 50 jobs":            {func(th *ThroughputResult, f *FaultsResult) { th.Submitted = 49 }, checkSubmitted},
		"a refused submission":          {func(th *ThroughputResult, f *FaultsResult) { th.SubmitErrors = 1 }, checkSubmitted},
		"nothing in the window":         {func(th *ThroughputResult, f *FaultsResult) { th.JobsInWindow = 0 }, checkWindow},
		"no completions in the window":  {func(th *ThroughputResult, f *FaultsResult) { th.CompletedInWindow = 0 }, checkWindow},
		"an unclaimed job":              {func(th *ThroughputResult, f *FaultsResult) { th.Unclaimed = 1 }, checkClaimed},
		"a claim before its submission": {func(th *ThroughputResult, f *FaultsResult) { th.NegativeLatency = 1 }, checkClaimed},
		"latency covers fewer jobs":     {func(th *ThroughputResult, f *FaultsResult) { th.Dispatch.N = 79 }, checkClaimed},
		"an invalid throughput run":     {func(th *ThroughputResult, f *FaultsResult) { th.Valid = false }, checkThroughputValid},
		"a job that did not succeed": {func(th *ThroughputResult, f *FaultsResult) {
			th.FinalStatuses = map[string]int{"SUCCEEDED": 119, "DEAD_LETTERED": 1}
		}, checkThroughputSucceeded},
		"a job still running": {func(th *ThroughputResult, f *FaultsResult) { th.NotTerminalAtDrainEnd = 1 }, checkThroughputSucceeded},
		"fewer than 50 fault jobs": {func(th *ThroughputResult, f *FaultsResult) {
			f.JobsInDatabase, f.JobsSubmitted, f.Succeeded = 49, 49, 49
		}, checkFaultJobs},
		"rows that do not match":          {func(th *ThroughputResult, f *FaultsResult) { f.JobsInDatabase, f.Succeeded = 59, 59 }, checkFaultJobs},
		"no kill made":                    {func(th *ThroughputResult, f *FaultsResult) { f.KillsDone, f.Kills = 0, nil }, checkKill},
		"two kills":                       {func(th *ThroughputResult, f *FaultsResult) { f.KillsDone = 2 }, checkKill},
		"a kill without a database time":  {func(th *ThroughputResult, f *FaultsResult) { f.Kills[0].KilledAt = time.Time{} }, checkKill},
		"a kill that hit nothing":         {func(th *ThroughputResult, f *FaultsResult) { f.Affected, f.Recovery = 0, Summary{} }, checkRecovered},
		"an attempt never replaced":       {func(th *ThroughputResult, f *FaultsResult) { f.Unrecovered = 1 }, checkRecovered},
		"a non-positive recovery":         {func(th *ThroughputResult, f *FaultsResult) { f.Kills[0].RecoverySeconds = []float64{0} }, checkPositive},
		"a fault job that did not finish": {func(th *ThroughputResult, f *FaultsResult) { f.Succeeded = 59 }, checkFaultsSucceeded},
		"an invalid fault run":            {func(th *ThroughputResult, f *FaultsResult) { f.Valid = false }, checkFaultsValid},
	} {
		th, f := goodSmoke()
		tc.mutate(&th, &f)

		require.Containsf(t, failed(smokeChecks(th, nil, f, nil)), tc.want, "%s must fail %q", name, tc.want)
	}
}

func TestSmokeChecks_ARunThatCouldNotStartFailsInsteadOfPassingVacuously(t *testing.T) {
	th, f := goodSmoke()

	got := failed(smokeChecks(ThroughputResult{}, errors.New("postgres is down"), f, nil))
	require.Contains(t, got, checkThroughputRan)

	got = failed(smokeChecks(th, nil, FaultsResult{}, errors.New("no binaries")))
	require.Contains(t, got, checkFaultsRan)
}
