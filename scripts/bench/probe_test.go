package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/scripts/readdb"
)

func TestRun_TheRecoveryProbeIsNeverRecorded(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"recovery-probe", "--record"}, &stdout, &stderr)

	require.Equal(t, exitUsage, code)
	require.Contains(t, stderr.String(), "recovery-probe is an investigation and is never recorded")
	require.Empty(t, stdout.String(), "refused before anything started")
}

func TestValidateRecordable_RefusesTheProbeWhateverElseIsTrue(t *testing.T) {
	o := defaultOptions()
	o.modes = []string{modeProbe}
	err := o.validateRecordable()
	require.ErrorContains(t, err, "recovery-probe is an investigation and is never recorded")
}

func TestParseArgs_TheProbesDefaultsAndFlags(t *testing.T) {
	o, err := parseArgs([]string{"recovery-probe"})
	require.NoError(t, err)
	require.NotNil(t, o.probe)
	require.Equal(t, profileShipped, o.profile)
	require.Equal(t, []string{"A", "B", "C"}, o.probe.conditions)
	require.Equal(t, map[string]int{"A": 10, "B": 20, "C": 10}, o.probe.trials, "the owner's planned N")
	require.Equal(t, 50*time.Millisecond, o.probe.bJob)
	require.Zero(t, o.probe.visibilityTimeout, "the broker's own default unless varied")
	require.Zero(t, o.probe.pollWait, "the profile's poll wait unless varied")
	require.Equal(t, fixedWorkers, o.workers)
	require.Equal(t, fixedConcurrency, o.concurrency)

	o, err = parseArgs([]string{"recovery-probe", "--conditions", "b", "--trials-b", "5",
		"--visibility-timeout", "15s", "--poll-wait", "2s", "--profile", "tuned"})
	require.NoError(t, err)
	require.Equal(t, []string{"B"}, o.probe.conditions)
	require.Equal(t, 5, o.probe.trials["B"])
	require.Equal(t, 15*time.Second, o.probe.visibilityTimeout)
	timings, err := probeTimings(o)
	require.NoError(t, err)
	require.Equal(t, 2*time.Second, timings.WorkerPollWait, "the variation replaces the profile's")
	require.Equal(t, 10*time.Second, timings.Lease, "and nothing else of the tuned profile")
}

func TestParseArgs_TheProbeRefusesWhatItCannotDo(t *testing.T) {
	for _, args := range [][]string{
		{"recovery-probe", "faults"},
		{"faults", "recovery-probe"},
		{"recovery-probe", "--workers", "3"}, // another mode's flag is not silently ignored
		{"recovery-probe", "--conditions", "D"},
		{"recovery-probe", "--conditions", "A,A"},
		{"recovery-probe", "--trials-c", "0"},
		{"recovery-probe", "--visibility-timeout", "1500ms"},
		{"recovery-probe", "--poll-wait", "500ms"},
		{"recovery-probe", "--profile", "fast"},
	} {
		_, err := parseArgs(args)
		require.Error(t, err, "%v", args)
	}
}

func TestBKillAt_PlacesTheKillAsKill23WasPlaced(t *testing.T) {
	// The shipped lease: 21 s before the last submission, so the lease runs out
	// 9 s after it.
	require.Equal(t, probeBSpan-21*time.Second, bKillAt(30*time.Second))
	// A 10 s lease moves the kill later, so expiry is still after the last submission.
	require.Equal(t, probeBSpan-9*time.Second, bKillAt(10*time.Second))
}

func TestParseClaims_ACountsOnlyClaimRequestsAndJoinsWhatTheyTook(t *testing.T) {
	log := []byte(`not json
{"time":"2026-10-10T10:00:00.100Z","msg":"job claimed","request_id":"r1","job_id":"11111111-1111-1111-1111-111111111111"}
{"time":"2026-10-10T10:00:00.101Z","msg":"http request","request_id":"r1","method":"POST","path":"/internal/v1/claims","status":200}
{"time":"2026-10-10T10:00:01.000Z","msg":"http request","request_id":"r2","method":"POST","path":"/internal/v1/claims","status":200}
{"time":"2026-10-10T10:00:02.000Z","msg":"http request","request_id":"r3","method":"POST","path":"/internal/v1/heartbeats","status":200}
{"time":"2026-10-10T10:00:03.000Z","msg":"job claimed","request_id":"r4","job_id":"22222222-2222-2222-2222-222222222222"}
{"time":"2026-10-10T10:00:03.001Z","msg":"http request","request_id":"r4","method":"POST","path":"/internal/v1/claims","status":200}
{"time":"2026-10-10T10:00:04.000Z","msg":"http request","request_id":"r5","method":"POST","path":"/internal/v1/claims","status":503}
`)
	claims := parseClaims(log)
	require.Len(t, claims, 4, "r1, r2, r4 and r5; not the heartbeat")
	require.Equal(t, "11111111-1111-1111-1111-111111111111", claims[0].jobID)
	require.Empty(t, claims[1].jobID, "r2 took nothing")

	base := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)
	this := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	w := claimsBetween(claims, base.Add(500*time.Millisecond), base.Add(5*time.Second), this)
	require.Equal(t, claimWindow{requests: 3, tookJob: 1, tookThis: 1, failed: 1}, w,
		"r1 ended before the window; r2 took nothing, r4 took this job, r5 failed")

	w = claimsBetween(claims, base.Add(time.Second), base.Add(3001*time.Millisecond), this)
	require.Equal(t, 1, w.requests, "the window is (from, to]: r2 at exactly from is out, r4 at exactly to is in")
}

func TestExcessSegment_NamesTheSegmentFurthestAboveItsBaseline(t *testing.T) {
	baseline := []time.Duration{29900 * time.Millisecond, 1100 * time.Millisecond, 0, 600 * time.Millisecond, 20 * time.Millisecond}
	slow := readdb.Segments{S1: 29950 * time.Millisecond, S2: 1200 * time.Millisecond, S3: -time.Millisecond,
		S4: 700 * time.Millisecond, S5: 18020 * time.Millisecond}
	name, by := excessSegment(slow, baseline)
	require.Equal(t, "S5", name)
	require.Equal(t, 18*time.Second, by)

	reconciler := slow
	reconciler.S5, reconciler.S2 = 20*time.Millisecond, 19*time.Second
	name, _ = excessSegment(reconciler, baseline)
	require.Equal(t, "S2", name)
}

func TestOverThreshold_IsLeasePlusFiveSecondsOrNeverRecovered(t *testing.T) {
	lease := 30 * time.Second
	require.False(t, overThreshold(hopResult{complete: true, recovery: 35 * time.Second}, lease), "exactly lease + 5 s is not over")
	require.True(t, overThreshold(hopResult{complete: true, recovery: 35*time.Second + time.Millisecond}, lease))
	require.True(t, overThreshold(hopResult{complete: false}, lease), "a recovery that never completed is over")
}

func TestParseArgs_NoRestartIsAProbeVariation(t *testing.T) {
	o, err := parseArgs([]string{"recovery-probe", "--no-restart"})
	require.NoError(t, err)
	require.True(t, o.probe.noRestart)
	o, err = parseArgs([]string{"recovery-probe"})
	require.NoError(t, err)
	require.False(t, o.probe.noRestart, "the killed worker is restarted unless asked, as the fault run does")
}

func TestParseClaims_ReadsTheRequestDurationSoTheMidpointCanBePlaced(t *testing.T) {
	log := []byte(`{"time":"2026-10-10T10:00:01.000Z","msg":"http request","request_id":"r","method":"POST","path":"/internal/v1/claims","status":200,"duration":40000000}`)
	claims := parseClaims(log)
	require.Len(t, claims, 1)
	require.Equal(t, 40*time.Millisecond, claims[0].took)
	require.True(t, claims[0].midpoint().Equal(time.Date(2026, 10, 10, 10, 0, 0, 980_000_000, time.UTC)),
		"the end of the request minus half its duration: %v", claims[0].midpoint())
}

func TestDescribeCapacity_NamesEachWorkerAndItsDeadBootLeases(t *testing.T) {
	got := describeCapacity([]readdb.AtCapacity{{Worker: "probeb-123-w01", Active: 4, DeadBoots: 1, Limit: 4}}, nil)
	require.Equal(t, "at their limit: w01 held 4 of 4 (1 on a dead boot)", got)
	require.Contains(t, describeCapacity(nil, nil), "no worker was at its limit")
}
