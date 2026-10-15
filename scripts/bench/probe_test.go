package main

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/scripts/internal/stack"
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
	require.Equal(t, map[string]int{"A": 10, "B": 20, "C": 10, "D": 10, "E": 10}, o.probe.trials, "the owner's planned N")
	require.Zero(t, o.probe.timeoutSeconds, "each condition's own timeout unless varied")
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
		{"recovery-probe", "--conditions", "F"},
		{"recovery-probe", "--conditions", "D,d"},
		{"recovery-probe", "--conditions", "D", "--trials-d", "0"},
		{"recovery-probe", "--conditions", "E", "--trials-e", "-1"},
		{"recovery-probe", "--timeout-seconds", "0"},
		{"recovery-probe", "--timeout-seconds", "-5"},
		{"recovery-probe", "--timeout-seconds", "1.5"},
		{"recovery-probe", "--timeout-seconds", "86401"},
		{"recovery-probe", "--timeout-seconds", "30s"},
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

func TestExcessSegment_NamesTheSegmentFurthestAboveItsNominalValue(t *testing.T) {
	baseline := nominalSegments(stack.ShippedTimings())
	require.Equal(t, []time.Duration{30 * time.Second, 2 * time.Second, 0, time.Second, 0}, baseline,
		"the lease, a reconciler poll, nothing, an outbox poll, nothing")
	slow := readdb.Segments{S1: 29950 * time.Millisecond, S2: 1200 * time.Millisecond, S3: -time.Millisecond,
		S4: 700 * time.Millisecond, S5: 18020 * time.Millisecond}
	name, by := excessSegment(slow, baseline)
	require.Equal(t, "S5", name)
	require.Equal(t, 18020*time.Millisecond, by)

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

func TestParseClaims_ReadsTheRequestDuration(t *testing.T) {
	log := []byte(`{"time":"2026-10-10T10:00:01.000Z","msg":"http request","request_id":"r","method":"POST","path":"/internal/v1/claims","status":200,"duration":40000000}`)
	claims := parseClaims(log)
	require.Len(t, claims, 1)
	require.Equal(t, 40*time.Millisecond, claims[0].took, "slog writes a duration in nanoseconds")
}

func TestDescribeCapacity_NamesEachWorkerAndItsDeadBootLeases(t *testing.T) {
	at := time.Date(2026, 10, 10, 23, 52, 45, 539_000_000, time.UTC)
	got := describeCapacity([]readdb.AtCapacity{{Worker: "probeb-123-w01", At: at, Active: 4, DeadBoots: 1, Limit: 4}}, nil)
	require.Equal(t, "at their limit: w01 held 4 of 4 (1 on a dead boot) at 23:52:45.539 UTC", got)
	require.Contains(t, describeCapacity(nil, nil), "no worker reached its limit")
}

func TestParseArgs_ConditionsDAndEAndTheTimeoutVariation(t *testing.T) {
	o, err := parseArgs([]string{"recovery-probe", "--conditions", "d,E", "--trials-d", "3", "--trials-e", "4",
		"--timeout-seconds", "300"})
	require.NoError(t, err)
	require.Equal(t, []string{"D", "E"}, o.probe.conditions)
	require.Equal(t, 3, o.probe.trials["D"])
	require.Equal(t, 4, o.probe.trials["E"])
	require.Equal(t, 300, o.probe.timeoutSeconds)

	o, err = parseArgs([]string{"recovery-probe", "--timeout-seconds", "1"})
	require.NoError(t, err)
	require.Equal(t, 1, o.probe.timeoutSeconds, "1 is the smallest whole number allowed")
	o, err = parseArgs([]string{"recovery-probe", "--timeout-seconds", "86400"})
	require.NoError(t, err)
	require.Equal(t, 86400, o.probe.timeoutSeconds, "the API's own upper bound")
}

func TestConditionTimeoutAndDuration_EachConditionsOwnUnlessVaried(t *testing.T) {
	require.Equal(t, 30, conditionTimeout(condLoaded, 0), "A keeps the recorded runs' 30 s")
	require.Equal(t, 30, conditionTimeout(condTail, 0))
	require.Equal(t, 300, conditionTimeout(condIdle, 0))
	require.Equal(t, 30, conditionTimeout(condRunning, 0), "D keeps the recorded runs' 30 s, the lease's length")
	require.Equal(t, 25, conditionTimeout(condRenewed, 0), "E's deadline falls before its renewed lease's expiry")
	for _, c := range []string{condLoaded, condTail, condIdle, condRunning, condRenewed} {
		require.Equal(t, 300, conditionTimeout(c, 300), "--timeout-seconds applies to %s", c)
	}

	b := 70 * time.Millisecond
	require.Equal(t, 50*time.Millisecond, conditionJobDuration(condLoaded, b))
	require.Equal(t, b, conditionJobDuration(condTail, b))
	require.Equal(t, 40*time.Second, conditionJobDuration(condIdle, b))
	require.Equal(t, 5*time.Second, conditionJobDuration(condRunning, b), "RUNNING at a 1 s kill, never renewed")
	require.Equal(t, 20*time.Second, conditionJobDuration(condRenewed, b), "longer than one 10 s renew interval")
	require.Less(t, conditionJobDuration(condRenewed, b), time.Duration(conditionTimeout(condRenewed, 0))*time.Second,
		"E's job would succeed if its worker lived")
	require.Less(t, conditionJobDuration(condRunning, b), stack.ShippedTimings().Lease/3,
		"D's job ends before the first renewal")
}

func TestRun_TheProbesNewFlagsAreRefusedWithRecord(t *testing.T) {
	for _, args := range [][]string{
		{"recovery-probe", "--conditions", "D", "--record"},
		{"recovery-probe", "--conditions", "E", "--trials-e", "2", "--record"},
		{"recovery-probe", "--timeout-seconds", "300", "--record"},
		{"recovery-probe", "--record", "--trials-d", "1", "--timeout-seconds", "30"},
	} {
		var stdout, stderr bytes.Buffer
		code := run(args, &stdout, &stderr)
		require.Equal(t, exitUsage, code, "%v", args)
		require.Contains(t, stderr.String(), "recovery-probe is an investigation and is never recorded", "%v", args)
		require.Empty(t, stdout.String(), "%v: refused before anything started", args)
	}
}

// outcome builds one KillOutcome for the report tests.
func outcome(state, status, job, lease string, acquired time.Duration, started, timeout, released, replacement *time.Duration) readdb.KillOutcome {
	at := func(d *time.Duration) *time.Time {
		if d == nil {
			return nil
		}
		v := probeBase.Add(*d)
		return &v
	}
	o := readdb.KillOutcome{
		JobID: uuid.New(), AttemptNumber: 1, StateAtKill: state, Status: status, JobStatus: job,
		LeaseStatus: lease, LeaseAcquiredAt: probeBase.Add(acquired), LeaseExpiresAt: probeBase.Add(acquired + 30*time.Second),
		StartedAt: at(started), TimeoutAt: at(timeout), LeaseReleasedAt: at(released), ReplacementClaimedAt: at(replacement),
	}
	if o.LeaseReleasedAt != nil {
		o.FinishedAt = o.LeaseReleasedAt
	}
	return o
}

var probeBase = time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)

func d(v time.Duration) *time.Duration { return &v }

// renewedPast is o with its lease renewed once and expiring at the given offset
// from probeBase: a lease that can outlive the deadline.
func renewedPast(o readdb.KillOutcome, expires time.Duration) readdb.KillOutcome {
	o.RenewalVersion = 1
	o.LeaseExpiresAt = probeBase.Add(expires)
	return o
}

func TestPrecedenceIdentity_TimedOutExactlyWhenResolvedAtOrAfterTheDeadlineAndTheLeaseOutlivedIt(t *testing.T) {
	// A killed RUNNING attempt never renewed: expires at acquired + 30 s, deadline at
	// started + 30 s, 12 ms later. A pass inside those 12 ms abandons it.
	abandoned := outcome(readdb.HeldRunning, "ABANDONED", "QUEUED", "EXPIRED", 0, d(12*time.Millisecond), d(30012*time.Millisecond), d(30005*time.Millisecond), d(31*time.Second))
	applies, holds := precedenceIdentity(abandoned)
	require.True(t, applies)
	require.True(t, holds, "resolved 7 ms before the deadline: ABANDONED")

	// M8D5: a pass AFTER the deadline abandons it too, because the lease lapsed
	// first (ADR-0025). The same instants recorded as TIMED_OUT, the old rule's
	// outcome, fail the identity.
	lapsedFirst := outcome(readdb.HeldRunning, "ABANDONED", "QUEUED", "EXPIRED", 0, d(12*time.Millisecond), d(30012*time.Millisecond), d(31*time.Second), d(31*time.Second))
	applies, holds = precedenceIdentity(lapsedFirst)
	require.True(t, applies)
	require.True(t, holds, "lapsed 12 ms before a deadline resolved 988 ms late: ABANDONED")
	oldRule := lapsedFirst
	oldRule.Status, oldRule.JobStatus = "TIMED_OUT", "RETRY_WAIT"
	_, holds = precedenceIdentity(oldRule)
	require.False(t, holds, "TIMED_OUT for a lease that lapsed before the deadline is the rule M8D5 replaced")

	// A lease renewed past the deadline, lapsed after it, resolved after both.
	timedOut := renewedPast(outcome(readdb.HeldRunning, "TIMED_OUT", "RETRY_WAIT", "EXPIRED", 0, d(12*time.Millisecond), d(30012*time.Millisecond), d(31*time.Second), d(34*time.Second)), 30500*time.Millisecond)
	applies, holds = precedenceIdentity(timedOut)
	require.True(t, applies)
	require.True(t, holds)
	abandonedAnyway := timedOut
	abandonedAnyway.Status = "ABANDONED"
	_, holds = precedenceIdentity(abandonedAnyway)
	require.False(t, holds, "ABANDONED for a lease that outlived a due deadline contradicts the precedence")

	// The tie: expires_at == timeout_at is TIMED_OUT.
	tie := timedOut
	tie.LeaseExpiresAt = *tie.TimeoutAt
	_, holds = precedenceIdentity(tie)
	require.True(t, holds, "expires_at == timeout_at, resolved after: TIMED_OUT")
	tieAbandoned := tie
	tieAbandoned.Status = "ABANDONED"
	_, holds = precedenceIdentity(tieAbandoned)
	require.False(t, holds, "ABANDONED at the tie contradicts the precedence")
	justBefore := tie
	justBefore.LeaseExpiresAt = tie.TimeoutAt.Add(-time.Microsecond)
	_, holds = precedenceIdentity(justBefore)
	require.False(t, holds, "one microsecond before the deadline is an abandonment, not a timeout")

	// The timeout sweep: resolved exactly at the deadline with the lease live.
	exact := timedOut
	exact.LeaseStatus = "RELEASED"
	exact.LeaseReleasedAt = exact.TimeoutAt
	_, holds = precedenceIdentity(exact)
	require.True(t, holds, "resolved exactly at the deadline is due: TIMED_OUT")
	wrong := exact
	wrong.Status = "ABANDONED"
	_, holds = precedenceIdentity(wrong)
	require.False(t, holds, "ABANDONED at the deadline with a live lease would contradict the precedence")

	leased := outcome(readdb.HeldLeased, "ABANDONED", "QUEUED", "EXPIRED", 0, nil, nil, d(31*time.Second), d(32*time.Second))
	_, holds = precedenceIdentity(leased)
	require.True(t, holds, "no deadline: ABANDONED")

	for _, status := range []string{"SUCCEEDED", "CANCELED", "RUNNING"} {
		o := timedOut
		o.Status = status
		applies, _ = precedenceIdentity(o)
		require.False(t, applies, "%s is not decided by the timeout precedence", status)
	}
}

func TestNoAbandoned_SplitsKillsThatAbandonedNothing(t *testing.T) {
	abandoned := outcome(readdb.HeldLeased, "ABANDONED", "QUEUED", "EXPIRED", 0, nil, nil, d(31*time.Second), d(32*time.Second))
	timedOut := outcome(readdb.HeldRunning, "TIMED_OUT", "RETRY_WAIT", "EXPIRED", 0, d(time.Millisecond), d(30001*time.Millisecond), d(31*time.Second), d(34*time.Second))
	dlq := timedOut
	dlq.JobStatus = "DEAD_LETTERED"
	succeeded := outcome(readdb.HeldRunning, "SUCCEEDED", "SUCCEEDED", "COMPLETED", 0, d(time.Millisecond), d(30001*time.Millisecond), d(40*time.Millisecond), nil)
	failed := succeeded
	failed.Status = "FAILED"
	finished := []readdb.Finished{{Status: "SUCCEEDED"}}

	require.Empty(t, noAbandoned(trialResult{worker: "w01", outcomes: []readdb.KillOutcome{timedOut, abandoned}}), "one ABANDONED is enough")
	require.Equal(t, "TIMED_OUT", noAbandoned(trialResult{worker: "w01", outcomes: []readdb.KillOutcome{timedOut}}))
	require.Equal(t, "TIMED_OUT", noAbandoned(trialResult{worker: "w01", outcomes: []readdb.KillOutcome{dlq}}), "dead-lettered on a timeout")
	require.Equal(t, "finished before the signal", noAbandoned(trialResult{worker: "w01", finishedBeforeKill: finished}))
	require.Equal(t, "finished before the signal", noAbandoned(trialResult{worker: "w01", outcomes: []readdb.KillOutcome{succeeded}}),
		"a report committing as the signal was sent")
	require.Equal(t, "other", noAbandoned(trialResult{worker: "w01", outcomes: []readdb.KillOutcome{failed}, finishedBeforeKill: finished}))
	require.Equal(t, "other", noAbandoned(trialResult{worker: "w01"}), "a victim, but nothing caught and nothing finished")
}

func TestSummarizeOutcomes_KeepsEveryDenominatorAndTheTwoFiguresApart(t *testing.T) {
	kill := probeBase.Add(time.Second)
	abandoned := outcome(readdb.HeldLeased, "ABANDONED", "QUEUED", "EXPIRED", 0, nil, nil, d(31*time.Second), d(33*time.Second))
	timedOut := renewedPast(outcome(readdb.HeldRunning, "TIMED_OUT", "RETRY_WAIT", "EXPIRED", 0, d(20*time.Millisecond), d(30020*time.Millisecond), d(31*time.Second), d(35*time.Second)), 30500*time.Millisecond)
	swept := outcome(readdb.HeldRunning, "TIMED_OUT", "RETRY_WAIT", "RELEASED", 0, d(10*time.Millisecond), d(25010*time.Millisecond), d(26*time.Second), d(29*time.Second))
	trials := []trialResult{
		{worker: "w01", killedAt: kill, outcomes: []readdb.KillOutcome{abandoned, timedOut}},
		{worker: "w01", killedAt: kill, outcomes: []readdb.KillOutcome{swept}},
		{worker: "w01", killedAt: kill, missed: true, finishedBeforeKill: []readdb.Finished{{Status: "SUCCEEDED"}}},
	}
	s := summarizeOutcomes(trials)
	require.Equal(t, 3, s.trials)
	require.Equal(t, 1, s.missed)
	require.Equal(t, 2, s.withHeld)
	require.Equal(t, map[string]int{readdb.HeldLeased: 1, readdb.HeldRunning: 2}, s.held)
	require.Equal(t, map[string]int{readdb.OutcomeAbandoned: 1, readdb.OutcomeTimedOut: 2}, s.final)
	require.Equal(t, 2, s.byState[readdb.HeldRunning][readdb.OutcomeTimedOut])
	require.Equal(t, map[string]int{"the expired-lease path": 1, "the timeout sweep, lease live": 1}, s.takenBy)
	require.Equal(t, 3, s.identityChecked)
	require.Equal(t, 3, s.identityHeld)
	require.Equal(t, []time.Duration{20 * time.Millisecond, 10 * time.Millisecond}, s.claimToStart, "RUNNING at the kill only")
	require.Equal(t, []time.Duration{32 * time.Second}, s.abandonedToReplacement, "the recovery figure")
	require.Equal(t, []time.Duration{34 * time.Second, 28 * time.Second}, s.timedOutToReplacement, "its own figure, never merged")
	require.Equal(t, 2, s.noAbandonedTrials)
	require.Equal(t, map[string]int{"TIMED_OUT": 1, "finished before the signal": 1}, s.noAbandoned)

	var out bytes.Buffer
	printOutcomeSummary(&out, condRunning, trials)
	text := out.String()
	require.Contains(t, text, "trials run 3 (2 counted, 1 missed and run again); trials with a victim: 3; trials with >=1 attempt unfinished at the kill: 2")
	require.Contains(t, text, "final, of 3: ABANDONED 1, DEAD_LETTERED_ON_ABANDON 0, TIMED_OUT 2, DEAD_LETTERED_ON_TIMEOUT 0, CANCELED 0, UNRESOLVED 0, OTHER 0")
	require.Contains(t, text, "precedence identity (TIMED_OUT exactly when released_at >= timeout_at and expires_at >= timeout_at): held in 3 of 3")
	require.Contains(t, text, "kill->replacement, ABANDONED (the recovery figure): 1; min 32000 ms, median 32000 ms, max 32000 ms")
	require.Contains(t, text, "kill->replacement, TIMED_OUT (its own figure, not a recovery): 2; min 28000 ms")
	require.Contains(t, text, "trials with no ABANDONED attempt: 2 of 3 with a victim (finished before the signal 1, TIMED_OUT 1, other 0); misses with no victim: 0")
}

func TestSummarizeOutcomes_AnUnreadableTrialIsCountedAndLeftOut(t *testing.T) {
	s := summarizeOutcomes([]trialResult{{outcomeErr: errors.New("read failed")}})
	require.Equal(t, 1, s.trials)
	require.Equal(t, 1, s.outcomeErrors)
	require.Zero(t, s.withHeld)
	require.Zero(t, s.noAbandonedTrials, "not classified on no data")
}

func TestOutcomesSettled_WaitsForEveryHeldAttemptToBeReplacedOrFinished(t *testing.T) {
	replaced := outcome(readdb.HeldRunning, "TIMED_OUT", "QUEUED", "EXPIRED", 0, d(time.Millisecond), d(30001*time.Millisecond), d(31*time.Second), d(34*time.Second))
	waiting := replaced
	waiting.JobStatus, waiting.ReplacementClaimedAt = "RETRY_WAIT", nil
	dlq := waiting
	dlq.JobStatus = "DEAD_LETTERED"
	running := replaced
	running.Status, running.ReplacementClaimedAt = "RUNNING", nil

	require.True(t, outcomesSettled([]readdb.KillOutcome{replaced}, 1))
	require.False(t, outcomesSettled([]readdb.KillOutcome{waiting}, 1), "RETRY_WAIT: the replacement is still to come")
	require.True(t, outcomesSettled([]readdb.KillOutcome{dlq}, 1), "dead-lettered: no replacement will come")
	require.False(t, outcomesSettled([]readdb.KillOutcome{running}, 1), "not resolved yet")
	require.False(t, outcomesSettled([]readdb.KillOutcome{replaced}, 2), "fewer outcomes than attempts held")
	require.False(t, outcomesSettled(nil, 0), "nothing to say settled about")
}

func TestPrintOutcomes_NamesEachAttemptsPathAndFigures(t *testing.T) {
	kill := probeBase.Add(2 * time.Second)
	o := renewedPast(outcome(readdb.HeldRunning, "TIMED_OUT", "RETRY_WAIT", "EXPIRED", 0, d(15*time.Millisecond), d(30015*time.Millisecond), d(31*time.Second), d(35*time.Second)), 30500*time.Millisecond)
	code := "timeout"
	o.ErrorCode = &code
	retry := o.FinishedAt.Add(900 * time.Millisecond)
	o.RetryAt = &retry
	var out bytes.Buffer
	printOutcomes(&out, trialResult{killedAt: kill, chosenAt: kill.Add(-4 * time.Millisecond), timeoutSeconds: 30, held: 1, outcomes: []readdb.KillOutcome{o}})
	text := out.String()
	require.Contains(t, text, "held when dead 1, outcomes 1: agree\n")
	require.Contains(t, text, "outcomes (timeout_seconds 30): victim chosen at PostgreSQL 12:00:01.996 UTC, 4 ms before the kill; 1 attempt(s) unfinished at the kill (LEASED 0, RUNNING 1, CREATED_AFTER_KILL 0); 0 finished between the choice and the kill")
	require.Contains(t, text, "RUNNING at the kill -> TIMED_OUT (job now RETRY_WAIT); code timeout; lease EXPIRED, renewed 1x; claim->start 15 ms; timeout_at - expires_at -485 ms; resolved 500 ms after expires_at by the expired-lease path; retry_at = finished + 900 ms; kill->replacement 33000 ms; precedence identity HOLDS")

	out.Reset()
	printOutcomes(&out, trialResult{killedAt: kill, held: 2, outcomes: []readdb.KillOutcome{o}})
	require.Contains(t, out.String(), "held when dead 2, outcomes 1: MISMATCH: fewer outcomes than attempts held when dead\n")
	s := summarizeOutcomes([]trialResult{{worker: "w01", killedAt: kill, held: 2, outcomes: []readdb.KillOutcome{o}},
		{worker: "w02", killedAt: kill, held: 1, outcomes: []readdb.KillOutcome{o}}})
	require.Equal(t, 1, s.heldMismatch)
	require.Equal(t, 1, s.heldAgree)

	out.Reset()
	printOutcomes(&out, trialResult{outcomeErr: errors.New("boom")})
	require.Equal(t, "outcomes: UNAVAILABLE: boom\n", out.String())
}

func TestNoAbandoned_AMissWithNoVictimIsItsOwnCategory(t *testing.T) {
	// A miss that never chose a victim (no worker held a targetable attempt) caught
	// nothing because nothing was aimed at: it is not "other" (review B3).
	noVictim := trialResult{missed: true, missReason: "no worker held an attempt"}
	require.Equal(t, "no victim", noAbandoned(noVictim))

	// A victim that was chosen but whose kill caught nothing and saw nothing finish
	// is still "other".
	require.Equal(t, "other", noAbandoned(trialResult{worker: "w01"}))

	kill := probeBase.Add(time.Second)
	abandoned := outcome(readdb.HeldLeased, "ABANDONED", "QUEUED", "EXPIRED", 0, nil, nil, d(31*time.Second), d(33*time.Second))
	trials := []trialResult{
		{worker: "w01", killedAt: kill, outcomes: []readdb.KillOutcome{abandoned}},
		noVictim,
	}
	var out bytes.Buffer
	printOutcomeSummary(&out, condLoaded, trials)
	text := out.String()
	require.Contains(t, text, "trials run 2 (1 counted, 1 missed and run again); trials with a victim: 1; trials with >=1 attempt unfinished at the kill: 1")
	require.Contains(t, text, "trials with no ABANDONED attempt: 0 of 1 with a victim (finished before the signal 0, TIMED_OUT 0, other 0); misses with no victim: 1")
}
