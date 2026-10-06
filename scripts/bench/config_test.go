package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/scripts/internal/stack"
)

func TestParseArgs_ModesComeFirstAndFlagsAfter(t *testing.T) {
	o, err := parseArgs([]string{"throughput", "faults", "--record", "--profile", "tuned", "--seed", "7"})
	require.NoError(t, err)

	require.Equal(t, []string{modeThroughput, modeFaults}, o.modes)
	require.True(t, o.record)
	require.Equal(t, "tuned", o.profile)
	require.Equal(t, int64(7), o.seed)
}

func TestParseArgs_DefaultsAreTheFixedDefinitions(t *testing.T) {
	o, err := parseArgs([]string{"throughput", "faults"})
	require.NoError(t, err)

	require.Equal(t, 12, o.workers)
	require.Equal(t, 4, o.concurrency)
	require.Equal(t, 1000.0, o.rate)
	require.Equal(t, 30*time.Second, o.warmup)
	require.Equal(t, 5*time.Minute, o.window)
	require.Equal(t, 10000, o.jobs)
	require.GreaterOrEqual(t, o.kills, 20)
	require.Equal(t, "shipped", o.profile)
	require.False(t, o.record, "nothing is recorded unless --record is given")
	require.NoError(t, o.validateRecordable(), "the defaults must be a valid recorded run")
}

func TestParseArgs_RefusesWhatItCannotRun(t *testing.T) {
	for name, args := range map[string][]string{
		"no mode":             {},
		"only flags":          {"--record"},
		"unknown mode":        {"saturate"},
		"duplicate mode":      {"throughput", "throughput"},
		"smoke with another":  {"smoke", "faults"},
		"unknown flag":        {"throughput", "--bogus"},
		"a mode after a flag": {"throughput", "--seed", "1", "faults"},
		"unknown profile":     {"throughput", "--profile", "fast"},
		"zero rate":           {"throughput", "--rate", "0"},
		"negative window":     {"throughput", "--window", "-1s"},
	} {
		_, err := parseArgs(args)
		require.Errorf(t, err, "%s: %v", name, args)
	}
}

// The fixed definitions are enforced in code. A recorded run can use the shipped
// profile or the labelled tuned one, and nothing else about it may be changed:
// a number recorded under a different workload would be filed as this one.
func TestValidateRecordable_RefusesToRecordAnyChangedDefinition(t *testing.T) {
	recordable := func(mutate func(*options)) error {
		o, err := parseArgs([]string{"throughput", "faults", "--record"})
		require.NoError(t, err)
		mutate(&o)
		return o.validateRecordable()
	}

	require.NoError(t, recordable(func(o *options) {}))
	require.NoError(t, recordable(func(o *options) { o.profile = "tuned" }))
	require.NoError(t, recordable(func(o *options) { o.seed = 99 }), "the seed is recorded, not fixed")
	require.NoError(t, recordable(func(o *options) { o.kills = 40 }), "more kills than the minimum is allowed")
	require.NoError(t, recordable(func(o *options) { o.warmup = time.Minute; o.window = 10 * time.Minute }), "longer is allowed")

	for name, mutate := range map[string]func(*options){
		"workers":            func(o *options) { o.workers = 6 },
		"concurrency":        func(o *options) { o.concurrency = 8 },
		"rate":               func(o *options) { o.rate = 2000 },
		"a short warm-up":    func(o *options) { o.warmup = 29 * time.Second },
		"a short window":     func(o *options) { o.window = 4*time.Minute + 59*time.Second },
		"fewer jobs":         func(o *options) { o.jobs = 9999 },
		"more jobs":          func(o *options) { o.jobs = 10001 },
		"too few kills":      func(o *options) { o.kills = 19 },
		"a shorter deadline": func(o *options) { o.deadline = time.Minute },
		"only throughput":    func(o *options) { o.modes = []string{modeThroughput} },
		"only faults":        func(o *options) { o.modes = []string{modeFaults} },
		"smoke":              func(o *options) { o.modes = []string{modeSmoke} },
	} {
		require.Errorf(t, recordable(mutate), "a recorded run must refuse to change: %s", name)
	}
}

func TestValidateRecordable_ReportsEveryProblemAtOnce(t *testing.T) {
	o, _ := parseArgs([]string{"throughput", "faults", "--record"})
	o.workers, o.kills = 2, 3
	err := o.validateRecordable()
	require.Error(t, err)
	require.Contains(t, err.Error(), "workers")
	require.Contains(t, err.Error(), "kills")
}

func TestOptionsTimings_SelectsTheNamedProfile(t *testing.T) {
	shipped, err := options{profile: "shipped"}.timings()
	require.NoError(t, err)
	require.Equal(t, stack.ShippedTimings(), shipped)

	tuned, err := options{profile: "tuned"}.timings()
	require.NoError(t, err)
	require.Equal(t, stack.TunedTimings(), tuned)

	_, err = options{profile: "other"}.timings()
	require.Error(t, err)
}

func TestSmokeOptions_AreSmallButStillHonestAboutWhatTheyAre(t *testing.T) {
	o := smokeOptions()

	require.Equal(t, []string{modeSmoke}, o.modes)
	require.False(t, o.record, "smoke never records")
	require.Error(t, o.validateRecordable(), "smoke can never be a recorded run")
	require.Equal(t, "tuned", o.profile, "smoke uses the short-lease profile so it finishes in about a minute")
	require.GreaterOrEqual(t, o.smokeJobs(), 50, "at least 50 jobs")
	require.Equal(t, 1, o.kills, "one fault")
	require.Less(t, o.deadline, completionDeadline, "a failing smoke must not wait five minutes to say so")
}

func TestDefaultOptions_TheCompletionDeadlineIsTheFixedOne(t *testing.T) {
	require.Equal(t, 5*time.Minute, defaultOptions().deadline)
	require.Equal(t, completionDeadline, defaultOptions().deadline)
}

// TestOptions_RecordedRunsKeepTheirWorkloadAndTheirVictimSelection pins what a
// recorded run does and must keep doing: its fault run's jobs are the owner's
// fixed workload, demo.sleep for 50 ms, and its kills draw their victim as
// ADR-0020 defines it, not among attempts with time left. The defaults and a
// parsed --record invocation both carry that, and a run that changed either could
// not be recorded.
func TestOptions_RecordedRunsKeepTheirWorkloadAndTheirVictimSelection(t *testing.T) {
	recorded, err := parseArgs([]string{"throughput", "faults", "--record"})
	require.NoError(t, err)

	for name, o := range map[string]options{"the defaults": defaultOptions(), "a parsed --record invocation": recorded} {
		require.Equal(t, 50*time.Millisecond, o.faultJobDuration, "%s: the fault run's jobs sleep for 50 ms", name)
		require.Equal(t, jobDurationMS*time.Millisecond, o.faultJobDuration, name)
		require.False(t, o.targetableKills, "%s: the victim is drawn as before", name)
		require.Nil(t, targetingFor(o), "%s: no targeting, so killOne takes its original path", name)
	}
	require.NoError(t, recorded.validateRecordable())

	for name, mutate := range map[string]func(*options){
		"a longer fault-run job": func(o *options) { o.faultJobDuration = 3 * time.Second },
		"targeted kills":         func(o *options) { o.targetableKills = true },
	} {
		o := recorded
		mutate(&o)
		err := o.validateRecordable()
		require.Errorf(t, err, "a recorded run must refuse: %s", name)
	}
}

// TestSmokeOptions_LengthenOnlyTheFaultPhaseAndAimTheKill pins what the smoke does
// differently: its fault run's jobs are long enough that a kill aimed at an attempt
// with time left cannot find it finished, and its kill is aimed. Its throughput
// phase has no option for the duration and keeps the fixed 50 ms (see
// TestRunThroughput_UsesTheFixedWorkload).
func TestSmokeOptions_LengthenOnlyTheFaultPhaseAndAimTheKill(t *testing.T) {
	o := smokeOptions()

	require.Equal(t, smokeFaultJobDuration, o.faultJobDuration)
	require.Equal(t, 3*time.Second, o.faultJobDuration)
	require.True(t, o.targetableKills)
	require.Equal(t, 1500*time.Millisecond, o.targetMargin(), "half the duration must be left")

	target := targetingFor(o)
	require.NotNil(t, target)
	require.Equal(t, 3*time.Second, target.duration)
	require.Equal(t, 1500*time.Millisecond, target.margin)

	// The smoke's own bounds still hold with the longer jobs: a fault run of 60 jobs
	// over 16 slots drains in about 12 s, far inside the smoke's 90 s deadline. The
	// arithmetic is in CURRENT_STATE; this keeps the two numbers from drifting
	// apart without anyone noticing.
	slots := o.workers * o.concurrency
	drain := time.Duration((o.jobs+slots-1)/slots) * o.faultJobDuration
	require.Less(t, drain, o.deadline/2, "the jobs alone must take well under the completion deadline")
}
