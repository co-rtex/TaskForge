package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/co-rtex/TaskForge/scripts/internal/stack"
)

const (
	modeThroughput = "throughput"
	modeFaults     = "faults"
	modeSmoke      = "smoke"
	// modeProbe is M8D3's investigation of a slow worker-failure recovery. It
	// measures no target, is never recorded, and has no Make target; see probe.go.
	modeProbe = "recovery-probe"

	profileShipped = "shipped"
	profileTuned   = "tuned"
)

// The fixed definitions of M8A (docs/adr/0020-benchmark-methodology.md). A
// recorded run may not change any of them; validateRecordable enforces it. The
// flags exist so a developer can run a short version while building and
// debugging the harness, and so that a longer one than the minimum can be run.
const (
	fixedWorkers     = 12
	fixedConcurrency = 4 // workers x concurrency = 48, under the queue's max_concurrency of 100
	fixedRatePerMin  = 1000.0
	minWarmup        = 30 * time.Second
	minWindow        = 5 * time.Minute
	fixedFaultJobs   = 10000
	minKills         = 20

	// The workload, fixed by the owner: demo.sleep for 50ms. It is not searched
	// for and not tuned.
	jobType        = "demo.sleep"
	jobDurationMS  = 50
	jobMaxAttempts = 3  // the API's own default; spelled out so the record shows it
	jobTimeoutSecs = 30 // far above the 50ms of work, so a timeout is never the story

	// defaultKills is how many kills a full run makes. Each is at a seeded time
	// inside its own slot of the submission phase: 24 kills over ten minutes is one
	// about every 25 seconds, comfortably above minKills.
	defaultKills = 24
	// defaultSeed makes the recorded schedule reproducible. It is the date of M8A.
	defaultSeed int64 = 20261002

	// firstKillAfter keeps the first kill out of the stack's start-up, and
	// killMinGap keeps one kill's restart from overlapping the next.
	firstKillAfter = 20 * time.Second
	killMinGap     = 5 * time.Second

	// occupancyWait is how long a kill waits for a worker that is holding an
	// attempt before it settles for any live worker. With 50ms jobs at 1,000 a
	// minute a worker holds one only a few percent of the time, and a kill that
	// hit an idle worker would measure nothing.
	occupancyWait = 2 * time.Second

	// smokeFaultJobDuration is how long each job of the SMOKE's fault run sleeps.
	// The recorded runs keep jobDurationMS; the smoke does not, because a kill that
	// lands on a 50 ms attempt can find it finished by the time the SIGKILL is
	// sent (two database round trips later), and the smoke would then fail "the
	// kill hit an attempt" for a reason that has nothing to do with recovery. With
	// 3 s a worker holds an attempt for long enough that a kill aimed at one with
	// time left (see targetMargin) cannot miss. The arithmetic is in CURRENT_STATE.
	smokeFaultJobDuration = 3 * time.Second

	// completionDeadline is the fixed deadline for "SUCCEEDED by a fixed
	// deadline": five minutes after the last submission. Three lease windows of
	// the shipped profile, which is the slowest recovery any job can need.
	completionDeadline = 5 * time.Minute

	// drainTimeout bounds how long the throughput run waits for its jobs to
	// finish after the window closes.
	drainTimeout = 3 * time.Minute
)

// options is one invocation's choices.
type options struct {
	modes       []string
	record      bool
	profile     string
	seed        int64
	rate        float64
	workers     int
	concurrency int
	warmup      time.Duration
	window      time.Duration
	jobs        int
	kills       int
	// deadline is the fixed completion deadline of the fault run, measured from
	// the last submission. Only the smoke shortens it.
	deadline time.Duration
	// drain bounds how long the throughput run waits for its jobs to finish after
	// the window closes.
	drain time.Duration
	// faultJobDuration is how long each job of the fault run sleeps. A recorded run
	// uses the owner's fixed workload, demo.sleep for jobDurationMS, and
	// validateRecordable refuses anything else. Only the smoke lengthens it.
	faultJobDuration time.Duration
	// targetableKills makes each kill choose its victim only among workers
	// holding an attempt that still has time left, and not make the kill at all if
	// there is none; see killOne. It is off for a recorded run, whose victim
	// selection is fixed, and on only for the smoke.
	targetableKills bool
	// probe is the recovery probe's own settings, and nil for every other mode.
	probe *probeOptions
}

func defaultOptions() options {
	return options{
		profile: profileShipped, seed: defaultSeed, rate: fixedRatePerMin,
		workers: fixedWorkers, concurrency: fixedConcurrency,
		warmup: minWarmup, window: minWindow, jobs: fixedFaultJobs, kills: defaultKills,
		deadline: completionDeadline, drain: drainTimeout,
		faultJobDuration: jobDurationMS * time.Millisecond,
	}
}

// smokeOptions is the CI check's whole configuration, and it ignores every flag.
// It is small enough to finish in about a minute and uses the short-lease
// profile so a killed worker is recovered within seconds. It asserts that the
// harness produced valid measurements and records none.
func smokeOptions() options {
	o := defaultOptions()
	o.modes = []string{modeSmoke}
	o.profile = profileTuned
	o.rate = 1200 // 20 a second, so a worker is often holding an attempt
	o.workers, o.concurrency = 4, 4
	o.warmup, o.window = 2*time.Second, 4*time.Second
	o.jobs, o.kills = 60, 1
	o.deadline, o.drain = 90*time.Second, 60*time.Second
	// The fault phase's jobs are long and its kills are aimed (see smokeFaultJobDuration
	// and killOne). The throughput phase keeps the fixed 50 ms workload.
	o.faultJobDuration, o.targetableKills = smokeFaultJobDuration, true
	return o
}

// targetMargin is how much of a job's duration must be left for an attempt to be
// a target for a kill: half of it. The victim is read from PostgreSQL, then its
// session, then the clock, and then signalled, and an attempt with half its
// duration left cannot finish in that time.
func (o options) targetMargin() time.Duration { return o.faultJobDuration / 2 }

// smokeJobs is how many jobs the smoke's fault phase submits.
func (o options) smokeJobs() int { return o.jobs }

// timings returns the named profile's timings.
func (o options) timings() (stack.Timings, error) {
	switch o.profile {
	case profileShipped:
		return stack.ShippedTimings(), nil
	case profileTuned:
		return stack.TunedTimings(), nil
	default:
		return stack.Timings{}, fmt.Errorf("unknown profile %q: use %q or %q", o.profile, profileShipped, profileTuned)
	}
}

const usageText = `usage: go run ./scripts/bench <mode>... [flags]

modes
  throughput   offer 1,000 jobs/minute for a warm-up and a steady window, then drain
  faults       submit 10,000 jobs while workers are SIGKILLed on a seeded schedule
  smoke        a short run of both that asserts the harness measured validly and
               records nothing (make bench-smoke; the CI check)
  recovery-probe
               M8D3's investigation: kills one worker per trial, on a fresh stack
               each time, and splits each recovery into the segments PostgreSQL
               records. Never recorded, no Make target; runs by itself and takes
               only the flags listed under it below.

  A full run is "throughput faults --record" (make bench): it writes
  docs/benchmarks/<date>-<sha>.md and .json. Refuses on a dirty tree.

flags
  --record               write the record (needs both modes and every fixed definition)
  --profile shipped|tuned  timing profile (default shipped; tuned is always labelled)
  --seed N               seed of the fault schedule (default 20261002)
  --rate N               offered jobs per minute        (fixed at 1000 when recording)
  --workers N            worker processes               (fixed at 12 when recording)
  --concurrency N        slots per worker               (fixed at 4 when recording)
  --warmup D             warm-up before the window      (at least 30s when recording)
  --window D             steady window                  (at least 5m when recording)
  --jobs N               jobs in the fault run          (fixed at 10000 when recording)
  --kills N              workers killed in the fault run (at least 20 when recording)

recovery-probe flags
  --profile shipped|tuned  timing profile (default shipped)
  --seed N               seed of the victim draws (default 20261002)
  --conditions LIST      which conditions, of A,B,C (default A,B,C)
  --trials-a N           trials of A, loaded           (default 10)
  --trials-b N           trials of B, tail             (default 20)
  --trials-c N           trials of C, idle             (default 10)
  --b-job-duration D     B's job duration              (default 50ms)
  --visibility-timeout D the queue's VisibilityTimeout (default: the broker's own)
  --poll-wait D          the workers' poll wait        (default: the profile's)
  --out DIR              where the output and the logs go (default: a new temp dir)
`

// parseArgs reads the modes, which come first, and then the flags.
func parseArgs(args []string) (options, error) {
	o := defaultOptions()
	i := 0
	for i < len(args) && !strings.HasPrefix(args[i], "-") {
		o.modes = append(o.modes, args[i])
		i++
	}

	if slices.Contains(o.modes, modeProbe) {
		return parseProbeArgs(o, args[i:])
	}

	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.BoolVar(&o.record, "record", false, "")
	fs.StringVar(&o.profile, "profile", o.profile, "")
	fs.Int64Var(&o.seed, "seed", o.seed, "")
	fs.Float64Var(&o.rate, "rate", o.rate, "")
	fs.IntVar(&o.workers, "workers", o.workers, "")
	fs.IntVar(&o.concurrency, "concurrency", o.concurrency, "")
	fs.DurationVar(&o.warmup, "warmup", o.warmup, "")
	fs.DurationVar(&o.window, "window", o.window, "")
	fs.IntVar(&o.jobs, "jobs", o.jobs, "")
	fs.IntVar(&o.kills, "kills", o.kills, "")
	if err := fs.Parse(args[i:]); err != nil {
		return options{}, err
	}
	if fs.NArg() > 0 {
		return options{}, fmt.Errorf("%q is not a flag: name every mode before the flags", fs.Arg(0))
	}

	if len(o.modes) == 0 {
		return options{}, errors.New("name a mode: throughput, faults, smoke or recovery-probe")
	}
	seen := map[string]bool{}
	for _, m := range o.modes {
		if m != modeThroughput && m != modeFaults && m != modeSmoke {
			return options{}, fmt.Errorf("unknown mode %q", m)
		}
		if seen[m] {
			return options{}, fmt.Errorf("mode %q is named twice", m)
		}
		seen[m] = true
	}
	if seen[modeSmoke] && len(o.modes) > 1 {
		return options{}, errors.New("smoke runs by itself")
	}
	if _, err := o.timings(); err != nil {
		return options{}, err
	}
	switch {
	case o.rate <= 0:
		return options{}, errors.New("--rate must be positive")
	case o.workers < 1, o.concurrency < 1:
		return options{}, errors.New("--workers and --concurrency must be at least 1")
	case o.warmup < 0, o.window <= 0:
		return options{}, errors.New("--warmup must not be negative and --window must be positive")
	case o.jobs < 1, o.kills < 1:
		return options{}, errors.New("--jobs and --kills must be at least 1")
	}
	return o, nil
}

// validateRecordable returns every reason this invocation may not be recorded.
// A recorded run is the one a number gets filed under, so none of the fixed
// definitions may differ from what the filing says.
func (o options) validateRecordable() error {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	if slices.Contains(o.modes, modeProbe) {
		add("recovery-probe is an investigation and is never recorded: it measures where a recovery's time goes, " +
			"not a target, and it may run with settings ADR-0020 does not allow")
	}

	modes := slices.Clone(o.modes)
	slices.Sort(modes)
	if !slices.Equal(modes, []string{modeFaults, modeThroughput}) {
		add("a recorded run is a full run: name exactly `throughput faults`")
	}
	if o.workers != fixedWorkers {
		add("workers is %d; a recorded run uses %d", o.workers, fixedWorkers)
	}
	if o.concurrency != fixedConcurrency {
		add("concurrency is %d; a recorded run uses %d", o.concurrency, fixedConcurrency)
	}
	if o.rate != fixedRatePerMin {
		add("the offered rate is %v jobs/minute; a recorded run offers %v", o.rate, fixedRatePerMin)
	}
	if o.warmup < minWarmup {
		add("the warm-up is %s; a recorded run warms up for at least %s", o.warmup, minWarmup)
	}
	if o.window < minWindow {
		add("the steady window is %s; a recorded run measures at least %s", o.window, minWindow)
	}
	if o.jobs != fixedFaultJobs {
		add("the fault run submits %d jobs; a recorded run submits %d", o.jobs, fixedFaultJobs)
	}
	if o.kills < minKills {
		add("kills is %d; a recorded run makes at least %d", o.kills, minKills)
	}
	if o.deadline != completionDeadline {
		add("the completion deadline is %s; a recorded run uses %s", o.deadline, completionDeadline)
	}
	if want := jobDurationMS * time.Millisecond; o.faultJobDuration != want {
		add("the fault run's jobs sleep for %s; a recorded run's workload is demo.sleep for %s", o.faultJobDuration, want)
	}
	if o.targetableKills {
		add("kills are aimed at attempts with time left; a recorded run draws its victim as ADR-0020 defines it")
	}
	if o.profile != profileShipped && o.profile != profileTuned {
		add("profile %q is not %q or %q", o.profile, profileShipped, profileTuned)
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("this invocation cannot be recorded:\n  - %s", strings.Join(problems, "\n  - "))
}
