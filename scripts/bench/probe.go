package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/scripts/internal/stack"
	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// The recovery probe (M8D3, extended by M8D4). The headline benchmark recorded one worker-failure
// recovery of 50.04 s against a median of 32.02 s with a 30 s lease, and nothing in
// the record could say where the extra time went. The probe kills one worker per
// trial, under three conditions, and splits each recovery into the segments
// PostgreSQL records (readdb.Segments), together with what the broker held at the
// kill and just after the recovery event was published, and which notification
// produced the replacement claim.
//
// It is an investigation, not a measurement of a target: it is never recorded
// (validateRecordable refuses it), it has no Make target, and nothing it prints is
// a benchmark figure. Each trial runs on a stack of its own, started and stopped
// for that trial alone: a new run id, so a new key scope, a new broker queue and
// new api, outbox, scheduler, reconciler and worker processes. Nothing carries
// from one trial to the next but rows of other scopes in PostgreSQL, and before
// each trial the probe refuses to start if any other scope holds work the
// trial's services would act on (readdb.ForeignActivity).
//
// M8D4 added what became of every attempt a kill caught, whatever status it ended
// in (readdb.KillOutcomes), because a killed worker's RUNNING attempt can end
// TIMED_OUT rather than ABANDONED and then contributes nothing to the recovery
// figure; two conditions that make that happen on purpose, D and E; and a
// --timeout-seconds variation of the submitted jobs' timeout_seconds.

// The conditions.
const (
	condLoaded  = "A"
	condTail    = "B"
	condIdle    = "C"
	condRunning = "D"
	condRenewed = "E"
)

var conditionNames = map[string]string{
	condLoaded:  "loaded: kill under steady load, recovery while load continues",
	condTail:    "tail: kill so that lease expiry and recovery fall after submission has ended",
	condIdle:    "idle: one long job, nothing else submitted, its worker killed mid-job",
	condRunning: "running: a few 5 s jobs, a worker killed once its attempts are RUNNING and before any renewal",
	condRenewed: "renewed: a few 20 s jobs with a 25 s timeout, a worker killed after its lease was renewed",
}

// The probe's fixed settings. The fleet and the offered rate are the recorded runs'.
const (
	// probeAJobDuration is condition A's workload: the recorded runs' 50 ms.
	probeAJobDuration = jobDurationMS * time.Millisecond
	// probeAKillAfter keeps A's kill out of the stack's start-up.
	probeAKillAfter = 20 * time.Second
	// probeALoadAfterRecovery is how long A's load continues past kill + lease,
	// so the recovery happens while jobs are still arriving.
	probeALoadAfterRecovery = 20 * time.Second

	// probeBDefaultJobDuration is B's workload unless --b-job-duration says
	// otherwise: the recorded runs' 50 ms, which the targeted kill hits.
	probeBDefaultJobDuration = jobDurationMS * time.Millisecond
	// probeBSpan is how long B submits for.
	probeBSpan = 45 * time.Second
	// probeBKillBeforeEnd places B's kill as the headline run's kill 23 was placed:
	// at 578.9 s of a 599.96 s submission, 21.06 s before its end, so that with
	// the shipped 30 s lease the lease expires about 9 s after the last
	// submission. For a shorter lease the kill moves later (see bKillAt).
	probeBKillBeforeEnd = 21 * time.Second

	// probeCJobDuration is C's one job. It is longer than the lease, so the
	// worker renews it, and its timeout is far longer than that, so the lapsed
	// lease is an abandonment and never a timeout.
	probeCJobDuration = 40 * time.Second
	probeCJobTimeout  = 300 // seconds
	// probeCRunBeforeKill is how long C's job has been running when its worker is
	// killed: mid-job.
	probeCRunBeforeKill = 3 * time.Second

	// probeDJobDuration is condition D's workload: long enough that the victim's
	// attempts are certainly RUNNING at the kill, and shorter than the 10 s renew
	// interval, so none of them is ever renewed. probeDJobs are submitted at once,
	// and the kill comes probeDRunBeforeKill after the first is held, by which time
	// every one claimed has started. D keeps the recorded runs' timeout_seconds 30,
	// the lease's length, unless --timeout-seconds says otherwise.
	probeDJobDuration   = 5 * time.Second
	probeDJobs          = 8
	probeDRunBeforeKill = time.Second

	// probeEJobDuration is condition E's workload: longer than one renew interval,
	// so the worker renews the lease before the kill, and shorter than
	// probeEJobTimeout, so a worker left alive would succeed. The timeout is chosen
	// so the persisted deadline (start + 25 s) falls before the renewed lease's
	// expiry (renewal at about start + 10 s, plus the 30 s lease): the timeout sweep
	// reaches the attempt while its lease is still live.
	probeEJobDuration = 20 * time.Second
	probeEJobTimeout  = 25 // seconds
	probeEJobs        = 8
	// probeERenewalWait bounds how long E waits for a renewed attempt to kill, and
	// probeEDeadlineMargin is how far its deadline must still be at the kill.
	probeERenewalWait    = 25 * time.Second
	probeEDeadlineMargin = 5 * time.Second

	// probeOverThresholdSlack is what "over threshold" means: a recovery longer
	// than the lease plus this.
	probeOverThresholdSlack = 5 * time.Second
	// probeReplacementWait bounds how long after kill + lease the probe waits for
	// every replacement: longer than a 30 s visibility timeout plus the 60 s
	// re-notification, so a recovery held by either is still observed.
	probeReplacementWait = 120 * time.Second
	// probeDrainWait bounds how long a trial waits for its jobs to finish before
	// it stops, so that it leaves nothing for the next trial's services.
	probeDrainWait = 120 * time.Second
	// probePollEvery is how often the probe reads the timeline once a lease is
	// about to expire. It bounds how late the broker is read after a publish.
	probePollEvery = 20 * time.Millisecond
	// probeClaimLogSlack is how far past the replacement's claim the S5 claim count
	// reaches, because the api logs a request when it ends.
	probeClaimLogSlack = 500 * time.Millisecond
	// probeExtraTrials is how many trials beyond N a condition may run to replace
	// trials whose kill hit nothing.
	probeExtraTrials = 5

	// maxTimeoutSecs is the largest timeout_seconds the API accepts.
	maxTimeoutSecs = jobs.MaxTimeoutSeconds
)

// conditionJobDuration is how long each of a condition's jobs sleeps.
func conditionJobDuration(cond string, bJob time.Duration) time.Duration {
	switch cond {
	case condTail:
		return bJob
	case condIdle:
		return probeCJobDuration
	case condRunning:
		return probeDJobDuration
	case condRenewed:
		return probeEJobDuration
	default:
		return probeAJobDuration
	}
}

// conditionTimeout is the timeout_seconds a condition's jobs are submitted with:
// --timeout-seconds when it is given, and otherwise the condition's own. A, B and D
// keep the recorded runs' 30 s, which equals the shipped lease.
func conditionTimeout(cond string, override int) int {
	if override > 0 {
		return override
	}
	switch cond {
	case condIdle:
		return probeCJobTimeout
	case condRenewed:
		return probeEJobTimeout
	default:
		return jobTimeoutSecs
	}
}

// probeOptions is what a probe invocation chooses.
type probeOptions struct {
	conditions []string
	trials     map[string]int
	bJob       time.Duration
	// timeoutSeconds replaces every condition's timeout_seconds when positive; 0
	// keeps each condition's own (conditionTimeout).
	timeoutSeconds int
	// visibilityTimeout is the run queue's VisibilityTimeout attribute; 0 leaves it
	// to the broker. pollWait replaces the profile's TASKFORGE_WORKER_POLL_WAIT; 0
	// keeps it. They exist for the controlled variations.
	visibilityTimeout time.Duration
	pollWait          time.Duration
	// noRestart leaves the killed worker dead instead of restarting it under the
	// same name, so no new boot of that logical worker exists while the dead
	// boot's leases still count against its capacity.
	noRestart bool
	outDir    string
}

// parseProbeArgs reads the probe's flags. It takes only its own: a flag of the
// other modes is an error here, so nothing is silently ignored.
func parseProbeArgs(o options, args []string) (options, error) {
	if len(o.modes) != 1 {
		return options{}, errors.New("recovery-probe runs by itself")
	}
	po := &probeOptions{
		trials: map[string]int{condLoaded: 10, condTail: 20, condIdle: 10, condRunning: 10, condRenewed: 10},
		bJob:   probeBDefaultJobDuration,
	}
	var conditions string
	var trialsA, trialsB, trialsC, trialsD, trialsE int
	fs := flag.NewFlagSet("bench recovery-probe", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.BoolVar(&o.record, "record", false, "")
	fs.StringVar(&o.profile, "profile", o.profile, "")
	fs.Int64Var(&o.seed, "seed", o.seed, "")
	fs.StringVar(&conditions, "conditions", "A,B,C", "")
	fs.IntVar(&trialsA, "trials-a", po.trials[condLoaded], "")
	fs.IntVar(&trialsB, "trials-b", po.trials[condTail], "")
	fs.IntVar(&trialsC, "trials-c", po.trials[condIdle], "")
	fs.IntVar(&trialsD, "trials-d", po.trials[condRunning], "")
	fs.IntVar(&trialsE, "trials-e", po.trials[condRenewed], "")
	fs.IntVar(&po.timeoutSeconds, "timeout-seconds", 0, "")
	fs.DurationVar(&po.bJob, "b-job-duration", po.bJob, "")
	fs.DurationVar(&po.visibilityTimeout, "visibility-timeout", 0, "")
	fs.DurationVar(&po.pollWait, "poll-wait", 0, "")
	fs.BoolVar(&po.noRestart, "no-restart", false, "")
	fs.StringVar(&po.outDir, "out", "", "")
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if fs.NArg() > 0 {
		return options{}, fmt.Errorf("%q is not a flag", fs.Arg(0))
	}
	if _, err := o.timings(); err != nil {
		return options{}, err
	}
	timeoutSet := false
	fs.Visit(func(f *flag.Flag) { timeoutSet = timeoutSet || f.Name == "timeout-seconds" })
	po.trials[condLoaded], po.trials[condTail], po.trials[condIdle] = trialsA, trialsB, trialsC
	po.trials[condRunning], po.trials[condRenewed] = trialsD, trialsE
	for _, c := range strings.Split(conditions, ",") {
		c = strings.TrimSpace(strings.ToUpper(c))
		if _, ok := conditionNames[c]; !ok {
			return options{}, fmt.Errorf("unknown condition %q: use A, B, C, D or E", c)
		}
		if slices.Contains(po.conditions, c) {
			return options{}, fmt.Errorf("condition %q is named twice", c)
		}
		po.conditions = append(po.conditions, c)
	}
	for _, c := range po.conditions {
		if po.trials[c] < 1 {
			return options{}, fmt.Errorf("--trials-%s must be at least 1", strings.ToLower(c))
		}
	}
	switch {
	case po.bJob <= 0:
		return options{}, errors.New("--b-job-duration must be positive")
	case po.visibilityTimeout < 0 || po.visibilityTimeout%time.Second != 0:
		return options{}, errors.New("--visibility-timeout must be a whole number of seconds")
	case po.pollWait < 0 || po.pollWait%time.Second != 0:
		return options{}, errors.New("--poll-wait must be a whole number of seconds (the broker's wait is in seconds)")
	case timeoutSet && (po.timeoutSeconds < 1 || po.timeoutSeconds > maxTimeoutSecs):
		return options{}, fmt.Errorf("--timeout-seconds must be a whole number from 1 to %d (the API's own bounds)", maxTimeoutSecs)
	}
	o.workers, o.concurrency, o.rate = fixedWorkers, fixedConcurrency, fixedRatePerMin
	o.probe = po
	return o, nil
}

// probeTimings is the profile's timings with the probe's poll-wait variation.
func probeTimings(o options) (stack.Timings, error) {
	t, err := o.timings()
	if err != nil {
		return stack.Timings{}, err
	}
	if o.probe.pollWait > 0 {
		t.WorkerPollWait = o.probe.pollWait
	}
	return t, nil
}

// runProbe runs every trial of every condition asked for and writes everything it
// prints to <out>/recovery-probe.txt as well.
func runProbe(ctx context.Context, o options, stdout io.Writer) error {
	po := o.probe
	if po.outDir == "" {
		po.outDir = filepath.Join(os.TempDir(), "taskforge-recovery-probe-"+time.Now().UTC().Format("20060102T150405Z"))
	}
	if err := os.MkdirAll(po.outDir, 0o755); err != nil {
		return fmt.Errorf("create the output directory: %w", err)
	}
	outPath := filepath.Join(po.outDir, "recovery-probe.txt")
	file, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create the output file: %w", err)
	}
	out := io.MultiWriter(stdout, file)
	timings, err := probeTimings(o)
	if err != nil {
		file.Close()
		return err
	}

	fmt.Fprintf(out, "=== recovery-probe (M8D3, M8D4): an investigation, never recorded ===\n")
	fmt.Fprintf(out, "profile %q", o.profile)
	if o.profile != profileShipped {
		fmt.Fprintf(out, "  ** NOT the shipped defaults **")
	}
	fmt.Fprintln(out)
	fmt.Fprintf(out, "timings: lease %s, reconciler poll %s, outbox poll %s, worker poll wait %s, renotify after %s\n",
		timings.Lease, timings.PollInterval, timings.OutboxPoll, timings.WorkerPollWait, timings.RenotifyAfter)
	if po.pollWait > 0 {
		fmt.Fprintf(out, "VARIATION: the workers' poll wait is %s (the profile's is replaced)\n", po.pollWait)
	}
	if po.visibilityTimeout > 0 {
		fmt.Fprintf(out, "VARIATION: each trial's queue is created with VisibilityTimeout %s\n", po.visibilityTimeout)
	} else {
		fmt.Fprintf(out, "queue: created with the broker's default attributes, as every other mode creates it\n")
	}
	if po.noRestart {
		fmt.Fprintf(out, "VARIATION: the killed worker is NOT restarted\n")
	}
	if po.timeoutSeconds > 0 {
		fmt.Fprintf(out, "VARIATION: every condition's jobs are submitted with timeout_seconds %d\n", po.timeoutSeconds)
	}
	fmt.Fprintf(out, "fleet: %d workers x %d slots; offered rate %.0f jobs/minute (A and B)\n", o.workers, o.concurrency, o.rate)
	fmt.Fprintf(out, "workloads: A demo.sleep %s (timeout %ds); B demo.sleep %s (timeout %ds); C one demo.sleep %s (timeout %ds)\n",
		probeAJobDuration, conditionTimeout(condLoaded, po.timeoutSeconds), po.bJob, conditionTimeout(condTail, po.timeoutSeconds),
		probeCJobDuration, conditionTimeout(condIdle, po.timeoutSeconds))
	fmt.Fprintf(out, "workloads: D %d x demo.sleep %s (timeout %ds), kill %s after the first is held; E %d x demo.sleep %s (timeout %ds), kill after a renewal\n",
		probeDJobs, probeDJobDuration, conditionTimeout(condRunning, po.timeoutSeconds), probeDRunBeforeKill,
		probeEJobs, probeEJobDuration, conditionTimeout(condRenewed, po.timeoutSeconds))
	fmt.Fprintf(out, "over threshold: recovery > lease + %s = %s\n", probeOverThresholdSlack, timings.Lease+probeOverThresholdSlack)
	fmt.Fprintf(out, "seed %d; output and logs under %s\n\n", o.seed, po.outDir)

	// A read-only pool of the probe's own, for what it reads after a trial's stack
	// has stopped and closed the stack's pool.
	infra, err := stack.LoadInfra()
	if err != nil {
		file.Close()
		return err
	}
	db, err := readdb.Open(ctx, infra.DatabaseURL)
	if err != nil {
		file.Close()
		return err
	}
	defer db.Close()

	rng := rand.New(rand.NewSource(o.seed))
	results := map[string][]trialResult{}
	// ran is every trial of each condition, counted or missed: the denominators of
	// the outcome summary.
	ran := map[string][]trialResult{}
	var runErr error
	for _, c := range po.conditions {
		misses := 0
		for len(results[c]) < po.trials[c] {
			if ctx.Err() != nil {
				runErr = ctx.Err()
				break
			}
			n := len(results[c]) + misses + 1
			tr, err := runProbeTrial(ctx, o, timings, c, n, rng.Uint64(), db, out)
			if err != nil {
				runErr = fmt.Errorf("condition %s trial %d: %w", c, n, err)
				break
			}
			printTrial(out, tr, timings.Lease)
			ran[c] = append(ran[c], tr)
			if tr.missed {
				misses++
				if misses > probeExtraTrials {
					runErr = fmt.Errorf("condition %s: %d kills hit nothing; giving up", c, misses)
					break
				}
				continue
			}
			results[c] = append(results[c], tr)
		}
		if runErr != nil {
			break
		}
		if misses > 0 {
			fmt.Fprintf(out, "condition %s: %d trial(s) whose kill hit nothing were run again and are not counted\n\n", c, misses)
		}
	}
	for _, c := range po.conditions {
		if len(results[c]) > 0 {
			printSummary(out, c, results[c], timings)
		}
		if len(ran[c]) > 0 {
			printOutcomeSummary(out, c, ran[c])
		}
	}
	if err := file.Close(); err != nil && runErr == nil {
		runErr = err
	}
	if sum, err := fileSHA256(outPath); err == nil {
		fmt.Fprintf(stdout, "\nwrote %s\nsha256 %s\n", outPath, sum)
	}
	return runErr
}

func fileSHA256(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// brokerSnapshot is the run queue's two message counts at one instant.
type brokerSnapshot struct {
	at         time.Time // PostgreSQL's clock, read just before GetQueueAttributes
	visible    string
	notVisible string
	err        error
}

func (b brokerSnapshot) String() string {
	if b.err != nil {
		return "unavailable: " + b.err.Error()
	}
	if b.at.IsZero() {
		return "not taken"
	}
	return fmt.Sprintf("visible %s, in flight %s", b.visible, b.notVisible)
}

func snapshotBroker(ctx context.Context, st *stack.Stack) brokerSnapshot {
	var b brokerSnapshot
	b.at, b.err = readdb.ClockNow(ctx, st.DB)
	if b.err != nil {
		return b
	}
	attributes, err := st.QueueAttributes(ctx)
	if err != nil {
		b.err = err
		return b
	}
	b.visible = attributes["ApproximateNumberOfMessages"]
	b.notVisible = attributes["ApproximateNumberOfMessagesNotVisible"]
	return b
}

// hopResult is one abandoned attempt of a trial, measured.
type hopResult struct {
	hop       readdb.RecoveryHop
	segments  readdb.Segments
	complete  bool
	recovery  time.Duration // Recoveries' figure: the replacement's claim minus the kill
	identity  bool          // segments.Sum() == recovery
	published brokerSnapshot
	claims    claimWindow
}

// trialResult is one trial.
type trialResult struct {
	cond             string
	n                int
	missed           bool
	missReason       string
	killedAt         time.Time // PostgreSQL's clock
	localKill        time.Time // this machine's clock, read right after killedAt
	worker           string
	session          uuid.UUID
	restartedSession uuid.UUID // the victim's new boot
	held             int
	atKill           brokerSnapshot
	visibility       string
	submissionEndPG  time.Time // zero for C
	hops             []hopResult
	logDir           string
	notes            []string
	jobDuration      time.Duration
	empty            []emptyClaim // claims that took nothing, kill to last replacement
	allJobsTerminal  bool
	replacementsSeen bool

	// M8D4. chosenAt is PostgreSQL's clock read just before the query that found
	// the victim; outcomes is every attempt of the killed session not finished at
	// the kill (readdb.KillOutcomes); finishedBeforeKill is the session's attempts
	// that finished between the choice and the kill (readdb.FinishedBetween).
	chosenAt           time.Time
	timeoutSeconds     int
	outcomes           []readdb.KillOutcome
	finishedBeforeKill []readdb.Finished
	outcomeErr         error
}

// runProbeTrial runs one trial of one condition on a stack of its own.
func runProbeTrial(parent context.Context, o options, timings stack.Timings, cond string, n int, pick uint64,
	db readdb.Querier, out io.Writer) (tr trialResult, err error) {
	tr = trialResult{cond: cond, n: n}
	trialOptions := o
	trialOptions.faultJobDuration = conditionJobDuration(cond, o.probe.bJob)
	trialOptions.targetableKills = true
	tr.jobDuration = trialOptions.faultJobDuration
	tr.timeoutSeconds = conditionTimeout(cond, o.probe.timeoutSeconds)

	fmt.Fprintf(out, "--- %s trial %d: starting a fresh stack ---\n", cond, n)
	fl, err := startFleetWith(parent, trialOptions, out, func(so *stack.Options) {
		so.Prefix = "probe" + strings.ToLower(cond)
		so.Timing = timings
		if o.probe.visibilityTimeout > 0 {
			so.QueueAttributes = map[string]string{
				"VisibilityTimeout": fmt.Sprint(int(o.probe.visibilityTimeout / time.Second)),
			}
		}
	})
	if err != nil {
		return tr, err
	}
	closed := false
	closeFleet := func() {
		if !closed {
			closed = true
			fl.close()
		}
	}
	defer closeFleet()
	// After the fleet stops (deferred calls run last-in first-out, so this runs
	// after closeFleet), the trial's logs are complete; keep a copy with the output.
	defer func() {
		if tr.logDir == "" {
			return
		}
		dst := filepath.Join(o.probe.outDir, fmt.Sprintf("%s-trial-%02d-logs", cond, n))
		closeFleet()
		if copyErr := copyLogs(tr.logDir, dst); copyErr == nil {
			tr.logDir = dst
		}
	}()
	ctx, stop := fl.watch(parent)
	defer stop(nil)
	defer func() {
		if cause := failureCause(ctx); cause != nil {
			err = cause
		}
	}()
	st := fl.st
	tr.logDir = st.LogDir

	foreign, err := readdb.ForeignActivity(ctx, st.DB, st.Scope)
	if err != nil {
		return tr, err
	}
	if !foreign.None() {
		return tr, fmt.Errorf("other scopes hold %d non-terminal jobs and %d unpublished events; this trial's reconciler, "+
			"outbox and scheduler would act on them and put messages in its queue that it did not cause. Let them finish, "+
			"or point the probe at a database without them", foreign.NonTerminalJobs, foreign.PendingEvents)
	}
	if attributes, err := st.QueueAttributes(ctx); err == nil {
		tr.visibility = attributes["VisibilityTimeout"]
	} else {
		tr.visibility = "unavailable: " + err.Error()
	}

	// Load, and the kill's place in it.
	submit := newSubmitterFor(st.APIURL, st.APIKey, st.RunID, trialOptions.faultJobDuration, tr.timeoutSeconds)
	var loadDone chan struct{}
	var killAt time.Time
	start := time.Now()
	switch cond {
	case condLoaded:
		span := probeAKillAfter + timings.Lease + probeALoadAfterRecovery
		loadDone = probeLoad(ctx, submit, o.rate, span, start)
		killAt = start.Add(probeAKillAfter)
		st.Say("Submitting %.0f/minute for %s; the kill is at %s, so recovery falls while load continues.",
			o.rate, span, probeAKillAfter)
	case condTail:
		killAt = start.Add(bKillAt(timings.Lease))
		loadDone = probeLoad(ctx, submit, o.rate, probeBSpan, start)
		st.Say("Submitting %.0f/minute for %s; the kill is at %s, %s before the last submission.",
			o.rate, probeBSpan, bKillAt(timings.Lease), probeBSpan-bKillAt(timings.Lease))
	case condIdle:
		if err := submit.submit(ctx, 0); err != nil {
			return tr, fmt.Errorf("submit the idle condition's job: %w", err)
		}
		running, err := waitHeld(ctx, st.DB, st.Scope, 60*time.Second)
		if err != nil {
			return tr, err
		}
		killAt = running.Add(probeCRunBeforeKill)
		st.Say("One %s job, held; the kill is %s after that.", probeCJobDuration, probeCRunBeforeKill)
	case condRunning, condRenewed:
		count := probeDJobs
		if cond == condRenewed {
			count = probeEJobs
		}
		for i := 0; i < count; i++ {
			if err := submit.submit(ctx, i); err != nil {
				return tr, fmt.Errorf("submit condition %s's job %d: %w", cond, i, err)
			}
		}
		held, err := waitHeld(ctx, st.DB, st.Scope, 60*time.Second)
		if err != nil {
			return tr, err
		}
		if cond == condRunning {
			killAt = held.Add(probeDRunBeforeKill)
			st.Say("%d %s jobs (timeout %ds), held; the kill is %s after the first was.", count, probeDJobDuration, tr.timeoutSeconds, probeDRunBeforeKill)
		} else {
			// E's kill waits for a renewal instead (probeKill's selection), so it is
			// due as soon as something is held.
			killAt = held
			st.Say("%d %s jobs (timeout %ds), held; the kill waits for a renewed attempt.", count, probeEJobDuration, tr.timeoutSeconds)
		}
	}

	// The last submission's instant on PostgreSQL's clock, stamped as soon as the
	// load returns, so B can show that the recovery fell after it.
	var loadEnded chan time.Time
	if loadDone != nil {
		loadEnded = make(chan time.Time, 1)
		go func() {
			<-loadDone
			at, _ := readdb.ClockNow(ctx, st.DB)
			loadEnded <- at
		}()
	}

	if err := (realClock{}).SleepUntil(ctx, killAt); err != nil {
		return tr, err
	}
	selection := fl.targetableSelection()
	if cond == condRenewed {
		selection = fl.renewedSelection()
	}
	if err := fl.probeKill(ctx, pick, !o.probe.noRestart, selection, &tr); err != nil {
		return tr, err
	}
	if tr.missed {
		st.Say("Kill missed: %s. This trial is not counted.", tr.missReason)
		// What the kill did catch, if it chose a victim at all, still counts in the
		// outcome summary's denominators. A miss holds nothing when dead, so there is
		// nothing to wait for.
		if tr.worker != "" {
			tr.readOutcomes(ctx, st.DB, st.Scope)
		}
		if loadEnded != nil {
			<-loadEnded
		}
		return tr, nil
	}

	// Watch the recovery: the broker right after each recovery event is published,
	// and every replacement.
	// Polling starts a second before the earliest of the dead session's leases
	// expires. That is up to a lease after the kill, and less for a lease acquired
	// before it (C's is acquired about 3 s before its kill and never renewed) or
	// renewed before it (E's).
	from := tr.killedAt.Add(timings.Lease)
	expiries, err := readdb.ActiveLeaseExpiries(ctx, st.DB, st.Scope, tr.session)
	if err != nil {
		return tr, err
	}
	for _, e := range expiries {
		if e.Before(from) {
			from = e
		}
	}
	// A deadline that falls before every lease expiry (E's) is reconciled first.
	early, err := readdb.KillOutcomes(ctx, st.DB, st.Scope, tr.session, tr.killedAt)
	if err != nil {
		return tr, err
	}
	for _, k := range early {
		if k.TimeoutAt != nil && k.TimeoutAt.Before(from) {
			from = *k.TimeoutAt
		}
	}
	deadline := tr.killedAt.Add(timings.Lease + probeReplacementWait)
	hops, snapshots, err := watchRecovery(ctx, st, tr.session, tr.killedAt, from.Add(-time.Second), deadline, tr.held)
	if err != nil {
		return tr, err
	}
	tr.replacementsSeen = allReplaced(hops, tr.held)

	if loadEnded != nil {
		tr.submissionEndPG = <-loadEnded
	}
	endedBy, err := waitTerminal(ctx, realClock{}, st.DB, st.Scope, time.Now().Add(probeDrainWait))
	if err != nil {
		return tr, err
	}
	tr.allJobsTerminal = endedBy == "all jobs terminal"
	if !tr.allJobsTerminal {
		tr.notes = append(tr.notes, fmt.Sprintf("not every job was terminal %s after the load; the next trial's foreign-work check will say so", probeDrainWait))
	}

	// Final reads, after everything has settled.
	if hops, err = readdb.RecoveryTimeline(ctx, st.DB, st.Scope, tr.session); err != nil {
		return tr, err
	}
	recoveries, err := readdb.Recoveries(ctx, st.DB, st.Scope, tr.session)
	if err != nil {
		return tr, err
	}
	tr.readOutcomes(ctx, st.DB, st.Scope)
	figure := map[uuid.UUID]time.Duration{}
	for _, r := range recoveries {
		if d, ok := r.After(tr.killedAt); ok {
			figure[r.JobID] = d
		}
	}

	// The watchdog first, so that stopping the workers is not read as a crash.
	stop(nil)
	closeFleet()
	apiLog, logErr := os.ReadFile(filepath.Join(st.LogDir, "api.log"))
	if logErr != nil {
		tr.notes = append(tr.notes, "could not read the api log: "+logErr.Error())
	}
	claims := parseClaims(apiLog)
	offset := tr.localKill.Sub(tr.killedAt)
	// Every claim that took nothing between the kill and the last replacement,
	// with the logical workers at their limit at its midpoint. The database is
	// still reachable through the probe's own pool after the stack has stopped.
	end := tr.killedAt
	for _, h := range hops {
		if h.ReplacementCreatedAt != nil && h.ReplacementCreatedAt.After(end) {
			end = *h.ReplacementCreatedAt
		}
	}
	for _, c := range claims {
		ended := c.at.Add(-offset)
		began := ended.Add(-c.took)
		if c.jobID != "" || ended.Before(tr.killedAt) || began.After(end) {
			continue
		}
		capacity, capErr := readdb.WorkersAtCapacityDuring(parent, db, st.Scope, began, ended)
		tr.empty = append(tr.empty, emptyClaim{at: ended, capacity: describeCapacity(capacity, capErr)})
	}
	for _, h := range hops {
		r := hopResult{hop: h}
		r.segments, r.complete = h.Segments(tr.killedAt)
		if d, ok := figure[h.JobID]; ok {
			r.recovery = d
			r.identity = r.complete && r.segments.Sum() == d
		}
		if h.EventPublishedAt != nil {
			r.published = snapshots[h.EventPublishedAt.UnixNano()]
			if h.ReplacementCreatedAt != nil {
				// The api logs a request when it ends, after the claim's created_at,
				// so the window runs probeClaimLogSlack past the replacement's claim.
				r.claims = claimsBetween(claims, h.EventPublishedAt.Add(offset),
					h.ReplacementCreatedAt.Add(offset+probeClaimLogSlack), h.JobID)
			}
		}
		tr.hops = append(tr.hops, r)
	}
	return tr, nil
}

// bKillAt is B's kill offset into its submission: probeBKillBeforeEnd before the
// end for the shipped lease, and never so early that the lease would expire before
// the last submission (a shorter lease moves the kill later).
func bKillAt(lease time.Duration) time.Duration {
	before := min(probeBKillBeforeEnd, lease-time.Second)
	return probeBSpan - before
}

// probeLoad submits at rate for span from start, and closes the returned channel
// when every submission has returned.
func probeLoad(ctx context.Context, submit *submitter, rate float64, span time.Duration, start time.Time) chan struct{} {
	done := make(chan struct{})
	count := int(span.Minutes() * rate)
	go func() {
		defer close(done)
		pacer, err := NewPacer(start, rate)
		if err != nil {
			return
		}
		var inflight sync.WaitGroup
		slots := make(chan struct{}, maxInFlight)
		pacer.Run(ctx, realClock{}, count, func(i int) {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			inflight.Add(1)
			go func() {
				defer inflight.Done()
				defer func() { <-slots }()
				_ = submit.submit(ctx, i)
			}()
		})
		inflight.Wait()
	}()
	return done
}

// waitHeld waits until some worker holds an attempt of the scope, and returns
// PostgreSQL's clock at that moment.
func waitHeld(ctx context.Context, q readdb.Querier, scope string, limit time.Duration) (time.Time, error) {
	deadline := time.Now().Add(limit)
	for {
		held, err := readdb.Occupancy(ctx, q, scope)
		if err != nil {
			return time.Time{}, err
		}
		if len(held) > 0 {
			return time.Now(), nil
		}
		if time.Now().After(deadline) {
			return time.Time{}, fmt.Errorf("no worker held the job within %s", limit)
		}
		if err := (realClock{}).SleepUntil(ctx, time.Now().Add(probePollEvery)); err != nil {
			return time.Time{}, err
		}
	}
}

// probeKill aims a kill at a worker holding an attempt with time left, SIGKILLs it,
// reads the broker, and restarts it under the same name, as the fault run does.
// A kill that finds no target, or whose victim turns out to hold nothing once it
// is dead, is a miss.
func (f *fleet) probeKill(ctx context.Context, pick uint64, restart bool, sel victimSelection, tr *trialResult) error {
	st := f.st
	byName := map[string]*workerHandle{}
	var live []string
	for _, w := range f.workers {
		if w.current().Running() {
			byName[w.name] = w
			live = append(live, w.name)
		}
	}
	held, candidates, chosenAt := f.waitForVictim(ctx, live, sel)
	if len(candidates) == 0 {
		tr.missed, tr.missReason = true, fmt.Sprintf("no worker held %s within %s", sel.what, sel.wait)
		return nil
	}
	tr.chosenAt = chosenAt
	name, err := ChooseVictim(pick, candidates)
	if err != nil {
		return err
	}
	victim := byName[name]
	tr.worker = victim.label
	if tr.session, err = readdb.CurrentSession(ctx, st.DB, st.Scope, name); err != nil {
		return err
	}
	if tr.killedAt, err = readdb.ClockNow(ctx, st.DB); err != nil {
		return err
	}
	tr.localKill = time.Now()
	proc := victim.current()
	victim.setDown(true)
	if err := proc.Signal(syscall.SIGKILL); err != nil {
		return fmt.Errorf("SIGKILL: %w", err)
	}
	tr.atKill = snapshotBroker(ctx, st)
	if !proc.WaitExit(10 * time.Second) {
		return errors.New("the killed process did not exit within 10s")
	}
	if tr.held, err = readdb.HeldBySession(ctx, st.DB, st.Scope, tr.session); err != nil {
		return err
	}
	st.Say("Kill: SIGKILL %s (targetable attempts when chosen: %d; held when dead: %d) at PostgreSQL time %s.",
		victim.label, held[name], tr.held, tr.killedAt.UTC().Format("15:04:05.000"))
	if restart {
		fresh, err := st.StartWorkerNamed(ctx, victim.label, name, f.concurrency)
		if err != nil {
			return fmt.Errorf("restart: %w", err)
		}
		victim.replace(fresh)
		victim.setDown(false)
		if tr.restartedSession, err = readdb.CurrentSession(ctx, st.DB, st.Scope, name); err != nil {
			return err
		}
	}
	// Without a restart the victim stays marked down, so the watchdog does not read
	// the absence as a crash.
	if tr.held == 0 {
		tr.missed, tr.missReason = true, "the victim held no attempt by the time it was dead"
	}
	return nil
}

// watchRecovery polls the killed session's timeline from `from` until every one of
// the held attempts has a replacement or deadline passes. It reads the broker
// once just after each new publish instant it sees, keyed by that instant.
func watchRecovery(ctx context.Context, st *stack.Stack, session uuid.UUID, kill, from, deadline time.Time, held int) ([]readdb.RecoveryHop, map[int64]brokerSnapshot, error) {
	snapshots := map[int64]brokerSnapshot{}
	pgNow, err := readdb.ClockNow(ctx, st.DB)
	if err != nil {
		return nil, nil, err
	}
	// The two clocks agree to within the watchdog's tolerance; the wait is only
	// to avoid polling for the 30 s nothing can happen in.
	if wait := from.Sub(pgNow); wait > 0 {
		if err := (realClock{}).SleepUntil(ctx, time.Now().Add(wait)); err != nil {
			return nil, nil, err
		}
	}
	for {
		hops, err := readdb.RecoveryTimeline(ctx, st.DB, st.Scope, session)
		if err != nil {
			return nil, nil, err
		}
		for _, h := range hops {
			if h.EventPublishedAt == nil {
				continue
			}
			key := h.EventPublishedAt.UnixNano()
			if _, seen := snapshots[key]; !seen {
				snapshots[key] = snapshotBroker(ctx, st)
			}
		}
		if allReplaced(hops, held) {
			return hops, snapshots, nil
		}
		// An attempt that ended TIMED_OUT is no hop, so allReplaced would wait for
		// it until the deadline. Every held attempt resolved, and replaced or its
		// job finished, ends the watch as well.
		outcomes, err := readdb.KillOutcomes(ctx, st.DB, st.Scope, session, kill)
		if err != nil {
			return nil, nil, err
		}
		if outcomesSettled(outcomes, held) {
			return hops, snapshots, nil
		}
		now, err := readdb.ClockNow(ctx, st.DB)
		if err != nil {
			return nil, nil, err
		}
		if now.After(deadline) {
			return hops, snapshots, nil
		}
		if err := (realClock{}).SleepUntil(ctx, time.Now().Add(probePollEvery)); err != nil {
			return nil, nil, err
		}
	}
}

// victimSelection is how a probe kill finds its victim: count gives, for each
// worker, how many attempts it holds that the kill is aimed at, and wait bounds how
// long to wait for one. what names them for a miss.
type victimSelection struct {
	count func(ctx context.Context) (map[string]int, error)
	wait  time.Duration
	what  string
}

// targetableSelection is the smoke's aim (A to D): attempts of a worker's current
// session that are LEASED, or RUNNING with at least half their duration left.
func (f *fleet) targetableSelection() victimSelection {
	return victimSelection{
		count: func(ctx context.Context) (map[string]int, error) {
			return readdb.TargetableAttempts(ctx, f.st.DB, f.st.Scope, f.target.duration, f.target.margin)
		},
		wait: occupancyWait,
		what: fmt.Sprintf("an attempt with at least %s of its %s left", f.target.margin, f.target.duration),
	}
}

// renewedSelection is E's aim: a worker every one of whose current-session
// attempts is RUNNING, renewed, and with its deadline still probeEDeadlineMargin
// away.
func (f *fleet) renewedSelection() victimSelection {
	return victimSelection{
		count: func(ctx context.Context) (map[string]int, error) {
			return readdb.RenewedAttempts(ctx, f.st.DB, f.st.Scope, probeEDeadlineMargin)
		},
		wait: probeERenewalWait,
		what: fmt.Sprintf("only renewed RUNNING attempts with deadlines at least %s away", probeEDeadlineMargin),
	}
}

// waitForVictim is waitForTargetable with any selection, and it also returns
// chosenAt: PostgreSQL's clock read just before the query that found the
// candidates. An attempt that finishes after chosenAt was still unfinished when
// the victim was chosen. It is the probe's own so the smoke's loop is untouched.
func (f *fleet) waitForVictim(ctx context.Context, live []string, sel victimSelection) (held map[string]int, candidates []string, chosenAt time.Time) {
	deadline := time.Now().Add(sel.wait)
	for {
		at, err := readdb.ClockNow(ctx, f.st.DB)
		if err == nil {
			if held, err = sel.count(ctx); err == nil {
				candidates = targetableCandidates(held, live)
				if len(candidates) > 0 {
					return held, candidates, at
				}
			}
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			return held, nil, time.Time{}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// readOutcomes reads what became of every attempt the kill caught, and the
// session's attempts that finished between the choice and the kill. A failure is
// kept on the trial and printed, not fatal: the segment measurements stand alone.
func (tr *trialResult) readOutcomes(ctx context.Context, q readdb.Querier, scope string) {
	tr.outcomes, tr.outcomeErr = readdb.KillOutcomes(ctx, q, scope, tr.session, tr.killedAt)
	if tr.outcomeErr != nil || tr.chosenAt.IsZero() {
		return
	}
	tr.finishedBeforeKill, tr.outcomeErr = readdb.FinishedBetween(ctx, q, scope, tr.session, tr.chosenAt, tr.killedAt)
}

// outcomesSettled reports whether every attempt a kill caught is resolved and
// either replaced or its job finished, so nothing more will happen to it. It
// needs at least as many outcomes as attempts were held when the victim was dead.
func outcomesSettled(outcomes []readdb.KillOutcome, held int) bool {
	if len(outcomes) < held || len(outcomes) == 0 {
		return false
	}
	for _, o := range outcomes {
		if o.Outcome() == readdb.OutcomeUnresolved {
			return false
		}
		if o.ReplacementClaimedAt == nil && !terminalJobStatus(o.JobStatus) {
			return false
		}
	}
	return true
}

func terminalJobStatus(status string) bool {
	return status == "SUCCEEDED" || status == "DEAD_LETTERED" || status == "CANCELED"
}

// allReplaced reports whether all held attempts are abandoned and replaced.
func allReplaced(hops []readdb.RecoveryHop, held int) bool {
	if len(hops) < held {
		return false
	}
	for _, h := range hops {
		if h.ReplacementCreatedAt == nil {
			return false
		}
	}
	return true
}
