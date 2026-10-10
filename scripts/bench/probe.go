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

	"github.com/co-rtex/TaskForge/scripts/internal/stack"
	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// The recovery probe (M8D3). The headline benchmark recorded one worker-failure
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

// The conditions.
const (
	condLoaded = "A"
	condTail   = "B"
	condIdle   = "C"
)

var conditionNames = map[string]string{
	condLoaded: "loaded: kill under steady load, recovery while load continues",
	condTail:   "tail: kill so that lease expiry and recovery fall after submission has ended",
	condIdle:   "idle: one long job, nothing else submitted, its worker killed mid-job",
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
)

// probeOptions is what a probe invocation chooses.
type probeOptions struct {
	conditions []string
	trials     map[string]int
	bJob       time.Duration
	// visibilityTimeout is the run queue's VisibilityTimeout attribute; 0 leaves it
	// to the broker. pollWait replaces the profile's TASKFORGE_WORKER_POLL_WAIT; 0
	// keeps it. They exist for the controlled variations.
	visibilityTimeout time.Duration
	pollWait          time.Duration
	outDir            string
}

// parseProbeArgs reads the probe's flags. It takes only its own: a flag of the
// other modes is an error here, so nothing is silently ignored.
func parseProbeArgs(o options, args []string) (options, error) {
	if len(o.modes) != 1 {
		return options{}, errors.New("recovery-probe runs by itself")
	}
	po := &probeOptions{
		trials: map[string]int{condLoaded: 10, condTail: 20, condIdle: 10},
		bJob:   probeBDefaultJobDuration,
	}
	var conditions string
	var trialsA, trialsB, trialsC int
	fs := flag.NewFlagSet("bench recovery-probe", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.BoolVar(&o.record, "record", false, "")
	fs.StringVar(&o.profile, "profile", o.profile, "")
	fs.Int64Var(&o.seed, "seed", o.seed, "")
	fs.StringVar(&conditions, "conditions", "A,B,C", "")
	fs.IntVar(&trialsA, "trials-a", po.trials[condLoaded], "")
	fs.IntVar(&trialsB, "trials-b", po.trials[condTail], "")
	fs.IntVar(&trialsC, "trials-c", po.trials[condIdle], "")
	fs.DurationVar(&po.bJob, "b-job-duration", po.bJob, "")
	fs.DurationVar(&po.visibilityTimeout, "visibility-timeout", 0, "")
	fs.DurationVar(&po.pollWait, "poll-wait", 0, "")
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
	po.trials[condLoaded], po.trials[condTail], po.trials[condIdle] = trialsA, trialsB, trialsC
	for _, c := range strings.Split(conditions, ",") {
		c = strings.TrimSpace(strings.ToUpper(c))
		if _, ok := conditionNames[c]; !ok {
			return options{}, fmt.Errorf("unknown condition %q: use A, B or C", c)
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

	fmt.Fprintf(out, "=== recovery-probe (M8D3): an investigation, never recorded ===\n")
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
	fmt.Fprintf(out, "fleet: %d workers x %d slots; offered rate %.0f jobs/minute (A and B)\n", o.workers, o.concurrency, o.rate)
	fmt.Fprintf(out, "workloads: A demo.sleep %s; B demo.sleep %s; C one demo.sleep %s (timeout %ds)\n",
		probeAJobDuration, po.bJob, probeCJobDuration, probeCJobTimeout)
	fmt.Fprintf(out, "over threshold: recovery > lease + %s = %s\n", probeOverThresholdSlack, timings.Lease+probeOverThresholdSlack)
	fmt.Fprintf(out, "seed %d; output and logs under %s\n\n", o.seed, po.outDir)

	rng := rand.New(rand.NewSource(o.seed))
	results := map[string][]trialResult{}
	var runErr error
	for _, c := range po.conditions {
		misses := 0
		for len(results[c]) < po.trials[c] {
			if ctx.Err() != nil {
				runErr = ctx.Err()
				break
			}
			n := len(results[c]) + misses + 1
			tr, err := runProbeTrial(ctx, o, timings, c, n, rng.Uint64(), out)
			if err != nil {
				runErr = fmt.Errorf("condition %s trial %d: %w", c, n, err)
				break
			}
			printTrial(out, tr, timings.Lease)
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
			printSummary(out, c, results[c], timings.Lease)
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
	allJobsTerminal  bool
	replacementsSeen bool
}

// runProbeTrial runs one trial of one condition on a stack of its own.
func runProbeTrial(parent context.Context, o options, timings stack.Timings, cond string, n int, pick uint64, out io.Writer) (tr trialResult, err error) {
	tr = trialResult{cond: cond, n: n}
	trialOptions := o
	switch cond {
	case condLoaded:
		trialOptions.faultJobDuration = probeAJobDuration
	case condTail:
		trialOptions.faultJobDuration = o.probe.bJob
	case condIdle:
		trialOptions.faultJobDuration = probeCJobDuration
	}
	trialOptions.targetableKills = true
	tr.jobDuration = trialOptions.faultJobDuration

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
	timeout := jobTimeoutSecs
	if cond == condIdle {
		timeout = probeCJobTimeout
	}
	submit := newSubmitterFor(st.APIURL, st.APIKey, st.RunID, trialOptions.faultJobDuration, timeout)
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
	if err := fl.probeKill(ctx, pick, &tr); err != nil {
		return tr, err
	}
	if tr.missed {
		st.Say("Kill missed: %s. This trial is not counted.", tr.missReason)
		if loadEnded != nil {
			<-loadEnded
		}
		return tr, nil
	}

	// Watch the recovery: the broker right after each recovery event is published,
	// and every replacement.
	// Polling starts a second before the earliest of the dead session's leases
	// expires. That is up to a lease after the kill, and less for a lease the
	// worker renewed before it died (C's).
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
	deadline := tr.killedAt.Add(timings.Lease + probeReplacementWait)
	hops, snapshots, err := watchRecovery(ctx, st, tr.session, from.Add(-time.Second), deadline, tr.held)
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
func (f *fleet) probeKill(ctx context.Context, pick uint64, tr *trialResult) error {
	st := f.st
	byName := map[string]*workerHandle{}
	var live []string
	for _, w := range f.workers {
		if w.current().Running() {
			byName[w.name] = w
			live = append(live, w.name)
		}
	}
	held, candidates := f.waitForTargetable(ctx, live)
	if len(candidates) == 0 {
		tr.missed, tr.missReason = true, fmt.Sprintf("no worker held an attempt with time left within %s", occupancyWait)
		return nil
	}
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
	fresh, err := st.StartWorkerNamed(ctx, victim.label, name, f.concurrency)
	if err != nil {
		return fmt.Errorf("restart: %w", err)
	}
	victim.replace(fresh)
	victim.setDown(false)
	if tr.restartedSession, err = readdb.CurrentSession(ctx, st.DB, st.Scope, name); err != nil {
		return err
	}
	if tr.held == 0 {
		tr.missed, tr.missReason = true, "the victim held no attempt by the time it was dead"
	}
	return nil
}

// watchRecovery polls the killed session's timeline from `from` until every one of
// the held attempts has a replacement or deadline passes. It reads the broker
// once just after each new publish instant it sees, keyed by that instant.
func watchRecovery(ctx context.Context, st *stack.Stack, session uuid.UUID, from, deadline time.Time, held int) ([]readdb.RecoveryHop, map[int64]brokerSnapshot, error) {
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
