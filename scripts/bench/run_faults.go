package main

import (
	"context"
	"fmt"
	"io"
	"sync"
	"syscall"
	"time"

	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// runFaults submits o.jobs jobs at o.rate while killing workers on a seeded
// schedule, waits until every job is terminal or the fixed deadline after the
// last submission, and reads the outcome back from PostgreSQL.
//
// A kill is a SIGKILL of the worker's whole process group, with no chance to
// clean up, followed by a restart under the same worker name: the same logical
// worker, a new process boot, which is what a supervisor restarting a crashed
// worker produces. The restart is what keeps the fleet at its full size, so a
// run that cannot restart a worker is reported as degraded and not as a result.
func runFaults(parent context.Context, o options, out io.Writer) (res FaultsResult, err error) {
	span := time.Duration(float64(o.jobs) / o.rate * float64(time.Minute))
	// Scale the quiet start and the minimum gap down for a run that is itself
	// short (the smoke), and use the fixed values for a recorded one.
	first := min(firstKillAfter, span/3)
	gap := min(killMinGap, span/time.Duration(4*o.kills))
	schedule, err := Schedule(o.seed, o.kills, span, first, gap)
	if err != nil {
		return FaultsResult{}, err
	}

	fl, err := startFleet(parent, o, out)
	if err != nil {
		return FaultsResult{}, err
	}
	defer fl.close()
	ctx, stop := fl.watch(parent)
	defer stop(nil)
	defer func() {
		if cause := failureCause(ctx); cause != nil {
			err = cause
		}
	}()
	st := fl.st
	db := st.DB
	clk := realClock{}

	submit := newSubmitterWithDuration(st.APIURL, st.APIKey, st.RunID, o.faultJobDuration)
	start := time.Now()
	pacer, err := NewPacer(start, o.rate)
	if err != nil {
		return FaultsResult{}, err
	}
	st.Say("Submitting %d jobs at %.0f/minute over %s, with %d kills from seed %d (first at %s).",
		o.jobs, o.rate, span.Round(time.Second), o.kills, o.seed, schedule[0].At)

	killed := make(chan []killObservation, 1)
	go func() { killed <- fl.injectFaults(ctx, schedule, start) }()

	var inflight sync.WaitGroup
	slots := make(chan struct{}, maxInFlight)
	pacer.Run(ctx, clk, o.jobs, func(i int) {
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
	submissionEnd := time.Now()
	submittedAt, err := readdb.ClockNow(ctx, db)
	if err != nil {
		return FaultsResult{}, err
	}
	if ctx.Err() != nil {
		return FaultsResult{}, ctx.Err()
	}
	st.Say("All %d submissions done after %s.", o.jobs, submissionEnd.Sub(start).Round(time.Second))

	observations := <-killed
	st.Say("%d of %d kills made. Waiting for every job to finish, for at most %s after the last submission.",
		len(observations), o.kills, o.deadline)

	// The deadline is fixed against the last submission, not against whenever the
	// last kill's restart happened to finish.
	endedBy, err := waitTerminal(ctx, clk, db, st.Scope, submissionEnd.Add(o.deadline))
	if err != nil {
		return FaultsResult{}, err
	}
	st.Say("Stopped waiting (%s).", endedBy)

	// Read back what each kill left abandoned and what replaced it.
	for i := range observations {
		observations[i].Abandoned, err = readdb.Recoveries(ctx, db, st.Scope, observations[i].Session)
		if err != nil {
			return FaultsResult{}, err
		}
	}
	statuses, _, err := statusesAndTerminal(ctx, db, st.Scope)
	if err != nil {
		return FaultsResult{}, err
	}
	dlq, err := readdb.DLQReasons(ctx, db, st.Scope)
	if err != nil {
		return FaultsResult{}, err
	}
	inDatabase := 0
	for _, n := range statuses {
		inDatabase += n
	}

	sent := submit.stats()
	res = analyzeFaults(faultsInputs{
		profile: o.profile, seed: o.seed, workers: o.workers, concurrency: o.concurrency, targetRate: o.rate,
		jobsTarget: o.jobs, submitted: sent.OK, submitErrors: sent.Failed, retries: sent.Retries,
		jobsInDatabase: inDatabase,
		kills:          observations, scheduledKills: len(schedule),
		submissionDuration: submissionEnd.Sub(start), deadline: o.deadline,
		deadlineAt: submittedAt.Add(o.deadline), endedBy: endedBy,
		statuses: statuses, dlq: dlq,
		observed: fl.exitedOnTheirOwn(),
	})
	st.Say("%d of %d jobs SUCCEEDED (%.3f%%); %d abandoned attempts, %d never replaced; recovery worst %s over %d.",
		res.Succeeded, res.JobsInDatabase, floorTo(res.CompletionPercent, 3), res.Affected, res.Unrecovered,
		fmtDur(res.Recovery.Max), res.Recovery.N)
	if !res.Valid {
		return res, fmt.Errorf("the fault run is not valid: %v", res.Problems)
	}
	return res, nil
}

// injectFaults carries out the schedule: each kill at its offset from start.
func (f *fleet) injectFaults(ctx context.Context, schedule []Kill, start time.Time) []killObservation {
	var out []killObservation
	for i, k := range schedule {
		if err := (realClock{}).SleepUntil(ctx, start.Add(k.At)); err != nil {
			return out
		}
		out = append(out, f.killOne(ctx, i, k))
	}
	return out
}

// killOne SIGKILLs one worker and restarts it under the same name.
//
// With targeted kills on (only the smoke) the victim is drawn from the workers
// holding an attempt that still has at least targetMargin of its duration left, read
// on PostgreSQL's clock, waiting up to occupancyWait for one; if none appears the
// kill is NOT made and the observation says why, so the smoke fails the check that
// names that and a harness miss is not read as a recovery failure. Recorded runs
// never take that branch, and the paragraph below is exactly what they do.
//
// Which worker. The victim is drawn from the workers that are holding an attempt
// when the kill is due, waiting up to occupancyWait for one; if none ever does,
// from every worker that is running. With 50 ms jobs at the target rate a worker
// holds an attempt a few percent of the time, so killing a worker at random
// would almost always kill an idle one and measure nothing. The kill's record
// says which kind of victim it had. The draw is the schedule's seeded value; the
// candidates are sorted, so for the same state the same seed picks the same
// worker.
//
// What is read, and when. The session to be killed is read first, and then
// `SELECT clock_timestamp()`, and then the SIGKILL, so the stamp is as close
// before the kill as one round trip allows. What the kill left abandoned is read
// much later, by the caller, once recovery has run.
func (f *fleet) killOne(ctx context.Context, index int, k Kill) killObservation {
	ob := killObservation{Index: index, ScheduledOffset: k.At}
	st := f.st
	fail := func(format string, args ...any) killObservation {
		ob.RestartError = fmt.Sprintf(format, args...)
		return ob
	}

	byName := map[string]*workerHandle{}
	var live []string
	for _, w := range f.workers {
		if w.current().Running() {
			byName[w.name] = w
			live = append(live, w.name)
		}
	}
	if len(live) == 0 {
		return fail("no worker was running to kill")
	}

	var (
		held       map[string]int
		candidates []string
	)
	if f.target != nil {
		held, candidates = f.waitForTargetable(ctx, live)
		if len(candidates) == 0 {
			ob.NotMade = fmt.Sprintf("no worker held an attempt with at least %s of its %s left within %s",
				f.target.margin, f.target.duration, occupancyWait)
			return ob
		}
		ob.ChoseOccupied = true
	} else {
		var occupied []string
		held, occupied = f.waitForOccupied(ctx, live)
		candidates = occupied
		ob.ChoseOccupied = len(occupied) > 0
		if !ob.ChoseOccupied {
			candidates = live
		}
	}
	name, err := ChooseVictim(k.Pick, candidates)
	if err != nil {
		return fail("%v", err)
	}
	victim := byName[name]
	ob.Worker, ob.HeldAtSelection = victim.label, held[name]

	if ob.Session, err = readdb.CurrentSession(ctx, st.DB, st.Scope, name); err != nil {
		return fail("%v", err)
	}
	if ob.KilledAt, err = readdb.ClockNow(ctx, st.DB); err != nil {
		return fail("%v", err)
	}
	proc := victim.current()
	victim.setDown(true) // the injector's own kill, not a crash for the watchdog to abort on
	if err := proc.Signal(syscall.SIGKILL); err != nil {
		return fail("SIGKILL: %v", err)
	}
	if !proc.WaitExit(10 * time.Second) {
		return fail("the killed process did not exit within 10s")
	}
	st.Say("Kill %d: SIGKILL %s (held %d attempts) at PostgreSQL time %s.",
		index, victim.label, ob.HeldAtSelection, ob.KilledAt.UTC().Format("15:04:05.000"))

	restarted := time.Now()
	fresh, err := st.StartWorkerNamed(ctx, victim.label, name, f.concurrency)
	if err != nil {
		return fail("restart: %v", err)
	}
	victim.replace(fresh)
	victim.setDown(false)
	ob.RestartMS = float64(time.Since(restarted)) / float64(time.Millisecond)
	return ob
}

// waitForOccupied returns each live worker's held-attempt count and the live
// workers holding at least one, waiting up to occupancyWait for there to be one.
func (f *fleet) waitForOccupied(ctx context.Context, live []string) (held map[string]int, occupied []string) {
	deadline := time.Now().Add(occupancyWait)
	for {
		var err error
		if held, err = readdb.Occupancy(ctx, f.st.DB, f.st.Scope); err == nil {
			occupied = occupied[:0]
			for _, name := range live {
				if held[name] > 0 {
					occupied = append(occupied, name)
				}
			}
			if len(occupied) > 0 {
				return held, occupied
			}
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			return held, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// targeting is what aiming a kill needs: how long the jobs sleep, and how much of
// that must be left for an attempt to be a target.
type targeting struct{ duration, margin time.Duration }

// targetingFor is nil for every run but the smoke's, which is what keeps a recorded
// run on the original selection.
func targetingFor(o options) *targeting {
	if !o.targetableKills {
		return nil
	}
	return &targeting{duration: o.faultJobDuration, margin: o.targetMargin()}
}

// waitForTargetable returns each live worker's count of attempts a kill could still
// catch, and the live workers holding at least one, waiting up to occupancyWait for
// there to be one. It is waitForOccupied with the targetable query: the candidates
// are the same kind of thing, and ChooseVictim sorts them, so the seeded draw
// picks the same worker for the same state.
func (f *fleet) waitForTargetable(ctx context.Context, live []string) (held map[string]int, candidates []string) {
	deadline := time.Now().Add(occupancyWait)
	for {
		var err error
		if held, err = readdb.TargetableAttempts(ctx, f.st.DB, f.st.Scope, f.target.duration, f.target.margin); err == nil {
			candidates = targetableCandidates(held, live)
			if len(candidates) > 0 {
				return held, candidates
			}
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			return held, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// targetableCandidates is the live workers that hold at least one targetable
// attempt. A worker that is not running is not a candidate whatever the database
// says about its attempts: it cannot be killed twice.
func targetableCandidates(held map[string]int, live []string) []string {
	var out []string
	for _, name := range live {
		if held[name] > 0 {
			out = append(out, name)
		}
	}
	return out
}
