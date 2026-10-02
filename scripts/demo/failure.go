package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/workers"
)

const (
	// sleepMillis is how long each demo.sleep attempt runs. It must outlast the
	// time it takes to start a second worker and then kill the first, so the
	// first attempt is still running when it is killed; and the second attempt
	// sleeps it again in full, so it is also most of what a phase costs.
	sleepMillis = 8000

	// sleepTimeoutSeconds is each attempt's execution budget: well above the
	// sleep, so no attempt is ever timed out in place of being abandoned.
	sleepTimeoutSeconds = 30

	// sleepMaxAttempts leaves room for a third attempt, so that "no third attempt
	// was created" is a claim the system could have falsified.
	sleepMaxAttempts = 3

	// resumeGrace is the least time a resumed worker is given to act before the
	// durable state is read again: several heartbeat and renewal intervals of
	// failureTimings, and longer than one control-plane request timeout.
	resumeGrace = 4 * time.Second
)

// refusalCodes are error codes a control plane answers a stale worker with. They
// are matched on, not their wording, when printing the frozen worker's own log.
var refusalCodes = regexp.MustCompile(`fence_rejected|lease_expired|worker_session_unavailable|attempt_timed_out`)

func sleepPayload() string { return fmt.Sprintf(`{"duration_ms":%d}`, sleepMillis) }

// runFailure is `make demo-failure`: a worker is killed, and then another is
// frozen, while each is running a job.
func (d *demo) runFailure(ctx context.Context) {
	d.crashPhase(ctx)
	if ctx.Err() != nil {
		return
	}
	d.fencingPhase(ctx)
}

// waitRunningOn waits until the API shows the job RUNNING, with its first
// attempt RUNNING under the named worker.
func (d *demo) waitRunningOn(ctx context.Context, jobID, workerName string) (string, bool) {
	return d.waitFor(ctx, "the job to be RUNNING on "+shortWorker(workerName), 30*time.Second,
		func(ctx context.Context) (bool, string, error) {
			job, err := d.job(ctx, jobID)
			if err != nil {
				return false, "", err
			}
			attempts, err := d.attempts(ctx, jobID)
			if err != nil {
				return false, "", err
			}
			running := job.Status == string(jobs.StatusRunning) && len(attempts) == 1 &&
				attempts[0].Status == string(workers.AttemptRunning) && attempts[0].WorkerName == workerName
			return running, fmt.Sprintf("job %s; %s", job.Status, describeAttempts(attempts)), nil
		})
}

// crashPhase kills a worker with SIGKILL while it holds a RUNNING attempt, and
// shows another worker finish the job.
func (d *demo) crashPhase(ctx context.Context) {
	d.say("--- Phase 1: a worker is killed mid-job ---")

	workerA, nameA, err := d.startWorker(ctx, "a", 1)
	if err != nil {
		d.expect("crash: worker A started", false, "%v", err)
		return
	}
	jobID, err := d.submit(ctx, "demo.sleep", sleepPayload(), sleepMaxAttempts, sleepTimeoutSeconds)
	if err != nil {
		d.expect("crash: the job was submitted", false, "%v", err)
		return
	}
	d.say("Submitted job %s: demo.sleep for %dms. Only worker A is running.", jobID, sleepMillis)

	observed, ok := d.waitRunningOn(ctx, jobID, nameA)
	if !ok {
		d.expect("crash: the job ran on worker A", false, "%s", observed)
		return
	}
	sessionsA, err := d.workerSessions(ctx, nameA)
	if err != nil || len(sessionsA) != 1 {
		d.expect("crash: worker A has one process session", false, "sessions=%v err=%v", sessionsA, err)
		return
	}
	d.say("The API shows the job RUNNING on worker A (session %s).", short(sessionsA[0]))

	workerB, nameB, err := d.startWorker(ctx, "b", 1)
	if err != nil {
		d.expect("crash: worker B started", false, "%v", err)
		return
	}
	// Phase 2 begins "with only worker C running". A worker left over from here
	// would be eligible for that job too, and would take it before C could.
	defer func() {
		d.say("Stopping worker B, so the next phase starts with no worker running.")
		workerB.stop()
		// B's last long poll may still be open at the broker, and a notification
		// published while it is open can be handed to a connection nobody is
		// reading. The system repairs that (the scheduler re-notifies), but five
		// seconds of repair is not the beat phase 2 is about, so the broker is
		// given one poll wait to drop it first.
		select {
		case <-ctx.Done():
		case <-time.After(d.timing.workerPollWait + 500*time.Millisecond):
		}
	}()
	d.say("Started worker B, so there is somewhere for the job to go. Now SIGKILL worker A: no signal handler, no cleanup, no chance to say goodbye.")
	if err := workerA.signal(syscall.SIGKILL); err != nil {
		d.expect("crash: worker A was killed", false, "%v", err)
		return
	}
	workerA.waitExit(10 * time.Second)
	d.expect("crash: worker A died from SIGKILL", !workerA.running() && workerA.diedFromSignal(syscall.SIGKILL),
		"still running: %t", workerA.running())

	d.say("Nothing tells the control plane A is dead. Its heartbeats stop and its lease runs out; the reconciler notices both.")
	observed, ok = d.waitFor(ctx, "the job to reach SUCCEEDED", 90*time.Second,
		func(ctx context.Context) (bool, string, error) {
			job, err := d.job(ctx, jobID)
			if err != nil {
				return false, "", err
			}
			attempts, err := d.attempts(ctx, jobID)
			if err != nil {
				return false, "", err
			}
			return job.Status == string(jobs.StatusSucceeded), fmt.Sprintf("job %s; %s", job.Status, describeAttempts(attempts)), nil
		})
	d.expect("crash: the job reached SUCCEEDED", ok, "%s", observed)
	if !ok {
		return
	}

	job, err := d.job(ctx, jobID)
	if err != nil {
		d.expect("crash: the job was read back", false, "%v", err)
		return
	}
	attempts, err := d.attempts(ctx, jobID)
	if err != nil {
		d.expect("crash: the attempt history was read", false, "%v", err)
		return
	}
	d.say("Final timeline: %s.", describeAttempts(attempts))
	expectEqual(d, "crash: job status", string(jobs.StatusSucceeded), job.Status)
	expectEqual(d, "crash: attempts", 2, len(attempts))
	if len(attempts) != 2 {
		return
	}
	expectEqual(d, "crash: attempt 1 status", string(workers.AttemptAbandoned), attempts[0].Status)
	expectEqual(d, "crash: attempt 2 status", string(workers.AttemptSucceeded), attempts[1].Status)
	expectEqual(d, "crash: attempt 1 ran on worker A", nameA, attempts[0].WorkerName)
	expectEqual(d, "crash: attempt 2 ran on worker B", nameB, attempts[1].WorkerName)

	d.verifySessions(ctx, "crash", jobID, nameA, nameB)
}

// verifySessions checks, from PostgreSQL, which process session each of a job's
// first two attempts is bound to: the first to a session of the first worker,
// the second to a session of the second, and the two sessions different. The
// public API names the logical worker but deliberately not the session.
func (d *demo) verifySessions(ctx context.Context, prefix, jobID, firstWorker, secondWorker string) {
	bindings, err := d.attemptBindings(ctx, jobID)
	if err != nil || len(bindings) < 2 {
		d.expect(prefix+": the attempts' sessions were read", false, "bindings=%d err=%v", len(bindings), err)
		return
	}
	firstSessions, err := d.workerSessions(ctx, firstWorker)
	if err != nil {
		d.expect(prefix+": the first worker's sessions were read", false, "%v", err)
		return
	}
	secondSessions, err := d.workerSessions(ctx, secondWorker)
	if err != nil {
		d.expect(prefix+": the second worker's sessions were read", false, "%v", err)
		return
	}
	d.say("Read from PostgreSQL (the API withholds session ids): attempt 1 is bound to session %s, attempt 2 to session %s.",
		short(bindings[0].SessionID), short(bindings[1].SessionID))

	d.expect(prefix+": attempt 1 is bound to "+shortWorker(firstWorker)+"'s session",
		containsUUID(firstSessions, bindings[0].SessionID), "session %s", short(bindings[0].SessionID))
	d.expect(prefix+": attempt 2 is bound to "+shortWorker(secondWorker)+"'s session",
		containsUUID(secondSessions, bindings[1].SessionID), "session %s", short(bindings[1].SessionID))
	d.expect(prefix+": the two attempts ran under different sessions",
		bindings[0].SessionID != bindings[1].SessionID, "%s vs %s",
		short(bindings[0].SessionID), short(bindings[1].SessionID))
}

// durableView is the part of a job's stored state the fencing phase compares
// across the moment the frozen worker resumes.
type durableView struct {
	JobStatus    string
	Attempts     []string // "number:status:worker"
	ResultOwners []uuid.UUID
}

func (d *demo) durableView(ctx context.Context, jobID string) (durableView, []attemptView, error) {
	job, err := d.job(ctx, jobID)
	if err != nil {
		return durableView{}, nil, err
	}
	attempts, err := d.attempts(ctx, jobID)
	if err != nil {
		return durableView{}, nil, err
	}
	owners, err := d.resultOwners(ctx, jobID)
	if err != nil {
		return durableView{}, nil, err
	}
	view := durableView{JobStatus: job.Status, ResultOwners: owners}
	for _, a := range attempts {
		view.Attempts = append(view.Attempts, fmt.Sprintf("%d:%s:%s", a.AttemptNumber, a.Status, a.WorkerName))
	}
	return view, attempts, nil
}

// fencingPhase freezes a worker mid-job, lets its attempt be abandoned and
// finished by another worker, and then lets the frozen one wake up.
//
// A frozen process is the case a kill does not cover: it is not dead, it still
// holds its memory and its idea that it owns the job, and when it wakes it is
// going to act on that. What stops it is not anything the worker does; the
// stored state refuses it. The pass condition is therefore the durable outcome,
// read after the worker has had time to act, and not anything in its log.
func (d *demo) fencingPhase(ctx context.Context) {
	d.say("--- Phase 2: a frozen worker wakes up and acts on a job it no longer owns ---")

	workerC, nameC, err := d.startWorker(ctx, "c", 1)
	if err != nil {
		d.expect("fence: worker C started", false, "%v", err)
		return
	}
	jobID, err := d.submit(ctx, "demo.sleep", sleepPayload(), sleepMaxAttempts, sleepTimeoutSeconds)
	if err != nil {
		d.expect("fence: the job was submitted", false, "%v", err)
		return
	}
	d.say("Submitted job %s: demo.sleep for %dms. Only worker C is running.", jobID, sleepMillis)

	observed, ok := d.waitRunningOn(ctx, jobID, nameC)
	if !ok {
		d.expect("fence: the job ran on worker C", false, "%s", observed)
		return
	}
	d.say("The API shows the job RUNNING on worker C.")

	d.say("SIGSTOP worker C: frozen, not dead. It cannot heartbeat or renew its lease, and it stays this way until attempt 1 is ABANDONED.")
	if err := workerC.signal(syscall.SIGSTOP); err != nil {
		d.expect("fence: worker C was frozen", false, "%v", err)
		return
	}
	observed, ok = d.waitFor(ctx, "attempt 1 to be ABANDONED", 60*time.Second,
		func(ctx context.Context) (bool, string, error) {
			attempts, err := d.attempts(ctx, jobID)
			if err != nil {
				return false, "", err
			}
			return len(attempts) > 0 && attempts[0].Status == string(workers.AttemptAbandoned), describeAttempts(attempts), nil
		})
	d.expect("fence: attempt 1 was ABANDONED while C was frozen", ok, "%s", observed)
	if !ok {
		return
	}
	d.say("The API shows attempt 1 ABANDONED. Worker C is still frozen.")

	_, nameD, err := d.startWorker(ctx, "d", 1)
	if err != nil {
		d.expect("fence: worker D started", false, "%v", err)
		return
	}
	d.say("Started worker D.")
	observed, ok = d.waitFor(ctx, "attempt 2 to SUCCEED on worker D", 90*time.Second,
		func(ctx context.Context) (bool, string, error) {
			attempts, err := d.attempts(ctx, jobID)
			if err != nil {
				return false, "", err
			}
			done := len(attempts) >= 2 && attempts[1].Status == string(workers.AttemptSucceeded) &&
				attempts[1].WorkerName == nameD
			return done, describeAttempts(attempts), nil
		})
	d.expect("fence: attempt 2 SUCCEEDED on worker D", ok, "%s", observed)
	if !ok {
		return
	}
	before, _, err := d.durableView(ctx, jobID)
	if err != nil {
		d.expect("fence: the stored state was read before C resumed", false, "%v", err)
		return
	}
	d.say("Attempt 2 SUCCEEDED on worker D. Stored state: %s, attempts %v.", before.JobStatus, before.Attempts)

	d.say("SIGCONT worker C. Every timer it had is now overdue, and it still holds attempt 1's lease and fence in memory.")
	resumedAt := time.Now()
	if err := workerC.signal(syscall.SIGCONT); err != nil {
		d.expect("fence: worker C was resumed", false, "%v", err)
		return
	}
	// Give it time to act. A worker whose session has been fenced is expected to
	// stop, so waiting for it to exit usually ends the wait early; the minimum
	// below is what makes the wait mean something when it does not.
	workerC.waitExit(10 * time.Second)
	if wait := resumeGrace - time.Since(resumedAt); wait > 0 {
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
	d.say("Worker C has had %s to act (it is %s).", time.Since(resumedAt).Round(100*time.Millisecond), processState(workerC))

	after, attempts, err := d.durableView(ctx, jobID)
	if err != nil {
		d.expect("fence: the stored state was read after C resumed", false, "%v", err)
		return
	}
	d.say("Stored state after C resumed: %s, attempts %v.", after.JobStatus, after.Attempts)

	// The pass conditions: all of them are the stored outcome.
	expectEqual(d, "fence: job status", string(jobs.StatusSucceeded), after.JobStatus)
	expectEqual(d, "fence: attempts", 2, len(attempts))
	if len(attempts) >= 2 {
		expectEqual(d, "fence: attempt 1 status", string(workers.AttemptAbandoned), attempts[0].Status)
		expectEqual(d, "fence: attempt 2 status", string(workers.AttemptSucceeded), attempts[1].Status)
		expectEqual(d, "fence: attempt 1 ran on worker C", nameC, attempts[0].WorkerName)
		expectEqual(d, "fence: attempt 2 ran on worker D", nameD, attempts[1].WorkerName)
	}
	expectEqual(d, "fence: results stored for the job", 1, len(after.ResultOwners))
	if len(attempts) >= 2 && len(after.ResultOwners) == 1 {
		d.expect("fence: the result belongs to attempt 2", after.ResultOwners[0].String() == attempts[1].ID,
			"result attempt %s, attempt 2 is %s", short(after.ResultOwners[0]), shortID(attempts[1].ID))
	}
	if reflect.DeepEqual(before, after) {
		d.expect("fence: nothing C did changed the stored state", true,
			"job, attempts and result are identical before and after C resumed")
	} else {
		d.expect("fence: nothing C did changed the stored state", false, "before %v, after %v", before, after)
	}
	d.verifySessions(ctx, "fence", jobID, nameC, nameD)

	d.illustrateRefusal(workerC)
}

// illustrateRefusal prints what the frozen worker's own log says about being
// fenced. It is an illustration and not evidence: a worker's log is whatever it
// chose to write, and the expectations above are what decide whether the
// demonstration passed.
//
// What it finds depends on which of two defenses acted first, and that is worth
// saying plainly. A worker that sends a stale report is refused by the control
// plane with an error code, which is matched on here. But a worker frozen for
// longer than its lease has also let its own monotonic lease deadline pass, and
// the real worker checks that before it reports anything: it stops itself and
// never sends the stale call. Both leave the stored state untouched; this
// demonstration, with unmodified binaries, reliably produces the second.
func (d *demo) illustrateRefusal(p *proc) {
	data, err := os.ReadFile(p.log)
	if err != nil {
		d.say("Could not read %s's log: %v", p.label, err)
		return
	}
	var refusals, own []string
	for _, raw := range strings.Split(string(data), "\n") {
		var line struct {
			Level string `json:"level"`
			Msg   string `json:"msg"`
			Error string `json:"error"`
		}
		if json.Unmarshal([]byte(raw), &line) != nil {
			continue
		}
		text := strings.TrimSpace(fmt.Sprintf("%-5s %s %s", line.Level, line.Msg, line.Error))
		switch {
		case refusalCodes.MatchString(raw):
			refusals = append(refusals, truncate(text, 300))
		case line.Level == "WARN" || line.Level == "ERROR":
			own = append(own, truncate(text, 300))
		}
	}

	print := func(lines []string) {
		for _, line := range lines[:min(len(lines), 5)] {
			fmt.Fprintf(d.out, "           %s\n", line)
		}
	}
	if len(refusals) > 0 {
		d.say("The control plane refused the frozen worker; matched on the error code in %s's own log (illustration only, not a pass condition):", p.label)
		print(refusals)
		return
	}
	d.say("No control-plane refusal code appears in %s's log: it sent no stale report to refuse. Every lease deadline it held had passed during the freeze, so it stopped itself first. Its own account (illustration only, not a pass condition):", p.label)
	print(own)
}

func processState(p *proc) string {
	if p.running() {
		return "still running"
	}
	return "no longer running"
}

func containsUUID(set []uuid.UUID, id uuid.UUID) bool {
	for _, candidate := range set {
		if candidate == id {
			return true
		}
	}
	return false
}

// short renders an identifier compactly for narration.
func short(id uuid.UUID) string { return shortID(id.String()) }

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
