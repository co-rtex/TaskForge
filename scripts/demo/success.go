package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// The handler's stable failure code, as demo.fail reports it. It is repeated
// here rather than imported because it is the public contract a client reads
// back from an attempt, not an internal detail of the worker.
const demoFailureCode = "demo_failure"

// echoPayload is what the echo job submits and what its result must equal.
const echoPayload = `{"message":"hello from the demo","n":1}`

// watched is one job the success demonstration follows to a terminal state.
type watched struct {
	label string
	id    string
	last  string // the status last narrated, so only a change is printed
	final jobView
}

func isTerminal(status string) bool {
	switch jobs.Status(status) {
	case jobs.StatusSucceeded, jobs.StatusDeadLettered, jobs.StatusCanceled:
		return true
	default:
		return false
	}
}

// runSuccess is `make demo`: one worker, and three jobs that end three ways.
func (d *demo) runSuccess(ctx context.Context) {
	d.say("--- One worker, three jobs ---")
	_, workerName, err := d.startWorker(ctx, "w", 4)
	if err != nil {
		d.expect("a worker started", false, "%v", err)
		return
	}
	d.say("Started %s, which declares demo.echo, demo.fail and demo.sleep.", workerName)

	submissions := []struct {
		label, jobType, payload string
		maxAttempts             int
	}{
		{"echo", "demo.echo", echoPayload, 3},
		{"retry", "demo.fail", `{"class":"retryable"}`, 3},
		{"permanent", "demo.fail", `{"class":"permanent"}`, 3},
	}
	var all []*watched
	for _, s := range submissions {
		id, err := d.submit(ctx, s.jobType, s.payload, s.maxAttempts, 30)
		if err != nil {
			d.expect(s.label+": the job was submitted", false, "%v", err)
			return
		}
		d.say("Submitted the %s job %s: %s %s, max_attempts=%d.", s.label, id, s.jobType, s.payload, s.maxAttempts)
		all = append(all, &watched{label: s.label, id: id})
	}
	echo, retry, permanent := all[0], all[1], all[2]

	// One wait for all three, with one bound. Each job's status is narrated when
	// it changes, which is what makes the retry visible as it happens: the job
	// goes back to RETRY_WAIT between attempts instead of straight to its end.
	observed, ok := d.waitFor(ctx, "all three jobs to reach a terminal state", 60*time.Second,
		func(ctx context.Context) (bool, string, error) {
			done := true
			var summary []string
			for _, w := range all {
				job, err := d.job(ctx, w.id)
				if err != nil {
					return false, "", err
				}
				if job.Status != w.last {
					d.say("  %s job: %s", w.label, job.Status)
					w.last = job.Status
				}
				w.final = job
				summary = append(summary, w.label+"="+job.Status)
				done = done && isTerminal(job.Status)
			}
			return done, strings.Join(summary, " "), nil
		})
	d.expect("all three jobs reached a terminal state", ok, "%s", observed)
	if !ok {
		return
	}

	d.verifyEcho(ctx, echo)
	d.verifyRetry(ctx, retry)
	d.verifyPermanent(ctx, permanent)
}

func (d *demo) verifyEcho(ctx context.Context, w *watched) {
	attempts, err := d.attempts(ctx, w.id)
	if err != nil {
		d.expect("echo: the attempt history was read", false, "%v", err)
		return
	}
	d.say("echo job attempts: %s", describeAttempts(attempts))

	expectEqual(d, "echo: job status", string(jobs.StatusSucceeded), w.final.Status)
	expectEqual(d, "echo: attempts", 1, len(attempts))
	if len(attempts) == 1 {
		expectEqual(d, "echo: attempt 1 status", string(workers.AttemptSucceeded), attempts[0].Status)
	}

	body, err := d.result(ctx, w.id)
	if err != nil {
		d.expect("echo: the result was read back", false, "%v", err)
		return
	}
	expectJSONEqual(d, "echo: result read back equals the payload", []byte(echoPayload), body)
}

func (d *demo) verifyRetry(ctx context.Context, w *watched) {
	attempts, err := d.attempts(ctx, w.id)
	if err != nil {
		d.expect("retry: the attempt history was read", false, "%v", err)
		return
	}
	d.say("retry job attempts: %s", describeAttempts(attempts))
	for _, a := range attempts {
		if a.RetryAt != nil && a.RetryDelayMS != nil {
			d.say("  attempt %d failed; the control plane scheduled attempt %d after %dms, at %s",
				a.AttemptNumber, a.AttemptNumber+1, *a.RetryDelayMS, a.RetryAt.Local().Format("15:04:05.000"))
		}
	}

	expectEqual(d, "retry: job status", string(jobs.StatusDeadLettered), w.final.Status)
	expectEqual(d, "retry: attempts", 3, len(attempts))
	for _, a := range attempts {
		expectEqual(d, fmt.Sprintf("retry: attempt %d status", a.AttemptNumber),
			string(workers.AttemptFailed), a.Status)
	}
	classified := len(attempts) > 0
	for _, a := range attempts {
		classified = classified &&
			a.FailureClass != nil && *a.FailureClass == string(lifecycle.ClassRetryable) &&
			a.ErrorCode != nil && *a.ErrorCode == demoFailureCode
	}
	d.expect("retry: every attempt failed RETRYABLE with the demo code", classified, "%s / %s",
		lifecycle.ClassRetryable, demoFailureCode)

	// A retry time is recorded on every failure that was followed by another
	// attempt, and on none that was not: the last failure had no budget left to
	// retry with, so scheduling one would have been the bug.
	for _, a := range attempts {
		last := a.AttemptNumber == len(attempts)
		d.expect(fmt.Sprintf("retry: attempt %d retry time", a.AttemptNumber),
			(a.RetryAt != nil) == !last,
			"recorded=%t, expected recorded=%t", a.RetryAt != nil, !last)
	}

	entry, err := d.dlqEntry(ctx, w.id)
	if err != nil {
		d.expect("retry: the dead-letter entry was read", false, "%v", err)
		return
	}
	if entry == nil {
		d.expect("retry: a dead-letter entry exists", false, "none listed for this job")
		return
	}
	expectEqual(d, "retry: dead-letter reason", string(lifecycle.ReasonAttemptsExhausted), entry.Reason)
}

func (d *demo) verifyPermanent(ctx context.Context, w *watched) {
	attempts, err := d.attempts(ctx, w.id)
	if err != nil {
		d.expect("permanent: the attempt history was read", false, "%v", err)
		return
	}
	d.say("permanent job attempts: %s (max_attempts was 3)", describeAttempts(attempts))

	expectEqual(d, "permanent: job status", string(jobs.StatusDeadLettered), w.final.Status)
	expectEqual(d, "permanent: attempts", 1, len(attempts))
	if len(attempts) == 1 {
		a := attempts[0]
		expectEqual(d, "permanent: attempt 1 status", string(workers.AttemptFailed), a.Status)
		d.expect("permanent: attempt 1 failed PERMANENT with the demo code",
			a.FailureClass != nil && *a.FailureClass == string(lifecycle.ClassPermanent) &&
				a.ErrorCode != nil && *a.ErrorCode == demoFailureCode,
			"%s / %s", lifecycle.ClassPermanent, demoFailureCode)
		d.expect("permanent: no retry was scheduled", a.RetryAt == nil, "retry_at is %v", a.RetryAt)
	}

	entry, err := d.dlqEntry(ctx, w.id)
	if err != nil {
		d.expect("permanent: the dead-letter entry was read", false, "%v", err)
		return
	}
	if entry == nil {
		d.expect("permanent: a dead-letter entry exists", false, "none listed for this job")
		return
	}
	expectEqual(d, "permanent: dead-letter reason", string(lifecycle.ReasonPermanentFailure), entry.Reason)
}
