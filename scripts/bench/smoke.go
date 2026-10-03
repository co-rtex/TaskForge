package main

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// check is one claim the smoke makes about the harness's own output.
type check struct {
	name   string
	ok     bool
	detail string
}

// runSmoke runs a small throughput run and a small fault run, with the
// short-lease profile, and asserts that the harness produced valid measurements.
// It records nothing and it judges no target: a smoke run's numbers are not
// benchmark numbers. What it proves is that the harness runs, measures what it
// claims to, and fails when it should. It returns an error if any check fails.
func runSmoke(ctx context.Context, out io.Writer) error {
	o := smokeOptions()
	th, thErr := runThroughput(ctx, o, out)
	f, fErr := runFaults(ctx, o, out)
	if ctx.Err() != nil {
		return ctx.Err()
	}

	checks := smokeChecks(th, thErr, f, fErr)
	width, bad := 0, 0
	for _, c := range checks {
		width = max(width, len(c.name))
	}
	fmt.Fprintf(out, "\n=== make bench-smoke: checks ===\n")
	for _, c := range checks {
		verdict := "PASS"
		if !c.ok {
			verdict, bad = "FAIL", bad+1
		}
		fmt.Fprintf(out, "  %s  %-*s  %s\n", verdict, width, c.name, c.detail)
	}
	if bad > 0 {
		fmt.Fprintf(out, "=== RESULT: FAIL (%d of %d checks failed) ===\n\n", bad, len(checks))
		return fmt.Errorf("%d of %d smoke checks failed", bad, len(checks))
	}
	fmt.Fprintf(out, "=== RESULT: PASS (%d of %d checks met) ===\n\n", len(checks), len(checks))
	return nil
}

// The names of the checks, which the tests refer to so that each broken input
// must fail the check it is meant to break and not merely some check.
const (
	checkThroughputRan       = "the throughput run completed"
	checkSubmitted           = "throughput: at least 50 jobs submitted, none refused"
	checkWindow              = "throughput: the steady window measured jobs"
	checkClaimed             = "throughput: every in-window job was claimed, with a non-negative latency"
	checkThroughputValid     = "throughput run is valid"
	checkThroughputSucceeded = "throughput: every job reached SUCCEEDED"
	checkFaultsRan           = "the fault run completed"
	checkFaultJobs           = "faults: at least 50 jobs, every one in the database"
	checkKill                = "faults: one kill, stamped with PostgreSQL's time and the killed session"
	checkRecovered           = "faults: the kill hit an attempt, and it was recovered"
	checkPositive            = "faults: every recovery time is positive"
	checkFaultsSucceeded     = "faults: every job reached SUCCEEDED"
	checkFaultsValid         = "fault run is valid"
)

// minSmokeJobs is the least a smoke run may submit in each of its two phases.
const minSmokeJobs = 50

// smokeChecks decides what a smoke run's two results say about the harness. It is
// a pure function of them, so each check is tested by breaking its input.
func smokeChecks(th ThroughputResult, thErr error, f FaultsResult, fErr error) []check {
	var cs []check
	add := func(name string, ok bool, format string, args ...any) {
		cs = append(cs, check{name: name, ok: ok, detail: fmt.Sprintf(format, args...)})
	}

	add(checkThroughputRan, thErr == nil, "%v", errText(thErr))
	if thErr == nil || th.Submitted > 0 {
		add(checkSubmitted,
			th.Submitted >= minSmokeJobs && th.SubmitErrors == 0,
			"%d submitted, %d refused", th.Submitted, th.SubmitErrors)
		add(checkWindow,
			th.JobsInWindow > 0 && th.CompletedInWindow > 0,
			"%d created and %d succeeded inside the window (%.1f/min)", th.JobsInWindow, th.CompletedInWindow, th.ThroughputPerMin)
		add(checkClaimed,
			th.Unclaimed == 0 && th.NegativeLatency == 0 && th.Dispatch.N == th.JobsInWindow,
			"%d latencies for %d jobs, %d unclaimed, %d negative; p95 %s", th.Dispatch.N, th.JobsInWindow, th.Unclaimed, th.NegativeLatency, fmtDur(th.Dispatch.P95))
		add(checkThroughputValid, th.Valid, "%s", problems(th.Problems))
		add(checkThroughputSucceeded,
			th.Submitted > 0 && th.FinalStatuses["SUCCEEDED"] == th.Submitted && th.NotTerminalAtDrainEnd == 0,
			"%d of %d, %d not terminal; %s", th.FinalStatuses["SUCCEEDED"], th.Submitted, th.NotTerminalAtDrainEnd, counts(th.FinalStatuses))
	}

	add(checkFaultsRan, fErr == nil, "%v", errText(fErr))
	if fErr == nil || f.JobsSubmitted > 0 {
		add(checkFaultJobs,
			f.JobsInDatabase >= minSmokeJobs && f.JobsInDatabase == f.JobsSubmitted,
			"%d accepted, %d in the database", f.JobsSubmitted, f.JobsInDatabase)

		killOK := f.KillsDone == 1 && len(f.Kills) == 1 && !f.Kills[0].KilledAt.IsZero() && f.Kills[0].Session != ""
		add(checkKill, killOK, "%d kills made", f.KillsDone)

		add(checkRecovered,
			f.Affected >= 1 && f.Recovery.N >= 1 && f.Unrecovered == 0,
			"%d abandoned, %d replaced, %d never replaced", f.Affected, f.Recovery.N, f.Unrecovered)

		positive := len(f.Kills) > 0
		for _, k := range f.Kills {
			for _, s := range k.RecoverySeconds {
				positive = positive && s > 0
			}
		}
		add(checkPositive, positive, "worst %s", fmtDur(f.Recovery.Max))
		add(checkFaultsSucceeded,
			f.JobsInDatabase > 0 && f.Succeeded == f.JobsInDatabase,
			"%d of %d; %s", f.Succeeded, f.JobsInDatabase, counts(f.FinalStatuses))
		add(checkFaultsValid, f.Valid, "%s", problems(f.Problems))
	}
	return cs
}

func errText(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

func problems(p []string) string {
	if len(p) == 0 {
		return "no problems"
	}
	return strings.Join(p, "; ")
}
