package main

import (
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// setting is one TASKFORGE_* value (or a fixed parameter of the run) and what it
// was.
type setting struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// TargetRow is one PROJECT_SPEC section 7 target, what was measured against it,
// the verdict, and the settings in effect that govern the figure. The settings
// sit beside the verdict whether it is Met or MISSED, so a miss is never printed
// without the configuration that caused it.
type TargetRow struct {
	Name       string    `json:"target"`
	Target     string    `json:"value"`
	Measured   string    `json:"measured"`
	Verdict    string    `json:"verdict"`
	GovernedBy []setting `json:"governed_by"`
}

// Record is a whole recorded run: the SUMMARY .json is this, marshalled, and the
// .md is renderMarkdown of it. It holds aggregates and the fault timeline, never
// per-job rows.
type Record struct {
	SchemaVersion   int               `json:"schema_version"`
	RecordedAt      time.Time         `json:"recorded_at"`
	Commit          string            `json:"commit"`
	TreeClean       bool              `json:"working_tree_clean_at_start"`
	BinariesChecked bool              `json:"binaries_built_from_this_commit"`
	Command         string            `json:"command"`
	Profile         string            `json:"profile"`
	Seed            int64             `json:"seed"`
	Timing          map[string]string `json:"taskforge_timing_settings"`
	Environment     Environment       `json:"environment"`
	Throughput      *ThroughputResult `json:"throughput"`
	Faults          *FaultsResult     `json:"faults"`
	Targets         []TargetRow       `json:"targets"`
}

// governedBy picks the named settings out of the timing settings in effect.
func governedBy(timing map[string]string, names ...string) []setting {
	var out []setting
	for _, n := range names {
		if v, ok := timing[n]; ok {
			out = append(out, setting{n, v})
		}
	}
	return out
}

// evaluateTargets compares each part of the run that was made with its
// section 7 targets. A part that was not run gets no rows: nothing is invented.
func evaluateTargets(th *ThroughputResult, f *FaultsResult, timing map[string]string, workers, concurrency int) []TargetRow {
	var rows []TargetRow

	if th != nil {
		rows = append(rows, TargetRow{
			Name: "Sustained throughput", Target: "1,000 jobs/minute across 12 workers",
			Measured: fmt.Sprintf("%.2f jobs/min completed (offered %.2f/min; %d finished in the %s window)",
				floorTo(th.ThroughputPerMin, 2), floorTo(th.OfferedPerMin, 2), th.CompletedInWindow, th.Steady.Round(time.Second)),
			Verdict: JudgeThroughput(th.ThroughputPerMin, th.OfferedPerMin, th.SubmitErrors).String(),
			GovernedBy: append([]setting{
				{"workers", fmt.Sprint(workers)}, {"TASKFORGE_WORKER_CONCURRENCY", fmt.Sprint(concurrency)},
			}, governedBy(timing, "TASKFORGE_WORKER_POLL_WAIT")...),
		})
		rows = append(rows, TargetRow{
			Name: "Dispatch latency", Target: "p95 < 500 ms",
			Measured: fmt.Sprintf("p95 %s (n=%d; p50 %s, p99 %s, max %s; %d never claimed)",
				fmtDur(th.Dispatch.P95), th.Dispatch.N, fmtDur(th.Dispatch.P50), fmtDur(th.Dispatch.P99), fmtDur(th.Dispatch.Max), th.Unclaimed),
			Verdict: JudgeDispatch(th.Dispatch, th.Unclaimed).String(),
			GovernedBy: governedBy(timing, "TASKFORGE_OUTBOX_POLL_INTERVAL", "TASKFORGE_OUTBOX_CLAIM_TIMEOUT",
				"TASKFORGE_WORKER_POLL_WAIT"),
		})
	}

	if f != nil {
		rows = append(rows, TargetRow{
			Name: "Fault-injection volume", Target: "10,000 jobs",
			Measured: fmt.Sprintf("%d jobs accepted by the API, %d in the database, %d workers killed",
				f.JobsSubmitted, f.JobsInDatabase, f.KillsDone),
			Verdict:    JudgeFaultVolume(f.JobsInDatabase).String(),
			GovernedBy: []setting{{"jobs submitted", fmt.Sprint(f.JobsTarget)}},
		})
		rows = append(rows, TargetRow{
			Name: "Completion under fault injection", Target: "≥ 99.7%",
			Measured: fmt.Sprintf("%d of %d jobs SUCCEEDED (%.3f%%) by the deadline, %s after the last submission",
				f.Succeeded, f.JobsInDatabase, floorTo(f.CompletionPercent, 3), time.Duration(f.CompletionDeadlineSeconds*float64(time.Second))),
			Verdict: JudgeCompletion(f.Succeeded, f.JobsInDatabase).String(),
			GovernedBy: append([]setting{{"max_attempts per job", fmt.Sprint(jobMaxAttempts)}},
				governedBy(timing, "TASKFORGE_LEASE_DURATION", "TASKFORGE_SESSION_STALE_AFTER", "TASKFORGE_JOB_RETRY_BASE")...),
		})
		var measured string
		if f.Recovery.Valid() {
			measured = fmt.Sprintf("worst %s, p95 %s, p50 %s; %d abandoned attempts replaced, %d never replaced; %d kills",
				fmtDur(f.Recovery.Max), fmtDur(f.Recovery.P95), fmtDur(f.Recovery.P50), f.Recovery.N, f.Unrecovered, f.KillsDone)
		} else {
			measured = fmt.Sprintf("no kill left an attempt abandoned (%d kills; %d abandoned attempts never replaced)", f.KillsDone, f.Unrecovered)
		}
		rows = append(rows, TargetRow{
			Name: "Worker-failure recovery", Target: "< 30 s",
			Measured: measured,
			Verdict:  JudgeRecovery(f.Recovery, f.Unrecovered).String(),
			GovernedBy: governedBy(timing, "TASKFORGE_LEASE_DURATION", "TASKFORGE_LEASE_RENEW_INTERVAL",
				"TASKFORGE_SESSION_STALE_AFTER", "TASKFORGE_RECONCILER_POLL_INTERVAL", "TASKFORGE_OUTBOX_POLL_INTERVAL"),
		})
	}
	return rows
}

// floorTo truncates x toward zero at the given number of decimals. A figure
// printed beside a target is never rounded up to meet it.
func floorTo(x float64, decimals int) float64 {
	scale := math.Pow(10, float64(decimals))
	return math.Floor(x*scale) / scale
}

// fmtDur renders a duration for a table: milliseconds below a second, seconds
// above, both with enough digits to show the difference from a target.
func fmtDur(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%.1f ms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%.2f s", d.Seconds())
}

// recordPaths names the two files of a record: docs/benchmarks/<date>-<sha>.md
// and .json, with a -tuned suffix on the labelled second run. The date is UTC.
func recordPaths(dir string, when time.Time, shortSHA, profile string) (md, js string) {
	base := fmt.Sprintf("%s-%s", when.UTC().Format("2006-01-02"), shortSHA)
	if profile == profileTuned {
		base += "-tuned"
	}
	return filepath.Join(dir, base+".md"), filepath.Join(dir, base+".json")
}

func gib(b uint64) string { return fmt.Sprintf("%.1f GiB", float64(b)/(1<<30)) }

// renderMarkdown writes the human-readable record. It states what was run, on
// what, with which settings, and what came out, and it links to ADR-0020 for the
// definitions instead of restating them.
func renderMarkdown(r Record) string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }

	short := r.Commit
	if len(short) > 7 {
		short = short[:7]
	}
	label := "shipped defaults"
	title := fmt.Sprintf("Benchmark record %s, %s", r.RecordedAt.UTC().Format("2006-01-02"), short)
	if r.Profile == profileTuned {
		label = "TUNED"
		title += ", tuned profile"
	}

	w("# %s", title)
	w("")
	if r.Profile == profileTuned {
		w("> **This is the TUNED profile, not the shipped defaults.** It is the one labelled second run: a shorter lease,")
		w("> tighter liveness windows and faster scans. It shows what changing those settings buys. It never replaces")
		w("> the headline run, which uses the shipped defaults and is recorded in its own file.")
	} else {
		w("> This is the headline run: the **shipped defaults**, unchanged. A target that is missed here is recorded as")
		w("> missed, next to the settings that caused it. Nothing was re-run to improve it.")
	}
	w("")
	w("Times are UTC. Every figure below is measured on PostgreSQL's clock, not this machine's; the definitions are")
	w("in [ADR-0020](../adr/0020-benchmark-methodology.md).")
	w("")

	w("## What was run")
	w("")
	w("- **Commit:** `%s`, working tree clean at start: %s.", r.Commit, yesNo(r.TreeClean))
	w("- **Binaries:** every binary in `bin/` was stamped by `go build` with that revision and an unmodified tree: %s.", yesNo(r.BinariesChecked))
	w("- **Command:** `%s`", r.Command)
	w("- **Profile:** %s", label)
	w("- **Fault-schedule seed:** %d", r.Seed)
	w("- **Recorded:** %s", r.RecordedAt.UTC().Format(time.RFC3339))
	w("")

	e := r.Environment
	w("## Environment")
	w("")
	w("| | |")
	w("| --- | --- |")
	w("| CPU | %s |", e.CPUModel)
	w("| Cores | %d logical%s |", e.LogicalCores, physical(e.PhysicalCores))
	w("| Memory | %s |", gib(e.MemoryBytes))
	w("| OS | %s |", e.OS)
	w("| Power | %s |", e.PowerSource)
	w("| Low Power Mode | %s |", e.LowPowerMode)
	w("| Go | %s |", e.GoVersion)
	w("| Docker | client %s, server %s |", e.DockerClient, e.DockerServer)
	if e.DockerVMCPUs > 0 {
		w("| Docker VM | %d CPUs, %s |", e.DockerVMCPUs, gib(uint64(e.DockerVMMemory)))
	}
	w("| PostgreSQL server | %s |", e.PostgresVersion)
	w("| Compose images | %s |", strings.Join(e.ComposeImages, ", "))
	w("")

	w("## Settings in effect")
	w("")
	w("Every `TASKFORGE_*` timing value the services and workers were started with. Anything not listed is the")
	w("binary's own default.")
	w("")
	w("| Setting | Value |")
	w("| --- | --- |")
	names := make([]string, 0, len(r.Timing))
	for n := range r.Timing {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		w("| `%s` | %s |", n, r.Timing[n])
	}
	w("")

	w("## Results against PROJECT_SPEC §7")
	w("")
	w("| Target | Value | Measured | Verdict | Settings that govern it |")
	w("| --- | --- | --- | --- | --- |")
	for _, row := range r.Targets {
		verdict := row.Verdict
		if verdict == Missed.String() {
			verdict = "**" + verdict + "**"
		}
		var gov []string
		for _, s := range row.GovernedBy {
			gov = append(gov, fmt.Sprintf("`%s`=%s", s.Name, s.Value))
		}
		w("| %s | %s | %s | %s | %s |", row.Name, row.Target, row.Measured, verdict, strings.Join(gov, ", "))
	}
	w("")

	if t := r.Throughput; t != nil {
		w("## Throughput run")
		w("")
		w("- Workload: `%s` for %d ms, `max_attempts=%d`, `timeout_seconds=%d`; %d workers x %d slots.",
			jobType, jobDurationMS, jobMaxAttempts, jobTimeoutSecs, t.Workers, t.Concurrency)
		w("- Durations (PostgreSQL time): warm-up %s, steady window %s, drain %s.",
			durS(t.Warmup), durS(t.Steady), durS(t.Drain))
		w("- Window: %s to %s (half-open).", t.WindowStart.UTC().Format(time.RFC3339Nano), t.WindowEnd.UTC().Format(time.RFC3339Nano))
		w("- Offered load: %.2f jobs/min actual (target %.0f), %d jobs created in the window; pacer's worst lateness %.1f ms.",
			floorTo(t.OfferedPerMin, 2), t.TargetRate, t.JobsInWindow, t.PacerMaxLateMS)
		w("- Submitted %d, submit errors %d, retries %d.", t.Submitted, t.SubmitErrors, t.SubmitRetries)
		w("- Throughput: %d jobs reached SUCCEEDED inside the window = %.2f jobs/min.", t.CompletedInWindow, floorTo(t.ThroughputPerMin, 2))
		w("- Dispatch latency (attempt 1's claim minus the job's submission), n=%d: p50 %s, p95 %s, p99 %s, max %s; %d in-window jobs never claimed.",
			t.Dispatch.N, fmtDur(t.Dispatch.P50), fmtDur(t.Dispatch.P95), fmtDur(t.Dispatch.P99), fmtDur(t.Dispatch.Max), t.Unclaimed)
		w("- Supplementary, not the defined figure: submission to lease issuance, n=%d: p50 %s, p95 %s, p99 %s, max %s. It is later than the claim by whatever the claim transaction spent before sampling the clock.",
			t.DispatchLease.N, fmtDur(t.DispatchLease.P50), fmtDur(t.DispatchLease.P95), fmtDur(t.DispatchLease.P99), fmtDur(t.DispatchLease.Max))
		w("- Final statuses: %s. Not terminal when the drain ended: %d.", counts(t.FinalStatuses), t.NotTerminalAtDrainEnd)
		w("- Clock check: the window is %s on this machine's monotonic clock and PostgreSQL's differs by %.1f ms. Valid: %s.",
			durS(t.Steady), t.ClockDivergenceMS, yesNo(t.Valid))
		for _, p := range t.Problems {
			w("- **Problem:** %s", p)
		}
		w("")
	}

	if f := r.Faults; f != nil {
		w("## Fault-injection run")
		w("")
		w("- %d jobs offered at %.0f/min over %s; %d accepted by the API, %d in the database; submit errors %d, retries %d.",
			f.JobsTarget, f.TargetRate, durS(time.Duration(f.SubmissionSeconds*float64(time.Second))), f.JobsSubmitted, f.JobsInDatabase, f.SubmitErrors, f.SubmitRetries)
		w("- Seed %d; %d kills of %d scheduled. Each is a SIGKILL of a random running worker that was holding an attempt when one could be found, followed by a restart under the same worker name.", f.Seed, f.KillsDone, f.KillsScheduled)
		w("- Completion deadline: %s after the last submission, ended by: %s. At the end %d of %d jobs were SUCCEEDED (%.3f%%).",
			time.Duration(f.CompletionDeadlineSeconds*float64(time.Second)), f.EndedBy, f.Succeeded, f.JobsInDatabase, floorTo(f.CompletionPercent, 3))
		w("- Every final status: %s.", counts(f.FinalStatuses))
		w("- Dead-letter reasons: %s.", counts(f.DLQReasons))
		w("- Abandoned attempts: %d total, %d never replaced. Recovery (replacement's claim minus the kill), n=%d: p50 %s, p95 %s, max %s.",
			f.Affected, f.Unrecovered, f.Recovery.N, fmtDur(f.Recovery.P50), fmtDur(f.Recovery.P95), fmtDur(f.Recovery.Max))
		for _, p := range f.Problems {
			w("- **Problem:** %s", p)
		}
		w("")
		w("### Kill timeline")
		w("")
		w("| # | Offset | Killed at (PostgreSQL) | Worker | Held when chosen | Abandoned | Never replaced | Recovery | Restart |")
		w("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
		for _, k := range f.Kills {
			recovery := "none"
			if len(k.RecoverySeconds) > 0 {
				parts := make([]string, len(k.RecoverySeconds))
				for i, s := range k.RecoverySeconds {
					parts[i] = fmt.Sprintf("%.2f s", s)
				}
				recovery = strings.Join(parts, ", ")
			}
			restart := fmt.Sprintf("%.0f ms", k.RestartMS)
			if k.RestartError != "" {
				restart = "FAILED: " + k.RestartError
			}
			w("| %d | %.1f s | %s | %s | %d | %d | %d | %s | %s |", k.Index, k.ScheduledOffset,
				k.KilledAt.UTC().Format("15:04:05.000"), k.Worker, k.HeldAtSelection, k.Affected, k.Unrecovered, recovery, restart)
		}
		w("")
	}

	w("## Limitations")
	w("")
	w("- **Machine:** this is one machine, a laptop, with %d logical cores shared by the load generator, five services, up to 12 workers and Docker.", e.LogicalCores)
	w("- **Broker:** the broker is ElasticMQ, an SQS-compatible emulator, not Amazon SQS. Its latency and its visibility behavior are not SQS's.")
	w("- **Database:** PostgreSQL runs in local Docker, in a Linux VM with %d CPUs and %s, with its data directory on tmpfs (`compose.yaml`). There is no disk or network between the services and the database that a deployment would have.", e.DockerVMCPUs, gib(uint64(e.DockerVMMemory)))
	w("- **Run count:** this is a single run, not a distribution. There is no variance estimate and no confidence interval, and one run cannot say how much of a figure is the system and how much is the day.")
	w("- **Load generator:** the load generator and the system under test share a host, so the generator's own work competes with the system's for CPU.")
	w("- **Workload:** one handler, `demo.sleep` for 50 ms. It is not representative of a real job mix, and no saturation point was searched for.")
	w("- **Definitions:** dispatch latency reads `job_attempts.created_at`, which is PostgreSQL's `now()` and so the start of the claim transaction. A lock wait inside that transaction is not in it; the supplementary lease-issuance figure above is. Recovery is measured for the attempts a kill happened to hit, and kills prefer a worker that is holding an attempt, so it is the recovery of an occupied worker, not of a random one.")
	// The last line is the last Limitations bullet, and w ends it with one "\n".
	// Nothing follows it: a trailing w("") would end the file in a blank line, which
	// every committed record had to have trimmed by hand.
	w("- **Power and sleep:** a laptop on battery, or with Low Power Mode on, is throttled, and it sleeps. The run's PostgreSQL clock is checked against this process's monotonic clock every five seconds to catch a sleep or a stepped Docker VM clock, and the run aborts if they disagree, but throttling is not detected. The power source and Low Power Mode above are what the machine reported.")
	return b.String()
}

func yesNo(v bool) string {
	if v {
		return "yes"
	}
	return "NO"
}

func physical(n int) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf(", %d physical", n)
}

func durS(d time.Duration) string { return fmt.Sprintf("%.1f s", d.Seconds()) }

func counts(m map[string]int) string {
	if len(m) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s %d", k, m[k])
	}
	return strings.Join(parts, ", ")
}
