package main

import (
	"fmt"
	"time"

	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// maxClockDivergence is how far the steady window, measured in PostgreSQL time,
// may differ from the same window measured by this process's monotonic clock
// before the run is called invalid. Both measure the same 300 seconds, so they
// agree to a few milliseconds when nothing is wrong. They stop agreeing when the
// machine sleeps (a monotonic clock does not tick through sleep and the
// database's wall clock does), when a process is suspended, or when the clock is
// stepped. Two seconds is far above scheduling noise and far below any of those.
const maxClockDivergence = 2 * time.Second

// throughputInputs is everything the throughput figures are computed from. The
// time-valued ones are PostgreSQL instants and durations of PostgreSQL time,
// except monotonicWindow, which exists only to be compared with them.
type throughputInputs struct {
	profile      string
	workers      int
	concurrency  int
	targetRate   float64
	warmup       time.Duration
	drain        time.Duration
	window       Window
	pacer        PaceStats
	submitted    int
	submitErrors int
	retries      int

	dispatches  []readdb.Dispatch // every immediate job created in the window
	finishes    []time.Time       // every SUCCEEDED job of the run, by finish time
	statuses    map[string]int
	dlq         map[string]int
	nonTerminal int // jobs still not terminal when the drain ended

	monotonicWindow time.Duration

	// observed are problems the runner saw while the run was going, such as a
	// worker that exited on its own. Any one makes the run invalid.
	observed []string
}

// ThroughputResult is the throughput run's figures. It is also the JSON the
// summary records.
type ThroughputResult struct {
	Profile     string  `json:"profile"`
	Workers     int     `json:"workers"`
	Concurrency int     `json:"concurrency_per_worker"`
	TargetRate  float64 `json:"target_jobs_per_minute"`

	WarmupSeconds float64       `json:"warmup_seconds"`
	SteadySeconds float64       `json:"steady_window_seconds"`
	DrainSeconds  float64       `json:"drain_seconds"`
	WindowStart   time.Time     `json:"window_start"`
	WindowEnd     time.Time     `json:"window_end"`
	Warmup        time.Duration `json:"-"`
	Steady        time.Duration `json:"-"`
	Drain         time.Duration `json:"-"`

	Submitted      int     `json:"jobs_submitted"`
	SubmitErrors   int     `json:"submit_errors"`
	SubmitRetries  int     `json:"submit_retries"`
	PacerMaxLateMS float64 `json:"pacer_max_lateness_ms"`

	JobsInWindow      int     `json:"jobs_created_in_window"`
	OfferedPerMin     float64 `json:"offered_jobs_per_minute"`
	CompletedInWindow int     `json:"jobs_succeeded_in_window"`
	ThroughputPerMin  float64 `json:"throughput_jobs_per_minute"`

	Dispatch        Summary   `json:"-"`
	DispatchLease   Summary   `json:"-"`
	DispatchMS      summaryMS `json:"dispatch_latency_ms"`
	DispatchLeaseMS summaryMS `json:"dispatch_latency_to_lease_ms"`
	Unclaimed       int       `json:"jobs_in_window_never_claimed"`
	NegativeLatency int       `json:"claims_before_submission"`

	FinalStatuses         map[string]int `json:"final_statuses"`
	DLQReasons            map[string]int `json:"dlq_reasons"`
	NotTerminalAtDrainEnd int            `json:"jobs_not_terminal_at_drain_end"`

	ClockDivergence   time.Duration `json:"-"`
	ClockDivergenceMS float64       `json:"clock_divergence_ms"`
	Valid             bool          `json:"valid"`
	Problems          []string      `json:"problems"`
}

// analyzeThroughput turns a run's rows into its figures. It is a pure function of
// its inputs, which is how it is tested against data whose answers are known.
func analyzeThroughput(in throughputInputs) ThroughputResult {
	steady := in.window.Duration()
	res := ThroughputResult{
		Profile: in.profile, Workers: in.workers, Concurrency: in.concurrency, TargetRate: in.targetRate,
		Warmup: in.warmup, Steady: steady, Drain: in.drain,
		WarmupSeconds: in.warmup.Seconds(), SteadySeconds: steady.Seconds(), DrainSeconds: in.drain.Seconds(),
		WindowStart: in.window.Start, WindowEnd: in.window.End,
		Submitted: in.submitted, SubmitErrors: in.submitErrors, SubmitRetries: in.retries,
		PacerMaxLateMS: ms64(in.pacer.MaxLate),
		JobsInWindow:   len(in.dispatches),
		OfferedPerMin:  PerMinute(len(in.dispatches), steady),
		FinalStatuses:  in.statuses, DLQReasons: in.dlq, NotTerminalAtDrainEnd: in.nonTerminal,
		Valid: true, Problems: []string{},
	}

	// Throughput: this run's jobs that reached SUCCEEDED inside the window, by the
	// finish time PostgreSQL recorded, per minute of PostgreSQL time.
	res.CompletedInWindow = CountIn(in.window, in.finishes)
	res.ThroughputPerMin = PerMinute(res.CompletedInWindow, steady)

	// Dispatch latency: every job created in the window. An unclaimed one has no
	// latency yet; it is counted, never dropped.
	var latencies, leaseLatencies []time.Duration
	for _, d := range in.dispatches {
		latency, claimed := d.Latency()
		if !claimed {
			res.Unclaimed++
			continue
		}
		if latency < 0 {
			res.NegativeLatency++
		}
		latencies = append(latencies, latency)
		if lease, ok := d.LeaseLatency(); ok {
			leaseLatencies = append(leaseLatencies, lease)
		}
	}
	res.Dispatch, res.DispatchLease = Summarize(latencies), Summarize(leaseLatencies)
	res.DispatchMS, res.DispatchLeaseMS = toMS(res.Dispatch), toMS(res.DispatchLease)

	problem := func(format string, args ...any) {
		res.Valid = false
		res.Problems = append(res.Problems, fmt.Sprintf(format, args...))
	}
	for _, o := range in.observed {
		problem("%s", o)
	}
	if res.JobsInWindow == 0 {
		problem("no job was created inside the steady window, so there is nothing to measure")
	}
	if res.NegativeLatency > 0 {
		problem("%d job(s) were claimed before they were submitted according to the database; the clocks or the definition are wrong", res.NegativeLatency)
	}
	res.ClockDivergence = in.monotonicWindow - steady
	if res.ClockDivergence < 0 {
		res.ClockDivergence = -res.ClockDivergence
	}
	res.ClockDivergenceMS = ms64(res.ClockDivergence)
	if res.ClockDivergence > maxClockDivergence {
		problem("the steady window was %s in PostgreSQL time and %s on this machine's monotonic clock; "+
			"the machine probably slept or a process was suspended during the run", steady.Round(time.Millisecond), in.monotonicWindow.Round(time.Millisecond))
	}
	return res
}

// summaryMS is a Summary in milliseconds, for the JSON.
type summaryMS struct {
	N   int     `json:"n"`
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

func toMS(s Summary) summaryMS {
	return summaryMS{N: s.N, P50: ms64(s.P50), P95: ms64(s.P95), P99: ms64(s.P99), Max: ms64(s.Max)}
}

func ms64(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// Recoveries is what one kill's abandoned attempts came to.
type Recoveries struct {
	// Durations are the recovery times of the attempts that were replaced.
	Durations []time.Duration
	// Affected is every attempt the kill left ABANDONED.
	Affected int
	// Unrecovered is how many of those have no replacement attempt.
	Unrecovered int
	Problems    []string
}

// recoveryTimes computes, for one kill, each affected attempt's recovery: the
// replacement attempt's claim minus the kill, both PostgreSQL instants. An
// attempt with no replacement is counted as unrecovered and not left out, and a
// replacement claimed before the kill is a problem and not a negative number.
func recoveryTimes(kill time.Time, abandoned []readdb.Recovery) Recoveries {
	var out Recoveries
	for _, r := range abandoned {
		out.Affected++
		after, replaced := r.After(kill)
		switch {
		case !replaced:
			out.Unrecovered++
		case after < 0:
			out.Problems = append(out.Problems,
				fmt.Sprintf("job %s: attempt %d's replacement was claimed %s before the kill", r.JobID, r.AbandonedAttempt, -after))
		default:
			out.Durations = append(out.Durations, after)
		}
	}
	return out
}
