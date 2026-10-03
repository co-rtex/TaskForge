package main

import "time"

// The PROJECT_SPEC section 7 targets, and the rules that turn a measurement into
// Met or MISSED. The rules are fixed here, before any run, and ADR-0020 states
// them; none was chosen after seeing a number.
const (
	// TargetThroughputPerMin is "1,000 jobs/minute across 12 workers".
	TargetThroughputPerMin = 1000.0
	// throughputTolerance is the one tolerance in the rules. A paced run offers
	// the target rate to within one submission and completes it to within the
	// jobs in flight at each end of the window, so a measured value of 999.8 says
	// nothing about the system. The tolerance is 1%, one-sided (a rate above the
	// target is never excused), applies to throughput alone, and the unrounded
	// value is always printed beside the verdict.
	throughputTolerance = 0.01
	// TargetDispatchP95 is "p95 < 500 ms".
	TargetDispatchP95 = 500 * time.Millisecond
	// TargetFaultJobs is "10,000 jobs".
	TargetFaultJobs = 10000
	// TargetCompletionPermille is "at least 99.7%", as parts per thousand so the
	// comparison is in integers.
	TargetCompletionPermille = 997
	// TargetRecovery is "< 30 s".
	TargetRecovery = 30 * time.Second
)

// Verdict is the outcome of comparing one measurement with one target.
type Verdict int

const (
	// Met: measured, and the target holds.
	Met Verdict = iota + 1
	// Missed: measured, and the target does not hold, or the measurement left out
	// something that would have made it worse.
	Missed
	// NotMeasured: nothing was observed to compare, which is neither.
	NotMeasured
)

func (v Verdict) String() string {
	switch v {
	case Met:
		return "Met"
	case Missed:
		return "MISSED"
	default:
		return "NOT MEASURED"
	}
}

// JudgeThroughput compares the measured completions per minute with the target.
// It is Met only if the run offered the target load (to within the tolerance),
// completed it (to within the tolerance), and every submission was accepted. A
// system cannot be shown to sustain a load it was never offered.
func JudgeThroughput(measured, offered float64, submitErrors int) Verdict {
	floor := TargetThroughputPerMin - TargetThroughputPerMin*throughputTolerance
	switch {
	case submitErrors > 0, offered < floor, measured < floor:
		return Missed
	default:
		return Met
	}
}

// JudgeDispatch is Met only if the p95 is strictly under the target and no job
// in the window was left unclaimed. An unclaimed job has no latency yet, and
// leaving it out would drop the slowest observation of all.
func JudgeDispatch(s Summary, unclaimed int) Verdict {
	switch {
	case unclaimed > 0:
		return Missed
	case !s.Valid():
		return NotMeasured
	case s.P95 < TargetDispatchP95:
		return Met
	default:
		return Missed
	}
}

// JudgeFaultVolume is Met if at least the target number of jobs were submitted.
func JudgeFaultVolume(submitted int) Verdict {
	if submitted >= TargetFaultJobs {
		return Met
	}
	return Missed
}

// JudgeCompletion is Met if succeeded is at least 99.7% of total, in integers.
func JudgeCompletion(succeeded, total int) Verdict {
	switch {
	case total == 0:
		return NotMeasured
	case succeeded*1000 >= total*TargetCompletionPermille:
		return Met
	default:
		return Missed
	}
}

// JudgeRecovery is Met only if the worst recovery observed is strictly under the
// target and every abandoned attempt was replaced. It reads the maximum and not
// a percentile: a target that says "recovery < 30 s" is not met by a median that
// is, while one attempt took longer. With nothing abandoned there is nothing to
// compare, which is NotMeasured and not Met.
func JudgeRecovery(s Summary, unrecovered int) Verdict {
	switch {
	case unrecovered > 0:
		return Missed
	case !s.Valid():
		return NotMeasured
	case s.Max < TargetRecovery:
		return Met
	default:
		return Missed
	}
}
