package main

import (
	"slices"
	"time"
)

// Summary is the distribution of a set of durations: how many there were, and
// four order statistics.
type Summary struct {
	N   int
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
	Max time.Duration
}

// Valid reports whether anything was measured. A summary of zero values and a
// summary of nothing are different facts, and a verdict must not read the second
// as the first: a latency of zero would pass any target.
func (s Summary) Valid() bool { return s.N > 0 }

// NearestRank returns the percent-th percentile of sorted, which must be in
// ascending order, by the nearest-rank method: the value at 1-based rank
// ceil(percent/100 x n). It is the one method this harness uses, for every
// percentile it reports (ADR-0020).
//
// It returns a value that was observed. For an even count the median is the
// lower of the two middle values, not their average, and the p95 of 20 values is
// the 19th, not something between the 19th and the 20th. That is the choice that
// never reports a latency nobody saw.
//
// The rank is computed in integers. ceil(0.95 x n) in floating point lands one
// rank too high for some n where 0.95 x n is a whole number (n = 60 gives 58, and
// the true rank is 57), so the p95 of those sets would be a different
// observation.
func NearestRank(sorted []time.Duration, percent int) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	rank := (percent*n + 99) / 100
	rank = max(1, min(rank, n))
	return sorted[rank-1]
}

// Summarize sorts a copy of values, leaving the caller's slice as it was, and
// reports n, p50, p95, p99 and the maximum.
func Summarize(values []time.Duration) Summary {
	if len(values) == 0 {
		return Summary{}
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	return Summary{
		N:   len(sorted),
		P50: NearestRank(sorted, 50),
		P95: NearestRank(sorted, 95),
		P99: NearestRank(sorted, 99),
		Max: sorted[len(sorted)-1],
	}
}
