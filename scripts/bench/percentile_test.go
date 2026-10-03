package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

// series returns 1ms, 2ms, ... n ms: the value at rank r is r milliseconds, so a
// percentile's rank can be read straight off its result.
func series(n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = ms(i + 1)
	}
	return out
}

func TestNearestRank_OneValueIsEveryPercentile(t *testing.T) {
	for _, p := range []int{1, 50, 95, 99, 100} {
		require.Equal(t, ms(7), NearestRank([]time.Duration{ms(7)}, p), "p%d of one value", p)
	}
}

func TestNearestRank_TiesReturnTheTiedValue(t *testing.T) {
	sorted := []time.Duration{ms(5), ms(5), ms(5), ms(5), ms(900)}
	require.Equal(t, ms(5), NearestRank(sorted, 50))
	require.Equal(t, ms(5), NearestRank(sorted, 80), "rank 4 of 5 is still a 5")
	require.Equal(t, ms(900), NearestRank(sorted, 81), "rank ceil(4.05) = 5 is the 900")
	require.Equal(t, ms(900), NearestRank(sorted, 99))
}

// Nearest rank returns a value that was observed. For an even count it does not
// average the two middle values, which is the difference from linear
// interpolation and the reason this method is documented rather than assumed.
func TestNearestRank_EvenAndOddCountsReturnAnObservedMiddleValue(t *testing.T) {
	require.Equal(t, ms(2), NearestRank(series(4), 50), "n=4: rank ceil(2.0) = 2, not the 2.5 an interpolation gives")
	require.Equal(t, ms(3), NearestRank(series(5), 50), "n=5: rank ceil(2.5) = 3")
	require.Equal(t, ms(3), NearestRank(series(6), 50), "n=6: rank 3")
	require.Equal(t, ms(4), NearestRank(series(7), 50), "n=7: rank ceil(3.5) = 4")
}

// The exact p95 rank. The hand-computed ranks include counts where a
// floating-point ceil(0.95 x n) lands one rank too high (n = 60 gives 58 for a
// true rank of 57; so do 120, 220 and 500), which is where the answer to "which
// observation is the p95" quietly changes.
func TestNearestRank_P95RankIsExact(t *testing.T) {
	for n, rank := range map[int]int{
		1: 1, 2: 2, 3: 3, 10: 10, 19: 19,
		20: 19, 21: 20, 40: 38, 60: 57, 80: 76,
		100: 95, 101: 96, 120: 114, 200: 190, 220: 209, 500: 475, 1000: 950, 5000: 4750,
	} {
		require.Equalf(t, ms(rank), NearestRank(series(n), 95), "p95 of %d values is rank %d", n, rank)
	}
}

func TestNearestRank_P99AndP100(t *testing.T) {
	require.Equal(t, ms(99), NearestRank(series(100), 99))
	require.Equal(t, ms(100), NearestRank(series(101), 99), "rank ceil(99.99) = 100")
	require.Equal(t, ms(101), NearestRank(series(101), 100), "p100 is the maximum")
}

func TestSummarize_ReportsEveryFigureFromAnUnsortedInputWithoutMovingIt(t *testing.T) {
	in := []time.Duration{ms(30), ms(10), ms(20), ms(40)}
	got := Summarize(in)

	require.Equal(t, Summary{N: 4, P50: ms(20), P95: ms(40), P99: ms(40), Max: ms(40)}, got)
	require.Equal(t, []time.Duration{ms(30), ms(10), ms(20), ms(40)}, in, "the caller's slice is left as it was")
}

func TestSummarize_NoValuesIsAnEmptySummaryNotAZeroLatency(t *testing.T) {
	got := Summarize(nil)
	require.Zero(t, got.N)
	require.False(t, got.Valid(), "n=0 must be distinguishable from a measured zero")
	require.True(t, Summarize([]time.Duration{0}).Valid(), "a measured zero is valid")
}
