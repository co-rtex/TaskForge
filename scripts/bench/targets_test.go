package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVerdict_StringsAreTheOnesTheRecordPrints(t *testing.T) {
	require.Equal(t, "Met", Met.String())
	require.Equal(t, "MISSED", Missed.String())
	require.Equal(t, "NOT MEASURED", NotMeasured.String())
}

// Throughput: within the pre-declared 1% tolerance of the 1,000 jobs/minute
// target, and only if the run actually offered that much and no submission
// failed. A system cannot be shown to sustain a load it was never offered.
func TestJudgeThroughput(t *testing.T) {
	require.Equal(t, Met, JudgeThroughput(1000.0, 1000.0, 0))
	require.Equal(t, Met, JudgeThroughput(1003.2, 1000.1, 0))
	require.Equal(t, Met, JudgeThroughput(990.0, 1000.0, 0), "exactly at the tolerance is inside it")
	require.Equal(t, Missed, JudgeThroughput(989.99, 1000.0, 0), "just outside it")
	require.Equal(t, Missed, JudgeThroughput(700, 1000, 0))

	require.Equal(t, Missed, JudgeThroughput(1000, 950, 0), "it completed everything it was offered, but was offered 5% too little")
	require.Equal(t, Missed, JudgeThroughput(1000, 1000, 1), "a submission failed")
}

// Dispatch latency: p95 strictly under 500ms, nothing left unclaimed.
func TestJudgeDispatch(t *testing.T) {
	under := Summary{N: 5000, P95: 499 * time.Millisecond}
	require.Equal(t, Met, JudgeDispatch(under, 0))
	require.Equal(t, Missed, JudgeDispatch(Summary{N: 5000, P95: 500 * time.Millisecond}, 0), "< 500ms is strict")
	require.Equal(t, Missed, JudgeDispatch(under, 1), "one unclaimed job means the figure left out the slowest observation")
	require.Equal(t, NotMeasured, JudgeDispatch(Summary{}, 0))
}

func TestJudgeFaultVolume(t *testing.T) {
	require.Equal(t, Met, JudgeFaultVolume(10000))
	require.Equal(t, Met, JudgeFaultVolume(10001))
	require.Equal(t, Missed, JudgeFaultVolume(9999))
}

// Completion: at least 99.7% of the jobs submitted, in integers. 99.7% of
// 10,000 is exactly 9,970.
func TestJudgeCompletion(t *testing.T) {
	require.Equal(t, Met, JudgeCompletion(10000, 10000))
	require.Equal(t, Met, JudgeCompletion(9970, 10000), "exactly 99.7%")
	require.Equal(t, Missed, JudgeCompletion(9969, 10000))
	require.Equal(t, NotMeasured, JudgeCompletion(0, 0))
}

// Recovery: the worst observed recovery under 30s, every affected attempt
// replaced, and at least one affected attempt to measure. The maximum and not a
// percentile, because "round nothing in the system's favor".
func TestJudgeRecovery(t *testing.T) {
	require.Equal(t, Met, JudgeRecovery(Summary{N: 3, Max: 29*time.Second + 999*time.Millisecond}, 0))
	require.Equal(t, Missed, JudgeRecovery(Summary{N: 3, P50: 12 * time.Second, P95: 20 * time.Second, Max: 30 * time.Second}, 0), "< 30s is strict, and the median does not excuse the worst")
	require.Equal(t, Missed, JudgeRecovery(Summary{N: 3, Max: 5 * time.Second}, 1), "an attempt that was never replaced")
	require.Equal(t, NotMeasured, JudgeRecovery(Summary{}, 0), "no kill hit an attempt: nothing was measured")
	require.Equal(t, Missed, JudgeRecovery(Summary{}, 2), "attempts were abandoned and none replaced")
}
