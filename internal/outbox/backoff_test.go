package outbox

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func policy() BackoffPolicy {
	return BackoffPolicy{Base: time.Second, Max: time.Minute, Multiplier: 2, Jitter: 0.2}
}

// A nil random source disables jitter, which lets this assert exact growth.
func TestBackoff_GrowsExponentiallyWithoutJitter(t *testing.T) {
	p := policy()
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second}
	for i, w := range want {
		require.Equal(t, w, p.Delay(i+1, nil), "attempt %d", i+1)
	}
}

func TestBackoff_IsCappedAtMax(t *testing.T) {
	p := policy()
	require.Equal(t, time.Minute, p.Delay(7, nil))
	require.Equal(t, time.Minute, p.Delay(100, nil))
}

// math.Pow overflows to +Inf well before this; converting +Inf to a Duration is
// undefined, so the cap must be applied before the conversion.
func TestBackoff_HandlesEnormousAttemptCountsWithoutOverflow(t *testing.T) {
	p := policy()
	for _, attempts := range []int{1000, 100000, 1 << 30} {
		got := p.Delay(attempts, nil)
		require.Equal(t, time.Minute, got, "attempts=%d", attempts)
		require.Positive(t, got)
	}
}

func TestBackoff_JitterStaysWithinBounds(t *testing.T) {
	p := policy()
	rnd := rand.New(rand.NewSource(1))
	for attempts := 1; attempts <= 5; attempts++ {
		unjittered := p.Delay(attempts, nil)
		lo := time.Duration(float64(unjittered) * (1 - p.Jitter))
		hi := time.Duration(float64(unjittered) * (1 + p.Jitter))
		for i := 0; i < 500; i++ {
			got := p.Delay(attempts, rnd)
			require.GreaterOrEqual(t, got, lo, "attempt %d below jitter floor", attempts)
			require.LessOrEqual(t, got, hi, "attempt %d above jitter ceiling", attempts)
			require.LessOrEqual(t, got, p.Max, "jitter must never exceed Max")
		}
	}
}

func TestBackoff_IsDeterministicForASeed(t *testing.T) {
	p := policy()
	run := func() []time.Duration {
		rnd := rand.New(rand.NewSource(42))
		out := make([]time.Duration, 10)
		for i := range out {
			out[i] = p.Delay(i+1, rnd)
		}
		return out
	}
	require.Equal(t, run(), run())
}

func TestBackoff_ProducesVariationAcrossCalls(t *testing.T) {
	p := policy()
	rnd := rand.New(rand.NewSource(7))
	seen := map[time.Duration]struct{}{}
	for i := 0; i < 100; i++ {
		seen[p.Delay(3, rnd)] = struct{}{}
	}
	// Jittered delays must not collapse to one value, or a recovering fleet
	// would still stampede.
	require.Greater(t, len(seen), 50)
}

func TestBackoff_ToleratesDegenerateConfiguration(t *testing.T) {
	var zero BackoffPolicy
	require.Positive(t, zero.Delay(1, nil), "an unset policy must still produce a usable delay")

	inverted := BackoffPolicy{Base: time.Minute, Max: time.Second, Multiplier: 0.5}
	got := inverted.Delay(3, nil)
	require.Equal(t, time.Minute, got, "Max below Base is clamped up to Base, not down to zero")

	require.Equal(t, time.Second, BackoffPolicy{Base: time.Second, Max: time.Minute, Multiplier: 2}.Delay(0, nil),
		"attempt counts below 1 are treated as the first attempt")
}
