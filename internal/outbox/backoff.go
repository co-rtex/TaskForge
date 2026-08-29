package outbox

import (
	"math"
	"math/rand"
	"time"
)

// BackoffPolicy describes bounded exponential backoff with jitter.
type BackoffPolicy struct {
	Base       time.Duration
	Max        time.Duration
	Multiplier float64
	// Jitter is a fraction in [0,1]. The computed delay is scaled by a random
	// factor in [1-Jitter, 1+Jitter], which spreads retries so that a fleet of
	// publishers recovering from the same broker outage does not stampede.
	Jitter float64
}

// Delay returns how long to wait before retrying after the given number of
// attempts (1 means the first attempt just failed).
//
// The random source is a parameter rather than a package-level default so that
// tests are deterministic. A nil source disables jitter, which is what unit
// tests asserting exact growth want.
func (p BackoffPolicy) Delay(attempts int, rnd *rand.Rand) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	base := p.Base
	if base <= 0 {
		base = time.Second
	}
	mult := p.Multiplier
	if mult < 1 {
		mult = 1
	}
	max := p.Max
	if max < base {
		max = base
	}

	// math.Pow on a large exponent overflows to +Inf; converting that to a
	// Duration is undefined, so cap before the conversion rather than after.
	growth := math.Pow(mult, float64(attempts-1))
	delayF := float64(base) * growth
	if math.IsInf(delayF, 0) || delayF > float64(max) {
		delayF = float64(max)
	}

	if rnd != nil && p.Jitter > 0 {
		j := p.Jitter
		if j > 1 {
			j = 1
		}
		// factor in [1-j, 1+j]
		factor := 1 + j*(2*rnd.Float64()-1)
		delayF *= factor
	}

	if delayF < 0 {
		delayF = 0
	}
	if delayF > float64(max) {
		delayF = float64(max)
	}
	return time.Duration(delayF)
}
