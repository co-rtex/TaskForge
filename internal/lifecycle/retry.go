package lifecycle

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	mathrand "math/rand/v2"
	"sync"
	"time"
)

// JitterSource yields a value in [0, 1).
//
// It is an interface rather than a *rand.Rand for two reasons. Unit tests need
// a seeded, reproducible sequence so an asserted delay is a fact rather than a
// range. Production needs the opposite: every API and reconciler replica must
// draw from an independently seeded source, or a fleet recovering from the same
// dependency outage would compute the same retry instants and stampede.
//
// Implementations must be safe for concurrent use: one API process computes
// retry decisions on many request goroutines at once.
type JitterSource interface {
	Float64() float64
}

// lockedJitter guards a math/rand/v2 generator with a mutex.
//
// math/rand/v2's top-level functions are already goroutine-safe, but they are
// also unseedable, which would make every test non-deterministic. An explicit
// generator plus an explicit lock keeps one implementation for both uses.
type lockedJitter struct {
	mu  sync.Mutex
	rnd *mathrand.Rand
}

func (j *lockedJitter) Float64() float64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.rnd.Float64()
}

// NewSeededJitter returns a deterministic source. Tests use it; production must
// not, because two replicas started from the same seed would synchronize their
// retries exactly.
func NewSeededJitter(seed uint64) JitterSource {
	return &lockedJitter{rnd: mathrand.New(mathrand.NewPCG(seed, seed^0x9E3779B97F4A7C15))}
}

// NewCryptoSeededJitter returns a source seeded from the operating system's
// entropy, so independently started replicas never share a retry schedule.
//
// It returns an error rather than silently falling back to a time-based seed:
// two processes started in the same instant would otherwise be seeded
// identically, which is precisely the failure this exists to prevent.
func NewCryptoSeededJitter() (JitterSource, error) {
	var seed [16]byte
	if _, err := cryptorand.Read(seed[:]); err != nil {
		return nil, fmt.Errorf("seed jitter source from system entropy: %w", err)
	}
	return &lockedJitter{rnd: mathrand.New(mathrand.NewPCG(
		binary.LittleEndian.Uint64(seed[0:8]),
		binary.LittleEndian.Uint64(seed[8:16]),
	))}, nil
}

// RetryPolicy is bounded exponential backoff with proportional jitter, shared
// by worker-reported failures and reconciler-detected timeouts. One policy for
// both is deliberate: a job must not learn a different retry cadence depending
// on whether its worker managed to report the failure or simply stopped.
type RetryPolicy struct {
	Base       time.Duration
	Max        time.Duration
	Multiplier float64
	// Jitter is a fraction in [0, 1]. The nominal delay is scaled by a random
	// factor in [1-Jitter, 1+Jitter] and then clamped back into [0, Max].
	Jitter float64
}

// Validate reports every unusable setting at once.
//
// The finiteness checks are separate from the range checks, and they have to
// be: NaN compares false against everything, so `p.Multiplier < 1` and
// `p.Jitter < 0 || p.Jitter > 1` both accept NaN, and `+Inf` sails through the
// multiplier's lower bound. A policy carrying either would validate cleanly and
// then compute a delay of NaN, which converts to a nonsensical Duration.
func (p RetryPolicy) Validate() error {
	var problems []string
	if p.Base <= 0 {
		problems = append(problems, "base delay must be positive")
	}
	if p.Max < p.Base {
		problems = append(problems, "maximum delay must be at least the base delay")
	}
	if !isFinite(p.Multiplier) {
		problems = append(problems, "multiplier must be a finite number")
	} else if p.Multiplier < 1 {
		problems = append(problems, "multiplier must be at least 1")
	}
	if !isFinite(p.Jitter) {
		problems = append(problems, "jitter must be a finite number")
	} else if p.Jitter < 0 || p.Jitter > 1 {
		problems = append(problems, "jitter must be between 0 and 1")
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid retry policy: %v", problems)
	}
	return nil
}

// IsFinite reports whether v is a real number rather than NaN or an infinity.
//
// It is exported because configuration loading needs the same test at the
// boundary where a float first enters the process, and two implementations of
// "is this a usable number" would eventually disagree.
func IsFinite(v float64) bool { return isFinite(v) }

func isFinite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// Delay returns how long attempt number n should wait before its replacement
// becomes eligible. n is 1-based and counts total attempts, so the first
// failure of a job waits Base (before jitter).
//
//	nominal = min(Max, Base * Multiplier^(n-1))
//	factor  = 1 + Jitter*(2r - 1)
//	delay   = clamp(nominal * factor, 0, Max)
//
// A nil source disables jitter, which is what a caller asserting exact
// exponential growth wants.
func (p RetryPolicy) Delay(attemptNumber int, jitter JitterSource) time.Duration {
	if attemptNumber < 1 {
		attemptNumber = 1
	}
	base := p.Base
	if base <= 0 {
		base = time.Second
	}
	multiplier := p.Multiplier
	if multiplier < 1 {
		multiplier = 1
	}
	max := p.Max
	if max < base {
		max = base
	}

	// max_attempts allows 100, and a multiplier of 2 raised to 99 is far past
	// what a float64 can hold: math.Pow returns +Inf and converting +Inf to a
	// Duration is undefined. Clamp before the conversion, never after.
	growth := math.Pow(multiplier, float64(attemptNumber-1))
	delay := float64(base) * growth
	if math.IsNaN(delay) || math.IsInf(delay, 0) || delay > float64(max) {
		delay = float64(max)
	}

	// A non-finite fraction is dropped rather than applied. Validate rejects one
	// at startup, but Delay is also reachable from a policy built in code, and a
	// NaN fraction here would silently turn every delay into NaN.
	if jitter != nil && isFinite(p.Jitter) && p.Jitter > 0 {
		fraction := p.Jitter
		if fraction > 1 {
			fraction = 1
		}
		// The sample comes from an injected interface, so it is input rather
		// than something this package controls. A NaN or infinite sample would
		// poison the multiply below and reach the Duration conversion, so it is
		// clamped into the [0, 1) contract instead of trusted.
		sample := jitter.Float64()
		if !isFinite(sample) {
			sample = 0
		}
		if sample < 0 {
			sample = 0
		}
		if sample >= 1 {
			sample = math.Nextafter(1, 0)
		}
		delay *= 1 + fraction*(2*sample-1)
	}

	if delay < 0 {
		delay = 0
	}
	if delay > float64(max) {
		delay = float64(max)
	}
	return time.Duration(delay)
}

// Decision is what one terminal attempt outcome does to its job.
type Decision struct {
	// Retry is true when the job returns to RETRY_WAIT with a durable delay.
	Retry bool
	Delay time.Duration
	// DeadLetterReason is set when Retry is false. Cancellation never reaches
	// here: it produces neither a retry nor a DLQ entry.
	DeadLetterReason DLQReason
}

// Decide resolves one terminal attempt outcome against the attempt budget.
//
// attemptsUsed counts every attempt the job has ever had, including this one
// and including ABANDONED attempts (ADR-0009). maxAttempts counts total
// attempts including the first (PROJECT_SPEC.md section 4).
//
// Cancellation is not a case here on purpose: a canceled attempt is not a
// failure to be retried or dead-lettered, so passing ClassCanceled is a
// programming error and is reported as one.
func (p RetryPolicy) Decide(
	class FailureClass,
	attemptNumber, attemptsUsed, maxAttempts int,
	jitter JitterSource,
) (Decision, error) {
	switch class {
	case ClassPermanent:
		// Deliberately ignores remaining budget: retrying could not change the
		// answer, and burning the budget first would only delay the same result.
		return Decision{DeadLetterReason: ReasonPermanentFailure}, nil
	case ClassRetryable, ClassTimedOut:
		if attemptsUsed < maxAttempts {
			return Decision{Retry: true, Delay: p.Delay(attemptNumber, jitter)}, nil
		}
		return Decision{DeadLetterReason: ReasonAttemptsExhausted}, nil
	case ClassAbandoned:
		// ADR-0009, unchanged by M4: an abandoned attempt consumes the budget,
		// but recovery from it is immediate requeue, not retry. The work was
		// interrupted, not judged — there is no failure to back off from — so
		// the delay is deliberately zero and no jitter is drawn. Only the budget
		// arithmetic is shared, which is exactly the part that must not drift
		// between crash recovery and retry.
		if attemptsUsed < maxAttempts {
			return Decision{Retry: true, Delay: 0}, nil
		}
		return Decision{DeadLetterReason: ReasonAttemptsExhausted}, nil
	default:
		return Decision{}, fmt.Errorf("failure class %q has no retry decision", class)
	}
}
