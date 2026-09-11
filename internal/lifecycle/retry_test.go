package lifecycle

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testPolicy() RetryPolicy {
	return RetryPolicy{Base: time.Second, Max: time.Minute, Multiplier: 2, Jitter: 0.2}
}

func TestRetryPolicy_GrowsExponentiallyAndIsCapped(t *testing.T) {
	policy := testPolicy()

	// No jitter source: the assertion is about growth, not about spread.
	for _, tc := range []struct {
		attempt int
		want    time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
		{6, 32 * time.Second},
		{7, time.Minute}, // 64s would exceed Max
		{8, time.Minute},
	} {
		require.Equalf(t, tc.want, policy.Delay(tc.attempt, nil),
			"attempt %d", tc.attempt)
	}
}

// TestRetryPolicy_LargeAttemptNumbersDoNotOverflow pins the bug this ordering
// prevents: math.Pow(2, 99) is +Inf, and converting +Inf to a Duration is
// undefined. Clamping has to happen before the conversion.
func TestRetryPolicy_LargeAttemptNumbersDoNotOverflow(t *testing.T) {
	policy := testPolicy()

	// 100 is max_attempts' own ceiling; the rest are far past anything the
	// schema permits and must still be bounded.
	for _, attempt := range []int{50, 100, 1000, 1 << 20} {
		delay := policy.Delay(attempt, nil)
		require.Equalf(t, time.Minute, delay, "attempt %d must clamp to Max", attempt)
		require.Positivef(t, delay, "attempt %d must not overflow to a negative duration", attempt)
	}
}

func TestRetryPolicy_SeededJitterIsDeterministicAndBounded(t *testing.T) {
	policy := testPolicy()

	first := make([]time.Duration, 0, 8)
	source := NewSeededJitter(42)
	for attempt := 1; attempt <= 8; attempt++ {
		first = append(first, policy.Delay(attempt, source))
	}

	second := make([]time.Duration, 0, 8)
	replay := NewSeededJitter(42)
	for attempt := 1; attempt <= 8; attempt++ {
		second = append(second, policy.Delay(attempt, replay))
	}
	require.Equal(t, first, second, "one seed must produce one sequence")

	// Every value stays inside [nominal*(1-j), nominal*(1+j)] and inside Max.
	for i, delay := range first {
		attempt := i + 1
		nominal := policy.Delay(attempt, nil)
		low := time.Duration(float64(nominal) * 0.8)
		high := time.Duration(float64(nominal) * 1.2)
		if high > policy.Max {
			high = policy.Max
		}
		require.GreaterOrEqualf(t, delay, low, "attempt %d below the jitter band", attempt)
		require.LessOrEqualf(t, delay, high, "attempt %d above the jitter band", attempt)
		require.LessOrEqualf(t, delay, policy.Max, "attempt %d exceeded Max", attempt)
	}
}

func TestRetryPolicy_DifferentSeedsDiverge(t *testing.T) {
	policy := testPolicy()
	a := policy.Delay(4, NewSeededJitter(1))
	b := policy.Delay(4, NewSeededJitter(2))
	require.NotEqual(t, a, b, "distinct seeds must not produce identical delays")
}

// TestCryptoSeededJitter_ReplicasDoNotShareASchedule is the production half of
// the same property: two independently constructed sources must not agree, or a
// fleet recovering from one outage would retry in lockstep.
func TestCryptoSeededJitter_ReplicasDoNotShareASchedule(t *testing.T) {
	policy := testPolicy()

	replicaA, err := NewCryptoSeededJitter()
	require.NoError(t, err)
	replicaB, err := NewCryptoSeededJitter()
	require.NoError(t, err)

	const samples = 16
	var identical int
	for i := 0; i < samples; i++ {
		if policy.Delay(3, replicaA) == policy.Delay(3, replicaB) {
			identical++
		}
	}
	require.Lessf(t, identical, samples,
		"two crypto-seeded sources produced the same %d delays; they are not independently seeded", samples)
}

func TestRetryPolicy_JitterSourceIsSafeForConcurrentUse(t *testing.T) {
	policy := testPolicy()
	source := NewSeededJitter(7)

	// The race detector is the assertion here; the loop only has to make the
	// unsynchronized access happen if there is one.
	done := make(chan struct{})
	for worker := 0; worker < 8; worker++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 200; i++ {
				_ = policy.Delay(3, source)
			}
		}()
	}
	for worker := 0; worker < 8; worker++ {
		<-done
	}
}

func TestRetryPolicy_Validate(t *testing.T) {
	require.NoError(t, testPolicy().Validate())

	for name, policy := range map[string]RetryPolicy{
		"zero base":         {Base: 0, Max: time.Minute, Multiplier: 2, Jitter: 0.2},
		"max below base":    {Base: time.Minute, Max: time.Second, Multiplier: 2, Jitter: 0.2},
		"multiplier below1": {Base: time.Second, Max: time.Minute, Multiplier: 0.5, Jitter: 0.2},
		"negative jitter":   {Base: time.Second, Max: time.Minute, Multiplier: 2, Jitter: -0.1},
		"jitter above one":  {Base: time.Second, Max: time.Minute, Multiplier: 2, Jitter: 1.5},
	} {
		require.Errorf(t, policy.Validate(), "%s must be rejected", name)
	}
}

func TestDecide_RetryableUsesBudgetThenDeadLetters(t *testing.T) {
	policy := testPolicy()
	source := NewSeededJitter(9)

	decision, err := policy.Decide(ClassRetryable, 1, 1, 3, source)
	require.NoError(t, err)
	require.True(t, decision.Retry)
	require.Positive(t, decision.Delay)

	// The third attempt of a three-attempt job consumed the budget.
	decision, err = policy.Decide(ClassRetryable, 3, 3, 3, source)
	require.NoError(t, err)
	require.False(t, decision.Retry)
	require.Equal(t, ReasonAttemptsExhausted, decision.DeadLetterReason)
}

func TestDecide_TimeoutFollowsTheSamePolicyAsRetryableFailure(t *testing.T) {
	policy := testPolicy()

	failure, err := policy.Decide(ClassRetryable, 2, 2, 5, nil)
	require.NoError(t, err)
	timeout, err := policy.Decide(ClassTimedOut, 2, 2, 5, nil)
	require.NoError(t, err)
	require.Equal(t, failure, timeout,
		"a job must not learn a different cadence depending on whether its worker reported the failure")
}

func TestDecide_PermanentDeadLettersWithBudgetRemaining(t *testing.T) {
	decision, err := testPolicy().Decide(ClassPermanent, 1, 1, 10, NewSeededJitter(3))
	require.NoError(t, err)
	require.False(t, decision.Retry)
	require.Zero(t, decision.Delay)
	require.Equal(t, ReasonPermanentFailure, decision.DeadLetterReason)
}

// TestDecide_AbandonmentKeepsADR0009Behavior guards the boundary the roadmap
// draws: M4 must not quietly convert M3's immediate crash recovery into retry
// backoff.
func TestDecide_AbandonmentKeepsADR0009Behavior(t *testing.T) {
	policy := testPolicy()

	decision, err := policy.Decide(ClassAbandoned, 1, 1, 3, NewSeededJitter(11))
	require.NoError(t, err)
	require.True(t, decision.Retry)
	require.Zero(t, decision.Delay, "abandonment recovery is immediate: no backoff, no jitter")

	decision, err = policy.Decide(ClassAbandoned, 3, 3, 3, nil)
	require.NoError(t, err)
	require.False(t, decision.Retry)
	require.Equal(t, ReasonAttemptsExhausted, decision.DeadLetterReason)
}

func TestDecide_CancellationHasNoRetryDecision(t *testing.T) {
	_, err := testPolicy().Decide(ClassCanceled, 1, 1, 3, nil)
	require.Error(t, err, "cancellation produces neither a retry nor a DLQ entry")
}

func TestFailureClass_HandlerReportableSetIsClosed(t *testing.T) {
	require.True(t, ClassRetryable.ReportableByHandler())
	require.True(t, ClassPermanent.ReportableByHandler())

	// A worker cannot decide any of these about itself.
	require.False(t, ClassTimedOut.ReportableByHandler())
	require.False(t, ClassCanceled.ReportableByHandler())
	require.False(t, ClassAbandoned.ReportableByHandler())

	require.False(t, FailureClass("RETRY").Valid())
	require.False(t, FailureClass("retryable").Valid())
	for _, class := range []FailureClass{
		ClassRetryable, ClassPermanent, ClassTimedOut, ClassCanceled, ClassAbandoned,
	} {
		require.True(t, class.Valid())
	}
}

// zeroJitter always returns the bottom of the range, which is the one value
// that turns an overflowed nominal delay into NaN rather than into +Inf.
type zeroJitter struct{}

func (zeroJitter) Float64() float64 { return 0 }

// TestRetryPolicy_OverflowTimesTheLowestJitterFactorIsStillBounded pins the
// corner the ordinary overflow test cannot reach.
//
// At a large attempt number the nominal delay is +Inf. With full jitter and a
// source returning exactly 0, the factor is 1 + 1*(2*0-1) = 0, and +Inf * 0 is
// NaN — which compares false against every bound, so a clamp applied only after
// the jitter multiply would let NaN reach the Duration conversion. The guard
// before the multiply is what makes this case bounded.
//
// What an unguarded NaN converts to is architecture-dependent: amd64 yields the
// indefinite value -2^63, a nonsensical negative delay, while arm64 saturates to
// 0 and looks harmless. So this test fails loudly on one host and silently
// passes on another if the guard is removed. It is written as a bounds
// assertion rather than an equality one for exactly that reason: the property
// that matters is that no input produces a delay outside [0, Max], on any
// machine.
func TestRetryPolicy_OverflowTimesTheLowestJitterFactorIsStillBounded(t *testing.T) {
	policy := RetryPolicy{Base: time.Second, Max: time.Minute, Multiplier: 2, Jitter: 1}

	for _, attempt := range []int{1, 2, 60, 100, 1 << 20} {
		delay := policy.Delay(attempt, zeroJitter{})
		require.GreaterOrEqualf(t, delay, time.Duration(0),
			"attempt %d produced a negative delay", attempt)
		require.LessOrEqualf(t, delay, policy.Max,
			"attempt %d exceeded the configured maximum", attempt)
	}
}

// TestRetryPolicy_ValidateRejectsNonFiniteFloats covers the values that pass a
// range check by accident.
//
// A NaN compares false against every bound, so `Multiplier < 1` and
// `Jitter < 0 || Jitter > 1` both admit it. An infinity passes the multiplier
// bound outright. Both then reach arithmetic that produces a NaN delay and a
// Duration conversion whose result is architecture-dependent — amd64 yields the
// most negative int64, arm64 saturates to zero — so the same policy would
// schedule differently on different machines. They are rejected by name, before
// any comparison against a bound.
func TestRetryPolicy_ValidateRejectsNonFiniteFloats(t *testing.T) {
	nan := math.NaN()
	posInf := math.Inf(1)
	negInf := math.Inf(-1)

	for name, policy := range map[string]RetryPolicy{
		"NaN multiplier":            {Base: time.Second, Max: time.Minute, Multiplier: nan, Jitter: 0.2},
		"+Inf multiplier":           {Base: time.Second, Max: time.Minute, Multiplier: posInf, Jitter: 0.2},
		"-Inf multiplier":           {Base: time.Second, Max: time.Minute, Multiplier: negInf, Jitter: 0.2},
		"NaN jitter":                {Base: time.Second, Max: time.Minute, Multiplier: 2, Jitter: nan},
		"+Inf jitter":               {Base: time.Second, Max: time.Minute, Multiplier: posInf, Jitter: posInf},
		"-Inf jitter":               {Base: time.Second, Max: time.Minute, Multiplier: 2, Jitter: negInf},
		"NaN multiplier and jitter": {Base: time.Second, Max: time.Minute, Multiplier: nan, Jitter: nan},
	} {
		err := policy.Validate()
		require.Errorf(t, err, "%s must be rejected", name)
		require.Containsf(t, err.Error(), "finite",
			"%s must be rejected as non-finite rather than as an out-of-range value", name)
	}
}

// TestIsFinite_MatchesTheOnlyValuesArithmeticCanUse pins the shared predicate
// configuration loading and policy validation both call, so the two boundaries
// can never disagree about what a usable number is.
func TestIsFinite_MatchesTheOnlyValuesArithmeticCanUse(t *testing.T) {
	for _, usable := range []float64{0, 1, -1, 0.5, 2, math.MaxFloat64, -math.MaxFloat64, math.SmallestNonzeroFloat64} {
		require.Truef(t, IsFinite(usable), "%v is a real number", usable)
	}
	for _, unusable := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		require.Falsef(t, IsFinite(unusable), "%v is not a real number", unusable)
	}
}

// TestRetryPolicy_DelayClampsANonFiniteInjectedJitterSampleExactly is the
// defense one layer below Validate.
//
// The sample comes from an injected interface, so it is input this package does
// not control even when the policy itself validated. A NaN multiplied into the
// delay reaches the Duration conversion, and THAT conversion is
// architecture-dependent: amd64 yields the most negative int64, arm64 saturates
// to zero. A range assertion is therefore not enough to pin this — "between 0
// and Max" is satisfied by the arm64 answer of an entirely unguarded
// implementation, so the same test would pass on one machine and fail on
// another for the same bug.
//
// Each case asserts the exact delay the documented clamp defines instead.
func TestRetryPolicy_DelayClampsANonFiniteInjectedJitterSampleExactly(t *testing.T) {
	policy := RetryPolicy{Base: time.Second, Max: time.Minute, Multiplier: 2, Jitter: 0.5}

	// One wall value computed by hand, chosen because the arithmetic is exact in
	// binary floating point: sample 0 gives factor 1 + 0.5*(2*0 - 1) = 0.5, and
	// attempt 1's nominal delay is the 1s base. Anything unguarded lands on 0 or
	// a negative duration here, on every architecture.
	require.Equal(t, 500*time.Millisecond, policy.Delay(1, constantJitter(math.NaN())),
		"a NaN sample is dropped to 0, so attempt 1 gets base * 0.5")

	// Every NON-FINITE sample clamps to exactly 0, including +Inf. The
	// finiteness check runs before the range checks, and that ordering is
	// deliberate rather than incidental: a non-finite sample is not a value that
	// was too large, it is a source that is broken, so there is no "high" end for
	// it to belong to. A finite sample below the range clamps to the same floor.
	floor := constantJitter(0)
	for name, sample := range map[string]float64{
		"NaN":             math.NaN(),
		"+Inf":            math.Inf(1),
		"-Inf":            math.Inf(-1),
		"below the range": -0.25,
	} {
		for attempt := 1; attempt <= 4; attempt++ {
			require.Equalf(t, policy.Delay(attempt, floor), policy.Delay(attempt, constantJitter(sample)),
				"%s must clamp to the same delay as a sample of 0 on attempt %d", name, attempt)
		}
	}

	// A FINITE sample at or above the range clamps to the largest float below 1,
	// which is the largest value the [0, 1) contract actually permits.
	ceiling := constantJitter(math.Nextafter(1, 0))
	for name, sample := range map[string]float64{
		"at the range top": 1,
		"above the range":  4,
		"far above":        1e300,
	} {
		for attempt := 1; attempt <= 4; attempt++ {
			require.Equalf(t, policy.Delay(attempt, ceiling), policy.Delay(attempt, constantJitter(sample)),
				"%s must clamp to the same delay as the largest sample below 1 on attempt %d", name, attempt)
		}
	}

	// The clamped results are still real delays inside the policy's own bounds,
	// and the two clamps are genuinely different answers -- a guard that
	// collapsed every sample to one value would satisfy the equalities above.
	require.NotEqual(t, policy.Delay(1, floor), policy.Delay(1, ceiling))
	for attempt := 1; attempt <= 4; attempt++ {
		for _, source := range []JitterSource{floor, ceiling} {
			delay := policy.Delay(attempt, source)
			require.Positive(t, delay)
			require.LessOrEqual(t, delay, policy.Max)
		}
	}
}

// TestRetryPolicy_DelayIgnoresANonFiniteJitterFraction covers a policy built in
// code rather than loaded from configuration, which never passed Validate.
func TestRetryPolicy_DelayIgnoresANonFiniteJitterFraction(t *testing.T) {
	for _, fraction := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		policy := RetryPolicy{Base: time.Second, Max: time.Minute, Multiplier: 2, Jitter: fraction}
		require.Equal(t, time.Second, policy.Delay(1, constantJitter(0.9)),
			"a non-finite jitter fraction must be dropped, leaving the nominal delay")
	}
}

// constantJitter is a source that returns one value forever, including values
// outside the [0, 1) contract a real source promises.
type constantJitter float64

func (c constantJitter) Float64() float64 { return float64(c) }

// TestRetryPolicy_DelayIsAlwaysWholeMillisecondsAndNeverRoundsAPositiveDelayAway
// pins the quantization the persisted decision depends on.
//
// job_attempts.retry_delay_ms stores whole milliseconds. A delay that does not
// survive that round trip is a decision the database cannot describe: the
// transition would be chosen from one number and reconstructed from another. So
// the policy hands out only values the storage can hold, and it rounds a
// positive delay UP, because a configured positive backoff silently becoming an
// immediate retry is a materially different behavior under load.
func TestRetryPolicy_DelayIsAlwaysWholeMillisecondsAndNeverRoundsAPositiveDelayAway(t *testing.T) {
	for name, base := range map[string]time.Duration{
		"1ns":                     time.Nanosecond,
		"a nanosecond under 1ms":  time.Millisecond - time.Nanosecond,
		"a microsecond":           time.Microsecond,
		"500 microseconds":        500 * time.Microsecond,
		"exactly 1ms":             time.Millisecond,
		"1ms and a nanosecond":    time.Millisecond + time.Nanosecond,
		"a whole 250ms":           250 * time.Millisecond,
		"not a whole millisecond": 1500*time.Microsecond + 7*time.Nanosecond,
	} {
		t.Run(name, func(t *testing.T) {
			// Max equals Base and no jitter, so the nominal delay IS base and the
			// only thing under test is what quantization does to it.
			policy := RetryPolicy{Base: base, Max: base, Multiplier: 1, Jitter: 0}
			delay := policy.Delay(1, nil)

			require.Zero(t, delay%time.Millisecond,
				"a delay the database cannot store is a decision it cannot describe")
			require.GreaterOrEqual(t, delay, time.Millisecond,
				"a positive backoff must never round away to an immediate retry")
			require.GreaterOrEqual(t, delay, base.Truncate(time.Millisecond),
				"rounding is upward, so the result is never below the truncated input")
			require.Equal(t, delay, time.Duration(delay.Milliseconds())*time.Millisecond,
				"the delay must survive the millisecond round trip the attempt row does")
		})
	}
}

// TestRetryPolicy_ZeroDelayStaysImmediate keeps ADR-0009's immediate requeue
// distinguishable from the shortest real backoff. Rounding a positive delay up
// must not also invent one where the calculation produced none.
func TestRetryPolicy_ZeroDelayStaysImmediate(t *testing.T) {
	policy := testPolicy()
	decision, err := policy.Decide(ClassAbandoned, 1, 1, 3, NewSeededJitter(4))
	require.NoError(t, err)
	require.True(t, decision.Retry)
	require.Zero(t, decision.Delay, "an abandoned attempt is requeued immediately, not backed off")

	// And a jittered calculation that lands on exactly zero stays zero. Full
	// jitter with a sample of 0 gives factor 1 + 1*(2*0 - 1) = 0, so the whole
	// delay is zero however large the base is.
	zeroed := RetryPolicy{Base: time.Minute, Max: time.Hour, Multiplier: 2, Jitter: 1}
	require.Zero(t, zeroed.Delay(1, constantJitter(0)),
		"a delay the calculation reduced to zero is an immediate retry, not a 1ms backoff")
	require.Zero(t, zeroed.Delay(5, constantJitter(0)))
}

// TestRetryPolicy_DelaySaturatesAtTheLargestRepresentableDelay is the boundary
// an unchecked float-to-Duration conversion gets wrong on half the machines that
// run this.
//
// float64(math.MaxInt64) rounds UP to exactly 2^63, one past what an int64
// holds, and Go leaves that conversion undefined: arm64 saturates to
// math.MaxInt64 while amd64 yields the most negative int64. The negative value
// then read as "no delay", so the same policy produced a maximal backoff on one
// architecture and an IMMEDIATE RETRY on the other.
//
// These assertions go through the public Delay, not the internal helper,
// because the conversion under test lives on the public path.
func TestRetryPolicy_DelaySaturatesAtTheLargestRepresentableDelay(t *testing.T) {
	// math.MaxInt64 nanoseconds floored to whole milliseconds.
	const want = 9223372036854 * time.Millisecond
	require.Equal(t, "2562047h47m16.854s", want.String(),
		"the documented ceiling, pinned so a change to it is deliberate")

	sources := map[string]JitterSource{
		"no jitter source":   nil,
		"minimum sample":     constantJitter(0),
		"maximum sample":     constantJitter(math.Nextafter(1, 0)),
		"out-of-range below": constantJitter(-1),
		"out-of-range above": constantJitter(4),
		"NaN sample":         constantJitter(math.NaN()),
		"+Inf sample":        constantJitter(math.Inf(1)),
	}
	attempts := []int{1, 2, 40, 99, 100, 1000}

	// Jitter disabled: the sample is ignored entirely, so every source and every
	// attempt number must land on the ceiling. This is the case that used to
	// return 0s on amd64 and the ceiling on arm64.
	unjittered := RetryPolicy{
		Base: time.Duration(math.MaxInt64), Max: time.Duration(math.MaxInt64),
		Multiplier: 2, Jitter: 0,
	}
	for name, source := range sources {
		for _, attempt := range attempts {
			delay := unjittered.Delay(attempt, source)
			require.NotZerof(t, delay,
				"%s on attempt %d collapsed a maximal backoff into an immediate retry", name, attempt)
			require.Equalf(t, want, delay,
				"%s on attempt %d must saturate at the same value on every architecture", name, attempt)
		}
	}

	// Jitter enabled: the result depends on the sample, but every one of them is
	// an exact, architecture-independent number that is positive, storable, and
	// within the ceiling.
	jittered := RetryPolicy{
		Base: time.Duration(math.MaxInt64), Max: time.Duration(math.MaxInt64),
		Multiplier: 2, Jitter: 0.5,
	}
	for name, source := range sources {
		for _, attempt := range attempts {
			delay := jittered.Delay(attempt, source)
			require.NotZerof(t, delay,
				"%s on attempt %d collapsed a maximal backoff into an immediate retry", name, attempt)
			require.Positivef(t, delay, "%s on attempt %d produced a negative delay", name, attempt)
			require.LessOrEqualf(t, delay, want, "%s on attempt %d exceeded the representable ceiling", name, attempt)
			require.Zerof(t, delay%time.Millisecond, "%s on attempt %d is not storable", name, attempt)
		}
	}

	// The two jitter extremes, pinned exactly. A sample of 0 with half jitter
	// halves the ceiling -- 2^62 nanoseconds, rounded up to whole milliseconds --
	// and the largest sample leaves it at the ceiling.
	require.Equal(t, 4611686018428*time.Millisecond, jittered.Delay(1, constantJitter(0)),
		"the minimum sample halves a maximal delay, exactly and on every architecture")
	require.Equal(t, want, jittered.Delay(1, constantJitter(math.Nextafter(1, 0))))

	// Full jitter at sample 0 zeroes the CALCULATION, which is a real immediate
	// retry rather than an overflow collapsing into one.
	zeroed := RetryPolicy{
		Base: time.Duration(math.MaxInt64), Max: time.Duration(math.MaxInt64),
		Multiplier: 1, Jitter: 1,
	}
	require.Zero(t, zeroed.Delay(1, constantJitter(0)))
	require.Equal(t, want, zeroed.Delay(1, constantJitter(math.Nextafter(1, 0))))
}

// TestRetryPolicy_ValidPoliciesNeverExceedTheirMaximum is the property the
// granularity contract exists to make true as written.
//
// Max is a strict upper bound on a value stored in whole milliseconds, so it has
// to be expressible in that unit -- otherwise the effective bound is a silently
// floored version of the configured one, and "delay <= Max" is true only after
// an unstated adjustment.
func TestRetryPolicy_ValidPoliciesNeverExceedTheirMaximum(t *testing.T) {
	policies := map[string]RetryPolicy{
		"sub-millisecond base":     {Base: time.Nanosecond, Max: time.Millisecond, Multiplier: 2, Jitter: 0.2},
		"non-whole base":           {Base: 1500*time.Microsecond + 7*time.Nanosecond, Max: 2 * time.Millisecond, Multiplier: 2, Jitter: 0.5},
		"exactly one millisecond":  {Base: time.Millisecond, Max: time.Millisecond, Multiplier: 2, Jitter: 1},
		"ordinary":                 {Base: time.Second, Max: time.Minute, Multiplier: 2, Jitter: 0.2},
		"no jitter":                {Base: 250 * time.Millisecond, Max: 10 * time.Second, Multiplier: 3, Jitter: 0},
		"full jitter":              {Base: time.Second, Max: 5 * time.Second, Multiplier: 2, Jitter: 1},
		"multiplier one":           {Base: 7 * time.Millisecond, Max: 7 * time.Millisecond, Multiplier: 1, Jitter: 0.5},
		"very large whole maximum": {Base: time.Second, Max: 1000000 * time.Millisecond, Multiplier: 10, Jitter: 0.9},
	}
	samples := []float64{0, 0.25, 0.5, math.Nextafter(1, 0)}

	for name, policy := range policies {
		require.NoErrorf(t, policy.Validate(), "%s must be a valid policy", name)
		for _, sample := range samples {
			for _, attempt := range []int{1, 2, 5, 50, 100} {
				delay := policy.Delay(attempt, constantJitter(sample))

				require.GreaterOrEqualf(t, delay, time.Duration(0),
					"%s attempt %d sample %v produced a negative delay", name, attempt, sample)
				require.LessOrEqualf(t, delay, policy.Max,
					"%s attempt %d sample %v exceeded its own maximum", name, attempt, sample)
				require.Zerof(t, delay%time.Millisecond,
					"%s attempt %d sample %v is not storable", name, attempt, sample)

				// Upward rounding, stated as the property rather than as a
				// constant: whenever the delay is positive and below the cap, it
				// is at least the smallest storable delay, and it is never below
				// the whole-millisecond floor of what the calculation asked for.
				if delay > 0 && delay < policy.Max {
					require.GreaterOrEqualf(t, delay, time.Millisecond,
						"%s attempt %d sample %v rounded a positive delay below 1ms", name, attempt, sample)
				}
			}
		}
	}
}

// TestRetryPolicy_UpwardRoundingIsExactBelowTheCap pins the rounding direction
// on values chosen so the expected answer is unambiguous.
func TestRetryPolicy_UpwardRoundingIsExactBelowTheCap(t *testing.T) {
	const cap = time.Minute
	for name, expected := range map[time.Duration]time.Duration{
		time.Nanosecond:                           time.Millisecond,
		time.Microsecond:                          time.Millisecond,
		500 * time.Microsecond:                    time.Millisecond,
		time.Millisecond - time.Nanosecond:        time.Millisecond,
		time.Millisecond:                          time.Millisecond,
		time.Millisecond + time.Nanosecond:        2 * time.Millisecond,
		1500*time.Microsecond + 7*time.Nanosecond: 2 * time.Millisecond,
		250 * time.Millisecond:                    250 * time.Millisecond,
	} {
		base, want := name, expected
		policy := RetryPolicy{Base: base, Max: cap, Multiplier: 1, Jitter: 0}
		require.NoError(t, policy.Validate())
		require.Equalf(t, want, policy.Delay(1, nil),
			"a base of %v must round up to %v", base, want)
	}
}

// TestDecide_DelayIsQuantizedBeforeItLeavesThePolicy proves the normalization
// happens once, at the source, rather than at each call site. Every caller gets
// a value that is already what will be stored.
func TestDecide_DelayIsQuantizedBeforeItLeavesThePolicy(t *testing.T) {
	policy := RetryPolicy{Base: time.Nanosecond, Max: time.Nanosecond, Multiplier: 1, Jitter: 0}
	decision, err := policy.Decide(ClassRetryable, 1, 1, 5, NewSeededJitter(7))
	require.NoError(t, err)
	require.True(t, decision.Retry)
	require.Equal(t, time.Millisecond, decision.Delay,
		"a sub-millisecond policy still yields a decision the attempt row can hold")
	require.Positive(t, decision.Delay.Milliseconds(),
		"and the persisted integer agrees that this is a delayed retry")
}

// TestRetryPolicy_DelayOnAnUnvalidatedPolicy covers the corners Validate now
// rejects, because Delay is still reachable from a policy built in code and has
// to stay safe and deterministic there.
//
// These are not behaviors a configured deployment can produce. They are stated
// so the boundaries are a decision rather than an accident.
func TestRetryPolicy_DelayOnAnUnvalidatedPolicy(t *testing.T) {
	subMillisecondMax := RetryPolicy{
		Base: time.Nanosecond, Max: time.Nanosecond, Multiplier: 1, Jitter: 0,
	}
	require.Error(t, subMillisecondMax.Validate(), "a sub-millisecond maximum is not a valid policy")
	require.Equal(t, time.Millisecond, subMillisecondMax.Delay(1, nil),
		"with no storable delay inside such a maximum, staying positive wins over collapsing to zero")

	nonWholeMax := RetryPolicy{
		Base: 5 * time.Millisecond, Max: 5*time.Millisecond + 500*time.Microsecond,
		Multiplier: 2, Jitter: 0,
	}
	require.Error(t, nonWholeMax.Validate(), "a non-whole-millisecond maximum is not a valid policy")
	require.Equal(t, 5*time.Millisecond, nonWholeMax.Delay(2, nil),
		"a maximum that is not a whole millisecond is floored, never exceeded")

	// The top of the range never panics and never goes negative.
	require.NotPanics(t, func() {
		huge := RetryPolicy{
			Base: time.Duration(math.MaxInt64), Max: time.Duration(math.MaxInt64),
			Multiplier: 2, Jitter: 1,
		}
		got := huge.Delay(100, constantJitter(math.Nextafter(1, 0)))
		require.Equal(t, 9223372036854*time.Millisecond, got)
	})
}

// TestRetryPolicy_MaximumGranularityRule states the contract Max has to satisfy
// and, just as importantly, the one Base does not.
//
// Max is a strict upper bound on a value stored in whole milliseconds, so it has
// to be expressible in that unit: a sub-millisecond Max bounds every delay below
// the smallest storable one, and a non-whole Max would be silently floored, so
// the effective bound would differ from the configured one. Base is an input to
// a calculation rather than a bound on a stored value, so it is free.
func TestRetryPolicy_MaximumGranularityRule(t *testing.T) {
	t.Run("a maximum below one millisecond is rejected", func(t *testing.T) {
		for _, max := range []time.Duration{
			time.Nanosecond, time.Microsecond, 500 * time.Microsecond,
			time.Millisecond - time.Nanosecond,
		} {
			policy := RetryPolicy{Base: time.Nanosecond, Max: max, Multiplier: 2, Jitter: 0.2}
			err := policy.Validate()
			require.Errorf(t, err, "a maximum of %v leaves no storable delay inside it", max)
			require.Contains(t, err.Error(), "at least 1ms")
		}
	})

	t.Run("a maximum that is not a whole millisecond is rejected", func(t *testing.T) {
		for _, max := range []time.Duration{
			time.Millisecond + time.Nanosecond,
			1500 * time.Microsecond,
			time.Second + time.Microsecond,
			5*time.Millisecond + 500*time.Microsecond,
		} {
			policy := RetryPolicy{Base: time.Millisecond, Max: max, Multiplier: 2, Jitter: 0.2}
			err := policy.Validate()
			require.Errorf(t, err, "a maximum of %v would be silently floored", max)
			require.Contains(t, err.Error(), "whole number of milliseconds")
		}
	})

	t.Run("exact millisecond multiples are accepted", func(t *testing.T) {
		for _, max := range []time.Duration{
			time.Millisecond, 2 * time.Millisecond, 250 * time.Millisecond,
			time.Second, time.Minute, time.Hour,
		} {
			policy := RetryPolicy{Base: time.Millisecond, Max: max, Multiplier: 2, Jitter: 0.2}
			require.NoErrorf(t, policy.Validate(), "a maximum of %v is exactly expressible", max)
		}
	})

	t.Run("a sub-millisecond or non-whole base is accepted with a valid maximum", func(t *testing.T) {
		for _, base := range []time.Duration{
			time.Nanosecond, time.Microsecond, 500 * time.Microsecond,
			time.Millisecond - time.Nanosecond,
			1500*time.Microsecond + 7*time.Nanosecond,
			time.Second + time.Nanosecond,
		} {
			policy := RetryPolicy{Base: base, Max: time.Minute, Multiplier: 2, Jitter: 0.2}
			require.NoErrorf(t, policy.Validate(),
				"a base of %v is an input to a calculation, not a bound on a stored value", base)
		}
	})

	t.Run("a valid sub-millisecond base still produces a storable delay", func(t *testing.T) {
		policy := RetryPolicy{Base: time.Nanosecond, Max: time.Millisecond, Multiplier: 2, Jitter: 0}
		require.NoError(t, policy.Validate())
		require.Equal(t, time.Millisecond, policy.Delay(1, nil),
			"a positive calculation rounds up to the smallest storable delay")
		require.LessOrEqual(t, policy.Delay(1, nil), policy.Max)
	})

	t.Run("the maximum must still be at least the base", func(t *testing.T) {
		policy := RetryPolicy{Base: time.Minute, Max: time.Second, Multiplier: 2, Jitter: 0.2}
		require.Error(t, policy.Validate())
	})
}
