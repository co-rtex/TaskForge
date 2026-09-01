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

// TestRetryPolicy_DelaySurvivesANonFiniteInjectedJitterSample is the defense one
// layer below Validate.
//
// The sample comes from an injected interface, so it is input this package does
// not control even when the policy itself validated. A NaN sample multiplied
// into the delay reaches the Duration conversion, and that conversion is
// undefined for NaN. The delay must stay inside [0, Max] for every sample a
// source can physically return.
func TestRetryPolicy_DelaySurvivesANonFiniteInjectedJitterSample(t *testing.T) {
	policy := RetryPolicy{Base: time.Second, Max: time.Minute, Multiplier: 2, Jitter: 0.5}

	for name, sample := range map[string]float64{
		"NaN":              math.NaN(),
		"+Inf":             math.Inf(1),
		"-Inf":             math.Inf(-1),
		"below the range":  -0.25,
		"at the range top": 1,
		"above the range":  4,
	} {
		for attempt := 1; attempt <= 4; attempt++ {
			delay := policy.Delay(attempt, constantJitter(sample))
			require.GreaterOrEqualf(t, delay, time.Duration(0),
				"%s sample produced a negative delay on attempt %d", name, attempt)
			require.LessOrEqualf(t, delay, policy.Max,
				"%s sample produced a delay past Max on attempt %d", name, attempt)
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
