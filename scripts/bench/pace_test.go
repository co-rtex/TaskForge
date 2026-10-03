package main

import (
	"context"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeClock stands in for the real one. SleepUntil never wakes early, as a real
// timer cannot, and wakes late by whatever the test says; a target already in
// the past returns at once, which is how a pacer that has fallen behind catches up.
type fakeClock struct {
	now   time.Time
	late  func(slot int) time.Duration
	slept int
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) SleepUntil(ctx context.Context, target time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if target.After(c.now) {
		c.slept++
		c.now = target.Add(c.late(c.slept))
	}
	return nil
}

func TestNewPacer_TheIntervalIsSixtyMillisecondsAtAThousandPerMinute(t *testing.T) {
	p, err := NewPacer(t0, 1000)
	require.NoError(t, err)
	require.Equal(t, 60*time.Millisecond, p.Interval())

	p, err = NewPacer(t0, 60)
	require.NoError(t, err)
	require.Equal(t, time.Second, p.Interval())
}

func TestNewPacer_RefusesARateThatIsNotAPositiveFiniteNumber(t *testing.T) {
	for _, rate := range []float64{0, -5, math.NaN(), math.Inf(1)} {
		_, err := NewPacer(t0, rate)
		require.Errorf(t, err, "rate %v", rate)
	}
}

func TestPacer_SlotsAreOnAnAbsoluteScheduleSoLatenessNeverAccumulates(t *testing.T) {
	p, err := NewPacer(t0, 1000)
	require.NoError(t, err)

	require.True(t, p.At(0).Equal(t0))
	require.True(t, p.At(1).Equal(t0.Add(60*time.Millisecond)))
	require.True(t, p.At(4999).Equal(t0.Add(4999*60*time.Millisecond)), "slot 4999 is 299.94s in, however late the others were")
}

func TestPacer_DueByCountsTheSlotsAtOrBeforeAnInstant(t *testing.T) {
	p, err := NewPacer(t0, 1000)
	require.NoError(t, err)

	require.Zero(t, p.DueBy(t0.Add(-time.Nanosecond)))
	require.Equal(t, 1, p.DueBy(t0), "slot 0 is due at the start")
	require.Equal(t, 1, p.DueBy(t0.Add(59*time.Millisecond+999*time.Microsecond)))
	require.Equal(t, 2, p.DueBy(t0.Add(60*time.Millisecond)))
	require.Equal(t, 5000, p.DueBy(t0.Add(299*time.Second+940*time.Millisecond)), "slot 4999 is the 5000th")
}

// The stated tolerance. Slots are absolute and a timer wakes late and never
// early, so with at most eight milliseconds of lateness against a sixty
// millisecond interval, the number of submissions in any window differs from the
// number of slots in it by at most one. At 1,000 jobs a minute over a five
// minute window that is one job in 5,000: 0.02%.
func TestPacer_Run_OfferedLoadStaysWithinOneSubmissionOfTheTargetInAnyWindow(t *testing.T) {
	const maxLate = 8 * time.Millisecond
	rng := rand.New(rand.NewPCG(1, 2))
	clk := &fakeClock{now: t0, late: func(int) time.Duration { return time.Duration(rng.Int64N(int64(maxLate) + 1)) }}
	p, err := NewPacer(t0, 1000)
	require.NoError(t, err)

	var firedAt []time.Time
	stats := p.Run(context.Background(), clk, 6000, func(int) { firedAt = append(firedAt, clk.Now()) })

	require.Equal(t, 6000, stats.Fired)
	require.LessOrEqual(t, stats.MaxLate, maxLate)
	for i, at := range firedAt {
		require.False(t, at.Before(p.At(i)), "slot %d fired early", i)
		require.LessOrEqual(t, at.Sub(p.At(i)), maxLate, "slot %d fired more than %s late", i, maxLate)
	}

	for _, w := range []Window{
		{Start: t0.Add(30 * time.Second), End: t0.Add(330 * time.Second)}, // the recorded steady window
		{Start: t0, End: t0.Add(5 * time.Minute)},
		{Start: t0.Add(61*time.Second + 7*time.Millisecond), End: t0.Add(181*time.Second + 7*time.Millisecond)},
	} {
		want := int(w.Duration() / p.Interval())
		got := CountIn(w, firedAt)
		require.InDeltaf(t, want, got, 1, "window %v..%v: %d submissions for %d slots", w.Start, w.End, got, want)
	}
}

// Falling behind must not lower the offered rate: the pacer fires the missed
// slots straight away, in order, and then is back on schedule.
func TestPacer_Run_CatchesUpAfterAStallWithoutSkippingOrDrifting(t *testing.T) {
	clk := &fakeClock{now: t0, late: func(n int) time.Duration {
		if n == 3 {
			return 700 * time.Millisecond // one long stall
		}
		return 0
	}}
	p, err := NewPacer(t0, 1000)
	require.NoError(t, err)

	var firedAt []time.Time
	stats := p.Run(context.Background(), clk, 50, func(int) { firedAt = append(firedAt, clk.Now()) })

	require.Equal(t, 50, stats.Fired, "no slot is skipped")
	require.GreaterOrEqual(t, stats.MaxLate, 600*time.Millisecond, "the stall is reported, not hidden")
	// Back on schedule by the end: slot 49 fires on its own slot, not after 700ms.
	require.True(t, firedAt[49].Equal(p.At(49)), "fired at %v, slot is %v", firedAt[49].Sub(t0), p.At(49).Sub(t0))
	// And the slots inside the stall fired immediately after it, not spread out.
	require.True(t, firedAt[5].Equal(firedAt[4]) || firedAt[5].Sub(firedAt[4]) < p.Interval())
}

func TestPacer_Run_StopsAtOnceWhenTheContextEnds(t *testing.T) {
	clk := &fakeClock{now: t0, late: func(int) time.Duration { return 0 }}
	p, err := NewPacer(t0, 1000)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	stats := p.Run(ctx, clk, -1, func(i int) {
		if i == 9 {
			cancel()
		}
	})

	require.Equal(t, 10, stats.Fired, "slot 9 fired, and nothing after the cancel")
}
