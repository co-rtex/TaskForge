package main

import (
	"context"
	"fmt"
	"math"
	"time"
)

// Pacer lays submissions on an absolute schedule: slot i is at start + i x
// interval, whatever time the earlier slots actually fired. A pacer that slept
// one interval after each submission would drift by the cost of every submit and
// would offer less than the rate it was given; this one cannot, because lateness
// never accumulates. A slot that is already late fires at once, and the pacer is
// back on schedule when it has caught up.
//
// It is an open-loop generator: the next slot does not wait for the last
// submission's response. Waiting would let a slow system lower the load offered
// to it, which is the opposite of measuring it.
type Pacer struct {
	start    time.Time
	interval time.Duration
}

// NewPacer paces perMinute slots a minute from start.
func NewPacer(start time.Time, perMinute float64) (Pacer, error) {
	if perMinute <= 0 || math.IsNaN(perMinute) || math.IsInf(perMinute, 0) {
		return Pacer{}, fmt.Errorf("the offered rate must be a positive, finite number of jobs a minute, got %v", perMinute)
	}
	interval := time.Duration(math.Round(float64(time.Minute) / perMinute))
	if interval <= 0 {
		return Pacer{}, fmt.Errorf("%v jobs a minute is faster than this pacer can express", perMinute)
	}
	return Pacer{start: start, interval: interval}, nil
}

// Interval is the time between slots.
func (p Pacer) Interval() time.Duration { return p.interval }

// At is the instant of slot i.
func (p Pacer) At(i int) time.Time { return p.start.Add(time.Duration(i) * p.interval) }

// DueBy is how many slots are at or before t.
func (p Pacer) DueBy(t time.Time) int {
	if t.Before(p.start) {
		return 0
	}
	return int(t.Sub(p.start)/p.interval) + 1
}

// Clock is the pacer's only view of time, so a test can supply a fake one. It is
// used to pace and to bound waits. It is never used to timestamp a measurement:
// those come from PostgreSQL (ADR-0020, the single-clock rule).
type Clock interface {
	Now() time.Time
	// SleepUntil returns once t has passed, or at once if it already has, or with
	// the context's error if the context ends first.
	SleepUntil(ctx context.Context, t time.Time) error
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) SleepUntil(ctx context.Context, t time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	wait := time.Until(t)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// PaceStats is how the pacer did. MaxLate is the worst lateness of any slot, and
// it goes into the record: an offered rate is only as good as its pacing.
type PaceStats struct {
	Fired   int
	MaxLate time.Duration
}

// Run calls fire(i) for slot 0, 1, 2, ... at each slot's instant, until count
// slots have fired (count < 0 means until ctx ends) or ctx ends. fire must not
// block for long: a submission that takes time belongs in its own goroutine, and
// the caller owns that.
func (p Pacer) Run(ctx context.Context, clk Clock, count int, fire func(i int)) PaceStats {
	var stats PaceStats
	for i := 0; count < 0 || i < count; i++ {
		target := p.At(i)
		if err := clk.SleepUntil(ctx, target); err != nil {
			return stats
		}
		if late := clk.Now().Sub(target); late > stats.MaxLate {
			stats.MaxLate = late
		}
		fire(i)
		stats.Fired++
	}
	return stats
}
