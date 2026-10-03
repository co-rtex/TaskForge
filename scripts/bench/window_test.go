package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, time.March, 4, 5, 6, 7, 0, time.UTC)

// The window is [Start, End): a finish exactly at Start is in the window, and a
// finish exactly at End is in the drain. ADR-0020 documents this, and it is the
// only rule that lets two adjacent windows count every instant exactly once.
func TestWindow_IsHalfOpen(t *testing.T) {
	w := Window{Start: t0, End: t0.Add(5 * time.Minute)}

	require.True(t, w.Contains(t0), "exactly at Start is in")
	require.True(t, w.Contains(t0.Add(time.Nanosecond)))
	require.True(t, w.Contains(w.End.Add(-time.Nanosecond)), "one tick before End is in")
	require.False(t, w.Contains(w.End), "exactly at End is out")
	require.False(t, w.Contains(t0.Add(-time.Nanosecond)), "one tick before Start is out")
}

func TestCountIn_JobsFinishingInWarmupOrDrainAreNotCounted(t *testing.T) {
	w := Window{Start: t0.Add(30 * time.Second), End: t0.Add(330 * time.Second)}
	finishes := []time.Time{
		t0, // warm-up
		t0.Add(29*time.Second + 999*time.Millisecond), // warm-up, just before
		w.Start,                   // on the boundary: counted
		t0.Add(100 * time.Second), // steady
		t0.Add(329*time.Second + 999*time.Millisecond), // steady, just before the end
		w.End,                     // on the boundary: drain
		t0.Add(400 * time.Second), // drain
	}

	require.Equal(t, 3, CountIn(w, finishes))
}

func TestCountIn_NothingInTheWindowIsZero(t *testing.T) {
	w := Window{Start: t0, End: t0.Add(time.Minute)}
	require.Zero(t, CountIn(w, nil))
	require.Zero(t, CountIn(w, []time.Time{t0.Add(-time.Hour), t0.Add(time.Hour)}))
}

func TestPerMinute(t *testing.T) {
	require.InDelta(t, 1000.0, PerMinute(5000, 5*time.Minute), 1e-9)
	require.InDelta(t, 999.8, PerMinute(4999, 5*time.Minute), 1e-9)
	require.InDelta(t, 600.0, PerMinute(10, time.Second), 1e-9)
	require.Zero(t, PerMinute(10, 0), "a zero-length window is not an infinite rate")
	require.Zero(t, PerMinute(10, -time.Second))
}

func TestWindow_DurationIsEndMinusStart(t *testing.T) {
	require.Equal(t, 300*time.Second, Window{Start: t0, End: t0.Add(300 * time.Second)}.Duration())
}
