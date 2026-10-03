package main

import "time"

// Window is a span of PostgreSQL time, half-open: it contains Start and does not
// contain End. Half-open is what lets a warm-up, a steady window and a drain
// that meet at their boundaries count every instant exactly once, and it is the
// answer to "a job finishing exactly on a boundary": one finishing exactly at
// Start is in the window, and one finishing exactly at End is in the drain.
type Window struct {
	Start, End time.Time
}

// Contains reports whether t is in [Start, End).
func (w Window) Contains(t time.Time) bool {
	return !t.Before(w.Start) && t.Before(w.End)
}

// Duration is End minus Start. Both are PostgreSQL instants, so this is a length
// of database time and not of this machine's clock.
func (w Window) Duration() time.Duration { return w.End.Sub(w.Start) }

// CountIn is how many of times fall inside the window.
func CountIn(w Window, times []time.Time) int {
	n := 0
	for _, t := range times {
		if w.Contains(t) {
			n++
		}
	}
	return n
}

// PerMinute converts a count over a span to a rate per minute. A span that is
// not positive has no rate; zero is returned rather than an infinity or a NaN.
func PerMinute(n int, over time.Duration) float64 {
	if over <= 0 {
		return 0
	}
	return float64(n) / over.Minutes()
}
