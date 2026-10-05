package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// clockDivergence is how far PostgreSQL's clock and this process's monotonic
// clock disagree about how much time has passed between two readings. It is the
// early form of the single-clock guard: a Docker VM whose clock is stepped after
// the host sleeps, or a host that sleeps itself, shows up here within seconds
// and not after a twenty minute run.
func TestClockDivergence(t *testing.T) {
	pg0, mono0 := t0, t0.Add(1000*time.Hour) // the two clocks have unrelated origins

	require.Equal(t, time.Duration(0), clockDivergence(pg0, pg0.Add(12*time.Second), mono0, mono0.Add(12*time.Second)),
		"both clocks saw 12 seconds")
	require.Equal(t, 49*time.Second, clockDivergence(pg0, pg0.Add(61*time.Second), mono0, mono0.Add(12*time.Second)),
		"the database's clock jumped forward: 61s on it, 12s here")
	require.Equal(t, 40*time.Second, clockDivergence(pg0, pg0.Add(10*time.Second), mono0, mono0.Add(50*time.Second)),
		"the host's clock moved on and the database's did not: the divergence is reported as a size, not a sign")
	require.Equal(t, 3*time.Millisecond, clockDivergence(pg0, pg0.Add(5003*time.Millisecond), mono0, mono0.Add(5*time.Second)))
}

func TestDivergedBeyondTolerance(t *testing.T) {
	require.False(t, divergedTooFar(0))
	require.False(t, divergedTooFar(maxClockDivergence), "exactly at the tolerance is inside it")
	require.True(t, divergedTooFar(maxClockDivergence+time.Millisecond))
}
