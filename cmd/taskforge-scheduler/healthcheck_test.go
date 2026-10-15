package main

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHealthcheck_ProbesItsOwnLivenessAddress proves the subcommand is
// wired to this binary's own health address (TASKFORGE_SCHEDULER_ADDR): it exits 0
// while the real health server answers there, even with a dependency down,
// because it checks liveness and not readiness, and 1 once nothing listens.
func TestHealthcheck_ProbesItsOwnLivenessAddress(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := newHealthServer(l.Addr().String(), discard(), nil, failingCheck("dependency"))
	go func() { _ = server.Serve(l) }()
	t.Setenv("TASKFORGE_SCHEDULER_ADDR", l.Addr().String())

	require.Equal(t, 0, runHealthcheck(), "liveness answers 200 whatever readiness says")
	require.NoError(t, server.Close())
	require.Equal(t, 1, runHealthcheck(), "nothing listening is unhealthy")
}
