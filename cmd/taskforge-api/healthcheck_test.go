package main

import (
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
)

// TestHealthcheck_ProbesTheAdminListener proves `taskforge-api healthcheck` is
// wired to the admin listener (TASKFORGE_API_ADDR), the one listener every
// configuration opens, and that it reads the real handler's /healthz: 0 while
// the admin handler answers there, 1 once nothing listens.
func TestHealthcheck_ProbesTheAdminListener(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	handler := api.NewServer(nil, api.Config{MaxRequestBytes: 4096}, discardLogger()).Handler()
	server := &http.Server{Handler: handler}
	go func() { _ = server.Serve(l) }()
	t.Setenv("TASKFORGE_API_ADDR", l.Addr().String())

	require.Equal(t, 0, runHealthcheck())
	require.NoError(t, server.Close())
	require.Equal(t, 1, runHealthcheck())
}
