package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/config"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

// TestPlanListeners_DefaultsOpenExactlyOne proves the default configuration is
// the process as it was: one listener, the admin one, on its loopback default.
func TestPlanListeners_DefaultsOpenExactlyOne(t *testing.T) {
	t.Setenv("TASKFORGE_API_ADDR", "")
	t.Setenv("TASKFORGE_API_PUBLIC_ADDR", "")
	t.Setenv("TASKFORGE_API_CONTROL_ADDR", "")
	t.Setenv("TASKFORGE_API_CONTROL_HOSTS", "")
	cfg, err := config.Load()
	require.NoError(t, err)
	require.Equal(t, []plannedListener{{api.ListenerAdmin, "127.0.0.1:8080"}}, planListeners(cfg))
}

func TestPlanListeners_OpensEachConfiguredListener(t *testing.T) {
	t.Setenv("TASKFORGE_API_PUBLIC_ADDR", "0.0.0.0:8090")
	t.Setenv("TASKFORGE_API_CONTROL_ADDR", "0.0.0.0:8091")
	t.Setenv("TASKFORGE_API_CONTROL_HOSTS", "api")
	cfg, err := config.Load()
	require.NoError(t, err)
	require.Equal(t, []plannedListener{
		{api.ListenerAdmin, "127.0.0.1:8080"},
		{api.ListenerPublic, "0.0.0.0:8090"},
		{api.ListenerControl, "0.0.0.0:8091"},
	}, planListeners(cfg))
}

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// TestServeAll_OneListenerFailingToBindStopsThemAll proves the process is
// never half up: when the control listener's address is taken, serveAll fails
// naming that listener and address, and the admin listener it had already bound
// is released (its address can be bound again at once): nothing was left serving.
func TestServeAll_OneListenerFailingToBindStopsThemAll(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer taken.Close()

	adminAddr := freeLoopbackAddr(t)
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {})
	servers := []namedServer{
		{"admin", newHTTPServer(adminAddr, handler)},
		{"public", newHTTPServer(freeLoopbackAddr(t), handler)},
		{"control", newHTTPServer(taken.Addr().String(), handler)},
	}

	done := make(chan error, 1)
	go func() { done <- serveAll(context.Background(), discardLogger(), servers, time.Second) }()
	select {
	case err := <-done:
		require.Error(t, err)
		require.Contains(t, err.Error(), "bind the control listener on "+taken.Addr().String())
	case <-time.After(10 * time.Second):
		t.Fatal("serveAll did not return after a bind failure; the process would be half up")
	}

	again, err := net.Listen("tcp", adminAddr)
	require.NoError(t, err, "the admin listener bound before the failure must have been closed")
	require.NoError(t, again.Close())
}

// TestServeAll_ShutdownDrainsEveryListener proves SIGTERM's path (the signal
// context being cancelled) drains every listener: a request in flight on each
// one when the signal arrives completes with its response, serveAll waits for
// both before returning, and returns nil, after which neither address is bound.
func TestServeAll_ShutdownDrainsEveryListener(t *testing.T) {
	release := make(chan struct{})
	var inFlight sync.WaitGroup
	inFlight.Add(2)
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		inFlight.Done()
		<-release
		w.WriteHeader(http.StatusOK)
	})
	addrs := []string{freeLoopbackAddr(t), freeLoopbackAddr(t)}
	servers := []namedServer{
		{"admin", newHTTPServer(addrs[0], handler)},
		{"control", newHTTPServer(addrs[1], handler)},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveAll(ctx, discardLogger(), servers, 10*time.Second) }()

	statuses := make(chan int, 2)
	for _, addr := range addrs {
		go func(addr string) {
			deadline := time.Now().Add(5 * time.Second)
			for {
				resp, err := http.Get("http://" + addr + "/")
				if err == nil {
					resp.Body.Close()
					statuses <- resp.StatusCode
					return
				}
				if time.Now().After(deadline) {
					statuses <- 0
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(addr)
	}
	inFlight.Wait()
	cancel()

	select {
	case <-done:
		t.Fatal("serveAll returned while requests were still in flight; shutdown did not drain them")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)

	require.Equal(t, http.StatusOK, <-statuses)
	require.Equal(t, http.StatusOK, <-statuses)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("serveAll did not return after draining")
	}
	for _, addr := range addrs {
		l, err := net.Listen("tcp", addr)
		require.NoErrorf(t, err, "%s must be released after shutdown", addr)
		require.NoError(t, l.Close())
	}
}
