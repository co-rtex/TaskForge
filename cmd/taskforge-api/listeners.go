package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/config"
)

// plannedListener is one listener this process opens: the address it binds and
// the route-table listener whose routes it serves.
type plannedListener struct {
	listener api.Listener
	addr     string
}

// planListeners is the listeners a configuration opens, admin first. With
// TASKFORGE_API_PUBLIC_ADDR and TASKFORGE_API_CONTROL_ADDR unset it is the admin
// listener alone, which is the process as it was before M8C1. See
// docs/adr/0026-listeners-by-surface.md.
func planListeners(cfg config.Config) []plannedListener {
	plan := []plannedListener{{api.ListenerAdmin, cfg.APIAddr}}
	if addr := strings.TrimSpace(cfg.APIPublicAddr); addr != "" {
		plan = append(plan, plannedListener{api.ListenerPublic, addr})
	}
	if addr := strings.TrimSpace(cfg.APIControlAddr); addr != "" {
		plan = append(plan, plannedListener{api.ListenerControl, addr})
	}
	return plan
}

// newHTTPServer is the one http.Server shape every listener gets, so the three
// cannot drift apart in their timeouts.
func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:    addr,
		Handler: handler,
		// Bounded timeouts stop a slow or stalled client from holding a
		// connection, and a goroutine, indefinitely.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// namedServer is a server and the listener it is for, for log lines and errors.
type namedServer struct {
	name   string
	server *http.Server
}

// serveAll runs every server until ctx is done or one of them stops, and then
// shuts every one down within grace. It returns nil only for a clean shutdown
// after ctx was done.
//
// It binds every address BEFORE it serves on any, so the process is never half
// up: if one address cannot be bound, the ones already bound are closed and the
// error names the listener and the address that failed, and nothing has served
// a request. Once serving, a listener that stops for any reason other than
// shutdown takes the others down with it, for the same reason: a process whose
// control listener died while its public one kept answering would look healthy
// to a load balancer while every worker was cut off.
func serveAll(ctx context.Context, log *slog.Logger, servers []namedServer, grace time.Duration) error {
	listeners := make([]net.Listener, 0, len(servers))
	for _, s := range servers {
		l, err := net.Listen("tcp", s.server.Addr)
		if err != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			return fmt.Errorf("bind the %s listener on %s: %w", s.name, s.server.Addr, err)
		}
		listeners = append(listeners, l)
	}

	failed := make(chan error, len(servers))
	for i, s := range servers {
		log.Info("api listening", slog.String("listener", s.name), slog.String("addr", listeners[i].Addr().String()))
		go func(s namedServer, l net.Listener) {
			if err := s.server.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
				failed <- fmt.Errorf("the %s listener on %s stopped: %w", s.name, s.server.Addr, err)
			}
		}(s, listeners[i])
	}

	var serveErr error
	select {
	case serveErr = <-failed:
		log.Error("http server failed; stopping every listener", slog.String("error", serveErr.Error()))
	case <-ctx.Done():
		log.Info("shutdown signal received")
	}

	// One grace period for all of them, drained concurrently: N listeners do
	// not get N times the budget.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	shutdownErrs := make(chan error, len(servers))
	for _, s := range servers {
		go func(s namedServer) {
			if err := s.server.Shutdown(shutdownCtx); err != nil {
				shutdownErrs <- fmt.Errorf("shut down the %s listener on %s: %w", s.name, s.server.Addr, err)
				return
			}
			shutdownErrs <- nil
		}(s)
	}
	var errs []error
	for range servers {
		if err := <-shutdownErrs; err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(append([]error{serveErr}, errs...)...)
}
