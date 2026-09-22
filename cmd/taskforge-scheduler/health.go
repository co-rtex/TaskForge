package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/co-rtex/TaskForge/internal/metrics"
)

// healthCheck is one named readiness dependency.
//
// A closure rather than a concrete dependency, matching the shape
// internal/api.ReadinessCheck already established. Before M6C these handlers
// held a *pgxpool.Pool and called database.Ping inside themselves, which meant
// the only way to exercise a failing dependency was to have a real PostgreSQL
// that was actually down -- so there were no tests at all. A closure is
// trivially substitutable, so every branch below is reachable from
// `make test-unit` with no infrastructure.
type healthCheck struct {
	Name  string
	Check func(context.Context) error
}

// newHealthServer exposes liveness, readiness, and metrics on one loopback
// listener.
//
// Liveness reports only that the process exists. Readiness checks every
// dependency under a bounded timeout so a hung one cannot hang the probe.
// Metrics are served here rather than on a separate listener for the reason
// docs/CURRENT_STATE.md records: every address in this system is already
// validated as loopback-only, so this endpoint crosses no boundary that
// /healthz and /readyz do not already cross.
func newHealthServer(addr string, log *slog.Logger, m *metrics.Metrics, checks ...healthCheck) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeHealth(w, log, http.StatusOK, map[string]any{"status": "alive"})
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		components := make(map[string]string, len(checks))
		ready := true
		for _, check := range checks {
			if err := check.Check(ctx); err != nil {
				components[check.Name] = "unavailable"
				ready = false
				log.Warn("readiness check failed",
					slog.String("component", check.Name), slog.String("error", err.Error()))
				continue
			}
			components[check.Name] = "ok"
		}

		status, state := http.StatusOK, "ready"
		if !ready {
			status, state = http.StatusServiceUnavailable, "not_ready"
		}
		writeHealth(w, log, status, map[string]any{"status": state, "components": components})
	})

	if m != nil {
		mux.Handle("GET /metrics", m.Handler())
	}

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
}

func writeHealth(w http.ResponseWriter, log *slog.Logger, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Error("write health response", slog.String("error", err.Error()))
	}
}
