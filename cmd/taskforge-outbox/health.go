package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/queue"
)

// newHealthServer exposes distinct liveness and readiness endpoints.
//
// Liveness reports only that the process exists. Readiness checks both
// dependencies the publisher actually needs — it cannot do its job without
// either — under a bounded timeout so a hung dependency cannot hang the probe.
func newHealthServer(addr string, pool *pgxpool.Pool, broker queue.Broker, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeHealth(w, log, http.StatusOK, map[string]any{"status": "alive"})
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		components := map[string]string{"postgres": "ok", "broker": "ok"}
		ready := true
		if err := database.Ping(ctx, pool); err != nil {
			components["postgres"] = "unavailable"
			ready = false
			log.Warn("readiness: postgres unavailable", slog.String("error", err.Error()))
		}
		if err := broker.Ping(ctx); err != nil {
			components["broker"] = "unavailable"
			ready = false
			log.Warn("readiness: broker unavailable", slog.String("error", err.Error()))
		}

		status, state := http.StatusOK, "ready"
		if !ready {
			status, state = http.StatusServiceUnavailable, "not_ready"
		}
		writeHealth(w, log, status, map[string]any{"status": state, "components": components})
	})

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
