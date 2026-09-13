package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/co-rtex/TaskForge/internal/database"
)

// newHealthServer exposes distinct liveness and readiness endpoints, matching
// the pattern the API, publisher, and reconciler already use.
//
// Liveness reports only that the process exists. Readiness checks the one
// dependency scheduling cannot work without, under a bounded timeout so a hung
// database cannot hang the probe. The scheduler holds no broker connection, so
// PostgreSQL is genuinely the whole dependency list.
func newHealthServer(addr string, pool *pgxpool.Pool, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeHealth(w, log, http.StatusOK, map[string]any{"status": "alive"})
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		components := map[string]string{"postgres": "ok"}
		status, state := http.StatusOK, "ready"
		if err := database.Ping(ctx, pool); err != nil {
			components["postgres"] = "unavailable"
			status, state = http.StatusServiceUnavailable, "not_ready"
			log.Warn("readiness: postgres unavailable", slog.String("error", err.Error()))
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
