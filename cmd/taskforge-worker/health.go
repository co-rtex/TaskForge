package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/co-rtex/TaskForge/internal/queue"
	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
)

type readinessState interface{ Ready() bool }

func newHealthServer(
	addr string,
	state readinessState,
	control workerruntime.ControlPlane,
	broker queue.Broker,
	log *slog.Logger,
) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeHealth(w, log, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
		defer cancel()

		components := map[string]string{"session": "ok", "control_plane": "ok", "broker": "ok"}
		ready := true
		if !state.Ready() {
			components["session"] = "unavailable"
			ready = false
		}
		if err := control.Ping(ctx); err != nil {
			components["control_plane"] = "unavailable"
			ready = false
			log.Warn("worker readiness: control plane unavailable", slog.String("error", err.Error()))
		}
		if err := broker.Ping(ctx); err != nil {
			components["broker"] = "unavailable"
			ready = false
			log.Warn("worker readiness: broker unavailable", slog.String("error", err.Error()))
		}

		status, value := http.StatusOK, "ready"
		if !ready {
			status, value = http.StatusServiceUnavailable, "not_ready"
		}
		writeHealth(w, log, status, map[string]any{"status": value, "components": components})
	})
	return &http.Server{
		Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second,
	}
}

func writeHealth(w http.ResponseWriter, log *slog.Logger, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Error("write worker health response", slog.String("error", err.Error()))
	}
}
