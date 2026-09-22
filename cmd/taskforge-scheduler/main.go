// Command taskforge-scheduler makes durable work reachable: it promotes delayed
// and retry-waiting jobs once PostgreSQL says they are due, and it re-notifies
// queued work whose only advisory notification was lost.
//
// It runs as its own process because eligibility must not depend on the API, a
// publisher, or any particular worker being alive. It holds no broker
// connection at all — it writes the authoritative outbox event, and
// taskforge-outbox owns every byte that reaches the broker. Multiple instances
// are safe: every decision is revalidated under row locks against PostgreSQL
// time, and each promotion names the exact eligibility generation it expects.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/co-rtex/TaskForge/internal/config"
	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/scheduler"
	"github.com/co-rtex/TaskForge/internal/telemetry"
)

func main() {
	os.Exit(run())
}

func run() int {
	if err := config.LoadDotEnv(".env"); err != nil {
		slog.Error("read .env", slog.String("error", err.Error()))
		return 1
	}
	cfg, err := config.Load()
	log := telemetry.NewLogger(os.Stdout, cfg.LogLevel, "taskforge-scheduler")
	if err != nil {
		log.Error("configuration invalid", slog.String("error", err.Error()))
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Tracing is configured before anything else does work, so every span this
	// process emits belongs to one provider. Disabled by default: with
	// TASKFORGE_OTEL_EXPORTER unset or "none" no exporter is built, no
	// goroutine starts, and no endpoint is dialed.
	tracing, err := telemetry.StartTracing(ctx, telemetry.TracingConfig{
		Exporter: cfg.OTelExporter,
		Endpoint: cfg.OTelEndpoint,
		Service:  "taskforge-scheduler",
	}, os.Stdout)
	if err != nil {
		log.Error("configure tracing", slog.String("error", err.Error()))
		return 1
	}
	// Deferred rather than placed on the graceful path, so it runs on every
	// exit including an early error return. It is bounded, idempotent, and a
	// no-op when tracing is disabled, and it deliberately survives ctx being
	// canceled by SIGTERM so buffered spans still flush.
	defer func() {
		if err := tracing.Shutdown(ctx); err != nil {
			log.Error("shut down tracing", slog.String("error", err.Error()))
		}
	}()

	pool, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("connect to database", slog.String("error", err.Error()))
		return 1
	}
	defer pool.Close()

	engine := scheduler.New(jobs.NewStore(pool), scheduler.Config{
		PollInterval:  cfg.SchedulerPollInterval,
		BatchSize:     cfg.SchedulerBatchSize,
		RenotifyAfter: cfg.SchedulerRenotifyAfter,
	}, log)

	healthServer := newHealthServer(cfg.SchedulerAddr, pool, log)
	go func() {
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server failed", slog.String("error", err.Error()))
		}
	}()

	runErr := engine.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		log.Error("health server shutdown failed", slog.String("error", err.Error()))
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		log.Error("scheduler stopped with error", slog.String("error", runErr.Error()))
		return 1
	}
	return 0
}
