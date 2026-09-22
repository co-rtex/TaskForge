// Command taskforge-reconciler repairs durable state no live process will
// repair: stale worker sessions, and expired leases whose attempts must be
// abandoned so their capacity is released and recoverable work is requeued.
//
// It runs as its own process because recovery must not depend on the API, the
// publisher, or any particular worker being alive. Multiple instances are safe:
// every decision is revalidated under row locks against PostgreSQL time.
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
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/reconciler"
	"github.com/co-rtex/TaskForge/internal/telemetry"
	"github.com/co-rtex/TaskForge/internal/workers"
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
	log := telemetry.NewLogger(os.Stdout, cfg.LogLevel, "taskforge-reconciler")
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
		Service:  "taskforge-reconciler",
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

	// Independently crypto-seeded per process, for the same reason the API's is:
	// several reconciler replicas detecting the same batch of timeouts must not
	// schedule their retries for the same instant.
	jitter, err := lifecycle.NewCryptoSeededJitter()
	if err != nil {
		log.Error("seed retry jitter", slog.String("error", err.Error()))
		return 1
	}

	store := workers.NewStore(pool, workers.StoreConfig{
		LeaseDuration: cfg.LeaseDuration,
		RetryPolicy:   cfg.RetryPolicy(),
		Jitter:        jitter,
	})
	engine := reconciler.New(store, reconciler.Config{
		StaleAfter:   cfg.SessionStaleAfter,
		PollInterval: cfg.ReconcilerPollInterval,
		BatchSize:    cfg.ReconcilerBatchSize,
	}, log)

	healthServer := newHealthServer(cfg.ReconcilerAddr, pool, log)
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
		log.Error("reconciler stopped with error", slog.String("error", runErr.Error()))
		return 1
	}
	return 0
}
