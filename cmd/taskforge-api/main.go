// Command taskforge-api serves TaskForge's HTTP API.
//
// The public /v1 surface authenticates with database-backed API keys, and the
// internal worker-control surface authenticates registration with a separate
// database-backed worker key -- see docs/adr/0014-worker-control-authentication.md.
// The key-management routes for both credential types are unauthenticated
// operator plumbing, so the process still binds to loopback only.
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

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/auth"
	"github.com/co-rtex/TaskForge/internal/config"
	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/objectstore"
	"github.com/co-rtex/TaskForge/internal/results"
	"github.com/co-rtex/TaskForge/internal/telemetry"
	"github.com/co-rtex/TaskForge/internal/workerauth"
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
	log := telemetry.NewLogger(os.Stdout, cfg.LogLevel, "taskforge-api")
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
		Service:  "taskforge-api",
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

	// Independently crypto-seeded per process. Two API replicas that shared a
	// deterministic seed would compute identical retry instants for jobs failing
	// at the same moment, which is exactly the stampede jitter exists to prevent.
	jitter, err := lifecycle.NewCryptoSeededJitter()
	if err != nil {
		log.Error("seed retry jitter", slog.String("error", err.Error()))
		return 1
	}

	// No network call here, deliberately: this process only ever reads an
	// object-located result on demand, so its boot must not depend on the
	// object store being reachable -- the same posture it already has
	// toward the broker, which it never touches directly at all.
	objects, err := objectstore.New(ctx, objectstore.Options{
		Endpoint: cfg.ResultsEndpoint, Region: cfg.ResultsRegion,
		AccessKeyID: cfg.ResultsAccessKeyID, SecretAccessKey: cfg.ResultsSecretAccessKey,
	})
	if err != nil {
		log.Error("configure result object store client", slog.String("error", err.Error()))
		return 1
	}

	// One store serves both the fenced internal worker-control surface and the
	// authenticated public GET /v1/workers read. They are separate interfaces
	// on the server (WorkerControl, WorkerReads) so a public read cannot reach
	// a fenced transition, but there is only ever one control plane behind
	// them.
	workerStore := workers.NewStore(pool, workers.StoreConfig{
		LeaseDuration: cfg.LeaseDuration,
		RetryPolicy:   cfg.RetryPolicy(),
		Jitter:        jitter,
	})

	server := api.NewServer(
		jobs.NewStore(pool),
		api.Config{
			MaxRequestBytes: cfg.MaxRequestBytes,
			RequestTimeout:  cfg.APIRequestTimeout,
		},
		log,
		api.ReadinessCheck{
			Name:  "postgres",
			Check: func(ctx context.Context) error { return database.Ping(ctx, pool) },
		},
	).WithWorkerControl(workerStore).
		WithWorkerReads(workerStore).
		WithAuth(auth.NewStore(pool)).WithWorkerAuth(workerauth.NewStore(pool)).
		WithResults(results.NewStore(pool), objects)

	httpServer := &http.Server{
		Addr:    cfg.APIAddr,
		Handler: server.Handler(),
		// Bounded timeouts stop a slow or stalled client from holding a
		// connection, and a goroutine, indefinitely.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("api listening",
			slog.String("addr", cfg.APIAddr),
			slog.Int64("max_request_bytes", cfg.MaxRequestBytes))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		log.Error("http server failed", slog.String("error", err.Error()))
		return 1
	case <-ctx.Done():
		log.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", slog.String("error", err.Error()))
		return 1
	}
	log.Info("api stopped")
	return 0
}
