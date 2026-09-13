// Command taskforge-api serves TaskForge's HTTP API.
//
// The public /v1 surface authenticates with database-backed API keys. The
// internal surface -- worker control, and the key-management routes that mint
// the credentials the public surface checks -- is unauthenticated operator
// plumbing, so the process still binds to loopback only.
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
	log := telemetry.NewLogger(os.Stdout, cfg.LogLevel, "taskforge-api")
	if err != nil {
		log.Error("configuration invalid", slog.String("error", err.Error()))
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	server := api.NewServer(
		jobs.NewStore(pool),
		api.Config{
			MaxRequestBytes: cfg.MaxRequestBytes,
			RequestTimeout:  cfg.APIRequestTimeout,
			DevScope:        cfg.DevScope,
		},
		log,
		api.ReadinessCheck{
			Name:  "postgres",
			Check: func(ctx context.Context) error { return database.Ping(ctx, pool) },
		},
	).WithWorkerControl(workers.NewStore(pool, workers.StoreConfig{
		LeaseDuration: cfg.LeaseDuration,
		RetryPolicy:   cfg.RetryPolicy(),
		Jitter:        jitter,
	})).WithAuth(auth.NewStore(pool))

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
			// The development scope now attributes only the internal
			// worker-control surface. Public requests carry their own scope on
			// an API key.
			slog.String("worker_control_scope", cfg.DevScope),
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
