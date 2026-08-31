// Command taskforge-worker registers one process session, consumes advisory
// work notifications with a bounded local pool, and executes only trusted
// handlers compiled into this binary.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/config"
	"github.com/co-rtex/TaskForge/internal/queue/sqsbroker"
	"github.com/co-rtex/TaskForge/internal/telemetry"
	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
	"github.com/co-rtex/TaskForge/internal/workers"
)

func main() { os.Exit(run()) }

func run() int {
	if err := config.LoadDotEnv(".env"); err != nil {
		slog.Error("read .env", slog.String("error", err.Error()))
		return 1
	}
	shared, sharedErr := config.Load()
	workerConfig, workerErr := config.LoadWorker()
	log := telemetry.NewLogger(os.Stdout, shared.LogLevel, "taskforge-worker")
	if sharedErr != nil {
		log.Error("shared configuration invalid", slog.String("error", sharedErr.Error()))
		return 1
	}
	if workerErr != nil {
		log.Error("worker configuration invalid", slog.String("error", workerErr.Error()))
		return 1
	}
	// Each surface is valid on its own; this is the check neither can make alone.
	if err := config.ValidateWorkerTimings(shared, workerConfig); err != nil {
		log.Error("worker timing configuration invalid", slog.String("error", err.Error()))
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	broker, err := sqsbroker.New(ctx, sqsbroker.Options{
		Endpoint: shared.BrokerEndpoint, Region: shared.BrokerRegion,
		QueueName: shared.BrokerQueueName, AccessKeyID: shared.BrokerAccessKeyID,
		SecretAccessKey: shared.BrokerSecretAccessKey,
	})
	if err != nil {
		log.Error("connect to broker", slog.String("error", err.Error()))
		return 1
	}

	httpClient := &http.Client{Timeout: workerConfig.RequestTimeout}
	control := workerruntime.NewClient(workerConfig.APIBaseURL, httpClient)
	registry := workerruntime.NewRegistry()
	if err := registry.Register("demo.echo", workerruntime.DemoEcho{}); err != nil {
		log.Error("register trusted handler", slog.String("error", err.Error()))
		return 1
	}
	runner := workerruntime.NewRunner(control, broker, registry, workerruntime.RunnerConfig{
		Registration: workers.Registration{
			SessionID: uuid.New(), Name: workerConfig.Name, Hostname: workerConfig.Hostname,
			WorkerGroup: workerConfig.WorkerGroup, ConcurrencyLimit: workerConfig.Concurrency,
			Capabilities: workerConfig.Capabilities,
		},
		Queue: workerConfig.Queue, PollWait: workerConfig.PollWait,
		RetryAttempts: 3, RetryDelay: 100 * time.Millisecond, ErrorBackoff: time.Second,
		ShutdownTimeout: workerConfig.ShutdownTimeout,
		// Liveness and renewal cadence come from validated shared configuration,
		// because the thresholds they race are enforced server-side.
		HeartbeatInterval: shared.HeartbeatInterval,
		SessionStaleAfter: shared.SessionStaleAfter,
		RenewInterval:     shared.LeaseRenewInterval,
	}, log)

	healthServer := newHealthServer(workerConfig.HealthAddr, runner, control, broker, log)
	listener, err := net.Listen("tcp", workerConfig.HealthAddr)
	if err != nil {
		log.Error("bind worker health server", slog.String("error", err.Error()))
		return 1
	}
	healthFailures := make(chan error, 1)
	go func() {
		if err := healthServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("worker health server failed", slog.String("error", err.Error()))
			healthFailures <- err
			stop()
		}
	}()

	runErr := runner.Run(ctx)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), workerConfig.ShutdownTimeout)
	defer cancel()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		log.Error("worker health server shutdown failed", slog.String("error", err.Error()))
		return 1
	}
	select {
	case err := <-healthFailures:
		log.Error("worker stopped after health server failure", slog.String("error", err.Error()))
		return 1
	default:
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		log.Error("worker stopped with error", slog.String("error", runErr.Error()))
		return 1
	}
	return 0
}
