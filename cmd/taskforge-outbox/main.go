// Command taskforge-outbox publishes pending outbox events to the broker.
//
// It runs as its own process so that delivery can be scaled, restarted, and
// reasoned about independently of the API. Multiple instances are safe: claiming
// is serialized by PostgreSQL.
package main

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/co-rtex/TaskForge/internal/config"
	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/outbox"
	"github.com/co-rtex/TaskForge/internal/queue/sqsbroker"
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
	log := telemetry.NewLogger(os.Stdout, cfg.LogLevel, "taskforge-outbox")
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

	broker, err := sqsbroker.New(ctx, sqsbroker.Options{
		Endpoint:        cfg.BrokerEndpoint,
		Region:          cfg.BrokerRegion,
		QueueName:       cfg.BrokerQueueName,
		AccessKeyID:     cfg.BrokerAccessKeyID,
		SecretAccessKey: cfg.BrokerSecretAccessKey,
	})
	if err != nil {
		log.Error("connect to broker", slog.String("error", err.Error()))
		return 1
	}
	log.Info("broker resolved", slog.String("queue_url", broker.QueueURL()))

	store := outbox.NewStore(pool)
	publisher := outbox.NewPublisher(store, broker, outbox.PublisherConfig{
		BatchSize:    cfg.OutboxBatchSize,
		PollInterval: cfg.OutboxPollInterval,
		ClaimTimeout: cfg.OutboxClaimTimeout,
		Backoff: outbox.BackoffPolicy{
			Base:       cfg.OutboxBackoffBase,
			Max:        cfg.OutboxBackoffMax,
			Multiplier: cfg.OutboxBackoffMultiplier,
			Jitter:     cfg.OutboxBackoffJitter,
		},
	}, log, rand.New(rand.NewSource(time.Now().UnixNano())))

	healthServer := newHealthServer(cfg.OutboxAddr, pool, broker, log)
	go func() {
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server failed", slog.String("error", err.Error()))
		}
	}()

	runErr := publisher.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		log.Error("health server shutdown failed", slog.String("error", err.Error()))
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		log.Error("publisher stopped with error", slog.String("error", runErr.Error()))
		return 1
	}
	return 0
}
