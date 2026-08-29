// Command taskforge-migrate applies TaskForge's database migrations.
//
// It is deliberately separate from the services: a service that migrates on
// startup makes rolling deploys race each other, and makes it impossible to
// review a schema change independently of a code change.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/co-rtex/TaskForge/internal/config"
	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/telemetry"
)

func main() {
	if err := config.LoadDotEnv(".env"); err != nil {
		panic(err)
	}
	cfg, err := config.Load()
	log := telemetry.NewLogger(os.Stdout, cfg.LogLevel, "taskforge-migrate")
	if err != nil {
		log.Error("configuration invalid", slog.String("error", err.Error()))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	applied, err := database.Migrate(ctx, cfg.DatabaseURL, log)
	if err != nil {
		log.Error("migration failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	if applied == 0 {
		log.Info("schema already up to date")
		return
	}
	log.Info("migrations complete", slog.Int("applied", applied))
}
