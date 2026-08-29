// Package database owns the PostgreSQL connection pool and schema migrations.
//
// PostgreSQL is the authoritative store for all TaskForge control-plane state.
// See docs/adr/0001-postgresql-as-authoritative-state.md.
package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens a pooled connection and verifies it is usable before returning,
// so a misconfigured DSN fails at startup rather than on the first request.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// Ping checks database reachability under a caller-supplied bound. Readiness
// probes use it, so it must never block indefinitely.
func Ping(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return fmt.Errorf("no database pool")
	}
	return pool.Ping(ctx)
}
