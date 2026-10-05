// Package readdb is the developer scripts' read-only view of TaskForge's
// control-plane database: a handle that refuses writes, and (in
// measurements.go) the queries the benchmark harness and its tests share.
//
// It lives under scripts/ and not under internal/ because two different trees
// import it: scripts/internal/stack and scripts/bench, and tests/integration,
// which Go's internal-package rule would keep out of anything below
// scripts/internal. It is not part of the product: `make build` compiles
// ./cmd/... and nothing else.
package readdb

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// maxConns bounds the pool. The benchmark's watchdog, its fault injector and its
// main flow each read at their own pace, and nothing here is a load source, so a
// handful is plenty.
const maxConns = 8

// Open returns a pool of connections to PostgreSQL, every one opened with
// default_transaction_read_only=on, so every transaction on it, including the
// implicit one around a single statement, is read-only unless the caller
// deliberately asks otherwise.
//
// It is a pool and not one connection because the handle is shared. A run's
// watchdog, its fault injector and its main flow all read through it from their
// own goroutines, and a *pgx.Conn is not safe for that: it fails with "conn busy"
// on one goroutine and races inside its statement cache on another, which can
// kill the process outright ("fatal error: concurrent map writes").
//
// What the read-only setting is, and what it is not. It is a guard against a
// script that writes by mistake: an INSERT, UPDATE, DELETE or TRUNCATE sent over
// this handle fails with SQLSTATE 25006. It is not a security boundary. The
// setting is the session's own, a statement can undo it (SET
// default_transaction_read_only = off, or BEGIN READ WRITE), and the role behind
// the connection is the same role the services use. The point is that no code in
// this repository does that.
func Open(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse the database URL: %w", err)
	}
	// RuntimeParams are sent in the startup packet, so the setting is in force
	// before the first statement on every connection the pool ever opens, rather
	// than set afterwards by a statement that could be skipped.
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open the connection pool: %w", err)
	}
	// A pool connects lazily. Reach the database now, so a wrong URL or a stopped
	// PostgreSQL fails here with its own error and not at the first query.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	return pool, nil
}
