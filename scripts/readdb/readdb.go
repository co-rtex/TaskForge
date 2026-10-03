// Package readdb is the developer scripts' read-only view of TaskForge's
// control-plane database: a connection that refuses writes, and (in
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

	"github.com/jackc/pgx/v5"
)

// Open connects to PostgreSQL with default_transaction_read_only=on, so every
// transaction the connection starts, including the implicit one around a
// single statement, is read-only unless the caller deliberately asks otherwise.
//
// What this is, and what it is not. It is a guard against a script that writes
// by mistake: an INSERT, UPDATE, DELETE or TRUNCATE sent over this connection
// fails with SQLSTATE 25006. It is not a security boundary. The setting is the
// session's own, a statement can undo it (SET default_transaction_read_only =
// off, or BEGIN READ WRITE), and the role behind the connection is the same
// role the services use. The point is that no code in this repository does that.
func Open(ctx context.Context, databaseURL string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse the database URL: %w", err)
	}
	// RuntimeParams are sent in the startup packet, so the setting is in force
	// before the first statement rather than set afterwards by a statement that
	// could be skipped.
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	return conn, nil
}
