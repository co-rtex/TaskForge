package database

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/co-rtex/TaskForge/migrations"
)

// advisoryLockKey serializes concurrent migration runs. Two processes starting
// at once must not interleave DDL; the loser waits and then observes that every
// migration is already applied.
const advisoryLockKey int64 = 8010773204718151

// Migration is one numbered SQL file.
type Migration struct {
	Version  int
	Name     string
	SQL      string
	Checksum string
}

// LoadMigrations reads and orders the embedded migrations, rejecting malformed
// or duplicated version numbers rather than silently skipping them.
func LoadMigrations() ([]Migration, error) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}

	var out []Migration
	seen := map[int]string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		prefix, _, ok := strings.Cut(strings.TrimSuffix(e.Name(), ".sql"), "_")
		if !ok {
			return nil, fmt.Errorf("migration %q must be named NNNN_description.sql", e.Name())
		}
		version, err := strconv.Atoi(prefix)
		if err != nil {
			return nil, fmt.Errorf("migration %q has a non-numeric version prefix: %w", e.Name(), err)
		}
		if other, dup := seen[version]; dup {
			return nil, fmt.Errorf("duplicate migration version %d: %q and %q", version, other, e.Name())
		}
		seen[version] = e.Name()

		body, err := fs.ReadFile(migrations.FS, e.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", e.Name(), err)
		}
		sum := sha256.Sum256(body)
		out = append(out, Migration{
			Version:  version,
			Name:     e.Name(),
			SQL:      string(body),
			Checksum: hex.EncodeToString(sum[:]),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// Migrate applies every unapplied migration in order and returns how many ran.
//
// Each migration runs inside its own transaction, so a failure leaves the schema
// at the last fully applied version rather than half-migrated. Already-applied
// migrations have their checksum verified: editing a migration that has shipped
// is an error, not a silent no-op.
func Migrate(ctx context.Context, dsn string, log *slog.Logger) (int, error) {
	pending, err := LoadMigrations()
	if err != nil {
		return 0, err
	}
	if len(pending) == 0 {
		return 0, ErrNoMigrations
	}

	// Migration files contain multiple statements, which PostgreSQL's extended
	// query protocol rejects. The simple protocol allows them.
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return 0, fmt.Errorf("parse database url: %w", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return 0, fmt.Errorf("connect for migration: %w", err)
	}
	defer conn.Close(context.Background())

	// The lock is taken before anything is created. CREATE TABLE IF NOT EXISTS is
	// NOT atomic against a concurrent creation of the same table: two processes
	// starting together race on the table's implicit composite type and one fails
	// with a duplicate-key error on pg_type. Serializing first removes that race.
	// pg_advisory_lock needs no schema of its own, so it is safe to take first.
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockKey); err != nil {
		return 0, fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		// Best effort: the lock is released anyway when the connection closes.
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryLockKey)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    INTEGER     PRIMARY KEY,
			name       TEXT        NOT NULL,
			checksum   TEXT        NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return 0, fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := appliedMigrations(ctx, conn)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, m := range pending {
		if have, ok := applied[m.Version]; ok {
			if have != m.Checksum {
				return count, fmt.Errorf(
					"migration %d (%s) was modified after being applied: recorded checksum %s, file checksum %s; add a new migration instead of editing an applied one",
					m.Version, m.Name, have, m.Checksum)
			}
			continue
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return count, fmt.Errorf("begin migration %d: %w", m.Version, err)
		}
		if _, err := tx.Exec(ctx, m.SQL); err != nil {
			_ = tx.Rollback(ctx)
			return count, fmt.Errorf("apply migration %d (%s): %w", m.Version, m.Name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
			m.Version, m.Name, m.Checksum); err != nil {
			_ = tx.Rollback(ctx)
			return count, fmt.Errorf("record migration %d: %w", m.Version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return count, fmt.Errorf("commit migration %d: %w", m.Version, err)
		}

		count++
		if log != nil {
			log.Info("migration applied", slog.Int("version", m.Version), slog.String("name", m.Name))
		}
	}
	return count, nil
}

func appliedMigrations(ctx context.Context, conn *pgx.Conn) (map[int]string, error) {
	rows, err := conn.Query(ctx, `SELECT version, checksum FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	defer rows.Close()

	out := map[int]string{}
	for rows.Next() {
		var v int
		var sum string
		if err := rows.Scan(&v, &sum); err != nil {
			return nil, fmt.Errorf("scan schema_migrations: %w", err)
		}
		out[v] = sum
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schema_migrations: %w", err)
	}
	return out, nil
}

// ErrNoMigrations reports an empty embed, which almost always means the embed
// directive stopped matching the migrations directory.
var ErrNoMigrations = errors.New("no migrations were embedded")
