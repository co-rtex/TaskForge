//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/database"
)

// TestMigrations_M5AUpgradeAddsAnEmptyTableAndTouchesNothingElse rehearses the
// upgrade a running M4 deployment actually performs.
//
// It is deliberately the simplest upgrade test in this repository, and that is
// the claim it is making. 0011, 0012 and 0013 each had to repair real rows, and
// each needed a per-row eligibility rule to avoid damaging correct data. 0014
// creates one empty table: no earlier milestone ever persisted a credential, so
// there is nothing to reconstruct and nothing that could be reconstructed
// wrongly. This proves that rather than asserting it.
func TestMigrations_M5AUpgradeAddsAnEmptyTableAndTouchesNothingElse(t *testing.T) {
	freshDSN := withFreshDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	migrations, err := database.LoadMigrations()
	require.NoError(t, err)
	require.Len(t, migrations, 14)
	require.Equal(t, 14, migrations[13].Version)
	require.Equal(t, "0014_api_keys.sql", migrations[13].Name)

	cfg, err := pgx.ParseConfig(freshDSN)
	require.NoError(t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Stop at 0013: this database is exactly what M4 shipped, with its
	// schema_migrations rows written, so the upgrade below is the real runner's
	// upgrade of a real previous release rather than a hand-applied
	// approximation of one.
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    INTEGER     PRIMARY KEY,
			name       TEXT        NOT NULL,
			checksum   TEXT        NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	require.NoError(t, err)
	for _, migration := range migrations[:13] {
		require.NoErrorf(t, execMigration(ctx, conn, migration), "migration %d", migration.Version)
		_, err := conn.Exec(ctx,
			`INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
			migration.Version, migration.Name, migration.Checksum)
		require.NoError(t, err)
	}

	// Real M1-M4 data across every table an upgrade could plausibly disturb.
	var (
		queuedJob, deadJob  = uuid.New(), uuid.New()
		workerID, sessionID = uuid.New(), uuid.New()
		deadAttempt         = uuid.New()
	)
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO jobs (id, scope, queue, job_type, payload, status, priority,
		                  max_attempts, timeout_seconds, available_at, created_at,
		                  updated_at, notification_generation, last_notification_at)
		VALUES ('%[1]s', 'legacy-scope', 'default', 'demo.echo', '{"m":1}', 'QUEUED',
		        50, 3, 300, now(), now(), now(), 1, now()),
		       ('%[2]s', 'legacy-scope', 'default', 'demo.echo', '{"m":2}', 'DEAD_LETTERED',
		        50, 1, 300, now(), now(), now(), 1, now());

		INSERT INTO workers (id, scope, name) VALUES ('%[3]s', 'legacy-scope', 'legacy-worker');

		INSERT INTO worker_sessions (id, worker_id, scope, hostname, worker_group,
		                             concurrency_limit, capabilities, supported_job_types,
		                             status, registered_at, last_heartbeat_at)
		VALUES ('%[4]s', '%[3]s', 'legacy-scope', 'legacy.local', 'default', 4,
		        '{cpu}', '{demo.echo}', 'HEALTHY', now(), now());

		INSERT INTO job_attempts (id, job_id, scope, queue, attempt_number, worker_id,
		                          worker_session_id, status, created_at, started_at, finished_at)
		VALUES ('%[5]s', '%[2]s', 'legacy-scope', 'default', 1, '%[3]s', '%[4]s',
		        'FAILED', now(), now(), now());

		INSERT INTO dlq_entries (id, scope, queue, job_id, terminal_attempt_id, reason, created_at)
		VALUES (gen_random_uuid(), 'legacy-scope', 'default', '%[2]s', '%[5]s',
		        'ATTEMPTS_EXHAUSTED', now());

		INSERT INTO idempotency_records (scope, idempotency_key, job_id, request_fingerprint)
		VALUES ('legacy-scope', 'legacy-key', '%[1]s', repeat('a', 64));

		INSERT INTO outbox_events (id, event_type, schema_version, payload, status, created_at)
		VALUES (gen_random_uuid(), 'work.available', 1, '{"queue":"default"}', 'PENDING', now());`,
		queuedJob, deadJob, workerID, sessionID, deadAttempt))
	require.NoError(t, err)

	// A content digest per table, not just a row count: a migration that
	// rewrote a column in place would keep the counts identical.
	tables := []string{
		"queues", "jobs", "workers", "worker_sessions", "job_attempts",
		"leases", "dlq_entries", "dlq_replays", "idempotency_records", "outbox_events",
	}
	digest := func(t *testing.T, table string) string {
		t.Helper()
		var sum *string
		require.NoErrorf(t, conn.QueryRow(ctx, fmt.Sprintf(
			`SELECT md5(string_agg(row_data, '|' ORDER BY row_data))
			 FROM (SELECT %s::text AS row_data FROM %s) rows`, table, table)).Scan(&sum),
			"digest %s", table)
		if sum == nil {
			return "<empty>"
		}
		return *sum
	}

	before := map[string]string{}
	for _, table := range tables {
		before[table] = digest(t, table)
	}

	// The upgrade itself, through the real runner.
	applied, err := database.Migrate(ctx, freshDSN, discardLogger())
	require.NoError(t, err, "0014 must apply to a database holding real M4 data")
	require.Equal(t, 1, applied, "only 0014 is pending from an M4 database")

	t.Run("api_keys exists and starts empty", func(t *testing.T) {
		var exists bool
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			 WHERE table_schema='public' AND table_name='api_keys')`).Scan(&exists))
		require.True(t, exists)

		var rows int
		require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM api_keys`).Scan(&rows))
		require.Zero(t, rows,
			"there is no credential history to reconstruct, so this table starts empty on every upgrade path")
	})

	t.Run("not one M1 through M4 row changed", func(t *testing.T) {
		for _, table := range tables {
			require.Equalf(t, before[table], digest(t, table),
				"0014 changed rows in %s; it must touch nothing but its own table", table)
		}
	})

	t.Run("re-running the upgrade is a no-op", func(t *testing.T) {
		again, err := database.Migrate(ctx, freshDSN, discardLogger())
		require.NoError(t, err)
		require.Equal(t, 0, again, "a second run must apply nothing")

		for _, table := range tables {
			require.Equalf(t, before[table], digest(t, table), "a second run changed %s", table)
		}
		var rows int
		require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM api_keys`).Scan(&rows))
		require.Zero(t, rows)
	})

	t.Run("0014 is recorded with the checksum of its own file", func(t *testing.T) {
		var recorded, name string
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT name, checksum FROM schema_migrations WHERE version = 14`).Scan(&name, &recorded))
		require.Equal(t, "0014_api_keys.sql", name)
		require.Equal(t, migrations[13].Checksum, recorded)
	})

	// The legacy scope still owns its jobs. Nothing about authenticating the
	// public surface rewrites who existing rows belong to, which is the failure
	// that would silently hide every pre-upgrade job from its own operator.
	t.Run("existing jobs keep their scope", func(t *testing.T) {
		var scope string
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT scope FROM jobs WHERE id = $1`, queuedJob).Scan(&scope))
		require.Equal(t, "legacy-scope", scope)
	})
}
