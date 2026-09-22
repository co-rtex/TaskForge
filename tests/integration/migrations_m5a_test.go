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

// TestMigrations_M5UpgradeAddsTwoEmptyCredentialTablesAndTouchesNothingElse
// rehearses the upgrade a running M4 deployment actually performs today: M5A's
// 0014 and M5B's 0015 together, since both are now pending from an M4
// baseline and a real deployment applies whatever is pending in one run.
//
// It is deliberately the simplest upgrade test in this repository, and that is
// the claim it is making. 0011, 0012 and 0013 each had to repair real rows, and
// each needed a per-row eligibility rule to avoid damaging correct data. 0014
// creates one empty table and 0015 creates a second empty table plus one
// additive nullable column: no earlier milestone ever persisted a worker
// credential, so there is nothing to reconstruct and nothing that could be
// reconstructed wrongly. This proves that rather than asserting it.
func TestMigrations_M5UpgradeAddsTwoEmptyCredentialTablesAndTouchesNothingElse(t *testing.T) {
	freshDSN := withFreshDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	migrations, err := database.LoadMigrations()
	require.NoError(t, err)
	require.Len(t, migrations, 18)
	require.Equal(t, 14, migrations[13].Version)
	require.Equal(t, "0014_api_keys.sql", migrations[13].Name)
	require.Equal(t, 15, migrations[14].Version)
	require.Equal(t, "0015_worker_keys.sql", migrations[14].Name)

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
	//
	// worker_sessions is deliberately NOT here: 0015 adds a real, additive
	// column to it (worker_key_id), so its row shape legitimately changes.
	// That column's effect is checked explicitly below instead of folded
	// into a digest that would only prove the column exists, not that every
	// pre-existing value survived untouched.
	// outbox_events is absent here and gets its own column-restricted digest
	// below, for exactly the reason worker_sessions already does: a later
	// migration (M6B's 0018) added columns to it, so a whole-row digest would
	// change on a pure schema addition and stop measuring what this test is
	// about, which is whether any ROW changed.
	tables := []string{
		"queues", "jobs", "workers", "job_attempts",
		"leases", "dlq_entries", "dlq_replays", "idempotency_records",
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
	// worker_sessions' own digest, restricted to the columns that existed
	// before 0015, so this still catches 0015 mutating a pre-existing value
	// even though it cannot use the same "whole row" comparison every other
	// table gets.
	workerSessionsColumns := `id, worker_id, scope, hostname, worker_group, concurrency_limit,
		capabilities, supported_job_types, status, registered_at, last_heartbeat_at`
	digestWorkerSessions := func(t *testing.T) string {
		t.Helper()
		var sum *string
		require.NoError(t, conn.QueryRow(ctx, fmt.Sprintf(
			`SELECT md5(string_agg(row_data, '|' ORDER BY row_data))
			 FROM (SELECT (%s)::text AS row_data FROM worker_sessions) rows`,
			workerSessionsColumns)).Scan(&sum))
		if sum == nil {
			return "<empty>"
		}
		return *sum
	}
	beforeWorkerSessions := digestWorkerSessions(t)

	// outbox_events, restricted to the columns that existed before 0018 added
	// traceparent and tracestate. Same reasoning as worker_sessions above.
	outboxColumns := `id, event_type, schema_version, payload, status, attempts,
		available_at, claimed_at, published_at, last_error, created_at,
		job_id, notification_generation`
	digestOutboxEvents := func(t *testing.T) string {
		t.Helper()
		var sum *string
		require.NoError(t, conn.QueryRow(ctx, fmt.Sprintf(
			`SELECT md5(string_agg(row_data, '|' ORDER BY row_data))
			 FROM (SELECT (%s)::text AS row_data FROM outbox_events) rows`,
			outboxColumns)).Scan(&sum))
		if sum == nil {
			return "<empty>"
		}
		return *sum
	}
	beforeOutboxEvents := digestOutboxEvents(t)

	// The upgrade itself, through the real runner.
	applied, err := database.Migrate(ctx, freshDSN, discardLogger())
	require.NoError(t, err, "0014, 0015, and 0016 must apply to a database holding real M4 data")
	require.Equal(t, 5, applied,
		"0014, 0015, 0016, 0017, and 0018 are pending from an M4 database")

	t.Run("worker_sessions.worker_key_id exists, is nullable, and is NULL on every pre-existing row", func(t *testing.T) {
		var nullCount, total int
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT count(*) FILTER (WHERE worker_key_id IS NULL), count(*) FROM worker_sessions`).
			Scan(&nullCount, &total))
		require.Positive(t, total, "the seeded M4 session must still be present")
		require.Equal(t, total, nullCount,
			"a session that predates worker keys has no credential to record, and must not have one invented")
	})

	t.Run("every pre-existing worker_sessions column is untouched", func(t *testing.T) {
		require.Equal(t, beforeWorkerSessions, digestWorkerSessions(t))
	})

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

	t.Run("worker_keys exists and starts empty", func(t *testing.T) {
		var exists bool
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			 WHERE table_schema='public' AND table_name='worker_keys')`).Scan(&exists))
		require.True(t, exists)

		var rows int
		require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM worker_keys`).Scan(&rows))
		require.Zero(t, rows,
			"there is no worker credential history to reconstruct, so this table starts empty on every upgrade path")
	})

	t.Run("not one M1 through M4 row changed", func(t *testing.T) {
		for _, table := range tables {
			require.Equalf(t, before[table], digest(t, table),
				"0014 or 0015 changed rows in %s; each must touch nothing but its own table", table)
		}
		require.Equal(t, beforeOutboxEvents, digestOutboxEvents(t),
			"0014 or 0015 changed rows in outbox_events")
	})

	t.Run("re-running the upgrade is a no-op", func(t *testing.T) {
		again, err := database.Migrate(ctx, freshDSN, discardLogger())
		require.NoError(t, err)
		require.Equal(t, 0, again, "a second run must apply nothing")

		for _, table := range tables {
			require.Equalf(t, before[table], digest(t, table), "a second run changed %s", table)
		}
		require.Equal(t, beforeWorkerSessions, digestWorkerSessions(t))
		require.Equal(t, beforeOutboxEvents, digestOutboxEvents(t),
			"a second run changed outbox_events")
		var apiKeyRows, workerKeyRows int
		require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM api_keys`).Scan(&apiKeyRows))
		require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM worker_keys`).Scan(&workerKeyRows))
		require.Zero(t, apiKeyRows)
		require.Zero(t, workerKeyRows)
	})

	t.Run("0014 and 0015 are recorded with the checksum of their own files", func(t *testing.T) {
		for _, migration := range []struct {
			version int
			name    string
			index   int
		}{
			{14, "0014_api_keys.sql", 13},
			{15, "0015_worker_keys.sql", 14},
		} {
			var recorded, name string
			require.NoError(t, conn.QueryRow(ctx,
				`SELECT name, checksum FROM schema_migrations WHERE version = $1`, migration.version).
				Scan(&name, &recorded))
			require.Equal(t, migration.name, name)
			require.Equal(t, migrations[migration.index].Checksum, recorded)
		}
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
