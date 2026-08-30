//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/database"
)

// withFreshDatabase creates an empty database, hands back its DSN, and drops it
// afterwards. "Applies cleanly" has to mean from nothing, not from whatever the
// developer's database happens to contain.
func withFreshDatabase(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	name := "taskforge_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:24]

	admin, err := pgx.Connect(ctx, dsn())
	require.NoError(t, err)
	defer admin.Close(ctx)

	_, err = admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{name}.Sanitize()))
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := pgx.Connect(cleanupCtx, dsn())
		if err != nil {
			return
		}
		defer conn.Close(cleanupCtx)
		_, _ = conn.Exec(cleanupCtx,
			fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", pgx.Identifier{name}.Sanitize()))
	})

	return replaceDatabaseName(t, dsn(), name)
}

func replaceDatabaseName(t *testing.T, baseDSN, name string) string {
	t.Helper()
	cfg, err := pgx.ParseConfig(baseDSN)
	require.NoError(t, err)
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, name)
}

func TestMigrations_ApplyCleanlyToAFreshDatabase(t *testing.T) {
	freshDSN := withFreshDatabase(t)
	ctx := context.Background()

	applied, err := database.Migrate(ctx, freshDSN, discardLogger())
	require.NoError(t, err)
	require.Positive(t, applied)

	conn, err := pgx.Connect(ctx, freshDSN)
	require.NoError(t, err)
	defer conn.Close(ctx)

	t.Run("expected tables exist", func(t *testing.T) {
		for _, table := range []string{"queues", "jobs", "idempotency_records", "outbox_events", "schema_migrations"} {
			var exists bool
			require.NoError(t, conn.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM information_schema.tables
				 WHERE table_schema='public' AND table_name=$1)`, table).Scan(&exists))
			require.True(t, exists, "table %s is missing", table)
		}
	})

	// Tables for later milestones must not be created in advance.
	t.Run("no speculative tables", func(t *testing.T) {
		for _, table := range []string{"job_attempts", "workers", "worker_sessions", "leases", "results", "dlq_entries", "api_keys"} {
			var exists bool
			require.NoError(t, conn.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM information_schema.tables
				 WHERE table_schema='public' AND table_name=$1)`, table).Scan(&exists))
			require.False(t, exists, "table %s belongs to a later milestone and must not exist yet", table)
		}
	})

	t.Run("idempotency is enforced by a constraint", func(t *testing.T) {
		var isPrimary bool
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_constraint c
				JOIN pg_class t ON t.oid = c.conrelid
				WHERE t.relname = 'idempotency_records' AND c.contype = 'p'
			)`).Scan(&isPrimary))
		require.True(t, isPrimary, "(scope, idempotency_key) must be enforced by the database, not by application code")
	})

	t.Run("publisher scan index exists and is partial", func(t *testing.T) {
		var def string
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE indexname = 'outbox_events_due_idx'`).Scan(&def))
		require.Contains(t, def, "available_at")
		require.Contains(t, def, "WHERE (status = 'PENDING'::text)")
	})

	t.Run("the default queue is seeded", func(t *testing.T) {
		var n int
		require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM queues WHERE name = 'default'`).Scan(&n))
		require.Equal(t, 1, n)
	})

	t.Run("status check constraint covers the full state machine", func(t *testing.T) {
		var def string
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c
			JOIN pg_class t ON t.oid = c.conrelid
			WHERE t.relname = 'jobs' AND c.conname = 'jobs_status_check'`).Scan(&def))
		for _, s := range []string{"PENDING", "QUEUED", "LEASED", "RUNNING", "RETRY_WAIT",
			"CANCEL_REQUESTED", "SUCCEEDED", "CANCELED", "DEAD_LETTERED"} {
			require.Contains(t, def, s)
		}
		require.NotContains(t, def, "'FAILED'", "there is deliberately no job-level FAILED status")
	})
}

func TestMigrations_AreIdempotent(t *testing.T) {
	freshDSN := withFreshDatabase(t)
	ctx := context.Background()

	first, err := database.Migrate(ctx, freshDSN, discardLogger())
	require.NoError(t, err)
	require.Positive(t, first)

	second, err := database.Migrate(ctx, freshDSN, discardLogger())
	require.NoError(t, err)
	require.Equal(t, 0, second, "a second run must apply nothing")
}

// Two processes starting at once must not interleave DDL. The advisory lock
// makes the loser wait and then observe that everything is already applied.
func TestMigrations_AreSafeToRunConcurrently(t *testing.T) {
	freshDSN := withFreshDatabase(t)

	const runners = 4
	results := make(chan int, runners)
	errs := make(chan error, runners)
	start := make(chan struct{})

	for i := 0; i < runners; i++ {
		go func() {
			<-start
			applied, err := database.Migrate(context.Background(), freshDSN, discardLogger())
			if err != nil {
				errs <- err
				return
			}
			results <- applied
		}()
	}
	close(start)

	total := 0
	for i := 0; i < runners; i++ {
		select {
		case err := <-errs:
			t.Fatalf("concurrent migration failed: %v", err)
		case n := <-results:
			total += n
		case <-time.After(60 * time.Second):
			t.Fatal("concurrent migration timed out")
		}
	}
	require.Equal(t, 1, total, "exactly one runner may apply the migration set")
}

// Constraints, not application code, are the last line of defence. If these
// could be violated, every invariant built on top of them would be advisory.
func TestSchema_ConstraintsRejectInvalidRows(t *testing.T) {
	reset(t)
	ctx := context.Background()

	insert := func(t *testing.T, column, value string) error {
		t.Helper()
		cols := map[string]string{
			"status": "'QUEUED'", "priority": "50", "max_attempts": "3",
			"timeout_seconds": "300", "payload": `'{"a":1}'::jsonb`,
			"job_type": "'demo.echo'", "queue": "'default'", "scope": "'s'",
		}
		cols[column] = value
		_, err := testPool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO jobs (id, scope, queue, job_type, payload, status, priority, max_attempts, timeout_seconds)
			VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s)`,
			cols["scope"], cols["queue"], cols["job_type"], cols["payload"],
			cols["status"], cols["priority"], cols["max_attempts"], cols["timeout_seconds"]))
		return err
	}

	cases := map[string]struct{ column, value string }{
		"unknown status":       {"status", "'NOT_A_STATUS'"},
		"job-level FAILED":     {"status", "'FAILED'"},
		"priority above range": {"priority", "101"},
		"negative priority":    {"priority", "-1"},
		"zero max_attempts":    {"max_attempts", "0"},
		"zero timeout":         {"timeout_seconds", "0"},
		"timeout above a day":  {"timeout_seconds", "86401"},
		"payload not object":   {"payload", `'[1,2]'::jsonb`},
		"uppercase job_type":   {"job_type", "'Demo.Echo'"},
		"unknown queue":        {"queue", "'no-such-queue'"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			require.Error(t, insert(t, tc.column, tc.value))
		})
	}

	t.Run("a valid row is accepted", func(t *testing.T) {
		require.NoError(t, insert(t, "status", "'QUEUED'"))
	})
}

func TestSchema_OutboxPublishedStateMustBeConsistent(t *testing.T) {
	reset(t)
	ctx := context.Background()

	// A published event without a timestamp is contradictory and must be refused.
	_, err := testPool.Exec(ctx, `
		INSERT INTO outbox_events (id, event_type, schema_version, payload, status, published_at)
		VALUES (gen_random_uuid(), 'work.available', 1, '{}'::jsonb, 'PUBLISHED', NULL)`)
	require.Error(t, err)

	// So is a pending event that claims to have been published.
	_, err = testPool.Exec(ctx, `
		INSERT INTO outbox_events (id, event_type, schema_version, payload, status, published_at)
		VALUES (gen_random_uuid(), 'work.available', 1, '{}'::jsonb, 'PENDING', now())`)
	require.Error(t, err)
}
