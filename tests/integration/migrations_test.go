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
	"github.com/jackc/pgx/v5/pgconn"
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
		for _, table := range []string{
			"queues", "jobs", "idempotency_records", "outbox_events",
			"workers", "worker_sessions", "job_attempts", "leases", "schema_migrations",
		} {
			var exists bool
			require.NoError(t, conn.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM information_schema.tables
				 WHERE table_schema='public' AND table_name=$1)`, table).Scan(&exists))
			require.True(t, exists, "table %s is missing", table)
		}
	})

	// Tables for later milestones must not be created in advance.
	t.Run("no speculative tables", func(t *testing.T) {
		for _, table := range []string{"results", "dlq_entries", "api_keys", "audit_events"} {
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

	t.Run("claim ordering column and partial index exist", func(t *testing.T) {
		var nullable, defaultValue string
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT is_nullable, column_default
			FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'jobs' AND column_name = 'available_at'`,
		).Scan(&nullable, &defaultValue))
		require.Equal(t, "NO", nullable)
		require.Contains(t, defaultValue, "now()")

		var def string
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE indexname = 'jobs_claim_idx'`).Scan(&def))
		require.Contains(t, def, "priority DESC")
		require.Contains(t, def, "available_at")
		require.Contains(t, def, "WHERE (status = 'QUEUED'::text)")
	})

	t.Run("one active lease per job is a partial unique invariant", func(t *testing.T) {
		var def string
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE indexname = 'leases_one_active_per_job_idx'`).Scan(&def))
		require.Contains(t, def, "UNIQUE")
		require.Contains(t, def, "WHERE (status = 'ACTIVE'::text)")
	})

	// Migration 0003 removed this index because M2 had no query that used it.
	// M3's reconciler scans exactly these columns with exactly this predicate, so
	// 0007 puts it back — and the test names the query that justifies it.
	t.Run("expired-lease scan index exists and is partial", func(t *testing.T) {
		var def string
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE indexname = 'leases_active_expiry_idx'`).Scan(&def))
		require.Contains(t, def, "expires_at")
		require.Contains(t, def, "id")
		require.Contains(t, def, "WHERE (status = 'ACTIVE'::text)")
	})

	t.Run("stale-heartbeat scan index exists and covers the current-session set", func(t *testing.T) {
		var def string
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE indexname = 'worker_sessions_current_heartbeat_idx'`).Scan(&def))
		require.Contains(t, def, "last_heartbeat_at")
		require.Contains(t, def, "id")
		// The same three statuses worker_sessions_one_current_per_worker_idx calls
		// current, so the scan and the uniqueness invariant cannot drift apart.
		for _, status := range []string{"STARTING", "HEALTHY", "DRAINING"} {
			require.Contains(t, def, status)
		}
	})

	t.Run("renewal identity is globally unique and partial", func(t *testing.T) {
		var def string
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE indexname = 'leases_last_renewal_request_id_idx'`).Scan(&def))
		require.Contains(t, def, "UNIQUE")
		require.Contains(t, def, "last_renewal_request_id")
		require.Contains(t, def, "IS NOT NULL")
	})

	t.Run("one current process session per logical worker is enforced", func(t *testing.T) {
		var def string
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE indexname = 'worker_sessions_one_current_per_worker_idx'`).Scan(&def))
		require.Contains(t, def, "UNIQUE")
		require.Contains(t, def, "HEALTHY")
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

// TestMigrations_CarryRealM1DataThroughEveryM2Migration is the upgrade-safety
// proof for an existing M1 deployment. It seeds representative real M1 rows
// after migration 0001 and then applies every M2 migration in order, so the
// backfill, the dropped speculative index, the timeline constraints, and the
// globally unique notification claim are all exercised against data that was
// already durable before M2 existed.
func TestMigrations_CarryRealM1DataThroughEveryM2Migration(t *testing.T) {
	freshDSN := withFreshDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	migrations, err := database.LoadMigrations()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(migrations), 7)
	for i, want := range []int{1, 2, 3, 4, 5, 6, 7} {
		require.Equal(t, want, migrations[i].Version, "migration order is versioned and deterministic")
	}

	cfg, err := pgx.ParseConfig(freshDSN)
	require.NoError(t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	defer conn.Close(context.Background())
	require.NoError(t, execMigration(ctx, conn, migrations[0]))

	// Representative real M1 ingress: a durable queued job, its idempotency
	// record, and its still-pending outbox notification.
	jobID, eventID := uuid.New(), uuid.New()
	secondJobID, secondEventID := uuid.New(), uuid.New()
	createdAt := time.Date(2026, 8, 28, 12, 34, 56, 123000000, time.UTC)
	laterAt := createdAt.Add(90 * time.Second)
	_, err = conn.Exec(ctx, `
		INSERT INTO jobs (
			id, scope, queue, job_type, payload, status, priority,
			max_attempts, timeout_seconds, created_at, updated_at
		) VALUES ($1, 'upgrade-test', 'default', 'demo.echo', '{"message":"preserve"}',
		          'QUEUED', 50, 3, 300, $2, $2);
		INSERT INTO jobs (
			id, scope, queue, job_type, payload, status, priority,
			max_attempts, timeout_seconds, created_at, updated_at
		) VALUES ($4, 'upgrade-test', 'default', 'demo.echo', '{"message":"second"}',
		          'QUEUED', 70, 3, 300, $5, $5);
		INSERT INTO idempotency_records (scope, idempotency_key, request_fingerprint, job_id)
		VALUES ('upgrade-test', 'existing-key', repeat('a', 64), $1);
		INSERT INTO outbox_events (id, event_type, schema_version, payload, status, created_at)
		VALUES ($3, 'work.available', 1, '{"queue":"default"}', 'PENDING', $2);
		INSERT INTO outbox_events (id, event_type, schema_version, payload, status, created_at, published_at)
		VALUES ($6, 'work.available', 1, '{"queue":"default"}', 'PUBLISHED', $5, $5);`,
		jobID, createdAt, eventID, secondJobID, laterAt, secondEventID)
	require.NoError(t, err)

	for _, migration := range migrations[1:7] {
		require.NoError(t, execMigration(ctx, conn, migration),
			"migration %04d must apply to a database holding real M1 data", migration.Version)
	}

	t.Run("0002 backfilled eligibility and routing without touching M1 state", func(t *testing.T) {
		var availableAt time.Time
		var status, workerGroup string
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT j.available_at, j.status, q.worker_group
			FROM jobs j JOIN queues q ON q.name = j.queue
			WHERE j.id = $1`, jobID).Scan(&availableAt, &status, &workerGroup))
		require.True(t, createdAt.Equal(availableAt), "available_at must preserve the original created_at instant")
		require.Equal(t, "QUEUED", status)
		require.Equal(t, "default", workerGroup)

		var secondAvailableAt time.Time
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT available_at FROM jobs WHERE id = $1`, secondJobID).Scan(&secondAvailableAt))
		require.True(t, laterAt.Equal(secondAvailableAt))
	})

	t.Run("all seeded M1 rows survive every M2 migration", func(t *testing.T) {
		var idempotencyJob uuid.UUID
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT job_id FROM idempotency_records
			WHERE scope = 'upgrade-test' AND idempotency_key = 'existing-key'`).Scan(&idempotencyJob))
		require.Equal(t, jobID, idempotencyJob)

		var pending, published string
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT status FROM outbox_events WHERE id = $1`, eventID).Scan(&pending))
		require.Equal(t, "PENDING", pending)
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT status FROM outbox_events WHERE id = $1`, secondEventID).Scan(&published))
		require.Equal(t, "PUBLISHED", published)

		var jobCount int
		require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&jobCount))
		require.Equal(t, 2, jobCount)
	})

	t.Run("0003 dropped the speculative index and 0007 restored it with a real query", func(t *testing.T) {
		var def string
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE indexname = 'leases_active_expiry_idx'`).Scan(&def))
		require.Contains(t, def, "expires_at")
		require.Contains(t, def, "WHERE (status = 'ACTIVE'::text)")
	})

	// The remaining assertions need real M2 rows on top of the upgraded M1 data.
	workerID, sessionID := uuid.New(), uuid.New()
	attemptID, leaseID := uuid.New(), uuid.New()
	_, err = conn.Exec(ctx, `
		INSERT INTO workers (id, scope, name) VALUES ($1, 'upgrade-test', 'upgrade-worker');
		INSERT INTO worker_sessions (
			id, worker_id, scope, hostname, worker_group, concurrency_limit,
			capabilities, supported_job_types, status
		) VALUES ($2, $1, 'upgrade-test', 'upgrade.local', 'default', 4,
		          '{cpu}', '{demo.echo}', 'HEALTHY');`, workerID, sessionID)
	require.NoError(t, err)

	t.Run("0004 makes control-timeline order a hard invariant", func(t *testing.T) {
		for name, statement := range map[string]string{
			"worker session heartbeat before registration": `
				UPDATE worker_sessions
				SET last_heartbeat_at = registered_at - interval '1 second'
				WHERE id = '` + sessionID.String() + `'`,
			"worker session ended before registration": `
				UPDATE worker_sessions
				SET ended_at = registered_at - interval '1 second'
				WHERE id = '` + sessionID.String() + `'`,
		} {
			t.Run(name, func(t *testing.T) {
				_, err := conn.Exec(ctx, statement)
				require.Error(t, err, "the timeline constraint must reject this row")
			})
		}
	})

	_, err = conn.Exec(ctx, `
		INSERT INTO job_attempts (
			id, job_id, scope, queue, attempt_number, worker_id, worker_session_id, status
		) VALUES ($1, $2, 'upgrade-test', 'default', 1, $3, $4, 'LEASED');`,
		attemptID, jobID, workerID, sessionID)
	require.NoError(t, err)

	t.Run("0004 isolates leases_timeline_order from the 0002 lease constraints", func(t *testing.T) {
		// acquired < expires < renewed. That satisfies every constraint migration
		// 0002 created — expires_at > acquired_at, and status ACTIVE with a NULL
		// released_at — and violates only migration 0004's requirement that the
		// expiry follow the last renewal. Without this ordering the row would trip
		// the older leases_expiry_after_acquisition check and prove nothing about
		// 0004.
		insertLease := func(acquired, renewed, expires string) error {
			_, err := conn.Exec(ctx, fmt.Sprintf(`
				INSERT INTO leases (
					id, job_id, attempt_id, scope, queue, worker_id, worker_session_id,
					claim_request_id, status, acquired_at, renewed_at, expires_at
				) VALUES ('%s', '%s', '%s', 'upgrade-test', 'default', '%s', '%s', '%s',
				          'ACTIVE', now() %s, now() %s, now() %s)`,
				uuid.New(), jobID, attemptID, workerID, sessionID, uuid.New(),
				acquired, renewed, expires))
			return err
		}

		err := insertLease("- interval '10 seconds'", "+ interval '10 seconds'", "+ interval '5 seconds'")
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "23514", pgErr.Code, "a timeline violation is a check-constraint violation")
		require.Equal(t, "leases_timeline_order", pgErr.ConstraintName,
			"the row must fail on 0004's constraint, not on a 0002 lease constraint")

		// Positive control: the same shape with renewal before expiry is accepted,
		// which proves the rejection above was caused only by the timeline order.
		require.NoError(t, insertLease(
			"- interval '10 seconds'", "- interval '5 seconds'", "+ interval '5 minutes'"))
		_, err = conn.Exec(ctx, `DELETE FROM leases WHERE job_id = $1`, jobID)
		require.NoError(t, err)
	})

	claimRequestID := uuid.New()
	_, err = conn.Exec(ctx, `
		INSERT INTO leases (
			id, job_id, attempt_id, scope, queue, worker_id, worker_session_id,
			claim_request_id, status, acquired_at, renewed_at, expires_at
		) VALUES ($1, $2, $3, 'upgrade-test', 'default', $4, $5, $6, 'ACTIVE',
		          now(), now(), now() + interval '5 minutes')`,
		leaseID, jobID, attemptID, workerID, sessionID, claimRequestID)
	require.NoError(t, err)

	t.Run("0005 makes notification claims globally idempotent", func(t *testing.T) {
		var perSessionConstraint, globalConstraint bool
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM pg_constraint
			               WHERE conname = 'leases_worker_session_id_claim_request_id_key')`,
		).Scan(&perSessionConstraint))
		require.False(t, perSessionConstraint, "per-session claim uniqueness must be gone")
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM pg_constraint
			               WHERE conname = 'leases_claim_request_id_key' AND contype = 'u')`,
		).Scan(&globalConstraint))
		require.True(t, globalConstraint, "claim ids must be unique across every worker session")

		// A second, different session cannot consume the same durable event id.
		otherWorkerID, otherSessionID, otherAttemptID := uuid.New(), uuid.New(), uuid.New()
		_, err := conn.Exec(ctx, `
			INSERT INTO workers (id, scope, name) VALUES ($1, 'upgrade-test', 'upgrade-worker-two');
			INSERT INTO worker_sessions (
				id, worker_id, scope, hostname, worker_group, concurrency_limit,
				capabilities, supported_job_types, status
			) VALUES ($2, $1, 'upgrade-test', 'upgrade2.local', 'default', 4,
			          '{cpu}', '{demo.echo}', 'HEALTHY');
			INSERT INTO job_attempts (
				id, job_id, scope, queue, attempt_number, worker_id, worker_session_id, status
			) VALUES ($3, $4, 'upgrade-test', 'default', 1, $1, $2, 'LEASED');`,
			otherWorkerID, otherSessionID, otherAttemptID, secondJobID)
		require.NoError(t, err)

		_, err = conn.Exec(ctx, `
			INSERT INTO leases (
				id, job_id, attempt_id, scope, queue, worker_id, worker_session_id,
				claim_request_id, status, acquired_at, renewed_at, expires_at
			) VALUES ($1, $2, $3, 'upgrade-test', 'default', $4, $5, $6, 'ACTIVE',
			          now(), now(), now() + interval '5 minutes')`,
			uuid.New(), secondJobID, otherAttemptID, otherWorkerID, otherSessionID, claimRequestID)
		require.Error(t, err, "one outbox event may consume at most one claim globally")
	})

	t.Run("0006 defaults every existing lease to generation zero", func(t *testing.T) {
		var version int
		var identity *uuid.UUID
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT renewal_version, last_renewal_request_id FROM leases WHERE id = $1`,
			leaseID).Scan(&version, &identity))
		require.Equal(t, 0, version, "a lease that was never renewed is generation 0")
		require.Nil(t, identity, "generation 0 records no renewal identity")
	})

	// The constraint, not application code, is what makes "renewed" and "records
	// who renewed it" inseparable. Either half alone would make a replay
	// undetectable and let an ambiguous retry extend authority twice.
	t.Run("0006 rejects inconsistent renewal identity and generation", func(t *testing.T) {
		for name, statement := range map[string]string{
			"a generation with no identity": `
				UPDATE leases SET renewal_version = 1
				WHERE id = '` + leaseID.String() + `'`,
			"an identity with no generation": `
				UPDATE leases SET last_renewal_request_id = '` + uuid.New().String() + `'
				WHERE id = '` + leaseID.String() + `'`,
		} {
			t.Run(name, func(t *testing.T) {
				_, err := conn.Exec(ctx, statement)
				require.Error(t, err)
				var pgErr *pgconn.PgError
				require.ErrorAs(t, err, &pgErr)
				require.Equal(t, "23514", pgErr.Code)
				require.Equal(t, "leases_renewal_identity_consistent", pgErr.ConstraintName)
			})
		}

		// Positive control: setting both together is accepted, which proves the
		// rejections above were caused only by the inconsistency.
		identity := uuid.New()
		_, err := conn.Exec(ctx, `
			UPDATE leases SET renewal_version = 1, last_renewal_request_id = '`+identity.String()+`'
			WHERE id = '`+leaseID.String()+`'`)
		require.NoError(t, err)

		// And one renewal identity cannot be recorded on two different leases.
		// This needs a genuine second lease row, on a different job so the
		// one-active-lease-per-job index is not what rejects it.
		otherAttempt, otherLease := uuid.New(), uuid.New()
		_, err = conn.Exec(ctx, `
			INSERT INTO job_attempts (
				id, job_id, scope, queue, attempt_number, worker_id, worker_session_id, status
			) VALUES ('`+otherAttempt.String()+`', '`+secondJobID.String()+`', 'upgrade-test',
			          'default', 2, '`+workerID.String()+`', '`+sessionID.String()+`', 'LEASED');
			INSERT INTO leases (
				id, job_id, attempt_id, scope, queue, worker_id, worker_session_id,
				claim_request_id, status, acquired_at, renewed_at, expires_at
			) VALUES ('`+otherLease.String()+`', '`+secondJobID.String()+`', '`+otherAttempt.String()+`',
			          'upgrade-test', 'default', '`+workerID.String()+`', '`+sessionID.String()+`',
			          '`+uuid.New().String()+`', 'ACTIVE', now(), now(), now() + interval '5 minutes')`)
		require.NoError(t, err)

		_, err = conn.Exec(ctx, `
			UPDATE leases SET renewal_version = 1, last_renewal_request_id = '`+identity.String()+`'
			WHERE id = '`+otherLease.String()+`'`)
		require.Error(t, err, "a renewal identity is globally unique across leases")
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "23505", pgErr.Code)

		// Positive control: a distinct identity on that same second lease is fine,
		// so the rejection above was about identity reuse and nothing else.
		_, err = conn.Exec(ctx, `
			UPDATE leases SET renewal_version = 1, last_renewal_request_id = '`+uuid.New().String()+`'
			WHERE id = '`+otherLease.String()+`'`)
		require.NoError(t, err)

		_, err = conn.Exec(ctx, `
			DELETE FROM leases WHERE id = '`+otherLease.String()+`';
			DELETE FROM job_attempts WHERE id = '`+otherAttempt.String()+`';
			UPDATE leases SET renewal_version = 0, last_renewal_request_id = NULL
			WHERE id = '`+leaseID.String()+`'`)
		require.NoError(t, err)
	})

	t.Run("one active lease per job survives the upgrade", func(t *testing.T) {
		secondAttemptID := uuid.New()
		_, err := conn.Exec(ctx, `
			INSERT INTO job_attempts (
				id, job_id, scope, queue, attempt_number, worker_id, worker_session_id, status
			) VALUES ($1, $2, 'upgrade-test', 'default', 2, $3, $4, 'LEASED')`,
			secondAttemptID, jobID, workerID, sessionID)
		require.NoError(t, err)
		_, err = conn.Exec(ctx, `
			INSERT INTO leases (
				id, job_id, attempt_id, scope, queue, worker_id, worker_session_id,
				claim_request_id, status, acquired_at, renewed_at, expires_at
			) VALUES ($1, $2, $3, 'upgrade-test', 'default', $4, $5, $6, 'ACTIVE',
			          now(), now(), now() + interval '5 minutes')`,
			uuid.New(), jobID, secondAttemptID, workerID, sessionID, uuid.New())
		require.Error(t, err, "a job may never hold two active leases")
	})
}

func execMigration(ctx context.Context, conn *pgx.Conn, migration database.Migration) error {
	_, err := conn.Exec(ctx, migration.SQL)
	return err
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
	migrations, err := database.LoadMigrations()
	require.NoError(t, err)
	require.Equal(t, len(migrations), total, "exactly one runner may apply each migration")
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

// TestSchema_CompositeForeignKeysRejectMismatchedBindings is the database-level
// proof of reliability invariant 4: an attempt belongs to exactly one job and
// one worker process session, and a lease belongs to exactly one of those
// attempts. Application code is not the enforcement point — these composite
// foreign keys are, so each one gets a row that satisfies every other
// constraint and fails only on the binding under test.
func TestSchema_CompositeForeignKeysRejectMismatchedBindings(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()

	firstJob := createJob(t, "binding-job-one", "demo.echo", 60, nil)
	secondJob := createJob(t, "binding-job-two", "demo.echo", 50, nil)
	firstSession := registerWorker(t, store,
		workerRegistration("binding-worker-one", 2, nil, []string{"demo.echo"}))
	secondSession := registerWorker(t, store,
		workerRegistration("binding-worker-two", 2, nil, []string{"demo.echo"}))

	insertAttempt := func(id, jobID uuid.UUID, queue string, workerID, sessionID uuid.UUID, number int) error {
		_, err := testPool.Exec(ctx, `
			INSERT INTO job_attempts (
				id, job_id, scope, queue, attempt_number, worker_id, worker_session_id, status
			) VALUES ($1, $2, $3, $4, $5, $6, $7, 'LEASED')`,
			id, jobID, testScope, queue, number, workerID, sessionID)
		return err
	}
	requireConstraintViolation := func(t *testing.T, err error, constraint string) {
		t.Helper()
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "23503", pgErr.Code, "a mismatched binding must be a foreign-key violation")
		require.Equal(t, constraint, pgErr.ConstraintName)
	}

	t.Run("an attempt cannot bind a session to the wrong logical worker", func(t *testing.T) {
		// firstSession exists and secondSession's worker exists, but the pair
		// (firstSession.ID, secondSession.WorkerID, scope) never does.
		err := insertAttempt(uuid.New(), firstJob, "default",
			secondSession.WorkerID, firstSession.ID, 1)
		requireConstraintViolation(t, err, "job_attempts_session_fkey")
	})

	t.Run("an attempt cannot bind a session id that does not exist", func(t *testing.T) {
		err := insertAttempt(uuid.New(), firstJob, "default",
			firstSession.WorkerID, uuid.New(), 1)
		requireConstraintViolation(t, err, "job_attempts_session_fkey")
	})

	t.Run("an attempt cannot bind a real job to the wrong queue", func(t *testing.T) {
		err := insertAttempt(uuid.New(), firstJob, "other-queue",
			firstSession.WorkerID, firstSession.ID, 1)
		requireConstraintViolation(t, err, "job_attempts_job_fkey")
	})

	// A well-formed attempt for the lease-binding cases below.
	attemptID := uuid.New()
	require.NoError(t, insertAttempt(attemptID, firstJob, "default",
		firstSession.WorkerID, firstSession.ID, 1))

	insertLease := func(jobID, attemptID, workerID, sessionID uuid.UUID) error {
		_, err := testPool.Exec(ctx, `
			INSERT INTO leases (
				id, job_id, attempt_id, scope, queue, worker_id, worker_session_id,
				claim_request_id, status, acquired_at, renewed_at, expires_at
			) VALUES ($1, $2, $3, $4, 'default', $5, $6, $7, 'ACTIVE',
			          now(), now(), now() + interval '5 minutes')`,
			uuid.New(), jobID, attemptID, testScope, workerID, sessionID, uuid.New())
		return err
	}

	t.Run("a lease cannot point at another job's attempt", func(t *testing.T) {
		// secondJob exists, so leases_job_fkey is satisfied; only the composite
		// attempt binding (attempt, secondJob, ...) is impossible.
		err := insertLease(secondJob, attemptID, firstSession.WorkerID, firstSession.ID)
		requireConstraintViolation(t, err, "leases_attempt_binding_fkey")
	})

	t.Run("a lease cannot reassign an attempt to another worker session", func(t *testing.T) {
		// secondSession is a real current session, so leases_session_fkey is
		// satisfied; the attempt was never bound to it.
		err := insertLease(firstJob, attemptID, secondSession.WorkerID, secondSession.ID)
		requireConstraintViolation(t, err, "leases_attempt_binding_fkey")
	})

	t.Run("a lease cannot reference an attempt that does not exist", func(t *testing.T) {
		err := insertLease(firstJob, uuid.New(), firstSession.WorkerID, firstSession.ID)
		requireConstraintViolation(t, err, "leases_attempt_binding_fkey")
	})

	t.Run("the correctly bound lease is accepted", func(t *testing.T) {
		require.NoError(t, insertLease(firstJob, attemptID, firstSession.WorkerID, firstSession.ID))
	})
}
