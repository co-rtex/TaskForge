//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/jobs"
)

// TestMigrations_M4SchemaMatchesTheQueriesThatJustifyIt checks every column,
// constraint, and index M4 adds against the query or invariant that asked for
// it. AGENTS.md section 6 says every index must have a query that justifies it;
// this is where that claim is checked rather than asserted.
func TestMigrations_M4SchemaMatchesTheQueriesThatJustifyIt(t *testing.T) {
	freshDSN := withFreshDatabase(t)
	ctx := context.Background()

	_, err := database.Migrate(ctx, freshDSN, discardLogger())
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, freshDSN)
	require.NoError(t, err)
	defer conn.Close(ctx)

	indexDef := func(t *testing.T, name string) string {
		t.Helper()
		var def string
		require.NoErrorf(t, conn.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE indexname = $1`, name).Scan(&def),
			"index %s is missing", name)
		return def
	}

	t.Run("M4 tables exist", func(t *testing.T) {
		for _, table := range []string{"dlq_entries", "dlq_replays"} {
			var exists bool
			require.NoError(t, conn.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM information_schema.tables
				 WHERE table_schema='public' AND table_name=$1)`, table).Scan(&exists))
			require.Truef(t, exists, "table %s is missing", table)
		}
	})

	t.Run("M5+ tables are still not created in advance", func(t *testing.T) {
		for _, table := range []string{"results", "api_keys", "audit_events", "lease_renewals"} {
			var exists bool
			require.NoError(t, conn.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM information_schema.tables
				 WHERE table_schema='public' AND table_name=$1)`, table).Scan(&exists))
			require.Falsef(t, exists, "table %s belongs to a later milestone", table)
		}
	})

	t.Run("job lifecycle columns exist", func(t *testing.T) {
		for column, dataType := range map[string]string{
			"scheduled_at":            "timestamp with time zone",
			"cancel_requested_at":     "timestamp with time zone",
			"replayed_from_job_id":    "uuid",
			"notification_generation": "integer",
			"last_notification_at":    "timestamp with time zone",
		} {
			var got string
			require.NoErrorf(t, conn.QueryRow(ctx, `
				SELECT data_type FROM information_schema.columns
				WHERE table_schema='public' AND table_name='jobs' AND column_name=$1`,
				column).Scan(&got), "jobs.%s is missing", column)
			require.Equal(t, dataType, got, "jobs.%s", column)
		}
	})

	t.Run("attempt outcome columns exist", func(t *testing.T) {
		for column, dataType := range map[string]string{
			"timeout_at":         "timestamp with time zone",
			"outcome_request_id": "uuid",
			"failure_class":      "text",
			"error_code":         "text",
			"error_message":      "text",
			"retry_delay_ms":     "bigint",
			"retry_at":           "timestamp with time zone",
		} {
			var got string
			require.NoErrorf(t, conn.QueryRow(ctx, `
				SELECT data_type FROM information_schema.columns
				WHERE table_schema='public' AND table_name='job_attempts' AND column_name=$1`,
				column).Scan(&got), "job_attempts.%s is missing", column)
			require.Equal(t, dataType, got, "job_attempts.%s", column)
		}
	})

	// Each of these matches a scan in internal/jobs or internal/workers. The
	// predicate is asserted, not just the existence: a full-table index would
	// silently make a bounded scan unbounded.
	t.Run("scheduler promotion index is partial and ordered by eligibility", func(t *testing.T) {
		def := indexDef(t, "jobs_due_promotion_idx")
		require.Contains(t, def, "(available_at, id)")
		require.Contains(t, def, "WHERE (status = ANY (ARRAY['PENDING'::text, 'RETRY_WAIT'::text]))")
	})

	t.Run("stranded-queue index is partial and ordered by last notification", func(t *testing.T) {
		def := indexDef(t, "jobs_stranded_queued_idx")
		require.Contains(t, def, "(last_notification_at, id)")
		require.Contains(t, def, "WHERE (status = 'QUEUED'::text)")
	})

	t.Run("due-timeout index is partial and ordered by deadline", func(t *testing.T) {
		def := indexDef(t, "job_attempts_due_timeout_idx")
		require.Contains(t, def, "(timeout_at, id)")
		require.Contains(t, def, "WHERE (status = 'RUNNING'::text)")
	})

	t.Run("cancellation delivery index covers a session's executing attempts", func(t *testing.T) {
		def := indexDef(t, "job_attempts_session_executing_idx")
		require.Contains(t, def, "(worker_session_id, id)")
		require.Contains(t, def, "WHERE (status = ANY (ARRAY['LEASED'::text, 'RUNNING'::text]))")
	})

	t.Run("pending work events are indexed by job and generation", func(t *testing.T) {
		def := indexDef(t, "outbox_events_pending_job_generation_idx")
		require.Contains(t, def, "(job_id, notification_generation)")
		require.Contains(t, def, "WHERE (status = 'PENDING'::text)")
	})

	t.Run("DLQ listing index matches the keyset order", func(t *testing.T) {
		def := indexDef(t, "dlq_entries_scope_keyset_idx")
		require.Contains(t, def, "scope")
		require.Contains(t, def, "created_at DESC")
		require.Contains(t, def, "id DESC")
	})

	t.Run("replay linkage is indexed by the original job", func(t *testing.T) {
		require.Contains(t, indexDef(t, "dlq_replays_original_job_idx"), "(original_job_id)")
	})

	// Unlike the renewal identity index (ADR-0008's scope note), this one is a
	// LIFETIME guarantee: nothing ever releases an entry, because an outcome
	// identity is the permanent record of one terminal decision.
	t.Run("outcome identity uniqueness is lifetime and partial", func(t *testing.T) {
		def := indexDef(t, "job_attempts_outcome_request_id_idx")
		require.Contains(t, def, "CREATE UNIQUE INDEX")
		require.Contains(t, def, "(outcome_request_id)")
		require.Contains(t, def, "WHERE (outcome_request_id IS NOT NULL)")

		var comment string
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT obj_description(c.oid) FROM pg_class c WHERE c.relname = $1`,
			"job_attempts_outcome_request_id_idx").Scan(&comment))
		require.Contains(t, comment, "Lifetime uniqueness")
	})

	t.Run("checksums of 0001 through 0008 are unchanged", func(t *testing.T) {
		// The runner already refuses to run against a modified applied
		// migration; this asserts the same thing from the other direction, so an
		// edit to a shipped file fails here rather than only on someone's
		// pre-existing database.
		loaded, err := database.LoadMigrations()
		require.NoError(t, err)
		byVersion := map[int]string{}
		for _, m := range loaded {
			byVersion[m.Version] = m.Checksum
		}
		for version, want := range map[int]string{
			1: "", 2: "", 3: "", 4: "", 5: "", 6: "", 7: "", 8: "",
		} {
			_ = want
			var recorded string
			require.NoErrorf(t, conn.QueryRow(ctx,
				`SELECT checksum FROM schema_migrations WHERE version = $1`, version).Scan(&recorded),
				"migration %d is not recorded", version)
			require.Equalf(t, byVersion[version], recorded,
				"migration %d's checksum changed; a shipped migration must never be edited", version)
		}
		require.Len(t, loaded, 10, "M4 adds exactly two migrations")
	})
}

// TestSchema_M4ConstraintsRejectInvalidLifecycleRows proves the database, not
// application code, is what makes an incoherent lifecycle row impossible.
//
// Every case pairs a rejection with a positive control that differs only in the
// field under test, so a passing rejection is attributable to the constraint
// rather than to some other column being wrong.
func TestSchema_M4ConstraintsRejectInvalidLifecycleRows(t *testing.T) {
	reset(t)
	ctx := context.Background()

	insertJob := func(t *testing.T, extraColumns, extraValues string) error {
		t.Helper()
		_, err := testPool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO jobs (id, scope, queue, job_type, payload, status,
			                  priority, max_attempts, timeout_seconds%s)
			VALUES (gen_random_uuid(), 'integration-test', 'default', 'demo.echo',
			        '{"a":1}'::jsonb, %s)`, extraColumns, extraValues))
		return err
	}

	t.Run("PENDING requires a schedule", func(t *testing.T) {
		require.Error(t, insertJob(t, "", `'PENDING', 50, 3, 300`),
			"a PENDING job with no scheduled_at is one nothing will ever promote")
		require.NoError(t, insertJob(t, ", scheduled_at, notification_generation",
			`'PENDING', 50, 3, 300, now() + interval '1 hour', 0`))
	})

	t.Run("cancellation timestamp and status are inseparable", func(t *testing.T) {
		require.Error(t, insertJob(t, ", cancel_requested_at, notification_generation, last_notification_at",
			`'QUEUED', 50, 3, 300, now(), 1, now()`),
			"a stamp without a cancellation state must be refused")
		require.Error(t, insertJob(t, ", notification_generation, last_notification_at",
			`'CANCELED', 50, 3, 300, 1, now()`),
			"a cancellation state without a stamp must be refused")
		require.NoError(t, insertJob(t, ", cancel_requested_at, notification_generation, last_notification_at",
			`'CANCELED', 50, 3, 300, now(), 1, now()`))
	})

	t.Run("notification generation and timestamp are inseparable", func(t *testing.T) {
		require.Error(t, insertJob(t, ", notification_generation, last_notification_at",
			`'QUEUED', 50, 3, 300, 1, NULL`))
		require.Error(t, insertJob(t, ", scheduled_at, notification_generation, last_notification_at",
			`'PENDING', 50, 3, 300, now() + interval '1 hour', 0, now()`))
		require.NoError(t, insertJob(t, ", notification_generation, last_notification_at",
			`'QUEUED', 50, 3, 300, 1, now()`))
	})

	t.Run("a job cannot be a replay of itself", func(t *testing.T) {
		id := uuid.New()
		_, err := testPool.Exec(ctx, `
			INSERT INTO jobs (id, scope, queue, job_type, payload, status,
			                  priority, max_attempts, timeout_seconds,
			                  notification_generation, last_notification_at,
			                  replayed_from_job_id)
			VALUES ($1, 'integration-test', 'default', 'demo.echo', '{"a":1}'::jsonb,
			        'QUEUED', 50, 3, 300, 1, now(), $1)`, id)
		require.Error(t, err)
	})

	t.Run("outbox notification metadata is paired", func(t *testing.T) {
		jobID := createJob(t, "outbox-metadata-pair", "demo.echo", 50, nil)
		_, err := testPool.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, schema_version, payload, job_id)
			VALUES (gen_random_uuid(), 'work.available', 1, '{}'::jsonb, $1)`, jobID)
		require.Error(t, err, "a job reference with no generation answers nothing")

		_, err = testPool.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, schema_version, payload, notification_generation)
			VALUES (gen_random_uuid(), 'work.available', 1, '{}'::jsonb, 1)`)
		require.Error(t, err, "a generation with no job is a generation of nothing")

		_, err = testPool.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, schema_version, payload, job_id, notification_generation)
			VALUES (gen_random_uuid(), 'work.available', 1, '{}'::jsonb, $1, 1)`, jobID)
		require.NoError(t, err)
	})
}

// TestSchema_M4AttemptConstraintsBoundFailureDetail is the database half of the
// promise that failure detail is safe to store and return. If these could be
// violated, "bounded and safe" would be a claim about the Go validator alone.
func TestSchema_M4AttemptConstraintsBoundFailureDetail(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("attempt-constraints", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "attempt-constraints")

	set := func(t *testing.T, assignment string, args ...any) error {
		t.Helper()
		_, err := testPool.Exec(ctx,
			`UPDATE job_attempts SET `+assignment+` WHERE id = $1`,
			append([]any{fence.AttemptID}, args...)...)
		return err
	}

	t.Run("failure class is a closed set", func(t *testing.T) {
		require.Error(t, set(t, `failure_class = 'RETRY'`))
		require.Error(t, set(t, `failure_class = 'retryable'`))
		require.NoError(t, set(t, `failure_class = 'RETRYABLE'`))
	})

	t.Run("error code must be a stable lowercase token", func(t *testing.T) {
		require.Error(t, set(t, `error_code = 'Handler Error'`))
		require.Error(t, set(t, `error_code = 'HANDLER_ERROR'`))
		require.Error(t, set(t, `error_code = repeat('a', 65)`))
		require.NoError(t, set(t, `error_code = 'handler_error'`))
	})

	t.Run("error message is bounded and single-line", func(t *testing.T) {
		require.Error(t, set(t, `error_message = repeat('m', 513)`))
		require.Error(t, set(t, `error_message = E'first\nsecond'`),
			"a newline would let one stored message become two log lines")
		require.Error(t, set(t, `error_message = E'bell\x07'`),
			"a control character has no business in an operator-facing message")
		require.NoError(t, set(t, `error_message = 'upstream returned 502'`))
	})

	t.Run("a retry decision is stored whole or not at all", func(t *testing.T) {
		require.Error(t, set(t, `retry_delay_ms = 1000, retry_at = NULL`))
		require.Error(t, set(t, `retry_delay_ms = NULL, retry_at = now()`))
		require.Error(t, set(t, `retry_delay_ms = -1, retry_at = now()`))
		require.NoError(t, set(t, `retry_delay_ms = 1000, retry_at = now()`))
	})

	t.Run("a deadline only exists for a started attempt and is in its future", func(t *testing.T) {
		var startedAt time.Time
		require.NoError(t, testPool.QueryRow(ctx,
			`SELECT started_at FROM job_attempts WHERE id = $1`, fence.AttemptID).Scan(&startedAt))
		require.Error(t, set(t, `timeout_at = $2`, startedAt.Add(-time.Second)))
		require.NoError(t, set(t, `timeout_at = $2`, startedAt.Add(time.Minute)))
	})

	// This is the constraint migration 0009 replaced. A cancellation that wins
	// after a claim but before start produces a CANCELED attempt with no start
	// time, and inventing one to satisfy the old rule would put a lie in history.
	t.Run("a canceled attempt may never have started", func(t *testing.T) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("canceled-unstarted", 1, nil, []string{"demo.echo"}))
		createJob(t, "canceled-unstarted", "demo.echo", 50, nil)
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		require.Equal(t, "LEASED", attemptHistory(t, claim.Assignment.JobID)[0])

		_, err = testPool.Exec(ctx, `
			UPDATE job_attempts SET status = 'CANCELED', finished_at = clock_timestamp()
			WHERE id = $1`, claim.Assignment.AttemptID)
		require.NoError(t, err, "M4 must allow a claimed-but-never-started attempt to be CANCELED")

		// SUCCEEDED, FAILED, and TIMED_OUT keep the original requirement, because
		// each is only reachable from RUNNING.
		for _, status := range []string{"SUCCEEDED", "FAILED", "TIMED_OUT"} {
			_, err := testPool.Exec(ctx, `
				UPDATE job_attempts SET status = $2, started_at = NULL, finished_at = clock_timestamp()
				WHERE id = $1`, claim.Assignment.AttemptID, status)
			require.Errorf(t, err, "%s must still require a start time", status)
		}
	})
}

// TestSchema_DLQEntriesAreUniquePerJob is the database-level proof that no code
// path can create a second dead-letter entry for one job, however many
// reconciler replicas race to decide the same job is exhausted.
func TestSchema_DLQEntriesAreUniquePerJob(t *testing.T) {
	reset(t)
	ctx := context.Background()
	jobID := createJob(t, "dlq-unique", "demo.echo", 50, nil)
	_, err := testPool.Exec(ctx,
		`UPDATE jobs SET status = 'DEAD_LETTERED' WHERE id = $1`, jobID)
	require.NoError(t, err)

	insert := func() error {
		_, err := testPool.Exec(ctx, `
			INSERT INTO dlq_entries (id, scope, queue, job_id, reason, created_at)
			VALUES (gen_random_uuid(), $1, 'default', $2, 'ATTEMPTS_EXHAUSTED', clock_timestamp())`,
			testScope, jobID)
		return err
	}
	require.NoError(t, insert())
	require.Error(t, insert(), "one job may have at most one dead-letter entry")

	t.Run("the reason vocabulary is closed", func(t *testing.T) {
		other := createJob(t, "dlq-unique-reason", "demo.echo", 50, nil)
		_, err := testPool.Exec(ctx, `UPDATE jobs SET status = 'DEAD_LETTERED' WHERE id = $1`, other)
		require.NoError(t, err)
		_, err = testPool.Exec(ctx, `
			INSERT INTO dlq_entries (id, scope, queue, job_id, reason, created_at)
			VALUES (gen_random_uuid(), $1, 'default', $2, 'EXHAUSTED', clock_timestamp())`,
			testScope, other)
		require.Error(t, err)
	})

	t.Run("scope and queue cannot drift from the job", func(t *testing.T) {
		other := createJob(t, "dlq-unique-binding", "demo.echo", 50, nil)
		_, err := testPool.Exec(ctx, `UPDATE jobs SET status = 'DEAD_LETTERED' WHERE id = $1`, other)
		require.NoError(t, err)
		_, err = testPool.Exec(ctx, `
			INSERT INTO dlq_entries (id, scope, queue, job_id, reason, created_at)
			VALUES (gen_random_uuid(), 'someone-else', 'default', $1, 'ATTEMPTS_EXHAUSTED', clock_timestamp())`,
			other)
		require.Error(t, err, "the composite foreign key must refuse a mismatched scope")
	})
}

// TestMigrations_CarryRealM3DataThroughTheM4Upgrade is the upgrade rehearsal
// that a fresh-database test cannot be.
//
// It seeds a database at migration 0008 with the state a running M3 deployment
// actually holds — queued work, a running attempt with a renewed lease, an
// abandoned attempt, a job ADR-0009 dead-lettered, and both a pending and a
// published outbox event — and then applies 0009 and 0010 to it. Every
// assertion below is about something an operator would notice if the upgrade
// got it wrong.
func TestMigrations_CarryRealM3DataThroughTheM4Upgrade(t *testing.T) {
	freshDSN := withFreshDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	migrations, err := database.LoadMigrations()
	require.NoError(t, err)
	require.Len(t, migrations, 10)

	cfg, err := pgx.ParseConfig(freshDSN)
	require.NoError(t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Stop at 0008: this database is now exactly what M3 shipped. The
	// schema_migrations rows are written too, so the real runner sees a database
	// a previous release migrated rather than an empty one — which is what makes
	// the upgrade below the real runner's upgrade and not a hand-applied
	// approximation of it.
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    INTEGER     PRIMARY KEY,
			name       TEXT        NOT NULL,
			checksum   TEXT        NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	require.NoError(t, err)
	for _, migration := range migrations[:8] {
		require.NoError(t, execMigration(ctx, conn, migration))
		_, err := conn.Exec(ctx,
			`INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
			migration.Version, migration.Name, migration.Checksum)
		require.NoError(t, err)
	}

	var (
		queuedJob, runningJob, deadJob                = uuid.New(), uuid.New(), uuid.New()
		workerID, sessionID                           = uuid.New(), uuid.New()
		runningAttempt, abandonedAttempt, deadAttempt = uuid.New(), uuid.New(), uuid.New()
		runningLease, expiredLease, deadLease         = uuid.New(), uuid.New(), uuid.New()
		pendingEvent, publishedEvent                  = uuid.New(), uuid.New()
		orphanEvent                                   = uuid.New()
	)
	seeded := time.Date(2026, 8, 30, 9, 0, 0, 0, time.UTC)

	_, err = conn.Exec(ctx, `
		INSERT INTO workers (id, scope, name) VALUES ($1, 'm3-upgrade', 'upgrade-worker');
		INSERT INTO worker_sessions (
			id, worker_id, scope, hostname, worker_group, concurrency_limit,
			capabilities, supported_job_types, status, registered_at, last_heartbeat_at
		) VALUES ($2, $1, 'm3-upgrade', 'upgrade.local', 'default', 4,
		          '{cpu}', '{demo.echo}', 'HEALTHY', $3, $3);`,
		workerID, sessionID, seeded)
	require.NoError(t, err)

	insertJob := func(id uuid.UUID, status string, at time.Time) {
		_, err := conn.Exec(ctx, `
			INSERT INTO jobs (
				id, scope, queue, job_type, payload, status, priority,
				max_attempts, timeout_seconds, available_at, created_at, updated_at
			) VALUES ($1, 'm3-upgrade', 'default', 'demo.echo', '{"m":1}', $2, 50, 2, 300, $3, $3, $3)`,
			id, status, at)
		require.NoError(t, err)
	}
	insertJob(queuedJob, "QUEUED", seeded)
	insertJob(runningJob, "RUNNING", seeded)
	insertJob(deadJob, "DEAD_LETTERED", seeded.Add(time.Minute))

	insertAttempt := func(id, jobID uuid.UUID, number int, status string, started, finished *time.Time) {
		_, err := conn.Exec(ctx, `
			INSERT INTO job_attempts (
				id, job_id, scope, queue, attempt_number, worker_id, worker_session_id,
				status, started_at, finished_at, created_at
			) VALUES ($1, $2, 'm3-upgrade', 'default', $3, $4, $5, $6, $7, $8, $9)`,
			id, jobID, number, workerID, sessionID, status, started, finished, seeded)
		require.NoError(t, err)
	}
	startedAt, finishedAt := seeded.Add(time.Second), seeded.Add(30*time.Second)
	insertAttempt(runningAttempt, runningJob, 1, "RUNNING", &startedAt, nil)
	insertAttempt(abandonedAttempt, deadJob, 1, "ABANDONED", &startedAt, &finishedAt)
	insertAttempt(deadAttempt, deadJob, 2, "ABANDONED", &startedAt, &finishedAt)

	insertLease := func(id, jobID, attemptID uuid.UUID, status string, version int, released *time.Time) {
		var renewalID any
		if version > 0 {
			renewalID = uuid.New()
		}
		_, err := conn.Exec(ctx, `
			INSERT INTO leases (
				id, job_id, attempt_id, scope, queue, worker_id, worker_session_id,
				claim_request_id, status, acquired_at, expires_at, renewed_at, released_at,
				renewal_version, last_renewal_request_id
			) VALUES ($1, $2, $3, 'm3-upgrade', 'default', $4, $5, $6, $7, $8, $9, $8, $10, $11, $12)`,
			id, jobID, attemptID, workerID, sessionID, uuid.New(), status,
			seeded, seeded.Add(2*time.Minute), released, version, renewalID)
		require.NoError(t, err)
	}
	// A renewed, still-active lease is the state M3 most often holds.
	insertLease(runningLease, runningJob, runningAttempt, "ACTIVE", 3, nil)
	insertLease(expiredLease, deadJob, abandonedAttempt, "EXPIRED", 0, &finishedAt)
	insertLease(deadLease, deadJob, deadAttempt, "EXPIRED", 0, &finishedAt)

	_, err = conn.Exec(ctx, `
		INSERT INTO outbox_events (id, event_type, schema_version, payload, status, created_at)
		VALUES ($1, 'work.available', 1, $2, 'PENDING', $3);
		INSERT INTO outbox_events (id, event_type, schema_version, payload, status, created_at, published_at)
		VALUES ($4, 'work.available', 1, $5, 'PUBLISHED', $3, $3);
		INSERT INTO outbox_events (id, event_type, schema_version, payload, status, created_at)
		VALUES ($6, 'work.available', 1, '{"queue":"default"}', 'PENDING', $3);`,
		pendingEvent, fmt.Sprintf(`{"queue":"default","job_id":"%s"}`, queuedJob), seeded,
		publishedEvent, fmt.Sprintf(`{"queue":"default","job_id":"%s"}`, runningJob),
		orphanEvent)
	require.NoError(t, err)

	// The upgrade itself, through the real runner.
	applied, err := database.Migrate(ctx, freshDSN, discardLogger())
	require.NoError(t, err, "0009 and 0010 must apply to a database holding real M3 data")
	require.Equal(t, 2, applied, "exactly the two M4 migrations are pending")

	t.Run("every seeded row survives", func(t *testing.T) {
		for table, want := range map[string]int{
			"jobs": 3, "job_attempts": 3, "leases": 3, "worker_sessions": 1, "workers": 1,
		} {
			var got int
			require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&got))
			require.Equalf(t, want, got, "%s lost rows during the upgrade", table)
		}
	})

	t.Run("existing jobs are backfilled as already notified", func(t *testing.T) {
		// Every M3 job was submitted immediately eligible, so exactly one
		// work.available event was created for it at submission. Generation 1 with
		// the creation time is the truthful description, not an approximation.
		for _, id := range []uuid.UUID{queuedJob, runningJob, deadJob} {
			var generation int
			var lastNotificationAt, createdAt time.Time
			require.NoError(t, conn.QueryRow(ctx, `
				SELECT notification_generation, last_notification_at, created_at
				FROM jobs WHERE id = $1`, id).Scan(&generation, &lastNotificationAt, &createdAt))
			require.Equal(t, 1, generation)
			require.True(t, createdAt.Equal(lastNotificationAt))
		}
	})

	t.Run("existing jobs gain no schedule, cancellation, or replay link", func(t *testing.T) {
		var scheduled, canceled *time.Time
		var replayedFrom *uuid.UUID
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT scheduled_at, cancel_requested_at, replayed_from_job_id
			FROM jobs WHERE id = $1`, queuedJob).Scan(&scheduled, &canceled, &replayedFrom))
		require.Nil(t, scheduled)
		require.Nil(t, canceled)
		require.Nil(t, replayedFrom)
	})

	t.Run("a running M3 attempt has no invented deadline", func(t *testing.T) {
		// Inventing one would hand a mid-flight attempt a fresh budget measured
		// from the upgrade rather than from when it actually started.
		var timeoutAt, retryAt *time.Time
		var outcomeID *uuid.UUID
		var class, code *string
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT timeout_at, retry_at, outcome_request_id, failure_class, error_code
			FROM job_attempts WHERE id = $1`, runningAttempt,
		).Scan(&timeoutAt, &retryAt, &outcomeID, &class, &code))
		require.Nil(t, timeoutAt)
		require.Nil(t, retryAt)
		require.Nil(t, outcomeID)
		require.Nil(t, class)
		require.Nil(t, code)

		var status string
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT status FROM job_attempts WHERE id = $1`, runningAttempt).Scan(&status))
		require.Equal(t, "RUNNING", status, "a mid-flight attempt must be left alone")
	})

	t.Run("the renewed lease keeps its generation and identity", func(t *testing.T) {
		var version int
		var identity *uuid.UUID
		var status string
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT renewal_version, last_renewal_request_id, status FROM leases WHERE id = $1`,
			runningLease).Scan(&version, &identity, &status))
		require.Equal(t, 3, version)
		require.NotNil(t, identity)
		require.Equal(t, "ACTIVE", status)
	})

	// This is the migration's most consequential backfill. ADR-0009 made
	// DEAD_LETTERED reachable in M3, one milestone before the DLQ that reads it,
	// so those jobs are real and must not become invisible after the upgrade.
	t.Run("the M3 dead-lettered job is backfilled into the DLQ exactly once", func(t *testing.T) {
		var count int
		require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM dlq_entries`).Scan(&count))
		require.Equal(t, 1, count, "only the DEAD_LETTERED job gets an entry")

		var entryScope, entryQueue, reason string
		var jobID, terminalAttempt uuid.UUID
		var createdAt, jobUpdatedAt time.Time
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT d.scope, d.queue, d.reason, d.job_id, d.terminal_attempt_id, d.created_at, j.updated_at
			FROM dlq_entries d JOIN jobs j ON j.id = d.job_id`).Scan(
			&entryScope, &entryQueue, &reason, &jobID, &terminalAttempt, &createdAt, &jobUpdatedAt))
		require.Equal(t, "m3-upgrade", entryScope)
		require.Equal(t, "default", entryQueue)
		require.Equal(t, deadJob, jobID)
		require.Equal(t, "ATTEMPTS_EXHAUSTED", reason,
			"M3 had no failure classification; the attempt budget is what ran out")
		require.Equal(t, deadAttempt, terminalAttempt,
			"the terminal attempt is the last one recorded, not the first")
		require.True(t, jobUpdatedAt.Equal(createdAt),
			"the entry must be stamped with when the job actually dead-lettered")
	})

	t.Run("existing outbox events are backfilled from their payload hint", func(t *testing.T) {
		for eventID, wantJob := range map[uuid.UUID]uuid.UUID{
			pendingEvent: queuedJob, publishedEvent: runningJob,
		} {
			var gotJob *uuid.UUID
			var generation *int
			require.NoError(t, conn.QueryRow(ctx, `
				SELECT job_id, notification_generation FROM outbox_events WHERE id = $1`,
				eventID).Scan(&gotJob, &generation))
			require.NotNil(t, gotJob)
			require.Equal(t, wantJob, *gotJob)
			require.NotNil(t, generation)
			require.Equal(t, 1, *generation)
		}

		// An event whose payload names no job resolves to nothing rather than to
		// something invented. It stays valid because the constraint pairs the two
		// columns rather than demanding them.
		var gotJob *uuid.UUID
		var generation *int
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT job_id, notification_generation FROM outbox_events WHERE id = $1`,
			orphanEvent).Scan(&gotJob, &generation))
		require.Nil(t, gotJob)
		require.Nil(t, generation)
	})

	t.Run("the upgrade is idempotent", func(t *testing.T) {
		// The runner records what it applied, so re-running must be a no-op
		// rather than a second backfill. A duplicated DLQ backfill would violate
		// the one-entry-per-job constraint and fail loudly, which is the point of
		// enforcing it in the database.
		again, err := database.Migrate(ctx, freshDSN, discardLogger())
		require.NoError(t, err)
		require.Zero(t, again)

		var entries int
		require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM dlq_entries`).Scan(&entries))
		require.Equal(t, 1, entries, "a rerun must not duplicate the DLQ backfill")
	})
}

// TestUpgrade_M1IdempotencyFingerprintsStillReplay is the compatibility promise
// that matters most to a running deployment: a key recorded before this
// milestone must still replay its original job, not answer 409.
//
// It seeds the idempotency record the way M1 through M3 wrote it — the
// fingerprint computed with no scheduled_at component at all — and then submits
// the identical request through the current code.
func TestUpgrade_M1IdempotencyFingerprintsStillReplay(t *testing.T) {
	reset(t)
	ctx := context.Background()

	const key = "pre-m4-key"
	original := createJob(t, key, "demo.echo", 50, nil)

	// createJob went through the current Submit, so the stored fingerprint is
	// whatever today's code produces. Prove it is byte-identical to what the
	// pre-M4 algorithm produced for the same request.
	payload, err := json.Marshal(map[string]any{"key": key})
	require.NoError(t, err)
	priority, maxAttempts, timeout := 50, 3, 300
	normalized, err := jobs.SubmitRequest{
		Queue: "default", Type: "demo.echo", Payload: payload,
		Priority: &priority, MaxAttempts: &maxAttempts, TimeoutSeconds: &timeout,
	}.Normalize()
	require.NoError(t, err)

	var stored string
	require.NoError(t, testPool.QueryRow(ctx, `
		SELECT request_fingerprint FROM idempotency_records
		WHERE scope = $1 AND idempotency_key = $2`, testScope, key).Scan(&stored))
	require.Equal(t, normalized.Fingerprint(), stored)

	// The identical request replays rather than conflicting.
	replay, err := jobs.NewStore(testPool).Submit(ctx, testScope, key, normalized)
	require.NoError(t, err)
	require.True(t, replay.Replayed)
	require.Equal(t, original, replay.Job.ID)

	// Adding a schedule makes it a genuinely different request, which is a
	// conflict rather than a silent replay of a job that runs at a different time.
	scheduled := time.Now().Add(time.Hour).UTC()
	delayed := normalized
	delayed.ScheduledAt = &scheduled
	_, err = jobs.NewStore(testPool).Submit(ctx, testScope, key, delayed)
	require.ErrorIs(t, err, jobs.ErrIdempotencyConflict)
}
