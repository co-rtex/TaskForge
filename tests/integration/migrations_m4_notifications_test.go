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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/jobs"
)

// TestMigrations_ReconstructNotificationHistoryFromRealM3Events is the upgrade
// case a one-notification-per-job assumption gets wrong.
//
// M3 published one work.available event per eligibility transition, and a job
// could become eligible more than once: an abandoned attempt requeues its job,
// and a job that has been abandoned twice has three events. Stamping every job
// generation 1 with last_notification_at = created_at is therefore not a
// description of what happened; it is a guess that is wrong for every job that
// was ever requeued, and it is wrong in two directions at once.
//
//   - last_notification_at reads as the job's creation instant rather than the
//     instant it was last advertised, so a job notified seconds ago looks
//     stranded and is re-notified immediately after the upgrade.
//   - every event for the job shares generation 1, so the current eligibility
//     transition is indistinguishable from ones that ended long ago. A stale
//     PENDING event suppresses re-notification of a transition it has nothing to
//     do with, and a stale broker delivery carries what looks like the current
//     generation.
//
// The seed here is what a real M3 database holds after abandonment and requeue,
// and every assertion is about behavior an operator would see.
func TestMigrations_ReconstructNotificationHistoryFromRealM3Events(t *testing.T) {
	freshDSN := withFreshDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	migrations, err := database.LoadMigrations()
	require.NoError(t, err)

	cfg, err := pgx.ParseConfig(freshDSN)
	require.NoError(t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Stop at 0008, recording what was applied, so the upgrade below is the real
	// runner's upgrade of a real M3 database.
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

	// Times relative to the database's own clock, because the assertions below
	// are about how recently a job was advertised.
	var now time.Time
	require.NoError(t, conn.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now))
	ago := func(d time.Duration) time.Time { return now.Add(-d) }

	insertJob := func(id uuid.UUID, createdAt time.Time) {
		_, err := conn.Exec(ctx, `
			INSERT INTO jobs (
				id, scope, queue, job_type, payload, status, priority,
				max_attempts, timeout_seconds, available_at, created_at, updated_at
			) VALUES ($1, 'm3-renotify', 'default', 'demo.echo', '{"m":1}', 'QUEUED', 50, 5, 300, $2, $2, $2)`,
			id, createdAt)
		require.NoError(t, err)
	}
	insertEvent := func(id, jobID uuid.UUID, status string, createdAt time.Time) {
		var publishedAt any
		if status == "PUBLISHED" {
			publishedAt = createdAt
		}
		_, err := conn.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, schema_version, payload, status, created_at, published_at)
			VALUES ($1, 'work.available', 1, $2, $3, $4, $5)`,
			id, fmt.Sprintf(`{"queue":"default","job_id":"%s"}`, jobID), status, createdAt, publishedAt)
		require.NoError(t, err)
	}

	// recentlyRequeuedJob: abandoned twice, so three eligibility transitions, and
	// its newest notification was published 30 seconds ago. This is the job a
	// flat backfill re-notifies immediately after the upgrade for no reason: it
	// reads as last advertised 30 minutes ago, and nothing is pending to stop it.
	recentlyRequeuedJob := uuid.New()
	insertJob(recentlyRequeuedJob, ago(30*time.Minute))
	recentFirst, recentSecond, recentThird := uuid.New(), uuid.New(), uuid.New()
	insertEvent(recentFirst, recentlyRequeuedJob, "PUBLISHED", ago(28*time.Minute))
	insertEvent(recentSecond, recentlyRequeuedJob, "PUBLISHED", ago(14*time.Minute))
	insertEvent(recentThird, recentlyRequeuedJob, "PUBLISHED", ago(30*time.Second))

	// requeuedJob: also abandoned twice, but its newest notification is still
	// PENDING. The publisher is behind, and that is the one case where adding
	// another event is wrong.
	requeuedJob := uuid.New()
	insertJob(requeuedJob, ago(31*time.Minute))
	requeuedFirst, requeuedSecond, requeuedThird := uuid.New(), uuid.New(), uuid.New()
	insertEvent(requeuedFirst, requeuedJob, "PUBLISHED", ago(29*time.Minute))
	insertEvent(requeuedSecond, requeuedJob, "PUBLISHED", ago(15*time.Minute))
	insertEvent(requeuedThird, requeuedJob, "PENDING", ago(45*time.Second))

	// stalePendingJob: its FIRST notification was never published — the publisher
	// died mid-window — and a later transition was advertised successfully. The
	// stale PENDING event belongs to a transition that is over.
	stalePendingJob := uuid.New()
	insertJob(stalePendingJob, ago(40*time.Minute))
	staleOld, staleCurrent := uuid.New(), uuid.New()
	insertEvent(staleOld, stalePendingJob, "PENDING", ago(38*time.Minute))
	insertEvent(staleCurrent, stalePendingJob, "PUBLISHED", ago(20*time.Minute))

	// genuinelyStrandedJob: advertised once, long ago, and published. This is the
	// job re-notification actually exists for, and it must still be repaired.
	genuinelyStrandedJob := uuid.New()
	insertJob(genuinelyStrandedJob, ago(60*time.Minute))
	strandedEvent := uuid.New()
	insertEvent(strandedEvent, genuinelyStrandedJob, "PUBLISHED", ago(55*time.Minute))

	// The upgrade, through the real runner.
	applied, err := database.Migrate(ctx, freshDSN, discardLogger())
	require.NoError(t, err)
	require.Equal(t, 4, applied)

	eventGeneration := func(id uuid.UUID) int {
		var generation *int
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT notification_generation FROM outbox_events WHERE id = $1`, id).Scan(&generation))
		require.NotNilf(t, generation, "event %s must be assigned a generation", id)
		return *generation
	}
	jobNotification := func(id uuid.UUID) (int, time.Time) {
		var generation int
		var at time.Time
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT notification_generation, last_notification_at FROM jobs WHERE id = $1`,
			id).Scan(&generation, &at))
		return generation, at
	}

	t.Run("generations follow the order the events were actually created", func(t *testing.T) {
		require.Equal(t, 1, eventGeneration(recentFirst))
		require.Equal(t, 2, eventGeneration(recentSecond))
		require.Equal(t, 3, eventGeneration(recentThird))
		require.Equal(t, 1, eventGeneration(requeuedFirst))
		require.Equal(t, 2, eventGeneration(requeuedSecond))
		require.Equal(t, 3, eventGeneration(requeuedThird))
		require.Equal(t, 1, eventGeneration(staleOld))
		require.Equal(t, 2, eventGeneration(staleCurrent))
		require.Equal(t, 1, eventGeneration(strandedEvent))
	})

	t.Run("the latest transition is the current generation", func(t *testing.T) {
		for id, newest := range map[uuid.UUID]uuid.UUID{
			recentlyRequeuedJob:  recentThird,
			requeuedJob:          requeuedThird,
			stalePendingJob:      staleCurrent,
			genuinelyStrandedJob: strandedEvent,
		} {
			generation, _ := jobNotification(id)
			require.Equalf(t, eventGeneration(newest), generation,
				"job %s must carry the generation of its newest eligibility transition", id)
		}

		// And every older event is strictly behind it, so none of them can be
		// mistaken for the current transition.
		current, _ := jobNotification(recentlyRequeuedJob)
		require.Less(t, eventGeneration(recentFirst), current)
		require.Less(t, eventGeneration(recentSecond), current)
	})

	t.Run("last_notification_at is when the job was last advertised", func(t *testing.T) {
		for id, newestEvent := range map[uuid.UUID]uuid.UUID{
			recentlyRequeuedJob:  recentThird,
			requeuedJob:          requeuedThird,
			stalePendingJob:      staleCurrent,
			genuinelyStrandedJob: strandedEvent,
		} {
			_, at := jobNotification(id)
			var eventAt, jobCreatedAt time.Time
			require.NoError(t, conn.QueryRow(ctx,
				`SELECT created_at FROM outbox_events WHERE id = $1`, newestEvent).Scan(&eventAt))
			require.NoError(t, conn.QueryRow(ctx,
				`SELECT created_at FROM jobs WHERE id = $1`, id).Scan(&jobCreatedAt))
			require.WithinDurationf(t, eventAt, at, 0,
				"job %s must be stamped with its newest notification, not an approximation", id)
			require.Falsef(t, jobCreatedAt.Equal(at),
				"job %s was requeued, so its creation instant is not when it was last advertised", id)
		}
	})

	// The whole point of the two corrections above: the scheduler's own query,
	// run against the upgraded database, must not manufacture work.
	store := jobs.NewStore(poolFor(t, freshDSN))

	t.Run("the first scheduler pass repairs only what is genuinely stranded", func(t *testing.T) {
		_, recentBefore := jobNotification(recentlyRequeuedJob)

		stats, err := store.RenotifyStrandedQueued(ctx, 5*time.Minute, 50)
		require.NoError(t, err)

		// The job advertised 30 seconds ago is not stranded, whatever its
		// creation time says. Under a flat backfill it reads as last advertised
		// 30 minutes ago with nothing pending, and is re-notified for nothing.
		require.Zero(t, pendingEventsAtGeneration(t, freshDSN, recentlyRequeuedJob, 3),
			"a job advertised 30 seconds ago must not be re-notified")
		_, recentAfter := jobNotification(recentlyRequeuedJob)
		require.WithinDuration(t, recentBefore, recentAfter, 0,
			"a job that was not re-notified must not be restamped either")

		// The job whose newest transition is still PENDING is skipped rather
		// than duplicated: the publisher is behind, not the notification lost.
		require.Equal(t, 1, pendingEventsAtGeneration(t, freshDSN, requeuedJob, 3),
			"a pending notification for the current transition must not be duplicated")

		// A stale PENDING event at generation 1 belongs to a transition that is
		// over. Under a flat backfill it would share the current generation and
		// suppress this repair forever.
		require.Equal(t, 1, pendingEventsAtGeneration(t, freshDSN, stalePendingJob, 2),
			"the current transition must be re-advertised despite an older pending event")
		generation, _ := jobNotification(stalePendingJob)
		require.Equal(t, 2, generation,
			"re-notification advertises the same transition, so the generation does not move")

		// And the plainly stranded job is still repaired, which is what makes
		// the zero above a real result rather than an inert scheduler pass.
		require.Equal(t, 1, pendingEventsAtGeneration(t, freshDSN, genuinelyStrandedJob, 1))
		require.EqualValues(t, 2, stats.Renotified,
			"exactly the two genuinely stranded jobs were repaired")
	})

	t.Run("a pending event at the current generation keeps suppressing", func(t *testing.T) {
		// Every job is now stranded under a one-second interval, so the only
		// thing still holding requeuedJob back is the guard itself.
		_, err := store.RenotifyStrandedQueued(ctx, time.Second, 50)
		require.NoError(t, err)
		require.Equal(t, 1, pendingEventsAtGeneration(t, freshDSN, requeuedJob, 3),
			"a pending notification for the current transition must never be duplicated")
	})
}

// poolFor opens a pool against one of the temporary upgrade databases, so the
// real scheduler queries can run against it.
func poolFor(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func pendingEventsAtGeneration(t *testing.T, dsn string, jobID uuid.UUID, generation int) int {
	t.Helper()
	pool := poolFor(t, dsn)
	var count int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT count(*) FROM outbox_events
		WHERE job_id = $1 AND notification_generation = $2
		  AND event_type = 'work.available' AND status = 'PENDING'`,
		jobID, generation).Scan(&count))
	return count
}

// migrateThrough applies migrations 1..upTo to a fresh database, recording each
// in schema_migrations exactly as the real runner would, and returns a
// connection to it plus its DSN.
//
// Stopping part-way is the whole point: it produces a database at a real
// published release boundary, which the runner will then upgrade for real.
func migrateThrough(t *testing.T, ctx context.Context, upTo int) (*pgx.Conn, string) {
	t.Helper()
	freshDSN := withFreshDatabase(t)
	migrations, err := database.LoadMigrations()
	require.NoError(t, err)

	cfg, err := pgx.ParseConfig(freshDSN)
	require.NoError(t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(context.Background()) })

	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    INTEGER     PRIMARY KEY,
			name       TEXT        NOT NULL,
			checksum   TEXT        NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	require.NoError(t, err)
	for _, migration := range migrations {
		if migration.Version > upTo {
			break
		}
		require.NoErrorf(t, execMigration(ctx, conn, migration), "migration %d", migration.Version)
		_, err := conn.Exec(ctx,
			`INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
			migration.Version, migration.Name, migration.Checksum)
		require.NoError(t, err)
	}
	return conn, freshDSN
}

// notificationState is a job's notification metadata plus the generations of its
// events, which together are everything migration 0012 can touch.
type notificationState struct {
	generation       int
	lastNotification time.Time
	createdAt        time.Time
	eventGenerations []int
}

func readNotificationState(t *testing.T, ctx context.Context, conn *pgx.Conn, jobID uuid.UUID) notificationState {
	t.Helper()
	var state notificationState
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT notification_generation, last_notification_at, created_at
		FROM jobs WHERE id = $1`, jobID,
	).Scan(&state.generation, &state.lastNotification, &state.createdAt))

	rows, err := conn.Query(ctx, `
		SELECT notification_generation FROM outbox_events
		WHERE job_id = $1 AND event_type = 'work.available'
		ORDER BY created_at, id`, jobID)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var generation *int
		require.NoError(t, rows.Scan(&generation))
		require.NotNil(t, generation)
		state.eventGenerations = append(state.eventGenerations, *generation)
	}
	require.NoError(t, rows.Err())
	return state
}

// TestMigrations_PerJobReconstructionSurvivesMixedState is the upgrade path
// migration 0011's database-wide guard gets wrong.
//
// Migrations 0009 and 0010 were published before 0011 existed, so a deployment
// can be running M4 code against a database at 0010. As soon as that code
// advances one job's notification metadata, 0011's guard sees a database that is
// "no longer the 0009 backfill" and repairs NOTHING — leaving every untouched M3
// history with the wrong generation and a last_notification_at that makes it look
// stranded. One advanced job silently cancels the whole repair.
//
// Eligibility is a property of a job, so 0012 asks per job. This seeds exactly
// that mixed state and proves both halves: the advanced job is untouched, and the
// legacy job is reconstructed.
func TestMigrations_PerJobReconstructionSurvivesMixedState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	conn, freshDSN := migrateThrough(t, ctx, 10)

	var now time.Time
	require.NoError(t, conn.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now))
	ago := func(d time.Duration) time.Time { return now.Add(-d) }

	insertJob := func(id uuid.UUID, createdAt time.Time, generation int, lastNotification time.Time) {
		_, err := conn.Exec(ctx, `
			INSERT INTO jobs (
				id, scope, queue, job_type, payload, status, priority,
				max_attempts, timeout_seconds, available_at, created_at, updated_at,
				notification_generation, last_notification_at
			) VALUES ($1, 'mixed', 'default', 'demo.echo', '{"m":1}', 'QUEUED', 50, 5, 300,
			          $2, $2, $2, $3, $4)`,
			id, createdAt, generation, lastNotification)
		require.NoError(t, err)
	}
	insertEvent := func(jobID uuid.UUID, generation int, createdAt time.Time) uuid.UUID {
		id := uuid.New()
		_, err := conn.Exec(ctx, `
			INSERT INTO outbox_events (
				id, event_type, schema_version, payload, status, created_at, published_at,
				job_id, notification_generation
			) VALUES ($1, 'work.available', 1, $2, 'PUBLISHED', $3, $3, $4, $5)`,
			id, fmt.Sprintf(`{"queue":"default","job_id":"%s"}`, jobID), createdAt, jobID, generation)
		require.NoError(t, err)
		return id
	}

	// A job M4 legitimately advanced: promoted once, so its generation is 2 and
	// its last_notification_at is when that promotion advertised it. Every field
	// here is correct by construction and must survive untouched.
	advancedJob := uuid.New()
	insertJob(advancedJob, ago(50*time.Minute), 2, ago(3*time.Minute))
	insertEvent(advancedJob, 1, ago(48*time.Minute))
	insertEvent(advancedJob, 2, ago(3*time.Minute))

	// A job M4 created and then re-notified: generation stays 1, but
	// last_notification_at advanced past created_at. Also correct, also legacy-
	// looking at a glance, and also must survive untouched.
	renotifiedJob := uuid.New()
	insertJob(renotifiedJob, ago(40*time.Minute), 1, ago(2*time.Minute))
	insertEvent(renotifiedJob, 1, ago(39*time.Minute))
	insertEvent(renotifiedJob, 1, ago(2*time.Minute))

	// An untouched M3 job that was abandoned and requeued twice, stamped by
	// 0009's backfill: generation 1, last_notification_at = created_at, and all
	// three of its events labelled generation 1.
	legacyCreated := ago(30 * time.Minute)
	legacyJob := uuid.New()
	insertJob(legacyJob, legacyCreated, 1, legacyCreated)
	insertEvent(legacyJob, 1, ago(28*time.Minute))
	insertEvent(legacyJob, 1, ago(14*time.Minute))
	legacyNewest := ago(45 * time.Second)
	insertEvent(legacyJob, 1, legacyNewest)

	// A second untouched M3 job with a single event, which 0009 stamped
	// correctly by accident. Reconstruction must agree with what is already there
	// rather than churn the row.
	singleCreated := ago(20 * time.Minute)
	singleEventJob := uuid.New()
	insertJob(singleEventJob, singleCreated, 1, singleCreated)
	insertEvent(singleEventJob, 1, singleCreated)

	advancedBefore := readNotificationState(t, ctx, conn, advancedJob)
	renotifiedBefore := readNotificationState(t, ctx, conn, renotifiedJob)
	singleBefore := readNotificationState(t, ctx, conn, singleEventJob)

	// The upgrade, through the real runner: 0011 then 0012.
	applied, err := database.Migrate(ctx, freshDSN, discardLogger())
	require.NoError(t, err)
	require.Equal(t, 2, applied, "exactly 0011 and 0012 are pending from 0010")

	t.Run("M4-authored notification metadata is untouched", func(t *testing.T) {
		require.Equal(t, advancedBefore, readNotificationState(t, ctx, conn, advancedJob),
			"a promoted job's generation, timestamp, and event labels are already correct")
		require.Equal(t, renotifiedBefore, readNotificationState(t, ctx, conn, renotifiedJob),
			"a re-notified job keeps its generation, and its two same-generation events are ordinary")
	})

	t.Run("untouched legacy history is reconstructed", func(t *testing.T) {
		after := readNotificationState(t, ctx, conn, legacyJob)
		require.Equal(t, []int{1, 2, 3}, after.eventGenerations,
			"three eligibility transitions must stop sharing one generation")
		require.Equal(t, 3, after.generation,
			"the job's current generation is its newest transition's")
		require.WithinDuration(t, legacyNewest, after.lastNotification, 0,
			"a job advertised 45 seconds ago is not stranded, whatever its creation time")
		require.False(t, after.createdAt.Equal(after.lastNotification),
			"0009 stamped creation time here, which is exactly what was wrong")
	})

	t.Run("a correctly stamped legacy job is left alone", func(t *testing.T) {
		require.Equal(t, singleBefore, readNotificationState(t, ctx, conn, singleEventJob),
			"reconstruction that agrees with the stored value must not write the row")
	})

	t.Run("reconstruction is idempotent", func(t *testing.T) {
		// The runner records what it applied, so re-running is a no-op.
		again, err := database.Migrate(ctx, freshDSN, discardLogger())
		require.NoError(t, err)
		require.Zero(t, again)

		// And the repair itself is idempotent independently of that bookkeeping:
		// executing 0012's body a second time recomputes the same values and
		// writes nothing, which is what makes a partially applied upgrade safe
		// to resume.
		before := map[uuid.UUID]notificationState{}
		for _, id := range []uuid.UUID{advancedJob, renotifiedJob, legacyJob, singleEventJob} {
			before[id] = readNotificationState(t, ctx, conn, id)
		}

		migrations, err := database.LoadMigrations()
		require.NoError(t, err)
		var body string
		for _, migration := range migrations {
			if migration.Version == 12 {
				body = migration.SQL
			}
		}
		require.NotEmpty(t, body, "migration 0012 must be loadable")

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, body)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		for id, want := range before {
			require.Equalf(t, want, readNotificationState(t, ctx, conn, id),
				"re-running the repair changed job %s", id)
		}
	})
}

// TestMigrations_PerJobReconstructionIsCorrectFromAnEmptyDatabase keeps the
// per-job rule honest on the ordinary path.
//
// A fresh install runs 0009 through 0012 in one go with no rows to repair, and
// then M4 writes notification metadata normally. Nothing about the mixed-state
// repair may change what a new deployment ends up with.
func TestMigrations_PerJobReconstructionIsCorrectFromAnEmptyDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	freshDSN := withFreshDatabase(t)
	applied, err := database.Migrate(ctx, freshDSN, discardLogger())
	require.NoError(t, err)
	require.Equal(t, 12, applied, "a fresh database applies every migration")

	cfg, err := pgx.ParseConfig(freshDSN)
	require.NoError(t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Real submissions through the real store, so the metadata under test is
	// what production writes rather than what a test hand-assembles.
	pool := poolFor(t, freshDSN)
	store := jobs.NewStore(pool)
	payload, err := json.Marshal(map[string]any{"m": 1})
	require.NoError(t, err)
	priority, maxAttempts, timeout := 50, 3, 300
	normalized, err := jobs.SubmitRequest{
		Queue: "default", Type: "demo.echo", Payload: payload, Priority: &priority,
		MaxAttempts: &maxAttempts, TimeoutSeconds: &timeout,
	}.Normalize()
	require.NoError(t, err)
	submitted, err := store.Submit(ctx, "fresh-install", "fresh-install-key", normalized)
	require.NoError(t, err)

	state := readNotificationState(t, ctx, conn, submitted.Job.ID)
	require.Equal(t, 1, state.generation)
	require.Equal(t, []int{1}, state.eventGenerations)
	require.False(t, state.createdAt.Equal(state.lastNotification),
		"an M4-created job is stamped with post-lock server time, not its transaction start")

	// The one property that matters for 0012: a job M4 just created is NOT
	// eligible for legacy reconstruction, so nothing could ever rewrite it.
	var eligible bool
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM jobs
			WHERE id = $1 AND notification_generation = 1
			  AND last_notification_at = created_at
		)`, submitted.Job.ID).Scan(&eligible))
	require.False(t, eligible, "an M4-created job must never match the 0009 backfill stamp")
}
