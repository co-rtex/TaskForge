//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// insertRawJob writes one job row directly, so a test can build the shapes
// application code refuses to build and find out whether the database refuses
// them too.
func insertRawJob(t *testing.T, id uuid.UUID, scope, queue, status string, replayedFrom *uuid.UUID) {
	t.Helper()
	_, err := testPool.Exec(context.Background(), `
		INSERT INTO jobs (
			id, scope, queue, job_type, payload, status, priority,
			max_attempts, timeout_seconds, available_at, created_at, updated_at,
			notification_generation, last_notification_at, replayed_from_job_id
		) VALUES ($1, $2, $3, 'demo.echo', '{"m":1}', $4, 50, 3, 300,
		          now(), now(), now(), 1, now(), $5)`,
		id, scope, queue, status, replayedFrom)
	require.NoError(t, err)
}

// insertRawSession creates the worker and session an attempt row must reference.
func insertRawSession(t *testing.T, scope string) (workerID, sessionID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	workerID, sessionID = uuid.New(), uuid.New()
	_, err := testPool.Exec(ctx,
		`INSERT INTO workers (id, scope, name) VALUES ($1, $2, 'lineage-worker')`, workerID, scope)
	require.NoError(t, err)
	_, err = testPool.Exec(ctx, `
		INSERT INTO worker_sessions (
			id, worker_id, scope, hostname, worker_group, concurrency_limit,
			capabilities, supported_job_types, status, registered_at, last_heartbeat_at
		) VALUES ($1, $2, $3, 'lineage.local', 'default', 4,
		          '{cpu}', '{demo.echo}', 'HEALTHY', now(), now())`,
		sessionID, workerID, scope)
	require.NoError(t, err)
	return workerID, sessionID
}

func insertRawAttempt(t *testing.T, id, jobID uuid.UUID, scope, queue string, number int, workerID, sessionID uuid.UUID) {
	t.Helper()
	// One `now()` for all three instants rather than three `clock_timestamp()`
	// calls: the timeline CHECK requires created <= started <= finished, and
	// three independent samples in one statement are not ordered by anything.
	_, err := testPool.Exec(context.Background(), `
		INSERT INTO job_attempts (
			id, job_id, scope, queue, attempt_number, worker_id, worker_session_id,
			status, created_at, started_at, finished_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 'FAILED', now(), now(), now())`,
		id, jobID, scope, queue, number, workerID, sessionID)
	require.NoError(t, err)
}

func insertDLQEntry(t *testing.T, jobID uuid.UUID, scope, queue string, attemptID *uuid.UUID) error {
	t.Helper()
	_, err := testPool.Exec(context.Background(), `
		INSERT INTO dlq_entries (id, scope, queue, job_id, terminal_attempt_id, reason, created_at)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, 'PERMANENT_FAILURE', clock_timestamp())`,
		scope, queue, jobID, attemptID)
	return err
}

func insertReplay(t *testing.T, scope string, original, replacement uuid.UUID, key string) error {
	t.Helper()
	_, err := testPool.Exec(context.Background(), `
		INSERT INTO dlq_replays (scope, original_job_id, idempotency_key, replacement_job_id, created_at)
		VALUES ($1, $2, $3, $4, clock_timestamp())`,
		scope, original, key, replacement)
	return err
}

// TestSchema_DLQAndReplayRelationshipsAreEnforcedByTheDatabase covers the
// invariants that were previously true only because one code path happened to
// write consistent rows.
//
// "The application always writes it correctly" is not an invariant; it is a
// description of today's call sites. A DLQ entry pointing at another job's
// attempt, a replay whose original belongs to a different tenant, or a lineage
// row connecting two unrelated jobs are all rows a bug, a backfill, or a
// support script can produce — and every one of them is a cross-tenant data leak
// or a silently wrong operator view once it exists.
//
// Each case is paired with a positive control differing only in the field under
// test, so a passing rejection is attributable to the constraint rather than to
// some unrelated NOT NULL.
func TestSchema_DLQAndReplayRelationshipsAreEnforcedByTheDatabase(t *testing.T) {
	const otherScope = "someone-else"

	t.Run("a dead-letter entry cannot point at another job's attempt", func(t *testing.T) {
		reset(t)
		owner, stranger := uuid.New(), uuid.New()
		insertRawJob(t, owner, testScope, "default", "DEAD_LETTERED", nil)
		insertRawJob(t, stranger, testScope, "default", "DEAD_LETTERED", nil)
		workerID, sessionID := insertRawSession(t, testScope)
		ownAttempt, foreignAttempt := uuid.New(), uuid.New()
		insertRawAttempt(t, ownAttempt, owner, testScope, "default", 1, workerID, sessionID)
		insertRawAttempt(t, foreignAttempt, stranger, testScope, "default", 1, workerID, sessionID)

		require.Error(t, insertDLQEntry(t, owner, testScope, "default", &foreignAttempt),
			"the terminal attempt must belong to the job the entry is about")

		// The control: the same row with this job's own attempt is accepted, so
		// the rejection above is about the relationship and nothing else.
		require.NoError(t, insertDLQEntry(t, owner, testScope, "default", &ownAttempt))
	})

	t.Run("a dead-letter entry cannot point at another tenant's attempt", func(t *testing.T) {
		reset(t)
		job, foreignJob := uuid.New(), uuid.New()
		insertRawJob(t, job, testScope, "default", "DEAD_LETTERED", nil)
		insertRawJob(t, foreignJob, otherScope, "default", "DEAD_LETTERED", nil)

		// An attempt cannot even exist in a scope other than its job's: M1's
		// job_attempts_job_fkey already carries (job_id, scope, queue). So the
		// only way to reach another tenant's attempt is to name it directly.
		foreignWorker, foreignSession := insertRawSession(t, otherScope)
		foreignAttempt := uuid.New()
		insertRawAttempt(t, foreignAttempt, foreignJob, otherScope, "default", 1, foreignWorker, foreignSession)

		require.Error(t, insertDLQEntry(t, job, testScope, "default", &foreignAttempt),
			"scope is part of the relationship, not a label recorded beside it")
	})

	t.Run("a job's replay source cannot live in another scope", func(t *testing.T) {
		reset(t)
		foreignOriginal := uuid.New()
		insertRawJob(t, foreignOriginal, otherScope, "default", "DEAD_LETTERED", nil)

		replacement := uuid.New()
		_, err := testPool.Exec(context.Background(), `
			INSERT INTO jobs (
				id, scope, queue, job_type, payload, status, priority,
				max_attempts, timeout_seconds, available_at, created_at, updated_at,
				notification_generation, last_notification_at, replayed_from_job_id
			) VALUES ($1, $2, 'default', 'demo.echo', '{"m":1}', 'QUEUED', 50, 3, 300,
			          now(), now(), now(), 1, now(), $3)`,
			replacement, testScope, foreignOriginal)
		require.Error(t, err,
			"a replay link across scopes would expose one tenant's job through another's lineage")

		// The control: the identical row whose source is in the same scope.
		sameScopeOriginal := uuid.New()
		insertRawJob(t, sameScopeOriginal, testScope, "default", "DEAD_LETTERED", nil)
		insertRawJob(t, uuid.New(), testScope, "default", "QUEUED", &sameScopeOriginal)
	})

	t.Run("a replay's original must belong to the recorded scope", func(t *testing.T) {
		reset(t)
		foreignOriginal, replacement := uuid.New(), uuid.New()
		insertRawJob(t, foreignOriginal, otherScope, "default", "DEAD_LETTERED", nil)
		insertRawJob(t, replacement, testScope, "default", "QUEUED", nil)

		require.Error(t, insertReplay(t, testScope, foreignOriginal, replacement, "key-cross-original"),
			"a replay row cannot claim an original that belongs to another scope")
	})

	t.Run("a replay's replacement must belong to the recorded scope", func(t *testing.T) {
		reset(t)
		original := uuid.New()
		insertRawJob(t, original, testScope, "default", "DEAD_LETTERED", nil)

		// There are only two shapes a cross-scope replacement can take, and both
		// are refused — one at the job row, one at the replay row.
		crossScopeWithSource := uuid.New()
		_, err := testPool.Exec(context.Background(), `
			INSERT INTO jobs (
				id, scope, queue, job_type, payload, status, priority,
				max_attempts, timeout_seconds, available_at, created_at, updated_at,
				notification_generation, last_notification_at, replayed_from_job_id
			) VALUES ($1, $2, 'default', 'demo.echo', '{"m":1}', 'QUEUED', 50, 3, 300,
			          now(), now(), now(), 1, now(), $3)`,
			crossScopeWithSource, otherScope, original)
		require.Error(t, err,
			"a replacement in another scope cannot even record where it came from")

		crossScopeNoSource := uuid.New()
		insertRawJob(t, crossScopeNoSource, otherScope, "default", "QUEUED", nil)
		require.Error(t, insertReplay(t, testScope, original, crossScopeNoSource, "key-cross-replacement"),
			"a replay row cannot claim a replacement that belongs to another scope")

		// The control: the same row entirely inside one scope is accepted.
		sameScope := uuid.New()
		insertRawJob(t, sameScope, testScope, "default", "QUEUED", &original)
		require.NoError(t, insertReplay(t, testScope, original, sameScope, "key-same-scope"))
	})

	t.Run("replay lineage cannot connect two unrelated jobs", func(t *testing.T) {
		reset(t)
		original, unrelated := uuid.New(), uuid.New()
		insertRawJob(t, original, testScope, "default", "DEAD_LETTERED", nil)

		// A job that is nobody's replacement. Recording it as this original's
		// replacement would make the two views of one fact disagree: dlq_replays
		// would say it came from `original` and the job itself would not.
		insertRawJob(t, unrelated, testScope, "default", "QUEUED", nil)
		require.Error(t, insertReplay(t, testScope, original, unrelated, "key-no-lineage"),
			"a replacement with no replay source cannot be recorded as one")

		// A job that really is a replacement, but of something else.
		otherOriginal, misattributed := uuid.New(), uuid.New()
		insertRawJob(t, otherOriginal, testScope, "default", "DEAD_LETTERED", nil)
		insertRawJob(t, misattributed, testScope, "default", "QUEUED", &otherOriginal)
		require.Error(t, insertReplay(t, testScope, original, misattributed, "key-wrong-lineage"),
			"the two records of one replay must name the same original")

		// The control: the consistent row is accepted.
		consistent := uuid.New()
		insertRawJob(t, consistent, testScope, "default", "QUEUED", &original)
		require.NoError(t, insertReplay(t, testScope, original, consistent, "key-consistent"))
	})

	t.Run("the real replay path writes rows these constraints accept", func(t *testing.T) {
		// The constraints above are only correct if production traffic satisfies
		// them, so the same invariants are checked against a replay the real code
		// wrote rather than one this test assembled.
		reset(t)
		ctx := context.Background()
		control := controlStore()
		session := registerWorker(t, control,
			workerRegistration("lineage-real", 1, nil, []string{"demo.echo"}))
		fence := deadLetterJob(t, control, session, "lineage-real")

		result, err := jobStore().Replay(ctx, testScope, fence.JobID, "lineage-real-key")
		require.NoError(t, err)

		var recordedOriginal, replacementSource uuid.UUID
		var recordedScope string
		require.NoError(t, testPool.QueryRow(ctx, `
			SELECT r.original_job_id, r.scope, j.replayed_from_job_id
			FROM dlq_replays r JOIN jobs j ON j.id = r.replacement_job_id
			WHERE r.replacement_job_id = $1`, result.Replacement.ID,
		).Scan(&recordedOriginal, &recordedScope, &replacementSource))
		require.Equal(t, fence.JobID, recordedOriginal)
		require.Equal(t, testScope, recordedScope)
		require.Equal(t, recordedOriginal, replacementSource,
			"both records of one replay must already agree, or the constraint would reject it")

		var entryAttemptJob uuid.UUID
		require.NoError(t, testPool.QueryRow(ctx, `
			SELECT a.job_id FROM dlq_entries d
			JOIN job_attempts a ON a.id = d.terminal_attempt_id
			WHERE d.job_id = $1`, fence.JobID).Scan(&entryAttemptJob))
		require.Equal(t, fence.JobID, entryAttemptJob)
	})
}
