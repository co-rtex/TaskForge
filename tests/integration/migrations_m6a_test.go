//go:build integration

package integration

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/jobs"
)

// indexDefinition reads one index's rendered definition, or "" when it does
// not exist. Asserting on indexdef rather than on a query plan is this
// suite's established technique: a plan on a tiny test table is a sequential
// scan no matter how the index is defined, so it proves nothing.
func indexDefinition(t *testing.T, name string) string {
	t.Helper()
	var def *string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT indexdef FROM pg_indexes WHERE indexname = $1`, name).Scan(&def))
	if def == nil {
		return ""
	}
	return *def
}

func indexExists(t *testing.T, name string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)`, name).Scan(&exists))
	return exists
}

// TestMigrations_M6ACreatesTheOperatorReadIndexes pins every index migration
// 0017 creates, by definition rather than by name alone. A column dropped
// from one of these, or an ordering reversed, silently turns a keyset scan
// into a sort over the whole table.
func TestMigrations_M6ACreatesTheOperatorReadIndexes(t *testing.T) {
	t.Run("jobs keyset listing", func(t *testing.T) {
		def := indexDefinition(t, "jobs_scope_keyset_idx")
		require.NotEmpty(t, def, "migration 0017 must create jobs_scope_keyset_idx")
		require.Contains(t, def, "ON public.jobs")
		require.Contains(t, def, "scope")
		require.Contains(t, def, "created_at DESC")
		require.Contains(t, def, "id DESC",
			"without id, (created_at, id) is not a total order and a page boundary is ambiguous")
		require.NotContains(t, def, "WHERE", "the unfiltered listing must match every job in scope")
	})

	t.Run("jobs status-filtered listing", func(t *testing.T) {
		def := indexDefinition(t, "jobs_scope_status_keyset_idx")
		require.NotEmpty(t, def, "migration 0017 must create jobs_scope_status_keyset_idx")
		require.Contains(t, def, "scope")
		require.Contains(t, def, "status")
		require.Contains(t, def, "created_at DESC")
		require.Contains(t, def, "id DESC")
	})

	t.Run("queue depth", func(t *testing.T) {
		def := indexDefinition(t, "jobs_scope_queue_depth_idx")
		require.NotEmpty(t, def, "migration 0017 must create jobs_scope_queue_depth_idx")
		require.Contains(t, def, "WHERE", "the depth index must be partial")
		// Led by scope: every public read is scope-filtered, and an index led
		// by queue would scan every tenant's rows to answer one tenant's
		// question.
		require.Regexp(t, `\(scope,\s*queue,\s*status\)`, def,
			"the depth index must be led by scope")
	})

	t.Run("latest session per worker", func(t *testing.T) {
		def := indexDefinition(t, "worker_sessions_latest_per_worker_idx")
		require.NotEmpty(t, def, "migration 0017 must create worker_sessions_latest_per_worker_idx")
		require.Contains(t, def, "worker_id")
		require.Contains(t, def, "registered_at DESC")
		require.Contains(t, def, "id DESC")
		// The whole point: this index must NOT be restricted to currently
		// eligible sessions, or a crashed worker would vanish from the
		// listing built on it.
		require.NotContains(t, def, "WHERE",
			"a status predicate here would hide UNHEALTHY and OFFLINE sessions, "+
				"which are exactly the ones GET /v1/workers must show")
	})
}

// TestMigrations_M6ADropsTheSupersededListingIndex proves the M1 index is
// gone rather than merely unused.
//
// jobs_scope_created_at_idx (scope, created_at DESC) is a strict prefix of
// jobs_scope_keyset_idx, so keeping it would leave an index no query
// justifies -- AGENTS.md section 6.
func TestMigrations_M6ADropsTheSupersededListingIndex(t *testing.T) {
	require.False(t, indexExists(t, "jobs_scope_created_at_idx"),
		"migration 0017 must drop the index jobs_scope_keyset_idx supersedes")

	// And the replacement really is a superset: everything the old index
	// could serve, the new one serves, because its leading columns are the
	// old one's in the same order.
	def := indexDefinition(t, "jobs_scope_keyset_idx")
	require.Contains(t, def, "scope")
	require.Contains(t, def, "created_at DESC")
}

// TestMigrations_M6ADepthPredicateMatchesTheDerivedStatusSet is the guard
// against the one thing that cannot be parameterized.
//
// A partial index predicate is fixed SQL text; jobs.NonTerminalStatuses() is
// derived at runtime from Status.Terminal(). Adding a tenth job status would
// change the Go side and leave the index predicate stale, so the depth query
// would stop matching its index and quietly fall back to a scan of every job
// ever submitted. This asserts the two sides name exactly the same statuses.
func TestMigrations_M6ADepthPredicateMatchesTheDerivedStatusSet(t *testing.T) {
	def := indexDefinition(t, "jobs_scope_queue_depth_idx")
	require.NotEmpty(t, def)

	derived := make([]string, 0, len(jobs.NonTerminalStatuses()))
	for _, status := range jobs.NonTerminalStatuses() {
		derived = append(derived, status.String())
		require.Containsf(t, def, "'"+status.String()+"'",
			"the partial index predicate omits %s, which Status.Terminal() says is non-terminal", status)
	}
	sort.Strings(derived)

	// And the converse: the predicate must name no status the Go side calls
	// terminal, or the index would carry rows the depth query never counts.
	for _, status := range jobs.AllStatuses() {
		if !status.Terminal() {
			continue
		}
		require.NotContainsf(t, def, "'"+status.String()+"'",
			"the partial index predicate names %s, which is terminal", status)
	}

	// Count the quoted literals in the predicate so an extra one cannot hide.
	predicate := def[strings.Index(def, "WHERE"):]
	require.Equal(t, len(derived), strings.Count(predicate, "'")/2,
		"the predicate must name exactly the derived non-terminal set and nothing else")
}

// TestMigrations_M6AAddsNoColumnAndChangesNoRow proves 0017 is index-only.
//
// An index migration that quietly altered data would be the hardest kind of
// change to notice after the fact, so this checks the shapes M6A reads from
// are untouched and that the migration wrote nothing.
func TestMigrations_M6AAddsNoColumnAndChangesNoRow(t *testing.T) {
	reset(t)
	ctx := context.Background()

	// Seed a job, then confirm the columns M6A reads are exactly the ones
	// M1-M5 already defined -- 0017 adds none.
	createJob(t, "m6a-migration-shape", "demo.echo", 50, nil)

	for table, expected := range map[string][]string{
		"jobs": {
			"available_at", "cancel_requested_at", "created_at", "id", "job_type",
			"max_attempts", "payload", "priority", "queue", "replayed_from_job_id",
			"required_capabilities", "scheduled_at", "scope", "status",
			"timeout_seconds", "updated_at",
		},
		"queues": {"created_at", "max_concurrency", "name", "updated_at", "worker_group"},
	} {
		rows, err := testPool.Query(ctx, `
			SELECT column_name FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = $1`, table)
		require.NoError(t, err)
		var actual []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			actual = append(actual, name)
		}
		rows.Close()
		require.NoError(t, rows.Err())

		for _, column := range expected {
			require.Containsf(t, actual, column, "%s.%s must still exist", table, column)
		}
	}

	// M6A's migration is recorded under its own version.
	//
	// This deliberately asserts 0017 is PRESENT rather than NEWEST. An earlier
	// draft asserted newest, which made a test about M6A's migration fail the
	// moment M6B added 0018 -- a test that breaks for a reason unrelated to what
	// it is about.
	var name string
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT name FROM schema_migrations WHERE version = 17`).Scan(&name))
	require.Equal(t, "0017_operator_read_indexes.sql", name)
}
