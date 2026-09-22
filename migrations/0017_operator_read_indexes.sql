-- 0017_operator_read_indexes
--
-- Milestone M6A: the operator read APIs -- GET /v1/jobs,
-- GET /v1/jobs/{job_id}/attempts, GET /v1/workers, and GET /v1/queues.
-- These are the four V1-target routes docs/PROJECT_SPEC.md section 4 lists
-- and docs/ROADMAP.md's M5D and M5E both deferred with "these commands land
-- whenever those routes do".
--
-- This migration changes indexes only. It creates no table, alters no
-- column, and writes no row: every one of the four reads is a read, and the
-- data they serve has existed since M1-M4.
--
-- AGENTS.md section 6 requires a justifying query per index. Each one below
-- names the exact query it serves. Two facts are recorded here that are NOT
-- index creations, because "this query needs no new index" deserves the same
-- evidence as "this one does":
--
--   * GET /v1/jobs/{job_id}/attempts needs no new index. job_attempts already
--     carries UNIQUE (job_id, attempt_number) from migration 0002, and the
--     attempts read filters on job_id and orders by attempt_number ASC --
--     exactly that index's leading column and exactly its order.
--
--   * CREATE INDEX inside the embedded runner's transaction takes a lock that
--     blocks writes to jobs and worker_sessions while each index builds. That
--     is acceptable here and stated rather than assumed: no deployment of
--     TaskForge carries production data yet. A future migration against a live
--     database would need CREATE INDEX CONCURRENTLY, which cannot run inside a
--     transaction and would therefore need the runner to learn about
--     non-transactional migrations first.

-- ---------------------------------------------------------------------------
-- jobs: supersede the M1 listing index
-- ---------------------------------------------------------------------------
-- jobs_scope_created_at_idx (scope, created_at DESC) was created in migration
-- 0001 with a comment naming this milestone's endpoint as its future
-- justification. It cannot actually serve it: keyset pagination compares
-- (created_at, id) as a composite, and an index without id cannot produce that
-- total order. jobs_scope_keyset_idx below is a strict superset of it, so
-- keeping both would leave an index no query justifies.
--
-- Nothing else referenced it. GET /v1/jobs/{job_id} filters `id = $1 AND
-- scope = $2` and is served by the primary key on id, not by this index.
DROP INDEX jobs_scope_created_at_idx;

-- The unfiltered and queue-filtered listing:
--
--   SELECT ... FROM jobs
--   WHERE scope = $1
--     AND ($2::text IS NULL OR queue = $2)
--     AND ($3::timestamptz IS NULL OR (created_at, id) < ($3, $4))
--   ORDER BY created_at DESC, id DESC
--   LIMIT $5
--
-- queue is applied as a filter rather than indexed. Adding it as a second
-- column would break the ordering columns' contiguity and make this index
-- unusable for the unfiltered case, which is the common one; a separate
-- queue-led index is not created because no implemented query orders by
-- queue.
CREATE INDEX jobs_scope_keyset_idx
    ON jobs (scope, created_at DESC, id DESC);

COMMENT ON INDEX jobs_scope_keyset_idx IS
    'GET /v1/jobs: scope-filtered keyset listing, newest first. The id column '
    'is what makes (created_at, id) a total order, so a page boundary can '
    'neither duplicate nor omit a job when two jobs share an instant.';

-- The status-filtered listing is a SEPARATE index, not a widening of the one
-- above, because PostgreSQL 16 has no index skip scan. With status between the
-- equality column and the ordering columns, a query that does not constrain
-- status cannot use (scope, status, created_at DESC, id DESC) to produce
-- (created_at DESC, id DESC) order -- it would scan every status and sort.
-- Two indexes serve two genuinely different queries.
--
--   SELECT ... FROM jobs
--   WHERE scope = $1 AND status = $2
--     AND ($3::timestamptz IS NULL OR (created_at, id) < ($3, $4))
--   ORDER BY created_at DESC, id DESC
--   LIMIT $5
CREATE INDEX jobs_scope_status_keyset_idx
    ON jobs (scope, status, created_at DESC, id DESC);

COMMENT ON INDEX jobs_scope_status_keyset_idx IS
    'GET /v1/jobs?status=...: the status-filtered keyset listing. Separate '
    'from jobs_scope_keyset_idx because PostgreSQL 16 has no skip scan, so '
    'one index cannot serve both the filtered and the unfiltered query.';

-- ---------------------------------------------------------------------------
-- jobs: queue depth
-- ---------------------------------------------------------------------------
-- GET /v1/queues reports how much non-terminal work each queue currently
-- holds, for the authenticated scope:
--
--   SELECT queue, status, count(*) FROM jobs
--   WHERE scope = $1
--     AND status IN ('PENDING','QUEUED','LEASED','RUNNING',
--                    'RETRY_WAIT','CANCEL_REQUESTED')
--   GROUP BY queue, status
--
-- Led by scope, because every public read is scope-filtered -- an index led by
-- queue would have to scan every tenant's rows for one tenant's answer.
--
-- Partial on the non-terminal set, which is the whole point: terminal jobs
-- accumulate without bound over a deployment's life and are never counted
-- here, so they must never enter the index. The predicate is the exact
-- complement of jobs.Status.Terminal(), and
-- TestReadAPI_QueueDepthStatusesMatchTheIndexPredicate asserts the two agree
-- so a status added on either side cannot silently diverge from the other.
CREATE INDEX jobs_scope_queue_depth_idx
    ON jobs (scope, queue, status)
    WHERE status IN ('PENDING', 'QUEUED', 'LEASED', 'RUNNING',
                     'RETRY_WAIT', 'CANCEL_REQUESTED');

COMMENT ON INDEX jobs_scope_queue_depth_idx IS
    'GET /v1/queues: per-queue, per-status depth of non-terminal work in one '
    'scope. Partial so terminal history -- which grows without bound and is '
    'never counted -- stays out of the index entirely.';

-- ---------------------------------------------------------------------------
-- worker_sessions: the latest session per logical worker
-- ---------------------------------------------------------------------------
-- GET /v1/workers must show a worker whose process CRASHED, which is exactly
-- the worker an operator is looking for. That rules out
-- worker_sessions_one_current_per_worker_idx (migration 0002), whose predicate
-- is `status IN ('STARTING','HEALTHY','DRAINING')`: reconciliation moves a
-- stale session to UNHEALTHY (internal/workers/reconcile.go) and a replacement
-- registration moves the prior one to OFFLINE (internal/workers/store.go), and
-- both fall outside that predicate. A listing built on it would silently omit
-- every dead and replaced worker.
--
-- So the listing takes the latest session per worker regardless of status:
--
--   SELECT ... FROM workers w
--   CROSS JOIN LATERAL (
--       SELECT ... FROM worker_sessions s
--       WHERE s.worker_id = w.id
--       ORDER BY s.registered_at DESC, s.id DESC
--       LIMIT 1
--   ) s
--   WHERE w.scope = $1 ...
--
-- registered_at DESC, id DESC is a total order for the same reason the jobs
-- keyset is: two registrations can share an instant, and the id breaks the tie
-- deterministically.
CREATE INDEX worker_sessions_latest_per_worker_idx
    ON worker_sessions (worker_id, registered_at DESC, id DESC);

COMMENT ON INDEX worker_sessions_latest_per_worker_idx IS
    'GET /v1/workers: the LATERAL "latest session for this worker" lookup. '
    'Deliberately unfiltered by status, unlike '
    'worker_sessions_one_current_per_worker_idx, because a crashed '
    '(UNHEALTHY) or replaced (OFFLINE) session is precisely what an operator '
    'needs to see.';
