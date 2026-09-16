-- 0016_results
--
-- Milestone M5C: result storage. A small result is stored inline in
-- PostgreSQL; a result at or above a configured threshold is stored in an
-- S3-compatible object store (MinIO locally, S3 in the cloud direction) and
-- this table records where. See docs/PROJECT_SPEC.md item 13 and
-- docs/adr/0015-result-storage.md.
--
-- job_id is the primary key, not attempt_id. GET /v1/jobs/{job_id}/result is
-- keyed by job id, and internal/workers.Store.Succeed -- the only writer of
-- this table -- only ever inserts a row from its non-replay commit path,
-- which requires the job to still be RUNNING; an exact replay of an
-- already-SUCCEEDED outcome returns early and writes nothing further (see
-- Succeed's own comment on why that branch is safe). At most one row per job
-- is therefore the correct cardinality, not one row per attempt.
--
-- attempt_id is still recorded, NOT NULL, referencing the exact attempt that
-- produced the result -- operator traceability -- even though job_id alone
-- is the lookup key every implemented query uses.
--
-- scope is denormalized here exactly as job_attempts.scope and leases.scope
-- already are, so the retrieval endpoint filters this table directly by
-- scope without a join back to jobs. The composite foreign key below makes
-- that denormalized copy database-enforced against the real job, mirroring
-- migration 0011's replay-lineage foreign keys.
--
-- Known limitation (docs/adr/0015-result-storage.md, "Known limitation"
-- section): the object key a worker uploads to is attempt-scoped
-- (results/<scope>/<job_id>/<attempt_id>), not job-scoped, by deliberate
-- decision -- a job-scoped overwrite key was considered and rejected. An
-- attempt whose object upload succeeds but whose process then dies before it
-- calls Succeed leaves that attempt's uploaded object permanently orphaned:
-- a later replacement attempt uploads under its own, different attempt_id,
-- and this table's single row per job simply records whichever attempt
-- actually succeeded. The earlier object is never referenced by this table
-- again. That is a storage cost, never a correctness problem -- nothing is
-- ever observably SUCCEEDED with a broken result reference -- and garbage
-- collecting it is out of scope for this migration and this milestone.
--
-- There is no backfill. Every upgrade path reaches this migration with no
-- rows to reconstruct, because no earlier milestone ever persisted a result.

CREATE TABLE results (
    job_id           UUID        PRIMARY KEY REFERENCES jobs (id),
    attempt_id       UUID        NOT NULL REFERENCES job_attempts (id),

    -- Same domain and same bound as jobs.scope, job_attempts.scope, and
    -- leases.scope. See the foreign key below for how this stays truthful.
    scope            TEXT        NOT NULL CHECK (length(scope) BETWEEN 1 AND 128),

    location         TEXT        NOT NULL CHECK (location IN ('inline', 'object')),

    -- Exactly one shape is populated, tied to location by
    -- results_location_shape below: inline_body alone for 'inline';
    -- object_bucket, object_key, and checksum_sha256 together for 'object'.
    inline_body      JSONB,
    object_bucket    TEXT,
    object_key       TEXT,

    -- Populated for both locations: an operator-visible size even for an
    -- inline result, and the one fact that decided which location this row
    -- uses in the first place (internal/results.Classify).
    size_bytes       BIGINT      NOT NULL CHECK (size_bytes >= 0),

    -- Lowercase hex SHA-256 of the stored bytes, populated only for an
    -- object-located result -- see results_location_shape. An inline result
    -- is read back from PostgreSQL itself, which already guarantees the
    -- bytes are what was written; a separate integrity check would guard
    -- against nothing an object fetched over the network does not already
    -- need it for.
    checksum_sha256  TEXT        CHECK (checksum_sha256 IS NULL OR checksum_sha256 ~ '^[0-9a-f]{64}$'),

    -- Every trusted handler today returns json.RawMessage (see
    -- internal/worker/handler.go's Execute signature), so this is always
    -- 'application/json' in practice. It is a real column rather than a
    -- constant assumed in Go, so a future handler contract change is a
    -- stored fact rather than a silent mismatch.
    content_type     TEXT        NOT NULL DEFAULT 'application/json',

    -- PostgreSQL server time, never a caller's or worker's clock (AGENTS.md
    -- section 6).
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT results_location_shape CHECK (
        (location = 'inline'
            AND inline_body IS NOT NULL
            AND object_bucket IS NULL AND object_key IS NULL
            AND checksum_sha256 IS NULL)
        OR
        (location = 'object'
            AND inline_body IS NULL
            AND object_bucket IS NOT NULL AND object_key IS NOT NULL
            AND checksum_sha256 IS NOT NULL)
    ),

    -- Ties the denormalized scope above to the real job's scope, using the
    -- unique constraint migration 0011 already added for this exact purpose.
    -- A row naming a job that exists but under a different scope is a schema
    -- violation, not a possible application bug.
    CONSTRAINT results_job_id_scope_fkey
        FOREIGN KEY (job_id, scope) REFERENCES jobs (id, scope)
);

COMMENT ON TABLE results IS
    'At most one row per job that succeeded with a recorded result, keyed by '
    'job_id. A small result is inline (JSONB); a result at or above the '
    'configured threshold is stored in an S3-compatible object store, '
    'referenced by bucket and key. See docs/adr/0015-result-storage.md, '
    'including its Known limitation on orphaned attempt-scoped objects.';

COMMENT ON COLUMN results.object_key IS
    'Deterministic key results/<scope>/<job_id>/<attempt_id>, written by '
    'internal/worker/runner.go before the attempt reports success. '
    'Attempt-scoped, not job-scoped -- see the Known limitation in '
    'docs/adr/0015-result-storage.md.';

-- No index beyond the primary key. The one implemented query, the retrieval
-- endpoint, looks up by job_id -- already the primary key -- and checks the
-- fetched row's own scope column; that is a residual filter on one row
-- already found by its primary key, not a second predicate that would
-- justify its own index.
