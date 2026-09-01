-- 0010_logical_dlq_and_replay
--
-- Milestone M4, part two: the authoritative logical dead-letter queue and the
-- durable identity of a replay.
--
-- This is TaskForge's OWN dead-letter concept — jobs that failed permanently or
-- exhausted their attempt budget — and it lives in PostgreSQL because it is
-- authoritative state an operator acts on. It is not the broker's
-- infrastructure DLQ for unprocessable notification messages, and the two must
-- never be conflated (docs/ARCHITECTURE.md section 3).
--
-- Replay deliberately does NOT resurrect a terminal job. A terminal job never
-- returns to a non-terminal state (reliability invariant 2), so replay creates a
-- new job linked back through jobs.replayed_from_job_id and leaves the original,
-- its attempts, its leases, and its DLQ entry exactly as they were.

-- ---------------------------------------------------------------------------
-- dlq_entries
-- ---------------------------------------------------------------------------
CREATE TABLE dlq_entries (
    id                  UUID PRIMARY KEY,

    scope               TEXT        NOT NULL CHECK (length(scope) BETWEEN 1 AND 128),
    queue               TEXT        NOT NULL,

    -- One row per dead-lettered job, enforced by the database rather than by
    -- whichever code path happened to reach DEAD_LETTERED. Permanent failure,
    -- exhausted retryable failure, exhausted timeout, and exhausted M3
    -- abandonment all insert through one helper, and this is what makes a
    -- second insert impossible instead of merely unlikely.
    job_id              UUID        NOT NULL UNIQUE,

    -- The attempt whose outcome ended the job. NULL is reachable only for a
    -- cancellation-free dead-letter with no attempt at all, which no M4 path
    -- produces; it is nullable rather than NOT NULL so a future terminal reason
    -- that genuinely has no attempt does not need a lie to be recorded.
    terminal_attempt_id UUID,

    reason              TEXT        NOT NULL CHECK (
                            reason IN ('PERMANENT_FAILURE', 'ATTEMPTS_EXHAUSTED')),

    -- The PostgreSQL instant at which the job became DEAD_LETTERED, sampled
    -- after every authority lock in the transaction that dead-lettered it. It
    -- is both the operator-visible "when" and the keyset pagination key, so
    -- there is exactly one timestamp here and no second one to drift from it.
    created_at          TIMESTAMPTZ NOT NULL,

    -- Composite foreign keys, in the style migration 0002 established: it is
    -- impossible to record the right job id under the wrong scope or queue.
    CONSTRAINT dlq_entries_job_fkey
        FOREIGN KEY (job_id, scope, queue) REFERENCES jobs (id, scope, queue)
        ON DELETE RESTRICT,
    CONSTRAINT dlq_entries_attempt_fkey
        FOREIGN KEY (terminal_attempt_id) REFERENCES job_attempts (id)
        ON DELETE RESTRICT
);

COMMENT ON TABLE dlq_entries IS
    'Authoritative logical job DLQ: one row per DEAD_LETTERED job. Distinct from '
    'the broker''s infrastructure DLQ for unprocessable notification messages.';

-- GET /v1/dlq, scope-filtered, newest first, with (created_at, id) keyset
-- pagination so equal timestamps can neither duplicate nor skip a row.
--
--   SELECT ... FROM dlq_entries
--   WHERE scope = $1 AND ($2::timestamptz IS NULL OR (created_at, id) < ($2, $3))
--   ORDER BY created_at DESC, id DESC LIMIT $4
CREATE INDEX dlq_entries_scope_keyset_idx
    ON dlq_entries (scope, created_at DESC, id DESC);

-- ---------------------------------------------------------------------------
-- dlq_replays
-- ---------------------------------------------------------------------------
--
-- Replay identity. The composite primary key IS the idempotency guarantee, in
-- exactly the way idempotency_records is for submission: PostgreSQL, not
-- application code, is what makes two concurrent identical replay requests
-- collapse into one replacement job.
--
-- POST /v1/dlq/{job_id}/replay and POST /v1/jobs/{job_id}/retry are the same
-- operation and share this one namespace, so the same identity presented
-- through either route returns the same replacement rather than two jobs.
CREATE TABLE dlq_replays (
    scope              TEXT        NOT NULL CHECK (length(scope) BETWEEN 1 AND 128),
    original_job_id    UUID        NOT NULL,
    idempotency_key    TEXT        NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),

    -- UNIQUE, not merely referenced: one replacement job belongs to exactly one
    -- replay identity, so a replacement can never be double-claimed by two
    -- identities.
    replacement_job_id UUID        NOT NULL UNIQUE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (scope, original_job_id, idempotency_key),

    CONSTRAINT dlq_replays_original_fkey
        FOREIGN KEY (original_job_id) REFERENCES jobs (id) ON DELETE RESTRICT,
    CONSTRAINT dlq_replays_replacement_fkey
        FOREIGN KEY (replacement_job_id) REFERENCES jobs (id) ON DELETE RESTRICT,
    CONSTRAINT dlq_replays_replacement_is_not_original CHECK (
        replacement_job_id <> original_job_id
    )
);

COMMENT ON TABLE dlq_replays IS
    'One row per (scope, original job, Idempotency-Key). The primary key enforces '
    'exactly-one-replacement-job; different keys deliberately create different '
    'replacement jobs.';

-- Replay linkage for DLQ listing, and the lookup that answers "has this
-- original already been replayed".
--
--   SELECT count(*) FROM dlq_replays WHERE original_job_id = $1
CREATE INDEX dlq_replays_original_job_idx
    ON dlq_replays (original_job_id);

-- ---------------------------------------------------------------------------
-- Backfill: milestone M3's reachable DEAD_LETTERED jobs
-- ---------------------------------------------------------------------------
--
-- ADR-0009 made DEAD_LETTERED reachable one milestone before the DLQ that reads
-- it: an abandoned attempt that consumed the total attempt budget dead-letters
-- the job, because requeueing it would create work the claim predicate could
-- never claim. Those jobs are real and an operator must be able to list and
-- replay them, so they are backfilled here rather than left invisible.
--
-- ATTEMPTS_EXHAUSTED is the truthful reason: M3 had no failure classification
-- at all, and the terminal attempt is the last one recorded for the job — the
-- ABANDONED attempt whose finish consumed the budget.
INSERT INTO dlq_entries (id, scope, queue, job_id, terminal_attempt_id, reason, created_at)
SELECT gen_random_uuid(),
       j.scope,
       j.queue,
       j.id,
       (SELECT a.id
          FROM job_attempts a
         WHERE a.job_id = j.id
         ORDER BY a.attempt_number DESC
         LIMIT 1),
       'ATTEMPTS_EXHAUSTED',
       j.updated_at
FROM jobs j
WHERE j.status = 'DEAD_LETTERED'
  AND NOT EXISTS (SELECT 1 FROM dlq_entries d WHERE d.job_id = j.id);
