-- 0011_lifecycle_integrity_and_truthful_backfill
--
-- Three forward-only corrections to the M4 schema. Migrations 0009 and 0010
-- have been published and are immutable (AGENTS.md section 6), so everything
-- here is additive or a drop-and-re-add in a new file.
--
--   1. The outbox notification-metadata CHECK accepted the exact row it exists
--      to refuse.
--   2. The M3 notification backfill assumed a history M3 never guaranteed.
--   3. DLQ and replay lineage were only partly expressible relationally.
--
-- Each is a defect in what 0009 and 0010 asserted, not a change of decision, so
-- no ADR is superseded.

-- ---------------------------------------------------------------------------
-- 1. The paired notification metadata CHECK actually rejects an unpaired row
-- ---------------------------------------------------------------------------
--
-- Migration 0009 wrote the second branch as:
--
--     job_id IS NOT NULL AND notification_generation >= 1
--
-- A CHECK constraint only rejects a row when its expression evaluates to FALSE.
-- `NULL >= 1` is NULL, so for a row carrying a job reference and a NULL
-- generation the first branch is FALSE and the second is NULL, the whole
-- expression is NULL, and PostgreSQL accepts the row — which is precisely the
-- row the constraint exists to refuse. The explicit IS NOT NULL is load-bearing
-- rather than redundant.
ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_notification_metadata_paired;

ALTER TABLE outbox_events
    ADD CONSTRAINT outbox_events_notification_metadata_paired CHECK (
        (job_id IS NULL     AND notification_generation IS NULL) OR
        (job_id IS NOT NULL AND notification_generation IS NOT NULL
                           AND notification_generation >= 1)
    );

-- ---------------------------------------------------------------------------
-- 2. Reconstruct notification history from the events that actually exist
-- ---------------------------------------------------------------------------
--
-- Migration 0009 backfilled every job as `notification_generation = 1` with
-- `last_notification_at = created_at`, on the assumption that each job had
-- exactly one work.available event created at submission. M3 could not
-- guarantee that: reconciliation wrote an ADDITIONAL event every time it
-- requeued an abandoned attempt, so a job that crashed and recovered twice has
-- three events spanning three distinct eligibility transitions.
--
-- Two consequences, both real:
--
--   - A recently requeued job was stamped with its ORIGINAL creation time, so
--     the re-notification interval had already elapsed for it and the scheduler
--     would advertise it again immediately after the upgrade.
--   - Every one of that job's historical events was labelled generation 1, so
--     a long-published submission event was indistinguishable from the event
--     belonging to the transition actually in force. An old event could both
--     block a needed re-notification and impersonate the current one.
--
-- The reconstruction below numbers each job's events in the order they were
-- created and takes the newest as the job's current generation.
--
-- It is guarded, and the guard is what makes row_number() a correct answer
-- rather than a hopeful one. M3 created exactly one event per eligibility
-- transition and had no re-notification at all, so under M3-only history the
-- creation order of a job's events IS its transition sequence. M4's runtime
-- breaks that equivalence: a re-notification deliberately REUSES the current
-- generation, so a job with two same-generation events is ordinary rather than
-- mislabelled.
--
-- The guard therefore asks whether the database still carries exactly what
-- 0009's backfill produced and nothing else. If any job's metadata deviates,
-- M4 has already written notification state, that state is correct by
-- construction, and this migration touches nothing.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM jobs
        WHERE notification_generation <> 1
           OR last_notification_at IS DISTINCT FROM created_at
    ) THEN
        RAISE NOTICE 'skipping notification reconstruction: notification metadata is no longer the 0009 backfill';
        RETURN;
    END IF;

    -- Each job's events, numbered in creation order. Ties break on id so the
    -- result is deterministic; two events for one job created in the same
    -- transaction is not something M3 could produce.
    WITH ordered AS (
        SELECT id,
               row_number() OVER (PARTITION BY job_id ORDER BY created_at, id) AS generation
        FROM outbox_events
        WHERE event_type = 'work.available'
          AND job_id IS NOT NULL
    )
    UPDATE outbox_events o
    SET notification_generation = ordered.generation
    FROM ordered
    WHERE o.id = ordered.id;

    -- A job's current generation is its newest event's, and its last
    -- notification is when that event was created -- not when the job was.
    WITH history AS (
        SELECT job_id,
               count(*)        AS generations,
               max(created_at) AS newest
        FROM outbox_events
        WHERE event_type = 'work.available'
          AND job_id IS NOT NULL
        GROUP BY job_id
    )
    UPDATE jobs j
    SET notification_generation = history.generations,
        last_notification_at    = history.newest
    FROM history
    WHERE j.id = history.job_id;

    -- A job with no resolvable event is deliberately left as 0009 stamped it.
    -- Its events may simply have been purged by an operator, and the honest
    -- position there is "it certainly had one at submission": keeping
    -- generation 1 leaves the job reachable by bounded re-notification, while
    -- resetting it to 0 would make it permanently unadvertisable.
END $$;

-- ---------------------------------------------------------------------------
-- 3. DLQ and replay lineage, expressed relationally
-- ---------------------------------------------------------------------------
--
-- These are tenant and history invariants the database can state, and
-- AGENTS.md section 6 says an invariant the database can enforce is not left to
-- application code. Application code writes consistent rows today; that is not
-- the same as the rows being impossible to write wrongly.
--
-- The unique constraints below exist only so the composite foreign keys have
-- something to reference. Each is trivially satisfied already -- `id` alone is
-- a primary key in both tables -- so none of them constrains anything new by
-- itself.

ALTER TABLE jobs
    ADD CONSTRAINT jobs_id_scope_key UNIQUE (id, scope);

ALTER TABLE job_attempts
    ADD CONSTRAINT job_attempts_id_job_scope_queue_key UNIQUE (id, job_id, scope, queue);

-- Replay lineage cannot cross a scope. A composite self-reference makes
-- `replayed_from_job_id` name a job in the SAME scope, rather than any job
-- anywhere -- which is a tenant boundary, not a tidiness preference.
--
-- MATCH SIMPLE is what is wanted here: a NULL `replayed_from_job_id` satisfies
-- the constraint without a lookup, so an ordinary job is unaffected.
ALTER TABLE jobs DROP CONSTRAINT jobs_replayed_from_fkey;
ALTER TABLE jobs
    ADD CONSTRAINT jobs_replayed_from_fkey
        FOREIGN KEY (replayed_from_job_id, scope) REFERENCES jobs (id, scope)
        ON DELETE RESTRICT;

-- A dead-letter entry's terminal attempt must belong to that exact job, in that
-- exact scope and queue. Migration 0010 referenced the attempt by id alone, so
-- an entry for job A could point at job B's attempt and the database would
-- accept it.
ALTER TABLE dlq_entries DROP CONSTRAINT dlq_entries_attempt_fkey;
ALTER TABLE dlq_entries
    ADD CONSTRAINT dlq_entries_attempt_fkey
        FOREIGN KEY (terminal_attempt_id, job_id, scope, queue)
        REFERENCES job_attempts (id, job_id, scope, queue)
        ON DELETE RESTRICT;

-- Both jobs a replay names must belong to the scope the replay was recorded
-- under. Migration 0010 referenced each by id alone, so `dlq_replays.scope` was
-- an unverified label rather than a binding.
ALTER TABLE dlq_replays DROP CONSTRAINT dlq_replays_original_fkey;
ALTER TABLE dlq_replays
    ADD CONSTRAINT dlq_replays_original_fkey
        FOREIGN KEY (original_job_id, scope) REFERENCES jobs (id, scope)
        ON DELETE RESTRICT;

ALTER TABLE dlq_replays DROP CONSTRAINT dlq_replays_replacement_fkey;
ALTER TABLE dlq_replays
    ADD CONSTRAINT dlq_replays_replacement_fkey
        FOREIGN KEY (replacement_job_id, scope) REFERENCES jobs (id, scope)
        ON DELETE RESTRICT;

-- And the two halves of a replay must agree with each other. Without this, a
-- replay record could name a replacement whose own `replayed_from_job_id`
-- points somewhere else entirely -- two mutually contradictory accounts of the
-- same lineage, both individually valid.
--
-- `id` is already a primary key, so this unique constraint adds no restriction;
-- it exists so the foreign key below can reference the pair.
ALTER TABLE jobs
    ADD CONSTRAINT jobs_id_replay_source_key UNIQUE (id, replayed_from_job_id);

ALTER TABLE dlq_replays
    ADD CONSTRAINT dlq_replays_lineage_fkey
        FOREIGN KEY (replacement_job_id, original_job_id)
        REFERENCES jobs (id, replayed_from_job_id)
        ON DELETE RESTRICT;

COMMENT ON CONSTRAINT dlq_replays_lineage_fkey ON dlq_replays IS
    'The replacement job must itself record this original as its replay source. '
    'Both columns are NOT NULL, so this is always checked.';
