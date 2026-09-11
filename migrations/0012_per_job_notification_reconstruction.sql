-- 0012_per_job_notification_reconstruction
--
-- Replaces migration 0011's database-wide reconstruction guard with a per-job
-- one. Migrations 0009, 0010, and 0011 have been published and are immutable
-- (AGENTS.md section 6), so this is a new forward migration rather than an edit
-- to 0011.
--
-- 0011 asked one question of the WHOLE database: "does any job's notification
-- metadata deviate from what 0009's backfill produced?" If any did, it repaired
-- nothing. That is correct only for the upgrade path 0011 was tested on --
-- 0008 straight through to 0011, where no M4 code has ever run.
--
-- The real 0010 -> 0011 path is different. Migrations 0009 and 0010 were
-- published before 0011 existed, so a deployment can be running M4 code on a
-- database at 0010. The moment that code promotes, requeues, or re-notifies a
-- single job, that job's metadata legitimately deviates -- and 0011's guard then
-- refuses to repair EVERY OTHER job, including untouched M3 histories that still
-- carry the wrong generation and the wrong last_notification_at. One advanced
-- job silently cancels the entire repair.
--
-- Eligibility is a property of a job, not of the database, so this asks the same
-- question of each job independently.
--
--
-- THE PER-JOB ELIGIBILITY RULE
--
-- A job is eligible iff it still carries EXACTLY the stamp 0009's backfill
-- wrote, and it has at least one work.available event to reconstruct from:
--
--     notification_generation = 1
--     AND last_notification_at = created_at
--     AND at least one work.available event references it
--
-- That rule separates legacy history from M4-authored metadata precisely,
-- because every way M4 writes notification state breaks one of the two equalities:
--
--   * Promotion and crash-recovery requeue both INCREMENT the generation, so an
--     M4-advanced job has generation >= 2.
--   * Bounded re-notification keeps the generation but advances
--     last_notification_at, so it no longer equals created_at.
--   * A job M4 created is stamped with last_notification_at = clock_timestamp()
--     sampled after its authority locks, while created_at keeps its DEFAULT
--     now() -- the transaction's start. The two are different instants.
--   * A delayed job is stamped generation 0 with a NULL last_notification_at,
--     and NULL = created_at is NULL rather than true.
--
-- The last of those is the only one that is a matter of microseconds rather than
-- of kind, so it is not relied on alone: an M4-created job has exactly one event
-- whose created_at is that same transaction-start now(). Reconstructing it would
-- compute generation 1 and last_notification_at = created_at, which is what it
-- already holds. Every UPDATE below is guarded with IS DISTINCT FROM, so a row
-- whose reconstructed value equals its current one is not written at all. A job
-- that reaches the repair by that route is provably unchanged by it.
--
-- That same guard is what makes this migration idempotent: running the repair a
-- second time recomputes the same values and writes nothing.
--
--
-- WHY CREATION ORDER IS THE TRANSITION SEQUENCE FOR AN ELIGIBLE JOB
--
-- M3 wrote exactly one work.available event per eligibility transition and had
-- no re-notification, so for M3-only history a job's events in creation order
-- ARE its transitions in order. M4 breaks that equivalence -- a re-notification
-- deliberately REUSES the current generation, so two same-generation events are
-- ordinary rather than mislabelled -- which is exactly why an M4-touched job is
-- excluded above rather than renumbered.

-- Every job that still carries the 0009 stamp and has events to rebuild from.
-- Materialized once and reused by both statements so they cannot disagree about
-- which jobs are being repaired.
CREATE TEMPORARY TABLE eligible_legacy_jobs ON COMMIT DROP AS
SELECT j.id AS job_id
FROM jobs j
WHERE j.notification_generation = 1
  AND j.last_notification_at = j.created_at
  AND EXISTS (
      SELECT 1 FROM outbox_events e
      WHERE e.job_id = j.id
        AND e.event_type = 'work.available'
  );

-- Number each eligible job's events in creation order. Ties break on id so the
-- result is deterministic; two events for one job created in the same
-- transaction is not something M3 could produce.
--
-- Events belonging to ineligible jobs, and events that name no job at all, are
-- not in the CTE and are therefore left exactly as they are.
WITH ordered AS (
    SELECT e.id,
           row_number() OVER (PARTITION BY e.job_id ORDER BY e.created_at, e.id) AS generation
    FROM outbox_events e
    JOIN eligible_legacy_jobs l ON l.job_id = e.job_id
    WHERE e.event_type = 'work.available'
)
UPDATE outbox_events o
SET notification_generation = ordered.generation
FROM ordered
WHERE o.id = ordered.id
  AND o.notification_generation IS DISTINCT FROM ordered.generation;

-- A job's current generation is its newest event's, and its last notification is
-- when that event was created -- not when the job was. A job whose reconstructed
-- values already match is not written.
WITH history AS (
    SELECT e.job_id,
           count(*)          AS generations,
           max(e.created_at) AS newest
    FROM outbox_events e
    JOIN eligible_legacy_jobs l ON l.job_id = e.job_id
    WHERE e.event_type = 'work.available'
    GROUP BY e.job_id
)
UPDATE jobs j
SET notification_generation = history.generations,
    last_notification_at    = history.newest
FROM history
WHERE j.id = history.job_id
  AND (j.notification_generation IS DISTINCT FROM history.generations
       OR j.last_notification_at IS DISTINCT FROM history.newest);

COMMENT ON COLUMN jobs.notification_generation IS
    'Monotonic identifier of one eligibility transition. Incremented and stamped '
    'together with last_notification_at whenever the job newly becomes QUEUED. '
    'For jobs carried over from M3, migration 0012 reconstructed both from the '
    'ordering of the work.available events that actually exist, per job.';
