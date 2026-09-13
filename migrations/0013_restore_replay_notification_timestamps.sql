-- 0013_restore_replay_notification_timestamps
--
-- Repairs the one class of row migration 0012 moved BACKWARD.
--
-- Migrations 0009 through 0012 are published and immutable (AGENTS.md section
-- 6), so this is a new forward migration rather than an edit to 0012.
--
--
-- WHAT 0012 GOT WRONG
--
-- 0012 repairs a job only if it still carries exactly the stamp 0009's backfill
-- wrote: notification_generation = 1 AND last_notification_at = created_at. That
-- was believed to exclude every M4-authored job, because every way M4 writes
-- notification metadata was thought to break one of the two equalities.
--
-- One way does not. A DLQ replay creates its replacement job with all four of
-- created_at, updated_at, available_at, and last_notification_at set to the SAME
-- post-lock clock_timestamp() sample, and with notification_generation = 1. So
-- last_notification_at = created_at holds, and the replacement matches the
-- legacy fingerprint exactly.
--
-- Its work.available event does not share that instant. The event is inserted in
-- the same transaction but takes the column DEFAULT now(), which is the
-- TRANSACTION START time -- strictly earlier than the post-lock sample, and
-- arbitrarily earlier when the replay waited on the queue row lock. 0012 then
-- sets the job's last_notification_at to that older event timestamp, moving it
-- backward by however long the transaction waited.
--
-- The consequence is not cosmetic. last_notification_at is what bounded
-- re-notification measures staleness from, so a rewind large enough to cross the
-- re-notification interval makes the scheduler re-advertise a job that was just
-- advertised.
--
--
-- ELIGIBILITY: PROVE THE ROW WAS AFFECTED, DO NOT GUESS
--
-- Every condition below must hold, and together they describe a row only 0012
-- can have produced. Timestamp coincidence alone is not used to identify a
-- replay -- the explicit lineage M4 already records is.
--
--   1. dlq_replays.replacement_job_id = jobs.id, in the same scope. This is the
--      authoritative record that the replay transaction created this job, not an
--      inference from its shape.
--   2. jobs.replayed_from_job_id = dlq_replays.original_job_id. The job's own
--      lineage column agrees with that record, so the two independent halves of
--      one replay are consistent.
--   3. notification_generation = 1. A promotion or a crash-recovery requeue
--      increments it, so anything that has legitimately advanced since the
--      replay is excluded.
--   4. Exactly one work.available event references the job: the one the replay
--      transaction wrote. A re-notification would have added a second, and would
--      also have moved last_notification_at forward.
--   5. last_notification_at = that event's created_at. This is the value 0012
--      writes, so the row is not merely eligible for 0012 -- it demonstrably
--      carries 0012's output.
--   6. last_notification_at < created_at. Nothing in M4 ever stamps a
--      notification earlier than the job it belongs to: submission, promotion,
--      requeue, and re-notification all sample server time at or after the row
--      exists. A notification timestamp older than its own job is 0012's rewind
--      signature and nothing else's.
--
-- Conditions 3, 4, and 6 are also what keeps later valid M4 writes untouched: a
-- replacement that has since been promoted, requeued, or re-notified fails at
-- least one of them and is left exactly as it is.
--
--
-- WHAT IS RESTORED
--
-- last_notification_at is set back to the replacement job's own created_at,
-- which IS the authoritative instant the replay transaction assigned -- the
-- replay writes created_at, updated_at, available_at, and last_notification_at
-- from one clock_timestamp() sample, so created_at is a surviving copy of the
-- value 0012 overwrote. Nothing else is touched: generations, event rows, and
-- event metadata are left alone, because 0012 did not change them for these jobs
-- (the single event's row_number is 1, which is what it already held, and 0012's
-- IS DISTINCT FROM guard meant it was never written).
--
-- The update is idempotent. Once last_notification_at = created_at, condition 6
-- is false and the row is never selected again.

UPDATE jobs j
SET last_notification_at = j.created_at
FROM dlq_replays r
WHERE r.replacement_job_id = j.id
  AND r.scope = j.scope
  AND j.replayed_from_job_id = r.original_job_id
  AND j.notification_generation = 1
  AND j.last_notification_at < j.created_at
  AND (
      SELECT count(*) FROM outbox_events e
      WHERE e.job_id = j.id AND e.event_type = 'work.available'
  ) = 1
  AND j.last_notification_at = (
      SELECT e.created_at FROM outbox_events e
      WHERE e.job_id = j.id AND e.event_type = 'work.available'
  );

COMMENT ON COLUMN jobs.last_notification_at IS
    'PostgreSQL server time at which this job was last advertised as claimable. '
    'Bounded re-notification measures staleness from it, so it is never moved '
    'backward. Migration 0012 could move it backward for a replay-created '
    'replacement job; migration 0013 restores those to the instant the replay '
    'transaction assigned.';
