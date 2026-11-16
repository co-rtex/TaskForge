-- 0019_claimability_gated_renotification
--
-- ADR-0029: bounded re-notification is gated on claimability. A QUEUED job
-- that no current worker session could claim right now -- no HEALTHY session
-- of its scope has its worker group, capabilities and job type with a free
-- slot, or its queue is at capacity -- gets no replacement notification: there
-- is nobody for it to wake, and a published notification nobody can use is
-- held live by declining workers or simply unread, which is how one stuck job
-- used to accumulate one more live broker message every interval.
--
-- What a gate needs from the schema is a way for a job the gate deferred to
-- step out of the scan's way. The scan reads the oldest stranded jobs first
-- and is batch-limited, and a deferred job's last_notification_at does not
-- move (no notification was created, and that column means exactly "when the
-- most recent event for the current generation was created"). Without a
-- column of its own, deferred jobs would sort first on every pass forever and
-- starve every claimable stranded job behind them.
--
-- renotify_checked_at records when the scheduler last found the job's current
-- advertisement unclaimable. The scan orders by the later of the two
-- instants, so a deferred job is reconsidered once per re-notification
-- interval, like a notified one. It is never authority for anything: a value
-- older than last_notification_at (left by an earlier generation) is
-- harmlessly dominated by GREATEST, which ignores NULL.
--
-- ADD COLUMN with no DEFAULT is catalog-only: no rewrite, and every existing
-- row keeps its bytes, reading NULL, which means "never deferred".

ALTER TABLE jobs
    ADD COLUMN renotify_checked_at TIMESTAMPTZ;

COMMENT ON COLUMN jobs.renotify_checked_at IS
    'When bounded re-notification last deferred this job because no current '
    'worker session could claim it (ADR-0029). Orders the re-notification scan '
    'together with last_notification_at; never authority for any transition.';

-- Justified by jobs.Store.RenotifyStrandedQueued's candidate scan:
--
--   SELECT id FROM jobs
--   WHERE status = 'QUEUED'
--     AND GREATEST(last_notification_at, renotify_checked_at) < <cutoff>
--   ORDER BY GREATEST(last_notification_at, renotify_checked_at), id
--   LIMIT <batch>
--
-- It replaces jobs_stranded_queued_idx, whose one query this was, with the
-- same partial predicate and the scan's new key.
CREATE INDEX jobs_renotify_due_idx
    ON jobs ((GREATEST(last_notification_at, renotify_checked_at)), id)
    WHERE status = 'QUEUED';

DROP INDEX jobs_stranded_queued_idx;

-- Justified by the claimability gate, run under the queue and job locks for
-- each re-notification candidate:
--
--   SELECT ... FROM worker_sessions s
--   WHERE s.scope = $1 AND s.status = 'HEALTHY' AND s.worker_group = $2 AND ...
--
-- Every process boot adds a row and old rows are never deleted, so without it
-- the gate would read a scope's whole session history per candidate. Only
-- HEALTHY sessions can claim, and they are a small, bounded set.
CREATE INDEX worker_sessions_healthy_scope_idx
    ON worker_sessions (scope, worker_group)
    WHERE status = 'HEALTHY';
