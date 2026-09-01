-- 0009_job_lifecycle
--
-- Milestone M4, part one: the durable state the complete job lifecycle needs.
--
-- Three things become expressible that M3 could only describe in prose:
--
--   1. Scheduling. A job may be accepted now and become eligible later, and a
--      failed attempt may push its own job's next eligibility into the future.
--      `jobs.available_at` already IS that eligibility key, so nothing new is
--      invented for it; what is added is the requested `scheduled_at` and the
--      notification bookkeeping a scheduler needs to promote due work exactly
--      once and to re-notify work whose only notification was lost.
--
--   2. Outcomes. An attempt that fails must record WHY, in bounded and safe
--      terms, and must be able to answer the same question again if its
--      reporting round trip was ambiguous. That needs a retained outcome
--      identity and bounded typed failure detail.
--
--   3. Deadlines. `timeout_seconds` is a per-attempt execution budget. It has
--      to be a persisted instant, not a duration recomputed by whoever asks,
--      or an ambiguous Start retry would silently restart the clock and a
--      reconciler could not tell a timeout from an abandonment.
--
-- Migrations 0001-0008 are applied and immutable (AGENTS.md section 6). Where
-- this file changes an existing rule it drops and re-adds the constraint here.

-- ---------------------------------------------------------------------------
-- jobs: scheduling, cancellation, replay linkage, notification bookkeeping
-- ---------------------------------------------------------------------------

ALTER TABLE jobs
    ADD COLUMN scheduled_at        TIMESTAMPTZ,
    ADD COLUMN cancel_requested_at TIMESTAMPTZ,
    ADD COLUMN replayed_from_job_id UUID;

COMMENT ON COLUMN jobs.scheduled_at IS
    'Requested earliest execution instant, canonicalized to UTC at submission. '
    'NULL means immediate. available_at remains the authoritative eligibility key: '
    'it starts equal to scheduled_at and is later moved by retry backoff.';

COMMENT ON COLUMN jobs.cancel_requested_at IS
    'PostgreSQL time at which cancellation won, sampled after the job row lock.';

COMMENT ON COLUMN jobs.replayed_from_job_id IS
    'The terminal DEAD_LETTERED job this job replaces. Replay never mutates the '
    'original; it creates a new job linked back to it.';

ALTER TABLE jobs
    ADD CONSTRAINT jobs_replayed_from_fkey
        FOREIGN KEY (replayed_from_job_id) REFERENCES jobs (id) ON DELETE RESTRICT,
    ADD CONSTRAINT jobs_replay_is_not_self CHECK (
        replayed_from_job_id IS NULL OR replayed_from_job_id <> id
    );

-- PENDING is reachable only through delayed submission, so the two must agree.
-- A PENDING job with no requested schedule would be a job nothing will ever
-- promote for a reason anybody can read.
ALTER TABLE jobs
    ADD CONSTRAINT jobs_pending_requires_schedule CHECK (
        status <> 'PENDING' OR scheduled_at IS NOT NULL
    );

-- Cancellation timestamp and cancellation states are inseparable in both
-- directions: no stamp without a cancellation state, and no cancellation state
-- without a stamp.
ALTER TABLE jobs
    ADD CONSTRAINT jobs_cancellation_consistent CHECK (
        (cancel_requested_at IS NULL     AND status NOT IN ('CANCEL_REQUESTED', 'CANCELED')) OR
        (cancel_requested_at IS NOT NULL AND status IN     ('CANCEL_REQUESTED', 'CANCELED'))
    );

-- Notification bookkeeping.
--
-- A generation identifies ONE eligibility transition. It is incremented every
-- time a job newly becomes QUEUED — at submission, at scheduler promotion of a
-- delayed or retry-waiting job, and at crash-recovery requeue. That is what
-- lets bounded re-notification tell "the current transition still has an
-- unpublished notification" from "a stale event belonging to an attempt that is
-- already over", so an old publish-before-mark event can never suppress the
-- fresh event a new transition requires.
ALTER TABLE jobs
    ADD COLUMN notification_generation INTEGER NOT NULL DEFAULT 0
        CHECK (notification_generation >= 0),
    ADD COLUMN last_notification_at TIMESTAMPTZ;

COMMENT ON COLUMN jobs.notification_generation IS
    'Monotonic eligibility generation. 0 means no work.available event has ever '
    'been created for this job (a delayed job before its promotion). Incremented '
    'whenever the job newly becomes QUEUED.';

COMMENT ON COLUMN jobs.last_notification_at IS
    'PostgreSQL time of the most recent work.available event created for the '
    'current generation. Rate-limits bounded re-notification of stranded work.';

-- Backfill. Every job that exists when this migration runs was submitted
-- immediately eligible, which means M1 submission already created exactly one
-- work.available event for it in the submission transaction. Generation 1 with
-- the creation time is therefore the truthful description of what happened, not
-- an approximation.
UPDATE jobs
SET notification_generation = 1,
    last_notification_at    = created_at;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_notification_generation_consistent CHECK (
        (notification_generation = 0 AND last_notification_at IS NULL) OR
        (notification_generation > 0 AND last_notification_at IS NOT NULL)
    );

-- The scheduler's promotion scan: jobs whose durable eligibility time has
-- arrived, in deterministic order so a bounded batch is reproducible.
--
--   SELECT id FROM jobs
--   WHERE status IN ('PENDING', 'RETRY_WAIT') AND available_at <= clock_timestamp()
--   ORDER BY available_at, id LIMIT $1
CREATE INDEX jobs_due_promotion_idx
    ON jobs (available_at, id)
    WHERE status IN ('PENDING', 'RETRY_WAIT');

-- The scheduler's stranded-queue scan: claimable jobs whose last notification
-- is older than the configured re-notification interval.
--
--   SELECT id FROM jobs
--   WHERE status = 'QUEUED'
--     AND last_notification_at < clock_timestamp() - make_interval(secs => $1)
--   ORDER BY last_notification_at, id LIMIT $2
CREATE INDEX jobs_stranded_queued_idx
    ON jobs (last_notification_at, id)
    WHERE status = 'QUEUED';

-- ---------------------------------------------------------------------------
-- job_attempts: deadlines, outcome identity, bounded safe failure detail
-- ---------------------------------------------------------------------------

ALTER TABLE job_attempts
    ADD COLUMN timeout_at         TIMESTAMPTZ,
    ADD COLUMN outcome_request_id UUID,
    ADD COLUMN failure_class      TEXT,
    ADD COLUMN error_code         TEXT,
    ADD COLUMN error_message      TEXT,
    ADD COLUMN retry_delay_ms     BIGINT,
    ADD COLUMN retry_at           TIMESTAMPTZ;

COMMENT ON COLUMN job_attempts.timeout_at IS
    'Persisted per-attempt execution deadline, stamped once when this attempt '
    'started. Lease renewal never moves it: renewal extends lease authority, not '
    'the job''s timeout_seconds budget. The reconciler owns durable timeout '
    'detection against this column.';

COMMENT ON COLUMN job_attempts.outcome_request_id IS
    'Client-generated identity of the terminal outcome report (failure or '
    'cooperative cancellation acknowledgment) that produced this attempt''s '
    'recorded outcome. Retained for the lifetime of attempt history so an '
    'ambiguous report is answered with the committed decision rather than '
    'recomputed.';

COMMENT ON COLUMN job_attempts.failure_class IS
    'How the attempt ended, in retry-policy terms. TIMED_OUT and ABANDONED are '
    'server-authoritative; RETRYABLE and PERMANENT come from a trusted handler.';

-- Bounded, safe, and non-negotiable. Raw handler text, driver errors, panic
-- values, stack traces, and payload contents must never reach these columns;
-- the length and character bounds below are the database's half of that
-- promise, and internal/lifecycle enforces the same bounds before the insert.
ALTER TABLE job_attempts
    ADD CONSTRAINT job_attempts_failure_class_valid CHECK (
        failure_class IS NULL OR
        failure_class IN ('RETRYABLE', 'PERMANENT', 'TIMED_OUT', 'CANCELED', 'ABANDONED')
    ),
    ADD CONSTRAINT job_attempts_error_code_bounded CHECK (
        error_code IS NULL OR error_code ~ '^[a-z0-9][a-z0-9_.-]{0,63}$'
    ),
    ADD CONSTRAINT job_attempts_error_message_bounded CHECK (
        error_message IS NULL OR (
            octet_length(error_message) <= 512 AND
            error_message !~ '[[:cntrl:]]'
        )
    ),
    ADD CONSTRAINT job_attempts_retry_decision_consistent CHECK (
        (retry_delay_ms IS NULL AND retry_at IS NULL) OR
        (retry_delay_ms IS NOT NULL AND retry_at IS NOT NULL AND retry_delay_ms >= 0)
    ),
    -- A deadline only means something once the attempt has actually started,
    -- and it is always in that attempt's future. Legacy attempts carry NULL and
    -- are unaffected: this constraint deliberately does not demand a deadline
    -- on every started attempt, because attempts started before M4 have none.
    ADD CONSTRAINT job_attempts_timeout_after_start CHECK (
        timeout_at IS NULL OR (started_at IS NOT NULL AND timeout_at > started_at)
    );

-- One outcome identity may name at most one attempt, for the whole lifetime of
-- attempt history. Presenting an identity that already committed against a
-- DIFFERENT attempt is a caller error with a deterministic answer, and this
-- index is what makes that answer authoritative rather than a check-then-insert
-- race. Partial because most attempts (claimed, running, succeeded, abandoned)
-- never report one.
--
-- This is deliberately stronger than the renewal-identity index migration 0008
-- describes. A renewal identity is superseded by the next generation and stops
-- being stored; an outcome identity is the permanent record of one terminal
-- decision, so it is never released.
CREATE UNIQUE INDEX job_attempts_outcome_request_id_idx
    ON job_attempts (outcome_request_id)
    WHERE outcome_request_id IS NOT NULL;

COMMENT ON INDEX job_attempts_outcome_request_id_idx IS
    'Lifetime uniqueness: an outcome request id names at most one attempt, ever. '
    'Unlike leases_last_renewal_request_id_idx, nothing releases an entry here.';

-- The reconciler's due-timeout scan.
--
--   SELECT id FROM job_attempts
--   WHERE status = 'RUNNING' AND timeout_at <= clock_timestamp()
--   ORDER BY timeout_at, id LIMIT $1
CREATE INDEX job_attempts_due_timeout_idx
    ON job_attempts (timeout_at, id)
    WHERE status = 'RUNNING';

-- Active cancellation delivery: the attempts one process session could still be
-- executing. The heartbeat response joins these to jobs in CANCEL_REQUESTED.
--
--   SELECT ... FROM job_attempts a JOIN jobs j ... JOIN leases l ...
--   WHERE a.worker_session_id = $1 AND a.status IN ('LEASED', 'RUNNING')
--     AND j.status = 'CANCEL_REQUESTED' AND l.status = 'ACTIVE'
CREATE INDEX job_attempts_session_executing_idx
    ON job_attempts (worker_session_id, id)
    WHERE status IN ('LEASED', 'RUNNING');

-- Revised timeline consistency, forward-only.
--
-- Migration 0002 required every CANCELED attempt to have a start time. M4 makes
-- that false: cancellation can win after a job is claimed but before its attempt
-- starts, and the honest record of that is a CANCELED attempt that never
-- started. The alternative — inventing a started_at so the old constraint holds —
-- would put a lie in attempt history to satisfy a constraint.
--
-- SUCCEEDED, FAILED, and TIMED_OUT keep the original requirement, because each
-- of them can only be reached from RUNNING.
ALTER TABLE job_attempts DROP CONSTRAINT job_attempts_times_consistent;
ALTER TABLE job_attempts
    ADD CONSTRAINT job_attempts_times_consistent CHECK (
        (status = 'LEASED'  AND started_at IS NULL     AND finished_at IS NULL) OR
        (status = 'RUNNING' AND started_at IS NOT NULL AND finished_at IS NULL) OR
        (status IN ('SUCCEEDED', 'FAILED', 'TIMED_OUT')
            AND started_at IS NOT NULL AND finished_at IS NOT NULL) OR
        (status = 'CANCELED'  AND finished_at IS NOT NULL) OR
        (status = 'ABANDONED' AND finished_at IS NOT NULL)
    );

-- ---------------------------------------------------------------------------
-- outbox_events: relational notification metadata
-- ---------------------------------------------------------------------------
--
-- The envelope's data member already carries a job-id hint, but a hint inside
-- JSON is not something the scheduler may make a correctness decision on. The
-- scheduler has to ask "is there already an unpublished notification for THIS
-- job's CURRENT eligibility generation", and that question needs real columns
-- and a real index.
--
-- The published wire contract is unchanged, so no schema version is bumped:
-- these columns are control-plane metadata and are never serialized to the
-- broker.
ALTER TABLE outbox_events
    ADD COLUMN job_id                  UUID,
    ADD COLUMN notification_generation INTEGER;

COMMENT ON COLUMN outbox_events.job_id IS
    'Relational job reference for work.available events. Authoritative for the '
    'scheduler''s pending-event check, unlike the advisory job-id hint inside the '
    'published envelope.';

COMMENT ON COLUMN outbox_events.notification_generation IS
    'The jobs.notification_generation this event advertises. A stale event from '
    'an earlier generation never satisfies the current generation''s check.';

-- Backfill from the payload hint, which M1-M3 always wrote, resolved against a
-- real job row so nothing invented is stored. Jobs are never deleted, so this
-- resolves for every event those milestones created.
UPDATE outbox_events o
SET job_id                  = j.id,
    notification_generation = j.notification_generation
FROM jobs j
WHERE o.event_type = 'work.available'
  AND o.payload ? 'job_id'
  AND j.id::text = (o.payload ->> 'job_id');

ALTER TABLE outbox_events
    ADD CONSTRAINT outbox_events_job_fkey
        FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE,
    -- The pair is meaningless apart: a job reference with no generation cannot
    -- answer the scheduler's question, and a generation with no job has nothing
    -- to be a generation of.
    --
    -- The IS NOT NULL on the generation is load-bearing, not redundant. A CHECK
    -- constraint only rejects a row when it evaluates to FALSE, and
    -- `NULL >= 1` is NULL, not FALSE — so writing the second branch as
    -- `job_id IS NOT NULL AND notification_generation >= 1` would let a job
    -- reference with a NULL generation through, which is exactly the row this
    -- constraint exists to refuse.
    ADD CONSTRAINT outbox_events_notification_metadata_paired CHECK (
        (job_id IS NULL     AND notification_generation IS NULL) OR
        (job_id IS NOT NULL AND notification_generation IS NOT NULL
                           AND notification_generation >= 1)
    );

-- The scheduler's current-generation pending-event check.
--
--   SELECT EXISTS (SELECT 1 FROM outbox_events
--                  WHERE job_id = $1 AND notification_generation = $2
--                    AND status = 'PENDING')
CREATE INDEX outbox_events_pending_job_generation_idx
    ON outbox_events (job_id, notification_generation)
    WHERE status = 'PENDING';
