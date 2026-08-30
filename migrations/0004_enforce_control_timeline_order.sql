-- 0004_enforce_control_timeline_order
--
-- M2 state changes use one fresh PostgreSQL clock sample after authority-row
-- locks are acquired. These constraints make the resulting chronology a hard
-- invariant even for future writers that bypass the current store methods.

ALTER TABLE worker_sessions
    ADD CONSTRAINT worker_sessions_timeline_order CHECK (
        last_heartbeat_at >= registered_at
        AND (ended_at IS NULL OR ended_at >= registered_at)
    );

ALTER TABLE job_attempts
    ADD CONSTRAINT job_attempts_timeline_order CHECK (
        (started_at IS NULL OR started_at >= created_at)
        AND (finished_at IS NULL OR started_at IS NULL OR finished_at >= started_at)
    );

ALTER TABLE leases
    ADD CONSTRAINT leases_timeline_order CHECK (
        renewed_at >= acquired_at
        AND expires_at > renewed_at
        AND (released_at IS NULL OR released_at >= acquired_at)
    );
