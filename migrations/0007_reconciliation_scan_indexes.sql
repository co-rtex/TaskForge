-- 0007_reconciliation_scan_indexes
--
-- Milestone M3: the two scans taskforge-reconciler actually runs. Each index
-- exists because an implemented query orders by exactly these columns and
-- filters by exactly this predicate (AGENTS.md section 6).

-- Expired-lease scan: leases that are still the capacity ledger and whose
-- server-owned window has passed, in deterministic order so a bounded batch is
-- reproducible. Migration 0003 dropped this index because M2 had no such query;
-- the query now exists.
--
--   SELECT id FROM leases
--   WHERE status = 'ACTIVE' AND expires_at <= clock_timestamp()
--   ORDER BY expires_at, id LIMIT $1
CREATE INDEX leases_active_expiry_idx
    ON leases (expires_at, id)
    WHERE status = 'ACTIVE';

-- Stale-heartbeat scan: process sessions that are still current for their
-- logical worker and have not been heard from since the configured threshold.
-- The predicate matches the "current session" set that
-- worker_sessions_one_current_per_worker_idx already defines.
--
--   SELECT id FROM worker_sessions
--   WHERE status IN ('STARTING', 'HEALTHY', 'DRAINING')
--     AND last_heartbeat_at < clock_timestamp() - make_interval(secs => $1)
--   ORDER BY last_heartbeat_at, id LIMIT $2
CREATE INDEX worker_sessions_current_heartbeat_idx
    ON worker_sessions (last_heartbeat_at, id)
    WHERE status IN ('STARTING', 'HEALTHY', 'DRAINING');
