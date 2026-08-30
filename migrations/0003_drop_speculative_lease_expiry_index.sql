-- 0003_drop_speculative_lease_expiry_index
--
-- M2 never scans leases by expiry: heartbeat staleness, lease expiry, and
-- reconciliation are M3 behavior. Keep the forward-only 0002 migration
-- immutable after it was exercised locally, and remove the premature index in
-- a new migration instead. M3 will add the exact index its implemented scan
-- justifies.

DROP INDEX leases_active_expiry_idx;
