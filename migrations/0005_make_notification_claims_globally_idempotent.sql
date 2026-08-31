-- 0005_make_notification_claims_globally_idempotent
--
-- One outbox event may be delivered more than once and to different worker
-- sessions. Its event id is the claim_request_id, so uniqueness must be global:
-- per-session uniqueness would let two copies reserve two different queued
-- jobs. The claim path returns DUPLICATE_NOTIFICATION to the losing consumer.

ALTER TABLE leases
    DROP CONSTRAINT leases_worker_session_id_claim_request_id_key;

ALTER TABLE leases
    ADD CONSTRAINT leases_claim_request_id_key UNIQUE (claim_request_id);

COMMENT ON COLUMN leases.claim_request_id IS
    'Durable outbox event id; globally consumes one duplicated notification at most once.';
