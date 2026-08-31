-- 0006_lease_renewal_identity
--
-- Milestone M3: durable state for an idempotent, fenced lease renewal.
--
-- Renewal cannot be "set expires_at = clock_timestamp() + duration" on every
-- request. A worker that retries an ambiguous renewal would then silently
-- extend its authority twice, and two concurrent renewals for the same window
-- would both succeed. Renewal therefore carries a monotonic generation and the
-- client-generated identity of the request that produced it, so an exact replay
-- is recognizable and a stale or competing generation is a deterministic
-- conflict rather than an extra extension.

ALTER TABLE leases
    ADD COLUMN renewal_version INTEGER NOT NULL DEFAULT 0
        CHECK (renewal_version >= 0),
    ADD COLUMN last_renewal_request_id UUID;

COMMENT ON COLUMN leases.renewal_version IS
    'Monotonic renewal generation. 0 is the window issued by the claim itself.';

COMMENT ON COLUMN leases.last_renewal_request_id IS
    'Client-generated identity of the renewal that produced the current generation.';

-- A lease has either never been renewed (generation 0, no renewal identity) or
-- has been renewed and records the identity that did it. Nothing may record one
-- without the other.
ALTER TABLE leases
    ADD CONSTRAINT leases_renewal_identity_consistent CHECK (
        (renewal_version = 0 AND last_renewal_request_id IS NULL) OR
        (renewal_version > 0 AND last_renewal_request_id IS NOT NULL)
    );

-- Renewal identities are globally unique so reusing one against a different
-- lease is caught by the database rather than left to application code. The
-- store looks the identity up under the same locks and returns a stable domain
-- conflict; this index is what makes that check authoritative instead of a
-- check-then-update race. Partial because generation 0 stores no identity.
CREATE UNIQUE INDEX leases_last_renewal_request_id_idx
    ON leases (last_renewal_request_id)
    WHERE last_renewal_request_id IS NOT NULL;
