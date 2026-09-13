-- 0014_api_keys
--
-- Milestone M5A: database-backed API keys for the public API surface.
--
-- docs/PROJECT_SPEC.md section 6 already fixed the shape of this decision: a key
-- is high-entropy, returned exactly once, stored as a lookup prefix plus a
-- cryptographic hash, revocable, and scoped. This table is that sentence
-- expressed as schema. See docs/adr/0013-database-backed-api-key-authentication.md.
--
-- What this table deliberately is NOT:
--
--   * It is not a credential store for the internal worker-control surface.
--     Those routes still run under the single configured development scope and
--     stay loopback-bound (docs/CURRENT_STATE.md). Worker/control scopes are a
--     later milestone, and giving them user keys now would conflate two trust
--     boundaries that PROJECT_SPEC section 6 keeps separable.
--   * It is not an authorization model. A key carries exactly one scope, which
--     is the same tenancy boundary jobs, attempts, leases, and DLQ entries have
--     been filtered by since M1. RBAC is post-V1.
--
-- There is no backfill. Every upgrade path reaches this migration with no rows
-- to reconstruct, because no earlier milestone ever persisted a credential.

CREATE TABLE api_keys (
    id           UUID PRIMARY KEY,

    -- The tenancy boundary the authenticated caller acts within. Same domain as
    -- jobs.scope: every public read and mutation is already filtered by it, so
    -- this column decides which rows a key can ever reach. The length bound
    -- matches jobs.scope and dlq_entries.scope exactly rather than approximately,
    -- because a scope that fits here but not there would authenticate to a value
    -- no job could hold.
    scope        TEXT        NOT NULL CHECK (length(scope) BETWEEN 1 AND 128),

    -- Operator-facing label. It exists so a key can be recognized in a listing
    -- and revoked deliberately; nothing authenticates against it.
    name         TEXT        NOT NULL CHECK (length(name) BETWEEN 1 AND 128),

    -- The non-secret lookup segment of the presented key. It is what makes
    -- authentication one indexed equality probe instead of a scan over every
    -- row's hash, which is the whole reason the format is split in two.
    --
    -- UNIQUE, not partial-unique on live rows: a revoked key's prefix must never
    -- be reusable by a different key, or a log line or DLQ record naming that
    -- prefix would become ambiguous after revocation. The bound is wide enough
    -- for the 22-character base64url encoding of 16 random bytes that
    -- internal/auth generates, without pinning this schema to that exact
    -- encoding forever.
    prefix       TEXT        NOT NULL UNIQUE CHECK (length(prefix) BETWEEN 8 AND 32),

    -- Lowercase hex SHA-256 of the secret segment. The secret itself is returned
    -- to the caller exactly once, at creation, and is never stored, logged, or
    -- recoverable from this row.
    --
    -- A plain cryptographic hash rather than a password KDF is deliberate and is
    -- justified by the generator, not by convenience: the secret is 256 bits
    -- drawn from crypto/rand, so it has no guessable structure for a KDF's work
    -- factor to protect. The CHECK pins the stored shape so a future writer
    -- cannot quietly put a raw key, a truncated digest, or a different encoding
    -- in this column.
    secret_hash  TEXT        NOT NULL CHECK (secret_hash ~ '^[0-9a-f]{64}$'),

    -- PostgreSQL server time, never a caller's clock (AGENTS.md section 6).
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- NULL means live. Revocation is a forward-only stamp rather than a DELETE
    -- so that a revoked credential stays auditable and its prefix stays taken.
    revoked_at   TIMESTAMPTZ,

    -- A key cannot have been revoked before it existed.
    CONSTRAINT api_keys_revoked_after_created
        CHECK (revoked_at IS NULL OR revoked_at >= created_at)
);

COMMENT ON TABLE api_keys IS
    'Scoped, revocable API keys for the public /v1 surface. The secret is '
    'returned once at creation and stored only as a SHA-256 digest; prefix is '
    'the non-secret lookup segment. The internal worker-control surface does '
    'not authenticate against this table.';

COMMENT ON COLUMN api_keys.prefix IS
    'Non-secret lookup segment of the presented key. Unique for the lifetime of '
    'the table, including across revocation, so it never names two credentials.';

COMMENT ON COLUMN api_keys.secret_hash IS
    'Lowercase hex SHA-256 of the secret segment. Never the raw key.';

-- Justified by exactly one implemented query: the admin listing in
-- internal/auth.Store.List, which orders by created_at DESC, id DESC and takes a
-- bounded LIMIT. The id tiebreak is part of the index because two keys minted in
-- the same transaction share created_at, and an ordering that is not total would
-- make a bounded page non-deterministic.
CREATE INDEX api_keys_listing_idx ON api_keys (created_at DESC, id DESC);
