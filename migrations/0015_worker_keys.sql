-- 0015_worker_keys
--
-- Milestone M5B: database-backed worker credentials for the internal
-- worker-control surface, closing the limitation M5A recorded and left open.
--
-- docs/PROJECT_SPEC.md section 6 keeps worker/control scopes separable from
-- user scopes, and ADR-0013 deferred exactly this table rather than reusing
-- api_keys for it. This is that deferral resolved: worker_keys is its own
-- table, mirroring api_keys' shape column for column, because the credential
-- properties (high-entropy, prefix-plus-hash, revocable, scoped) are the same
-- ones PROJECT_SPEC already fixed -- only the trust boundary they guard
-- differs. See docs/adr/0014-worker-control-authentication.md.
--
-- What this table deliberately is NOT:
--
--   * It is not api_keys. A worker key authenticates registration of a
--     process session and nothing about the public /v1 surface; an API key
--     authenticates the public surface and nothing about worker control. A
--     caller holding one can never present it where the other is checked --
--     they are verified against different tables entirely.
--   * It is not an authorization model beyond the one scope every credential
--     in this system already carries. RBAC is post-V1.
--
-- There is no backfill for worker_keys itself: every upgrade path reaches
-- this migration with no rows to reconstruct, exactly as 0014 found for
-- api_keys, because no earlier milestone ever persisted a worker credential.

CREATE TABLE worker_keys (
    id           UUID PRIMARY KEY,

    -- The scope a worker registering with this key will operate under. Same
    -- domain and same bound as workers.scope and worker_sessions.scope: a
    -- worker-key scope that fit here but not there would authenticate a
    -- session into a value no worker row could ever hold.
    scope        TEXT        NOT NULL CHECK (length(scope) BETWEEN 1 AND 128),

    -- Operator-facing label, exactly as api_keys.name. Nothing authenticates
    -- against it.
    name         TEXT        NOT NULL CHECK (length(name) BETWEEN 1 AND 128),

    -- The non-secret lookup segment. UNIQUE across the whole table, not only
    -- live rows, for the identical reason api_keys.prefix is: a revoked
    -- worker key's prefix must never be reusable, or a log line naming it
    -- would become ambiguous after revocation.
    prefix       TEXT        NOT NULL UNIQUE CHECK (length(prefix) BETWEEN 8 AND 32),

    -- Lowercase hex SHA-256 of the secret segment, identical shape and
    -- identical justification to api_keys.secret_hash: the secret is 256
    -- bits from crypto/rand with no structure for a KDF to protect, so a
    -- plain digest is the honest choice, not a shortcut.
    secret_hash  TEXT        NOT NULL CHECK (secret_hash ~ '^[0-9a-f]{64}$'),

    -- PostgreSQL server time (AGENTS.md section 6).
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- NULL means live. A forward-only stamp, not a DELETE, so a revoked
    -- credential stays auditable and its prefix stays taken.
    revoked_at   TIMESTAMPTZ,

    CONSTRAINT worker_keys_revoked_after_created
        CHECK (revoked_at IS NULL OR revoked_at >= created_at)
);

COMMENT ON TABLE worker_keys IS
    'Scoped, revocable credentials for registering a worker process session. '
    'The secret is returned once at creation and stored only as a SHA-256 '
    'digest; prefix is the non-secret lookup segment. Distinct from api_keys: '
    'a worker key never authenticates the public /v1 surface.';

COMMENT ON COLUMN worker_keys.prefix IS
    'Non-secret lookup segment. Unique for the lifetime of the table, '
    'including across revocation, so it never names two credentials.';

COMMENT ON COLUMN worker_keys.secret_hash IS
    'Lowercase hex SHA-256 of the secret segment. Never the raw key.';

-- Justified by exactly one implemented query: the admin listing's
-- created_at DESC, id DESC keyset order, identical in shape and in
-- justification to api_keys_listing_idx.
CREATE INDEX worker_keys_listing_idx ON worker_keys (created_at DESC, id DESC);

-- ---------------------------------------------------------------------------
-- worker_sessions.worker_key_id
-- ---------------------------------------------------------------------------
--
-- Records which credential authenticated the registration that created this
-- session row, so a revocation can be recognized on that session's next
-- control-plane call without re-checking a credential on every call and
-- without touching a single line of the existing fenced Heartbeat, Claim,
-- Start, Succeed, Fail, AcknowledgeCancellation, or RenewLease transactions:
-- the HTTP layer reads this column once, alongside the scope it already
-- reads, before dispatching into that unmodified code.
--
-- Nullable, and deliberately not backfilled. worker_sessions is one row per
-- process boot -- short-lived and process-scoped, not a durable history like
-- jobs -- so a session row that predates this migration has no credential to
-- record, and inventing one would be a fabricated fact, not a repaired one.
-- A NULL here is simply never treated as revoked, which is exactly the
-- security posture every session had before this migration existed, so
-- nothing regresses for a row this migration cannot truthfully complete.
--
-- No FOREIGN KEY to worker_keys(scope) alongside id: a session's own `scope`
-- column already carries the value that mattered at registration time and is
-- never revised by a later credential rotation, so requiring the two to
-- stay in lockstep would only make a routine key rotation look like a data
-- inconsistency it is not.
ALTER TABLE worker_sessions
    ADD COLUMN worker_key_id UUID REFERENCES worker_keys (id);

COMMENT ON COLUMN worker_sessions.worker_key_id IS
    'The worker key that authenticated this session''s registration. NULL for '
    'sessions registered before this migration, which are never treated as '
    'revoked. Checked, alongside scope, on every subsequent control-plane '
    'call for this session -- see internal/workers.Store.SessionScope.';

-- Justified by exactly one implemented query: internal/workers.Store's new
-- SessionScope lookup reads scope and worker_key_id by session id, which is
-- already the primary key; this index exists for the reverse direction an
-- operator or a future revocation-audit query needs -- "which live sessions
-- did this key create" -- and costs nothing on the hot path.
CREATE INDEX worker_sessions_worker_key_id_idx
    ON worker_sessions (worker_key_id)
    WHERE worker_key_id IS NOT NULL;
