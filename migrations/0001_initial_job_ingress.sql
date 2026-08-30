-- 0001_initial_job_ingress
--
-- Durable job ingress: the minimum authoritative schema needed to accept a job
-- idempotently and record the broker notification in the same transaction.
--
-- Tables for attempts, workers, sessions, leases, results, DLQ, and API keys are
-- deliberately NOT created here. They arrive in the milestone that puts working
-- behavior on them (see docs/ROADMAP.md).
--
-- Status columns use TEXT + CHECK rather than PostgreSQL enum types so that they
-- can be evolved inside an ordinary transactional migration. The Go side stays
-- typed (internal/jobs.Status).

-- ---------------------------------------------------------------------------
-- queues
-- ---------------------------------------------------------------------------
CREATE TABLE queues (
    name            TEXT PRIMARY KEY
                    CHECK (name ~ '^[a-z0-9][a-z0-9._-]{0,63}$'),
    -- Global execution limit. Not enforced until the claim milestone (M2);
    -- stored now because it is part of the queue's definition, not of claiming.
    max_concurrency INTEGER     NOT NULL DEFAULT 100 CHECK (max_concurrency > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE queues IS
    'Named job queues. max_concurrency is enforced starting in milestone M2.';

INSERT INTO queues (name) VALUES ('default');

-- ---------------------------------------------------------------------------
-- jobs
-- ---------------------------------------------------------------------------
CREATE TABLE jobs (
    id                    UUID PRIMARY KEY,

    -- Authentication scope. Milestone M1 has no API keys and writes a single
    -- configured development scope; M5 replaces it with a real key scope.
    -- Idempotency is scoped by this column so keys can never leak across tenants.
    scope                 TEXT        NOT NULL CHECK (length(scope) BETWEEN 1 AND 128),

    queue                 TEXT        NOT NULL REFERENCES queues (name) ON DELETE RESTRICT,
    job_type              TEXT        NOT NULL CHECK (job_type ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),

    -- Immutable, canonicalized at submission. Always a JSON object.
    payload               JSONB       NOT NULL CHECK (jsonb_typeof(payload) = 'object'),

    -- Full V1 state machine (docs/ARCHITECTURE.md section 4). Only 'QUEUED' is
    -- reachable in M1; the rest are listed so the constraint does not need to be
    -- rewritten as each lifecycle milestone lands.
    status                TEXT        NOT NULL CHECK (status IN (
                              'PENDING', 'QUEUED', 'LEASED', 'RUNNING', 'RETRY_WAIT',
                              'CANCEL_REQUESTED', 'SUCCEEDED', 'CANCELED', 'DEAD_LETTERED')),

    priority              SMALLINT    NOT NULL CHECK (priority BETWEEN 0 AND 100),
    -- Total attempts, including the first.
    max_attempts          SMALLINT    NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    timeout_seconds       INTEGER     NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 86400),
    required_capabilities TEXT[]      NOT NULL DEFAULT '{}',

    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE jobs IS
    'Authoritative job records. Insert-only in milestone M1: no transition exists yet.';

-- Supports "list this tenant''s recent jobs", used by GET /v1/jobs/{id} scope
-- checks today and by the job list endpoint in a later milestone.
CREATE INDEX jobs_scope_created_at_idx ON jobs (scope, created_at DESC);

-- ---------------------------------------------------------------------------
-- idempotency_records
-- ---------------------------------------------------------------------------
-- The composite primary key IS the idempotency guarantee: PostgreSQL, not
-- application code, is what makes concurrent duplicate submissions collapse to
-- one job. See docs/ARCHITECTURE.md section 6.
CREATE TABLE idempotency_records (
    scope               TEXT        NOT NULL,
    idempotency_key     TEXT        NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),

    -- Hex SHA-256 over the canonicalized job-defining fields. Reusing a key with
    -- a different fingerprint is a conflict, not a replay.
    request_fingerprint TEXT        NOT NULL CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),

    job_id              UUID        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (scope, idempotency_key)
);

COMMENT ON TABLE idempotency_records IS
    'One row per (scope, Idempotency-Key). The primary key enforces exactly-one-job.';

-- ---------------------------------------------------------------------------
-- outbox_events
-- ---------------------------------------------------------------------------
-- Written in the SAME transaction as the state change it describes, so a job can
-- never be durable while its notification was never recorded.
-- See docs/adr/0004-transactional-outbox.md.
CREATE TABLE outbox_events (
    id             UUID PRIMARY KEY,
    event_type     TEXT        NOT NULL CHECK (length(event_type) BETWEEN 1 AND 64),
    schema_version INTEGER     NOT NULL CHECK (schema_version >= 1),

    -- Envelope "data" member. Identifiers and routing hints only: never the
    -- authoritative job payload.
    payload        JSONB       NOT NULL CHECK (jsonb_typeof(payload) = 'object'),

    status         TEXT        NOT NULL DEFAULT 'PENDING'
                               CHECK (status IN ('PENDING', 'PUBLISHED')),
    attempts       INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),

    -- Visibility gate. Claiming pushes this forward, so a publisher that dies
    -- mid-flight releases its events automatically instead of blocking them.
    available_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at     TIMESTAMPTZ,
    published_at   TIMESTAMPTZ,
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- A published event must record when, and an unpublished one must not.
    CONSTRAINT outbox_published_at_consistent CHECK (
        (status = 'PUBLISHED' AND published_at IS NOT NULL) OR
        (status = 'PENDING'   AND published_at IS NULL)
    )
);

COMMENT ON TABLE outbox_events IS
    'Pending broker notifications, committed atomically with the state they describe.';

-- The publisher''s only scan: due pending events in deterministic order. Partial
-- so it stays small as published rows accumulate.
CREATE INDEX outbox_events_due_idx
    ON outbox_events (available_at, id)
    WHERE status = 'PENDING';
