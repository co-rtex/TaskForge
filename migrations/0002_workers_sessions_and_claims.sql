-- 0002_workers_sessions_and_claims
--
-- Milestone M2: durable logical workers and process sessions, atomic job
-- claiming, attempts, and leases. PostgreSQL remains the serialization point
-- for every capacity and ownership decision.

-- A queue names the worker group allowed to claim from it. M1 created only the
-- default queue, so the backfill is unambiguous.
ALTER TABLE queues
    ADD COLUMN worker_group TEXT NOT NULL DEFAULT 'default'
        CHECK (worker_group ~ '^[a-z0-9][a-z0-9._-]{0,63}$');

COMMENT ON COLUMN queues.worker_group IS
    'Worker group allowed to claim this queue.';

-- available_at is the server-owned eligibility key used by the documented
-- deterministic claim order. Existing M1 jobs were immediately eligible, so
-- their original creation time is the truthful backfill.
ALTER TABLE jobs ADD COLUMN available_at TIMESTAMPTZ;
UPDATE jobs SET available_at = created_at;
ALTER TABLE jobs ALTER COLUMN available_at SET NOT NULL;
ALTER TABLE jobs ALTER COLUMN available_at SET DEFAULT now();

COMMENT ON COLUMN jobs.available_at IS
    'PostgreSQL-authoritative eligibility time used by claim and, later, scheduling.';

-- Composite foreign keys below make it impossible to bind attempts or leases
-- to the right id but the wrong scope or queue.
ALTER TABLE jobs
    ADD CONSTRAINT jobs_id_scope_queue_key UNIQUE (id, scope, queue);

-- The claim scan: one scope and queue, eligible QUEUED rows, then strict
-- priority with deterministic tie breaking. The query also filters handler
-- type and required capabilities before taking FOR UPDATE SKIP LOCKED.
CREATE INDEX jobs_claim_idx
    ON jobs (scope, queue, priority DESC, available_at ASC, created_at ASC, id ASC)
    WHERE status = 'QUEUED';

-- Supports the required-capabilities containment predicate in that scan.
CREATE INDEX jobs_claim_capabilities_idx
    ON jobs USING GIN (required_capabilities)
    WHERE status = 'QUEUED';

-- ---------------------------------------------------------------------------
-- workers and process sessions
-- ---------------------------------------------------------------------------
CREATE TABLE workers (
    id         UUID PRIMARY KEY,
    scope      TEXT        NOT NULL CHECK (length(scope) BETWEEN 1 AND 128),
    name       TEXT        NOT NULL
                           CHECK (name ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (scope, name),
    UNIQUE (id, scope)
);

COMMENT ON TABLE workers IS
    'Stable logical worker identities. One process boot is one worker_sessions row.';

CREATE TABLE worker_sessions (
    id                UUID PRIMARY KEY,
    worker_id         UUID        NOT NULL,
    scope             TEXT        NOT NULL CHECK (length(scope) BETWEEN 1 AND 128),
    hostname          TEXT        NOT NULL CHECK (length(hostname) BETWEEN 1 AND 255),
    worker_group      TEXT        NOT NULL
                                  CHECK (worker_group ~ '^[a-z0-9][a-z0-9._-]{0,63}$'),
    concurrency_limit SMALLINT    NOT NULL CHECK (concurrency_limit BETWEEN 1 AND 256),
    capabilities      TEXT[]      NOT NULL DEFAULT '{}'
                                  CHECK (cardinality(capabilities) <= 64)
                                  CHECK (array_position(capabilities, NULL) IS NULL),
    -- The trusted handler registry is declared at process registration. Claim
    -- never hands a process a job type it cannot execute.
    supported_job_types TEXT[]     NOT NULL
                                  CHECK (cardinality(supported_job_types) BETWEEN 1 AND 64)
                                  CHECK (array_position(supported_job_types, NULL) IS NULL),
    status             TEXT        NOT NULL CHECK (status IN (
                                      'STARTING', 'HEALTHY', 'DRAINING',
                                      'UNHEALTHY', 'OFFLINE')),
    registered_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at           TIMESTAMPTZ,

    CONSTRAINT worker_sessions_worker_fkey
        FOREIGN KEY (worker_id, scope) REFERENCES workers (id, scope)
        ON DELETE RESTRICT,
    UNIQUE (id, worker_id, scope)
);

COMMENT ON TABLE worker_sessions IS
    'One process lifetime. Leases bind to this id so a later boot cannot inherit them.';

-- Registration replaces the prior process boot transactionally. The old row
-- and its leases remain as history, but only the current session may claim or
-- report outcomes for this logical worker.
CREATE UNIQUE INDEX worker_sessions_one_current_per_worker_idx
    ON worker_sessions (worker_id)
    WHERE status IN ('STARTING', 'HEALTHY', 'DRAINING');

-- ---------------------------------------------------------------------------
-- attempts and leases
-- ---------------------------------------------------------------------------
CREATE TABLE job_attempts (
    id                UUID PRIMARY KEY,
    job_id            UUID        NOT NULL,
    scope             TEXT        NOT NULL CHECK (length(scope) BETWEEN 1 AND 128),
    queue             TEXT        NOT NULL,
    attempt_number    SMALLINT    NOT NULL CHECK (attempt_number > 0),
    worker_id         UUID        NOT NULL,
    worker_session_id UUID        NOT NULL,
    status            TEXT        NOT NULL CHECK (status IN (
                                      'LEASED', 'RUNNING', 'SUCCEEDED', 'FAILED',
                                      'TIMED_OUT', 'CANCELED', 'ABANDONED')),
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT job_attempts_job_fkey
        FOREIGN KEY (job_id, scope, queue) REFERENCES jobs (id, scope, queue)
        ON DELETE RESTRICT,
    CONSTRAINT job_attempts_session_fkey
        FOREIGN KEY (worker_session_id, worker_id, scope)
        REFERENCES worker_sessions (id, worker_id, scope)
        ON DELETE RESTRICT,
    CONSTRAINT job_attempts_times_consistent CHECK (
        (status = 'LEASED' AND started_at IS NULL AND finished_at IS NULL) OR
        (status = 'RUNNING' AND started_at IS NOT NULL AND finished_at IS NULL) OR
        (status IN ('SUCCEEDED', 'FAILED', 'TIMED_OUT', 'CANCELED')
            AND started_at IS NOT NULL AND finished_at IS NOT NULL) OR
        (status = 'ABANDONED' AND finished_at IS NOT NULL)
    ),
    UNIQUE (job_id, attempt_number),
    CONSTRAINT job_attempts_binding_key UNIQUE
        (id, job_id, scope, queue, worker_id, worker_session_id)
);

COMMENT ON TABLE job_attempts IS
    'Immutable attempt identities with typed lifecycle history; attempts are never overwritten.';

CREATE TABLE leases (
    id                UUID PRIMARY KEY,
    job_id            UUID        NOT NULL,
    attempt_id        UUID        NOT NULL UNIQUE,
    scope             TEXT        NOT NULL CHECK (length(scope) BETWEEN 1 AND 128),
    queue             TEXT        NOT NULL,
    worker_id         UUID        NOT NULL,
    worker_session_id UUID        NOT NULL,
    -- Generated once by the worker for one logical claim RPC. A retry after a
    -- committed-but-lost response returns this same assignment instead of
    -- consuming another capacity slot.
    claim_request_id  UUID        NOT NULL,
    status            TEXT        NOT NULL CHECK (status IN (
                                      'ACTIVE', 'COMPLETED', 'EXPIRED', 'RELEASED')),
    acquired_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at        TIMESTAMPTZ NOT NULL,
    renewed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at       TIMESTAMPTZ,

    CONSTRAINT leases_job_fkey
        FOREIGN KEY (job_id, scope, queue) REFERENCES jobs (id, scope, queue)
        ON DELETE RESTRICT,
    CONSTRAINT leases_session_fkey
        FOREIGN KEY (worker_session_id, worker_id, scope)
        REFERENCES worker_sessions (id, worker_id, scope)
        ON DELETE RESTRICT,
    CONSTRAINT leases_attempt_binding_fkey
        FOREIGN KEY (attempt_id, job_id, scope, queue, worker_id, worker_session_id)
        REFERENCES job_attempts
            (id, job_id, scope, queue, worker_id, worker_session_id),
    UNIQUE (worker_session_id, claim_request_id),
    CONSTRAINT leases_expiry_after_acquisition CHECK (expires_at > acquired_at),
    CONSTRAINT leases_release_consistent CHECK (
        (status = 'ACTIVE' AND released_at IS NULL) OR
        (status <> 'ACTIVE' AND released_at IS NOT NULL)
    )
);

-- Hard invariant: broker duplicates, API retries, or concurrent claimers can
-- never create two active owners for one job.
CREATE UNIQUE INDEX leases_one_active_per_job_idx
    ON leases (job_id)
    WHERE status = 'ACTIVE';

-- Capacity is derived from active leases. The claim transaction first locks
-- the queue row and then the current worker-session row. Queue capacity counts
-- by queue; worker capacity counts by logical worker so leases from a replaced
-- process boot cannot be ignored.
CREATE INDEX leases_active_queue_idx
    ON leases (queue)
    WHERE status = 'ACTIVE';

CREATE INDEX leases_active_worker_idx
    ON leases (worker_id)
    WHERE status = 'ACTIVE';

-- Future M3 reconciliation scans active leases by server-owned expiry.
CREATE INDEX leases_active_expiry_idx
    ON leases (expires_at, id)
    WHERE status = 'ACTIVE';
