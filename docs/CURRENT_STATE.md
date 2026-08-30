# Current State

This document is the source of truth for what is runnable now and what remains
planned. It describes the `feat/workers-atomic-claim` branch after Milestone M2,
based on M1 commit `1cb66cbc922e841d80a9e6318e81e44778555f5d`.

## Milestone status

- **M1 — durable ingress and transactional outbox:** complete.
- **M2 — worker sessions, atomic claims, and fenced execution:** complete.
- **M3 — heartbeat, lease renewal, and reconciliation:** not started.

## Runnable system

Four binaries build and run:

- `taskforge-api` accepts idempotent job submissions, job reads, worker-session
  registration, atomic claims, and fenced start/succeed transitions.
- `taskforge-outbox` publishes durable work-availability events to ElasticMQ.
- `taskforge-worker` polls ElasticMQ only while it has capacity and executes the
  trusted `demo.echo` handler through the control plane.
- `taskforge-migrate` applies numbered PostgreSQL migrations.

The schema consists of migrations `0001` through `0005`. M2 adds stable worker
identities, boot-scoped immutable sessions, attempts, fixed-duration leases,
capacity and claim indexes, control-timeline constraints, and globally unique
notification consumption.

## Implemented behavior

- A worker registers a new boot session with immutable worker group, concurrency,
  capabilities, and trusted handler types. Registering a replacement
  session fences the old one.
- A claim transaction locks queue, session, job, attempt, and lease state in a fixed
  order. It enforces queue and worker capacity, strict job priority, availability,
  worker group, capabilities, and trusted handler type.
- PostgreSQL supplies claim, lease, start, and completion time. Lease expiry is a
  fixed server-side duration in M2.
- The durable outbox event id is also the claim request id. Exact replay by the
  owning session returns the existing assignment; another session receives a safe
  duplicate-notification outcome. A database uniqueness constraint makes this
  global across sessions and processes.
- Workers acknowledge broker messages only after a durable assignment, a proven
  empty queue, or a globally consumed duplicate notification. Other no-match and
  error outcomes remain unacknowledged.
- Start and success are fenced by job, attempt, lease, worker session, and state.
  Successful completion releases durable capacity.
- The worker converts the database-reported remaining lease duration into a
  conservative monotonic local deadline and reserves completion time before expiry.
- Local slots bound polling and execution. Duplicate deliveries are collapsed in
  process, handler panics are contained, shutdown drains cooperative work, and
  session loss cancels the runner and removes readiness.
- API requests have bounded contexts. Service binds are loopback-only until control
  endpoints gain authentication.

## Deliberately not implemented yet

- Heartbeats, lease renewal, expired-lease reconciliation, attempt abandonment, and
  durable capacity repair are M3 work.
- Retry policy and failed, cancelled, and dead-letter outcomes are M4 work.
- If a worker crashes or a handler fails after claiming, the M2 lease remains active
  and consumes capacity. No new execution is authorized automatically.
- Only `demo.echo` is registered as a production worker handler. Work must fit
  within the fixed lease; longer work requires M3 renewal.
- An idle worker discovers replacement of its session on its next control-plane
  operation. Proactive discovery requires M3 heartbeat.
- There is no scheduler or re-notification loop. Losing the sole broker notification
  can strand a durable queued job until M4 adds promotion and re-notification.
- Cancellation is deferred to M4. Result bodies and richer status APIs are deferred
  to M5.
- Authentication, authorization, metrics, tracing, broker-retention policy, and
  production performance characterization remain future work.

These omissions are liveness and product gaps. The implemented M2 paths still keep
PostgreSQL authoritative and reject stale or duplicate mutations.

## Verification

The completed M2 tree passed these gates on 2026-08-29:

- `make lint`
- `make build`
- `make test-unit`
- `make test-integration`
- `make test-race` for both unit and integration packages
- `docker compose config --quiet`
- `make migrate` against the local database (`schema already up to date`)
- OpenAPI YAML parsing and `git diff --check`

The integration suite includes fresh-database migration, migration idempotency and
concurrency, real M1-data backfill through all M2 migrations, contested claims,
cross-queue claim-id reuse, capacity, database-clock expiry, fencing, request-timeout
rollback, and API → outbox → ElasticMQ → worker execution.

The four built binaries were also exercised together on isolated loopback ports. API,
outbox, and worker readiness all returned ready; submitted `demo.echo` job
`2868669b-e980-4d38-b0c7-dc94574e5d1c` durably reached `SUCCEEDED`; all three services
then stopped cleanly.

## Local environment

- Go 1.25 or newer
- PostgreSQL 16 on `localhost:5442`
- ElasticMQ on `localhost:9324`
- Docker Compose and Make

Run `make bootstrap`, `make up`, `make migrate`, and `make build`, then start the
API, outbox publisher, and worker as shown in the repository README.

## Next objective

M3 will add heartbeat, renewal, lease expiration, stale-state reconciliation, and
durable capacity recovery without weakening M2 fencing or claim idempotency.
