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
- A duplicate delivery releases its slot as soon as the durable claim decision is
  published; it does not wait for the leader's handler. The event's flight entry is
  still held for the whole leader path, so a duplicate arriving mid-execution joins
  as a follower and can never lead a second claim, replay Start on a RUNNING
  attempt, or invoke the handler again. Only `CLAIMED`, `QUEUE_EMPTY`, and
  `DUPLICATE_NOTIFICATION` release a receipt.
- API requests have bounded contexts. A worker-control request that exhausts its
  server-owned deadline returns HTTP 503 `service_unavailable` with the standard
  error envelope; genuine faults remain 500 `internal_error`. Service binds are
  loopback-only until control endpoints gain authentication.

  That deadline can elapse while acquiring a lock, while executing a statement, or
  during COMMIT, and a COMMIT cut short leaves the immediate outcome unknown. The
  503 therefore never promises that nothing committed. It tells the caller to retry
  the identical request under that operation's existing identity or fence:

  - registration replays its own `worker_session_id` and immutable registration body;
  - a claim replays its `worker_session_id` and `claim_request_id`;
  - start and succeed replay their attempt fence.

  Each of those is idempotent, so a replay returns the already-committed result
  rather than producing a second one. Per-endpoint retry guidance is documented in
  [api/openapi.yaml](../api/openapi.yaml); the shared Go message stays
  endpoint-neutral because one string serves all four operations.

## Effective handler budget in M2

The budget a handler actually gets is the **lesser** of:

- the job's `timeout_seconds`; and
- the remaining fixed lease window PostgreSQL reports at claim time, minus a small
  completion margin reserved for reporting the outcome.

A `timeout_seconds` larger than `TASKFORGE_LEASE_DURATION` is accepted for forward
compatibility, but until M3 adds lease renewal it cannot authorize execution beyond
one fixed lease. The 30-second lease default and the 300-second job-timeout default
are unchanged, and oversized timeouts are deliberately not rejected.

How that budget is enforced, precisely:

- The worker invokes the handler with a deadline-bearing `context.Context`.
- When the budget elapses, the worker cancels that context.
- A cooperative handler is expected to observe the cancellation and return.
- Go cannot forcibly terminate arbitrary handler code. An uncooperative handler
  may keep running until it returns on its own or the process exits, and the
  worker cannot preempt it. Hard cancellation needs isolated process or container
  execution, which is post-V1.
- The control-plane guarantee is the durable one: once the lease authority
  deadline has passed, the worker cannot successfully report completion, because
  the fenced transition is rejected against PostgreSQL server time. Late work
  therefore cannot commit an outcome, even though it may still be running.

Work that must run longer than one lease is M3 work.

## Operating concurrent workers

A logical worker is `(scope, name)`, and only one process session may be current for
it. `TASKFORGE_WORKER_NAME=local-worker` is a single-process development default:
every concurrently running worker must be given its own stable, distinct name.
Booting a second worker under the same name fences the first, and the fenced process
exits rather than continuing without authority. Names must also stay stable across
restarts of the same logical worker.

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

The M2 tree, including the pre-merge corrective pass, passed these gates on
2026-08-29:

- `make fmt`
- `make lint`
- `make build`
- `make test-unit`
- `make test-integration`
- `make test-race` for both unit and integration packages
- `docker compose config --quiet`
- `make migrate` against the local database (`schema already up to date`)
- OpenAPI YAML parsing, cross-checked against the Go error-code contract
- `git diff --check`

The integration suite includes fresh-database migration, migration idempotency and
concurrency, real seeded M1 data carried through migrations `0001` to `0005` in
order, composite foreign-key rejection of mismatched attempt/lease bindings,
contested claims, cross-queue claim-id reuse, queue and logical-worker capacity,
database-clock expiry, fencing, registration racing both claim and success,
request-deadline rollback, and API → outbox → ElasticMQ → worker execution.

The four built binaries were also exercised together on isolated loopback ports. API,
outbox, and worker readiness all returned ready; submitted `demo.echo` job
`91d06416-a506-4c24-9747-187f5a4f7d12` durably reached `SUCCEEDED` with one attempt
and a `COMPLETED` lease; all three services then stopped cleanly with no error-level
log lines.

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
