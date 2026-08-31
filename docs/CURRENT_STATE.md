# Current State

This document is the source of truth for what is runnable now and what remains
planned. Milestones M1 and M2 are merged into `main`; this document records the
implemented state through M3, which is on the
`feat/m3-heartbeats-reconciliation` branch and its draft pull request.

## Milestone status

- **M1 — durable ingress and transactional outbox:** complete.
- **M2 — worker sessions, atomic claims, and fenced execution:** complete.
- **M3 — heartbeat, lease renewal, and reconciliation:** complete.
- **M4 — retry, timeout, cancellation, DLQ, replay, delayed jobs:** not started.

## Runnable system

Five binaries build and run:

- `taskforge-api` accepts idempotent job submissions, job reads, worker-session
  registration, session heartbeat, atomic claims, fenced lease renewal, and
  fenced start/succeed transitions.
- `taskforge-outbox` publishes durable work-availability events to ElasticMQ.
- `taskforge-worker` polls ElasticMQ only while it has capacity, heartbeats its
  process session, executes the trusted `demo.echo` handler through the control
  plane, and renews each running attempt's lease.
- `taskforge-reconciler` marks stale sessions unhealthy, expires lapsed leases,
  abandons their attempts, releases the capacity they held, and requeues or
  dead-letters the job.
- `taskforge-migrate` applies numbered PostgreSQL migrations.

The schema consists of migrations `0001` through `0007`. M2 added stable worker
identities, boot-scoped immutable sessions, attempts, leases, capacity and claim
indexes, control-timeline constraints, and globally unique notification
consumption. M3 adds lease renewal generation and identity, and the two partial
indexes its reconciliation scans use.

## Implemented behavior

Everything recorded for M2 still holds. What M3 adds:

### Heartbeat

- A worker starts a heartbeat loop immediately after registration and continues
  it through a graceful drain, so long-running work is never abandoned because
  the process stopped taking new work.
- The request carries no timestamp. The control plane locks the exact
  `(worker_id, worker_session_id)` row, samples `clock_timestamp()` afterwards,
  and advances `last_heartbeat_at` monotonically. The accepted server time is
  returned so a worker can confirm liveness rather than assume it.
- Only a current `HEALTHY` session is accepted. A replaced, unhealthy, or ended
  session is rejected and is never returned to health; that process must
  establish a new boot session.
- Repeating a heartbeat is harmless. It may advance the receipt time again, which
  is what a heartbeat is for, but it can never create a session or revive a
  fenced one — so an ambiguous `503` is safe to retry with the identical request.
- A worker treats a definitive fence as fatal, retries transient and 5xx failures
  within its bounded policy, and stops presenting itself as ready and stops
  taking new work once it has gone a full staleness threshold without a
  confirmation. Because the loop runs while idle, a replaced worker discovers it
  without waiting for a broker delivery.

### Lease renewal

- A running attempt renews its lease under the complete existing fence — job,
  attempt, lease, worker, session — plus a client-generated renewal request id
  and the generation the caller expects to be current.
- A lease begins at generation 0, the window the claim itself issued. A renewal
  samples PostgreSQL time after every authority lock, sets `renewed_at` and
  `expires_at` from that sample, increments the generation, and records the
  identity that produced it.
- An exact replay of the committed renewal returns the stored window unchanged,
  so an ambiguous response cannot extend authority twice. A delayed older
  generation, a competing request for the same generation, and a generation from
  the future all mutate nothing and return a stable conflict. Reusing one renewal
  identity against a different lease returns that same domain conflict, never a
  leaked uniqueness error.
- Renewal never resurrects an expired, completed, released, or reconciled lease,
  and a renewal that waits across the expiry boundary is rejected against the
  fresh post-lock sample.
- The worker converts each server-measured remaining window into a conservative
  monotonic local deadline. It never compares its own wall clock with
  `expires_at`.
- **Renewal extends lease authority only.** The job's `timeout_seconds` budget is
  measured once from execution start and is never reset by a renewal.
- If renewal loses the fence, session, lease, or generation, the worker cancels
  its cooperative handler and reports nothing. If transient failures continue
  until renewal can no longer be confirmed before the conservative deadline, it
  does the same. Either way the durable recovery path is lease expiry and
  reconciliation, not a worker-side decision.

### Reconciliation

`taskforge-reconciler` runs a bounded periodic loop over a database-backed
`RunOnce` seam and is safe with N replicas. Session staleness and lease expiry
are **two distinct scans**, and both are needed: a session can stop heartbeating
while its lease is still valid, and a lease can expire while its session is
perfectly healthy — the worker deliberately leaves a lease active after a handler
error and keeps heartbeating.

1. A current session that has missed the staleness threshold is marked
   `UNHEALTHY` with an `ended_at` stamp, from server time sampled after its row
   lock. `OFFLINE` stays reserved for replacement and explicit shutdown, so an
   operator can tell a crash from a restart. A heartbeat that commits while the
   scan waits saves its session, because the decision is re-made after the lock.
2. Each expired `ACTIVE` lease is reconciled in **one transaction**: authority
   rows are locked in the established order, PostgreSQL time is sampled
   afterwards, the lease and its job/attempt binding and states are revalidated,
   the lease becomes `EXPIRED` with a `released_at`, the attempt becomes
   `ABANDONED` with a `finished_at` (from `LEASED` or `RUNNING` alike), and the
   job is either returned to `QUEUED` with a **fresh** `work.available` outbox
   event or marked `DEAD_LETTERED`.

Capacity is released solely by the lease ceasing to be `ACTIVE`. There is no
counter to decrement, so it cannot drift or go negative. The recovery event gets
a new id because the original event id was already globally consumed as the claim
identity; reusing it would collide with the attempt just abandoned.

A crash before commit leaves the old state intact and a later pass repairs it.
Repeated and concurrent reconciliation never produces a second abandonment, a
second capacity release, or a duplicate recovery event.

### The attempt-budget decision

An `ABANDONED` attempt counts toward `max_attempts`, exactly like any other
attempt. If budget remains, reconciliation requeues immediately — this is crash
recovery, not retry: no backoff, no jitter, no `RETRY_WAIT`, no failure
classification. If the abandonment consumed the total budget, the job becomes
`DEAD_LETTERED`, which is the minimal terminal consequence that avoids a job the
claim predicate could never claim again.

That reachable `DEAD_LETTERED` is a real, visible product gap: it can be read
through `GET /v1/jobs/{job_id}`, but there is no DLQ listing until M6's dashboard
and no replay until M4. The reasoning is recorded in
[ADR-0009](adr/0009-abandoned-attempts-consume-the-attempt-budget.md).

## Locking and time

The critical lock order is unchanged: `queue → worker session → job → attempt →
lease`. Claim additionally takes its claim-identity advisory lock first.

- Heartbeat and stale-session marking lock only the worker-session row, a prefix
  of that order.
- Renewal uses the same fenced path as start and succeed, so it takes the full
  order.
- Lease reconciliation takes the full order and deliberately does **not** require
  a healthy session. Its candidate scan reads lease ids with no lock at all —
  taking a lease lock first and then reaching for queue and session locks would
  be the one way to invert the order — and every mutable field is re-read and
  revalidated under the locks.

Every decision that can wait across an expiry or staleness boundary uses
`clock_timestamp()` sampled after the relevant locks, never transaction-start
`now()`.

## Effective handler budget

Work is no longer limited to one lease. A handler may run across many lease
windows for as long as renewal keeps succeeding. What still bounds one execution:

- the job's `timeout_seconds`, measured once from execution start and never
  extended by renewal; and
- lease authority, which ends the moment renewal cannot be confirmed.

How that is enforced is unchanged from M2 and remains a cooperative contract:

- The worker invokes the handler with a deadline-bearing `context.Context` and
  cancels it when either bound is reached.
- A cooperative handler is expected to observe the cancellation and return.
- Go cannot forcibly terminate arbitrary handler code. An uncooperative handler
  may keep running until it returns on its own or the process exits, and the
  worker cannot preempt it. Hard cancellation needs isolated process or container
  execution, which is post-V1.
- The control-plane guarantee is the durable one: once lease authority is gone,
  the worker cannot report completion, because PostgreSQL rejects the fenced
  transition — and reconciliation abandons that attempt and hands the job to
  another worker.

## Configuration added in M3

All have documented defaults, are validated at startup, and are never hardcoded
into domain logic. See [.env.example](../.env.example).

| Variable | Default | Validated relationship |
| --- | --- | --- |
| `TASKFORGE_HEARTBEAT_INTERVAL` | `5s` | must be positive |
| `TASKFORGE_SESSION_STALE_AFTER` | `15s` | ≥ 3 × heartbeat interval |
| `TASKFORGE_LEASE_RENEW_INTERVAL` | `10s` | ≤ ⅓ of `TASKFORGE_LEASE_DURATION` |
| `TASKFORGE_RECONCILER_ADDR` | `127.0.0.1:8083` | must bind to loopback |
| `TASKFORGE_RECONCILER_POLL_INTERVAL` | `2s` | must be positive |
| `TASKFORGE_RECONCILER_BATCH_SIZE` | `50` | between 1 and 1000 |

The two ratio rules exist so that one lost request cannot look like a dead
process, and one failed renewal cannot lose a lease.

## Operating concurrent workers

Unchanged from M2: a logical worker is `(scope, name)`, only one process session
may be current for it, and every concurrently running worker needs its own
stable, distinct `TASKFORGE_WORKER_NAME`. What M3 changes is the consequence of
getting it wrong — the fenced process now discovers it on its next heartbeat
rather than on its next control-plane operation, and its leases are recovered
rather than reserved forever.

## Deliberately not implemented yet

- Failure classification, retry policy, exponential job backoff, `RETRY_WAIT`,
  and timeout outcomes (`TIMED_OUT`) are M4. A handler that returns an error
  still reports no outcome; its lease expires into the M3 abandonment path.
- Cancellation is M4. `CANCEL_REQUESTED` and `CANCELED` remain unreachable, and
  reconciliation does not finalize cancellation.
- The DLQ API, listing, replay, and operator retry are M4. `DEAD_LETTERED` is
  reachable but only readable one job at a time.
- There is no scheduler and no general re-notification loop. The only
  re-notification M3 adds is the event reconciliation writes for the specific
  expired attempt it just requeued; recovering a lost *submission* notification
  still needs M4 promotion and re-notification.
- Only `demo.echo` is registered as a production worker handler.
- Result bodies and richer status APIs are M5. Authentication, authorization,
  metrics, tracing, broker-retention policy, and production performance
  characterization remain future work.

## Verification

The M3 tree passed these gates locally on 2026-08-31, against PostgreSQL 16 and
ElasticMQ started by `make up`:

- `make fmt` (no tracked file rewritten)
- `git diff --check origin/main...HEAD`
- `make lint`
- `make build`
- `make test-unit`
- `docker compose config --quiet`
- `make migrate`
- `make test-integration`
- `make test-race` for both unit and integration packages
- `go test -v -count=1 -run '^TestOpenAPI_' ./internal/api/`

Exact commands and real output are recorded in the pull request.

New coverage beyond the M1/M2 suites, all of which still pass unchanged:

**Migrations.** Fresh-database application through `0007`; the two reconciler
scan indexes exist, are partial, and match the predicates the real scans use; the
renewal identity index is globally unique and partial; real seeded M1 data
upgrades through every migration in order; and the renewal identity/generation
constraint rejects each half without the other, each with a positive control so
every rejection is attributable to the constraint under test. No M4+ table is
created.

**Unit and contract.** Heartbeat and renewal validation report every field
problem at once; client parsing rejects a heartbeat or renewal response that
could not have answered the request that was sent; ambiguous renewal failures are
separated from definitive authority loss; a long handler survives several lease
windows while renewal succeeds; retries reuse one renewal identity and
generation; definitive loss cancels the handler and prevents success; an
unresolved transient failure stops at the safety deadline; renewal never resets
the overall job timeout; idle session loss is fatal and drops readiness with no
broker delivery involved; unconfirmed liveness stops the worker at the stale
threshold; heartbeats continue through a graceful drain; and OpenAPI parses with
every implemented route, every stable error code, and a per-endpoint
ambiguous-retry contract for all six worker-control operations.

**PostgreSQL integration.** Heartbeat time is sampled after an authority lock
wait and advances monotonically; a fenced session cannot heartbeat, claim, renew,
start, or succeed, and a late heartbeat never revives it; heartbeat racing
session replacement has only valid serial outcomes; renewal is fenced by all five
identifiers; an exact replay returns the committed window without a second
extension; two distinct renewals for one generation have exactly one winner; a
renewal waiting across expiry is rejected without mutation; identity reuse across
leases is a domain conflict; an expired lease is reconciled even when its session
is healthy; attempts are abandoned from both `LEASED` and `RUNNING`; expiry
closes the lease, stamps the attempt, and releases queue and logical-worker
capacity atomically; recovery produces exactly one fresh pending event and a
claimable job; a replacement claims attempt 2 while attempt 1's history is
preserved; an exhausted budget dead-letters; repeated reconciliation is a no-op;
four concurrent reconcilers produce one repair each and no duplicate events; and
a fault injected before commit rolls back every change, after which a rerun
repairs it.

**Contention.** Renewal versus success, renewal versus reconciliation, and
success versus reconciliation, each arranged deliberately with an advisory-lock
gate in both orderings. Plus a saturation test running claim, heartbeat, renewal,
success, registration replacement, and reconciliation against one another on
separate connections; a lock-order inversion would surface there as a PostgreSQL
deadlock, and none occurred.

**End-to-end fault injection.** Worker A reaches a durably `RUNNING` attempt, its
authority path is severed mid-handler, its session is detected stale, its lease is
recorded `EXPIRED`, attempt 1 becomes `ABANDONED`, active capacity returns to
zero, the transactionally created recovery event is published through the real
outbox publisher and real ElasticMQ, worker B claims attempt 2 and completes it,
and the job ends `SUCCEEDED` with attempt history `[ABANDONED, SUCCEEDED]` and
lease history `[EXPIRED, COMPLETED]`. Every late heartbeat, renewal, and success
from worker A is then rejected and mutates nothing. No recovery state is
hand-written; the real reconciler produces all of it.

**No performance or recovery-time claim is made.** Recovery latency in the tests
is a function of deliberately short test-only thresholds and proves correctness,
not speed. The benchmark table in
[PROJECT_SPEC.md](PROJECT_SPEC.md) §7 remains unmeasured.

## Continuous integration

The same gates run on GitHub-hosted Linux runners.
[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) is triggered by pull
requests targeting `main` and by pushes to `main`, and splits the work across
three parallel jobs so one failure never hides another:

- **Format, lint, build, unit and OpenAPI tests** — an event-ranged
  `git diff --check`, `make fmt` followed by an assertion that the formatter
  rewrote no tracked file, `make lint`, `make build`, `make test-unit`, the
  `TestOpenAPI_*` contract tests invoked by name, and
  `docker compose config --quiet`.
- **Migrations and integration tests** — `make up`, `make migrate`,
  `make test-integration`.
- **Race detector (unit and integration)** — the same infrastructure lifecycle
  plus `make test-race`.

Both infrastructure jobs own a complete Compose lifecycle. They start it with
`make up`, so `scripts/wait-for-infra.sh` gates the tests on PostgreSQL's own
`pg_isready` probe and a real ElasticMQ `ListQueues` call rather than a sleep,
and they tear it down under `if: always()`. Bounded container status and
non-colored service logs are captured and uploaded only when a job failed, with
three-day retention.

The workflow needs no repository secret and no cloud credential. It reuses the
committed loopback PostgreSQL and ElasticMQ configuration, creates no `.env`,
grants only `contents: read`, does not persist checkout credentials, and pins
every action to a full commit SHA. The Go toolchain is resolved from `go.mod`
through `actions/setup-go`.

The M3 pull request's run is recorded in that pull request. The failure path —
diagnostic capture and artifact upload — has still not been exercised by a real
hosted failure.

## Local environment

- Go 1.25 or newer
- PostgreSQL 16 on `localhost:5442`
- ElasticMQ on `localhost:9324`
- Docker Compose and Make

Run `make bootstrap`, `make up`, `make migrate`, and `make build`, then start the
API, outbox publisher, worker, and reconciler as shown in the repository README.

## Next objective

M4 will add failure classification, retry with bounded backoff and injected
jitter, `RETRY_WAIT`, timeout outcomes, cancellation and the cancel-versus-complete
race, the logical DLQ with listing and replay, and a scheduler that promotes
delayed jobs and re-notifies stranded queued work — without weakening any M1, M2,
or M3 fence.
