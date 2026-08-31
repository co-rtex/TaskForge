# TaskForge — Architecture

Canonical owner of: component boundaries, data ownership, state machines, transaction
and locking strategy, idempotency, outbox and broker semantics, failure model,
reliability invariants, observability design, and technology choices.

Rationale for individual decisions lives in [adr/](adr/README.md).
Implementation status lives in [CURRENT_STATE.md](CURRENT_STATE.md).

**Status legend.** Every section is marked:

- **[IMPLEMENTED]** — built, and covered by tests that actually run.
- **[PARTIAL]** — some of it is built; the section says exactly which part.
- **[PLANNED]** — designed but not built. Nothing here works yet.

Milestones M1 (durable ingress/outbox) and M2 (workers, sessions, and atomic claim)
are built. Never promote a marker without a passing test.

**Implemented today:** durable submission and outbox delivery; the advisory broker
contract; logical-worker and process-session registration; immutable session
eligibility; globally idempotent notification consumption; strict-priority atomic
claim with queue/worker capacity; attempts and fixed leases; fenced start/success;
and bounded `demo.echo` execution. Heartbeat, renewal, reconciliation, failed
outcomes, retry, cancellation, delayed scheduling, and result storage remain planned.

---

## 1. Model: PostgreSQL-authoritative, pull-based claim — [PARTIAL]

Steps 1, 2, and 5-9 are implemented. Step 10 is implemented for fixed-lease start,
`demo.echo`, and successful outcome only; renewal and failure outcomes are planned.
Step 11 is implemented for start/success fencing. Steps 3, 4, and 12 are planned.

TaskForge is a **pull-based** system with a **PostgreSQL-authoritative** control
plane. The broker signals only that work may exist; PostgreSQL decides atomically
which job a requesting worker may take.

This resolves a contradiction that sinks naive designs: an SQS-style shared queue
cannot reliably route a specific message to one scheduler-selected worker. Priority,
capability matching, and capacity are therefore enforced by a SQL claim query, never
by queue ordering.

See [ADR-0003](adr/0003-pull-based-claim-with-broker-notification.md).

### Execution flow (V1 target)

```
 1. API validates a submission.
 2. ONE PostgreSQL transaction creates: job + idempotency record + outbox event.
 3. Scheduled and retry-waiting jobs stay durable in PostgreSQL.
 4. Scheduler promotes due jobs, creating outbox events transactionally.
 5. Outbox publisher sends a small work-availability notification to the broker.
 6. A worker polls only while it holds a free local slot.
 7. Worker presents worker id, session id, queue, and durable notification event id;
    the control plane loads immutable capabilities and handler types from the session.
 8. ONE transaction: enforce capacity, select highest-priority eligible job,
    create attempt + lease, move job QUEUED -> LEASED.
 9. Worker acknowledges the broker message only after the claim succeeds, the queue
    is empty, or that durable notification event was already consumed. Other
    no-match outcomes remain unacknowledged.
10. Worker starts the attempt (LEASED -> RUNNING), executes a trusted registered
    handler, renews its lease, and reports a fenced outcome.
11. The control plane accepts a completion only if job, attempt, lease, worker
    session, and current state are all still valid.
12. The reconciler idempotently repairs expired leases and stale state.
```

### What correctness must never depend on

Queue ordering · exactly-once broker delivery · process memory · worker-supplied
wall-clock time · a worker eventually returning · a single scheduler instance · a
single outbox publisher · a single reconciler instance.

---

## 2. Components — [PARTIAL]

| Component | Responsibility | Status |
| --- | --- | --- |
| `taskforge-api` | Validate and durably accept submissions; serve read APIs; serve internal worker control operations. | **Built** for submission/read plus registration, claim, fenced start, and fenced success. |
| `taskforge-outbox` | Publish pending outbox events to the broker with retry and backoff. | **Built** |
| `taskforge-migrate` | Apply schema migrations. | **Built** |
| `taskforge-scheduler` | Promote due `PENDING` and `RETRY_WAIT` jobs; re-notify stranded queued work. | Planned (M4) |
| `taskforge-worker` | Register a session, poll only from free bounded slots, claim, execute trusted handlers, and report fenced outcomes. | **Built** for fixed leases and `demo.echo`; renewal/failure reporting planned (M3/M4). |
| `taskforge-reconciler` | Expire leases, abandon stale attempts, release capacity, finalize cancellation, repair drift. | Planned (M3) |
| `taskforge-cli` | Operator and developer command-line interface. | Planned (M5) |

Every component is safe to run with N replicas.

---

## 3. Data ownership — [IMPLEMENTED] for the tables that exist

**PostgreSQL is authoritative** for job, attempt, lease, worker-session, idempotency,
outbox, and logical DLQ state. See [ADR-0001](adr/0001-postgresql-as-authoritative-state.md).

The broker holds **notifications only** — never authoritative job state. A broker
message carries event id, schema version, event type, queue, a non-authoritative
job-id hint, and trace metadata. It never carries the authoritative job payload; a
worker always reads authoritative work through the control plane.

Two distinct dead-letter concepts exist and must not be conflated:

1. **Broker infrastructure DLQ** — malformed or repeatedly unprocessable *notification
   messages*. An operational concern.
2. **TaskForge logical job DLQ** — jobs that failed permanently or exhausted their
   attempt budget. Lives in PostgreSQL and is **authoritative**.

---

## 4. Job state machine — [PARTIAL]

The full state set is enforced by a `CHECK` constraint. M2 implements the successful
path `QUEUED → LEASED → RUNNING → SUCCEEDED` with dedicated, fenced operations.
Every other transition in the V1 diagram remains planned.

```
PENDING ──► QUEUED ──► LEASED ──► RUNNING ──► SUCCEEDED
                ▲                     │
                │                     ├──► RETRY_WAIT ──► QUEUED
                └─────────────────────┤
                                      └──► DEAD_LETTERED

Cancellation:
  PENDING | QUEUED | RETRY_WAIT ──────────────► CANCELED
  LEASED  | RUNNING ──► CANCEL_REQUESTED ─────► CANCELED
```

| Status | Meaning |
| --- | --- |
| `PENDING` | Durable, not yet eligible (scheduled for the future). |
| `QUEUED` | Eligible to be claimed. |
| `LEASED` | An attempt and an active lease exist; execution has not been confirmed started. |
| `RUNNING` | The valid attempt is executing. |
| `RETRY_WAIT` | Waiting for a durable retry time. |
| `CANCEL_REQUESTED` | Cancellation won the race and is being delivered to the worker. |
| `SUCCEEDED` | Terminal. |
| `CANCELED` | Terminal. |
| `DEAD_LETTERED` | Terminal after permanent or exhausted failure. |

There is deliberately **no job-level `FAILED` status**: a single failed attempt is
not a job outcome. Permanent and exhausted failure are represented by
`DEAD_LETTERED`, and individual failures remain visible in attempt history.

Attempt statuses are separate: `LEASED`, `RUNNING`, `SUCCEEDED`, `FAILED`,
`TIMED_OUT`, `CANCELED`, `ABANDONED`.

### Race semantics

Completion and cancellation form a transactionally ordered race. Exactly one
state-changing operation wins:

- Valid success commits first → a later cancel returns conflict.
- Cancel commits first → a later success is rejected.

The same rule applies to two concurrent completions, completion vs. lease
expiration, completion vs. retry transition, duplicate cancellation, and duplicate
lease renewal. These are enforced with row locks, transaction predicates,
constraints, and affected-row checks — never with check-then-update application
logic.

A job in `LEASED` or `RUNNING` is never marked terminally `CANCELED` while its lease
could still submit a valid completion. If the worker disappears, reconciliation
finalizes cancellation only after the lease can no longer commit.

---

## 5. Domain model — [PARTIAL]

Job submission plus the successful lifecycle are implemented. Attempt, logical
worker, process session, and fixed lease records are implemented for M2; failure
history, heartbeat updates, renewal, and result references remain planned.

### Job
Id · auth scope · queue · job type · canonical immutable payload · status · priority
(0–100) · max attempts (total, including the first) · timeout · required capabilities
· scheduled time · next eligible time · cancellation-request time · timestamps ·
optional `replayed_from_job_id`.

### Attempt
Attempt id · job id · monotonically increasing attempt number (unique per job) ·
worker id · **worker-session id** · typed status · start and finish times. The lease
references the attempt through a constrained one-to-one binding. Failure
classification, bounded error detail, result reference, and trace id are planned.

Attempt history is preserved. The last error is never overwritten in place.

### Worker and process session
Logical worker identity is separate from one process boot:

- `worker_id` — the stable logical worker.
- `worker_session_id` — one process lifetime.

**Leases bind to the session**, so a restarted worker cannot renew a lease belonging
to its previous process, and the old process cannot renew after the new one starts.

Stored: hostname · worker group · concurrency limit · capabilities · registration
time · **server-received** heartbeat time · status
(`STARTING` / `HEALTHY` / `DRAINING` / `UNHEALTHY` / `OFFLINE`).

M2 stores an immutable capability set and trusted handler-type set on each process
session. Claims carry only identity, queue, and notification event id; PostgreSQL
loads eligibility from the locked session row. Registration initializes heartbeat
time, but periodic heartbeat updates begin in M3. See
[ADR-0006](adr/0006-session-bound-worker-eligibility.md).

Capacity is derived from active leases or a transactionally maintained reservation
ledger, and is reconcilable. A mutable `active_jobs` counter is never trusted as
unquestioned authority.

### Lease
Opaque lease id · job id · attempt id · worker id · worker-session id · acquired at ·
expires at · renewed at · typed status.

A partial unique index enforces **at most one active lease per job**. M2 issues one
fixed, server-owned lease window and rejects start or success at/after expiry using
database time sampled after all authority locks. Renewal is planned for M3; it will
be idempotent and fenced by session + attempt + lease and will never resurrect an
expired lease.

---

## 6. Idempotent submission — [IMPLEMENTED]

Scoped by **auth scope + `Idempotency-Key` header**.

1. Canonicalize all job-defining fields into a deterministic byte sequence.
2. Hash it into a stable request fingerprint.
3. In **one transaction**, create the idempotency record and the job.
4. Uniqueness is enforced by a PostgreSQL constraint, not application logic.
5. Duplicate with the same fingerprint → return the original job.
6. Same key with a different fingerprint → deterministic conflict.
7. Never depends on process memory; correct across replicas and restarts.

Idempotency scope never leaks between projects or API keys.

---

## 7. Transactional outbox — [IMPLEMENTED]

See [ADR-0004](adr/0004-transactional-outbox.md).

Any committed state transition that requires a broker notification writes its outbox
event **in the same transaction**. There is no window in which a job is durable but
its notification was never recorded.

Publisher loop:

1. Claim a batch of due pending events with `FOR UPDATE SKIP LOCKED`, increment the
   attempt counter, and push `available_at` forward by a fixed **claim timeout**.
   Commit. That advanced `available_at` is a visibility timeout: a publisher that
   dies mid-flight releases its events automatically once it lapses. It is
   deliberately not the retry backoff — a claim is not a failure.
2. Publish to the broker **outside** the transaction, so no lock is held across
   network I/O.
3. Mark the event published in a second transaction.

**The publish-before-mark window is a real at-least-once window.** A crash between
step 2 and step 3 causes the event to be published again after the claim timeout's
visibility window lapses. This is expected and harmless: notifications are advisory,
and the claim query is the thing that enforces single execution. Duplicate delivery can never produce two active
leases: the outbox event id is the globally unique claim id, and another session gets
the explicit safe `DUPLICATE_NOTIFICATION` outcome. See
[ADR-0007](adr/0007-globally-idempotent-notification-claims.md).

A publish *failure* is what applies retry backoff: the error is recorded and
`available_at` is set from a bounded exponential policy with jitter. The random
source is injected so tests are deterministic. A failed event is never marked
terminally failed — the job it refers to is already durable and would otherwise sit
queued forever — so stuck events stay visible and repairable. Event schemas are
versioned.

### Broker interface

Provider-neutral, exposing only capabilities actually needed: **publish**,
**long-poll receive**, and **acknowledge/delete**. TaskForge deliberately does not
define a universal `Nack`, because SQS provides no such primitive. Redelivery is
expressed through visibility timeout.

---

## 8. Scheduling and claim — [PARTIAL]

**Implemented:** immediate-job claim. **Planned (M4):** a scheduler that promotes due
`PENDING` and `RETRY_WAIT` jobs using PostgreSQL server time, creates notification
outbox events transactionally, and re-notifies stranded queued work under a bounded,
rate-limited policy. Until that scheduler exists, a lost sole notification can strand
a queued job.

The claim operation, in one transaction:

- accepts only a current, healthy worker session with free capacity;
- loads immutable handler types, capabilities, concurrency, and worker group from the
  locked process session, then matches queue, job type, and required capabilities;
- enforces queue and worker concurrency limits;
- orders eligible jobs deterministically:

```sql
ORDER BY priority DESC, available_at ASC, created_at ASC, id ASC
```

- globally consumes the durable notification event id, so duplicate delivery to a
  different session cannot reserve a second job;
- creates the attempt and active lease atomically; active leases are the capacity ledger;
- moves exactly one job from `QUEUED` to `LEASED`;
- returns the payload and fencing identifiers only after commit.

V1 uses **strict priority with deterministic tie-breaking**, not fairness. Starvation
of low-priority work under sustained high-priority load is a known and documented
property, and is tested. Weighted fairness and aging are post-V1.

---

## 9. Concurrency control — [IMPLEMENTED] for M1/M2 operations

Submission idempotency, outbox publishing, notification consumption, queue capacity,
logical-worker capacity, claim, start, and success are serialized in PostgreSQL.

- Queue-level global concurrency locks the queue row before counting active leases.
  Counting active rows without preventing concurrent claims is insufficient and is
  not used.
- Worker capacity locks the current worker-session row before counting active leases
  for the stable logical worker.
- `FOR UPDATE SKIP LOCKED` is used where a claim must not block other claimants; every
  use carries a comment naming the race it prevents.
- Lock order is documented wherever more than one row type is locked in a transaction.
- Transaction isolation assumptions are stated alongside each critical query.

M2 claims first take a transaction-scoped advisory lock derived from the global claim
request id, then use queue → current worker session → job → attempt → lease row order.
The identity lock makes same-id requests serialize even when they name different
queues; a hash collision only over-serializes. Fenced transitions use the same row
order without the identity lock. Queue capacity counts active leases by queue. Worker
capacity counts by stable logical worker, so restarting a process cannot evade a lease
held by its old session.

---

## 10. Retry, timeout, DLQ, replay — [PLANNED]

Bounded exponential backoff with injected jitter exists, but only for outbox *publication*. Job retry is planned.

Failure classes: **retryable**, **permanent**, **timeout**, **canceled**.

A retryable failure with remaining budget moves the job to `RETRY_WAIT` with a
durable next-eligibility time. Backoff is configurable exponential with initial
delay, multiplier, maximum, and bounded jitter. The random source is injected. The
scheduler never sleeps inside domain logic.

Permanent failure or an exhausted attempt budget moves the job to `DEAD_LETTERED`.

DLQ replay preserves terminal history by creating a **new** job linked through
`replayed_from_job_id`. The relationship between operator "retry" and "replay" is
defined explicitly rather than implemented ambiguously; see
[ROADMAP.md](ROADMAP.md) for the milestone that settles it.

---

## 11. Heartbeats, leases, and crash recovery — [PARTIAL]

M2 implements server-timed fixed lease issuance, current-session fencing, and
expiry checks for start and success. A worker turns PostgreSQL's sampled remaining
lease window into a conservative monotonic execution deadline with completion
margin; PostgreSQL remains authoritative for every state transition. Periodic
heartbeat, lease renewal, expiry transitions, and crash recovery are M3.

Server receipt time will be authoritative for heartbeat staleness. Planned
development defaults (configurable, never hardcoded into domain logic): heartbeat
every 5 s, unhealthy after 15 s.

M3 reconciliation will, when a session goes stale and its lease expires,
transactionally:

1. marks the session unhealthy or offline;
2. expires the active lease;
3. marks the attempt `ABANDONED`;
4. releases queue and worker capacity;
5. transitions the job according to retry budget and cancellation state;
6. writes the required audit and outbox events.

It is safe to run repeatedly and concurrently. A returning old worker is rejected by
lease and session fencing; its stale completion may be recorded in a bounded audit
event or log, but it never mutates the authoritative outcome.

---

## 12. Reliability invariants — [PARTIAL]

Each of these must have an automated test. Invariants 1, 3, 4, 6-12, 17, and 18 are
implemented and tested for the M1/M2 operations that exist. The full terminal-state,
multi-attempt, cancellation, retry, and reconciliation invariants remain targets for
their owning milestones.

1. PostgreSQL is authoritative for all control-plane state.
2. A terminal job never returns to a non-terminal state.
3. A job has at most one active lease.
4. An attempt belongs to exactly one job and one worker process session.
5. Attempt numbers increase monotonically and are unique per job.
6. A worker never exceeds its declared concurrency limit.
7. A queue never exceeds its configured global execution limit.
8. A stale or expired lease cannot commit an outcome.
9. A completion accepted once cannot be overwritten.
10. Duplicate submissions with the same scope, key, and canonical request produce one job.
11. Reusing a key with a different fingerprint produces a conflict.
12. Duplicate broker delivery cannot create two active leases.
13. Retry scheduling survives service restarts.
14. A canceled job cannot later become successful.
15. Capacity reservations cannot become negative.
16. Reconciliation operations are idempotent.
17. No correctness property depends solely on in-memory state.
18. Worker-supplied time is not authoritative for leases or heartbeat staleness.

---

## 13. Failure model — [PARTIAL]

| Failure | Response |
| --- | --- |
| Broker unavailable after DB commit | Job stays durable; outbox event stays pending and retries. No loss. |
| Broker duplicates a notification | Globally idempotent event-id consumption admits one claim; another session gets a safe duplicate outcome. |
| Broker loses a notification | Scheduler re-notification is planned for M4. In M2, loss of the only notification can strand a queued job. |
| Publisher crashes mid-publish | Event republished after the claim timeout/visibility window lapses. Documented at-least-once window. |
| Worker crashes mid-execution | M3 will mark the attempt `ABANDONED` and repair capacity. In M2 the active lease remains recorded and capacity stays reserved. |
| Worker returns after expiry | Start and completion are rejected by session + lease fencing; renewal arrives in M3. |
| API crashes mid-request | Transaction rolls back; no partial job, record, or event. |
| Two workers claim concurrently | One wins; the other gets a different job or none. |
| Database unavailable | Requests fail fast with a sanitized error; readiness fails; no fabricated success. |

---

## 14. Observability — [PARTIAL]

Structured JSON logging with correlation identifiers, and distinct liveness/readiness endpoints, are implemented. Tracing and metrics are planned (M6).

OpenTelemetry traces span: API submission → PostgreSQL transaction → outbox → broker
notification → claim → worker execution → result.

Structured JSON logs carry request id, job id, attempt id, worker id, worker-session
id, lease id, trace id, and queue. Never secrets; never unbounded payloads.

Metrics include submitted / completed / retried / dead-lettered counters; queued and
running gauges; queue-wait, execution, and end-to-end duration histograms; worker
health and utilization; active and expired leases; scheduler promotions and claims;
pending outbox events and publish failures; stale-completion rejections; and
reconciliation repairs.

**Unbounded values — job ids, attempt ids, request ids — are never used as metric
labels.**

Each service exposes **distinct** liveness and readiness endpoints. Liveness answers
only whether the process is alive. Readiness reflects whether the process can do its
job and checks its required dependencies with bounded timeouts. Liveness is an
intentional unconditional `200`; readiness is not.

---

## 15. Technology choices

| Concern | Choice | Rationale |
| --- | --- | --- |
| Services | Go | Concurrency primitives, static binaries, good operational story. |
| Database | PostgreSQL 16 | `SKIP LOCKED`, partial unique indexes, rich constraints, real transactions. |
| DB access | `pgx/v5`, explicit SQL | Reviewers must be able to read the exact query that enforces correctness. No ORM. |
| Enums | `TEXT` + `CHECK` constraint | Evolvable in ordinary transactional migrations, unlike PostgreSQL enum types. Go side stays typed. |
| Migrations | Numbered `.sql` + embedded runner | No extra binary to install; the SQL stays readable and reviewable. |
| Broker | ElasticMQ locally, AWS SQS Standard as direction | See [ADR-0005](adr/0005-elasticmq-for-local-broker.md). |
| Small results | PostgreSQL | Bounded size, transactional with state. |
| Large results | MinIO locally, S3 in cloud | Keeps unbounded blobs out of PostgreSQL. |
| Cache | None initially | Redis is not authoritative state and has no measured need yet. |
| HTTP | Go standard library | Routing needs are modest; a framework would add opacity. |
| Cloud direction | AWS ECS + RDS + SQS + S3 + ALB, via Terraform | Kubernetes is post-V1 and not required for V1. |

Redis is never authoritative job state. Paid infrastructure is never provisioned
without explicit authorization; V1 runs entirely locally.

---

## 16. Schema

**Implemented** (`migrations/0001` through `0005`): `queues`, `jobs`,
`idempotency_records`, `outbox_events`, `workers`, `worker_sessions`,
`job_attempts`, and `leases`, plus `schema_migrations` maintained by the runner.
M2 adds immediate eligibility time, worker-group routing, constrained session/
attempt/lease bindings, one current session per logical worker, one active lease per
job, globally unique notification claims, active-capacity indexes, and timeline-order
constraints.

**Planned:** `results`, `dlq_entries`, `api_keys`, `audit_events`.

Tables are created in the milestone that puts working behavior on them, not in
advance. Indexes will support eligibility and priority scans, queue and status
filters, idempotency lookup, pending outbox scans, and active queue/worker capacity.
Indexes for expiring leases, stale-heartbeat scans, attempt history, and dashboard
queries arrive only with the implemented query that justifies each one.
