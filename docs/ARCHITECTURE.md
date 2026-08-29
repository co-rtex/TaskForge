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

As of this document's last update, everything below is **[PLANNED]** unless marked
otherwise. Never promote a marker without a passing test.

---

## 1. Model: PostgreSQL-authoritative, pull-based claim

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
 7. Worker presents worker id, session id, queue, and capabilities to the claim op.
 8. ONE transaction: enforce capacity, select highest-priority eligible job,
    create attempt + lease, move job QUEUED -> LEASED.
 9. Worker acknowledges the broker message only after the claim succeeds, or the
    control plane confirms no eligible job remains.
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

## 2. Components

| Component | Responsibility |
| --- | --- |
| `taskforge-api` | Validate and durably accept submissions; serve read APIs; serve internal worker control operations. |
| `taskforge-outbox` | Publish pending outbox events to the broker with retry and backoff. |
| `taskforge-scheduler` | Promote due `PENDING` and `RETRY_WAIT` jobs; re-notify stranded queued work. |
| `taskforge-worker` | Register a session, poll, claim, execute trusted handlers, renew leases, report fenced outcomes. |
| `taskforge-reconciler` | Expire leases, abandon stale attempts, release capacity, finalize cancellation, repair drift. |
| `taskforge-cli` | Operator and developer command-line interface. |

Every component is safe to run with N replicas.

---

## 3. Data ownership

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

## 4. Job state machine

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

## 5. Domain model

### Job
Id · auth scope · queue · job type · canonical immutable payload · status · priority
(0–100) · max attempts (total, including the first) · timeout · required capabilities
· scheduled time · next eligible time · cancellation-request time · timestamps ·
optional `replayed_from_job_id`.

### Attempt
Attempt id · job id · monotonically increasing attempt number (unique per job) ·
worker id · **worker-session id** · lease id · typed status · start and finish times
· failure classification · bounded error code and sanitized message · result
reference · trace id.

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

Capacity is derived from active leases or a transactionally maintained reservation
ledger, and is reconcilable. A mutable `active_jobs` counter is never trusted as
unquestioned authority.

### Lease
Opaque lease id · job id · attempt id · worker id · worker-session id · acquired at ·
expires at · renewed at · typed status.

A partial unique index enforces **at most one active lease per job**. Renewal is
idempotent and fenced by session + attempt + lease. Renewal after expiry fails; it
never resurrects a lease.

---

## 6. Idempotent submission

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

## 7. Transactional outbox

See [ADR-0004](adr/0004-transactional-outbox.md).

Any committed state transition that requires a broker notification writes its outbox
event **in the same transaction**. There is no window in which a job is durable but
its notification was never recorded.

Publisher loop:

1. Claim a batch of due pending events with `FOR UPDATE SKIP LOCKED`, increment the
   attempt counter, and push `available_at` forward by a backoff interval. Commit.
   The advanced `available_at` acts as a visibility timeout, so a publisher that dies
   mid-flight releases its events automatically.
2. Publish to the broker **outside** the transaction, so no lock is held across
   network I/O.
3. Mark the event published in a second transaction.

**The publish-before-mark window is a real at-least-once window.** A crash between
step 2 and step 3 causes the event to be published again after its backoff expires.
This is expected and harmless: notifications are advisory, and the claim query is the
thing that enforces single execution. Duplicate delivery can never produce two active
leases.

Backoff is bounded exponential with jitter; the random source is injected so tests
are deterministic. Stuck events remain visible and repairable. Event schemas are
versioned.

### Broker interface

Provider-neutral, exposing only capabilities actually needed: **publish**,
**long-poll receive**, and **acknowledge/delete**. TaskForge deliberately does not
define a universal `Nack`, because SQS provides no such primitive. Redelivery is
expressed through visibility timeout.

---

## 8. Scheduling and claim

The scheduler promotes due `PENDING` and `RETRY_WAIT` jobs using **PostgreSQL server
time**, creating notification outbox events transactionally, and re-notifies stranded
queued work under a bounded, rate-limited policy. A lost, duplicated, or delayed
broker notification therefore cannot strand queued work forever.

The claim operation, in one transaction:

- accepts only a current, healthy worker session with free capacity;
- matches queue, worker group, and required capabilities;
- enforces queue and worker concurrency limits;
- orders eligible jobs deterministically:

```sql
ORDER BY priority DESC, available_at ASC, created_at ASC, id ASC
```

- creates the attempt, the capacity reservation, and the active lease atomically;
- moves exactly one job from `QUEUED` to `LEASED`;
- returns the payload and fencing identifiers only after commit.

V1 uses **strict priority with deterministic tie-breaking**, not fairness. Starvation
of low-priority work under sustained high-priority load is a known and documented
property, and is tested. Weighted fairness and aging are post-V1.

---

## 9. Concurrency control

- Queue-level global concurrency is serialized through a queue-capacity row or an
  equivalent transactionally sound reservation. Counting active rows without
  preventing concurrent claims is insufficient and is not used.
- Worker capacity locks the worker session or capacity ledger before reserving a slot.
- `FOR UPDATE SKIP LOCKED` is used where a claim must not block other claimants; every
  use carries a comment naming the race it prevents.
- Lock order is documented wherever more than one row type is locked in a transaction.
- Transaction isolation assumptions are stated alongside each critical query.

---

## 10. Retry, timeout, DLQ, replay

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

## 11. Heartbeats, leases, and crash recovery

Server receipt time is authoritative. Development defaults (configurable, never
hardcoded into domain logic): heartbeat every 5 s, unhealthy after 15 s.

When a session goes stale and its lease expires, reconciliation transactionally:

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

## 12. Reliability invariants

Each of these must have an automated test.

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

## 13. Failure model

| Failure | Response |
| --- | --- |
| Broker unavailable after DB commit | Job stays durable; outbox event stays pending and retries. No loss. |
| Broker duplicates a notification | Harmless. The claim query admits only one winner. |
| Broker loses a notification | Scheduler re-notification recovers the stranded job. |
| Publisher crashes mid-publish | Event republished after backoff. Documented at-least-once window. |
| Worker crashes mid-execution | Heartbeat goes stale, lease expires, attempt `ABANDONED`, capacity released, job retried. |
| Worker returns after expiry | Renewal and completion rejected by session + lease fencing. |
| API crashes mid-request | Transaction rolls back; no partial job, record, or event. |
| Two workers claim concurrently | One wins; the other gets a different job or none. |
| Database unavailable | Requests fail fast with a sanitized error; readiness fails; no fabricated success. |

---

## 14. Observability

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
job and checks its required dependencies with bounded timeouts. Neither is an
unconditional `200`.

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

## 16. Planned schema (V1)

Tables: `jobs`, `job_attempts`, `workers`, `worker_sessions`, `leases`, `queues`,
`idempotency_records`, `outbox_events`, `results`, `dlq_entries`, `api_keys`,
`audit_events`.

Tables are created in the milestone that puts working behavior on them, not in
advance. Indexes will support eligibility and priority scans, queue and status
filters, idempotency lookup, expiring active leases, stale-heartbeat scans, pending
outbox scans, attempt history, and dashboard queries — each added with the query
that justifies it.
