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

Milestones M1 (durable ingress/outbox), M2 (workers, sessions, and atomic claim),
M3 (heartbeats, lease renewal, and crash recovery), and M4 (the complete job
lifecycle) are built. Never promote a marker without a passing test.

**Implemented today:** durable immediate and delayed submission with outbox
delivery; the advisory broker contract; logical-worker and process-session
registration; immutable session eligibility; globally idempotent notification
consumption; strict-priority atomic claim with queue/worker capacity; attempts
and renewable leases; fenced start with a persisted per-attempt execution
deadline; fenced success, failure, and cooperative cancellation acknowledgment
under a retained outcome identity; server-timed session heartbeat carrying
cancellation directives; fenced, generation-versioned lease renewal;
stale-session detection; reconciliation of due attempt timeouts, unacknowledged
cancellations, and expired leases; durable retry with bounded exponential
backoff and injected jitter; public cancellation; the authoritative logical DLQ
with listing, replay, and operator retry; scheduler promotion of due delayed and
retry-waiting work; bounded recovery of stranded queued jobs; and bounded
`demo.echo` execution. Result storage, authentication, the CLI, the SDK, and the
dashboard remain planned.

---

## 1. Model: PostgreSQL-authoritative, pull-based claim — [IMPLEMENTED]

Every step of the flow below is implemented as of M4. Step 12's general drift
repair beyond timeouts, cancellations, expired leases, and stale sessions
remains open-ended by design: the reconciler repairs the specific conditions it
can define, not everything that might one day be wrong.

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

## 2. Components — [IMPLEMENTED]

| Component | Responsibility | Status |
| --- | --- | --- |
| `taskforge-api` | Validate and durably accept immediate and delayed submissions; serve read, cancellation, and DLQ/replay APIs; serve internal worker control operations. | **Built** |
| `taskforge-outbox` | Publish pending outbox events to the broker with retry and backoff. | **Built** |
| `taskforge-migrate` | Apply schema migrations. | **Built** |
| `taskforge-scheduler` | Promote due `PENDING` and `RETRY_WAIT` jobs; re-notify stranded queued work. Holds no broker connection. | **Built** |
| `taskforge-worker` | Register a session, poll only from free bounded slots, claim, execute trusted handlers, and report fenced outcomes including failures and cooperative cancellation. | **Built** for `demo.echo`; result persistence is M5. |
| `taskforge-reconciler` | Mark stale sessions, record due attempt timeouts, finalize unacknowledged cancellations, expire leases, abandon their attempts, and release capacity. | **Built**; general drift repair beyond these is later. |
| `taskforge-cli` | Operator and developer command-line interface. | Planned (M5) |

Every component is safe to run with N replicas.

---

## 3. Data ownership — [IMPLEMENTED]

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

## 4. Job state machine — [IMPLEMENTED]

The full state set is enforced by a `CHECK` constraint, and as of M4 every state
in it is reachable. Each transition has its own dedicated, fenced operation;
there is no generic "set status" path anywhere.

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

### The transition matrix

Every row is decided under the established lock order against one
`clock_timestamp()` sample taken after those locks.

| Case | Preconditions under locks | Job | Attempt | Lease | Durable side effects |
| --- | --- | --- | --- | --- | --- |
| Successful completion | Current healthy session, exact fence, `RUNNING`, active unexpired lease, server time before `timeout_at` | `RUNNING → SUCCEEDED` | `RUNNING → SUCCEEDED` | `ACTIVE → COMPLETED` | Finish and release timestamps; an exact replay is a no-op |
| Retryable failure, budget remaining | Exact live fence, before the deadline, `RETRYABLE`, attempts used `< max_attempts` | `RUNNING → RETRY_WAIT` | `RUNNING → FAILED` | `ACTIVE → RELEASED` | Safe error metadata, outcome identity, persisted jittered delay and `retry_at`, `available_at = retry_at`; **no** outbox event |
| Permanent failure | Exact live fence, before the deadline, `PERMANENT` | `RUNNING → DEAD_LETTERED` | `RUNNING → FAILED` | `ACTIVE → RELEASED` | Exactly one DLQ entry, `PERMANENT_FAILURE` |
| Attempts exhausted | A retryable failure, timeout, or abandonment consumes the final budget | executing → `DEAD_LETTERED` | truthful `FAILED`, `TIMED_OUT`, or `ABANDONED` | `RELEASED`, or `EXPIRED` when the lease had lapsed | Exactly one DLQ entry, `ATTEMPTS_EXHAUSTED` |
| Timeout | `RUNNING`, persisted `timeout_at <=` post-lock server time | `RUNNING → RETRY_WAIT` or `DEAD_LETTERED` | `RUNNING → TIMED_OUT` | `ACTIVE → RELEASED` or `EXPIRED` | A retry decision, or exactly one DLQ entry |
| Cancellation before claim | `PENDING`, `QUEUED`, or `RETRY_WAIT` | source → `CANCELED` | none created | none active | `cancel_requested_at`; no new outbox event |
| Cancellation while executing | `LEASED` or `RUNNING` | source → `CANCEL_REQUESTED` | unchanged for now | stays active for now | Heartbeat directive; start, success, failure, and renewal all stop committing |
| Cooperative cancellation | `CANCEL_REQUESTED`, exact fence, active lease | `CANCEL_REQUESTED → CANCELED` | `LEASED \| RUNNING → CANCELED` | `ACTIVE → RELEASED` | Outcome identity stored; a duplicate is harmless |
| Fallback cancellation | `CANCEL_REQUESTED`, lease expired | `CANCEL_REQUESTED → CANCELED` | `LEASED \| RUNNING → CANCELED` | `ACTIVE → EXPIRED` | Reconciler-owned and idempotent |
| Abandonment | Lease expired, no cancellation, no due deadline | `LEASED \| RUNNING → QUEUED` or `DEAD_LETTERED` | `→ ABANDONED` | `ACTIVE → EXPIRED` | Immediate requeue with a fresh generation and event, or one DLQ entry ([ADR-0009](adr/0009-abandoned-attempts-consume-the-attempt-budget.md)) |
| Delayed submission | Valid future `scheduled_at` | insert as `PENDING` | none | none | `available_at = scheduled_at`; no outbox event |
| Retry or delayed eligibility | `PENDING` or `RETRY_WAIT`, `available_at <=` post-lock server time | source → `QUEUED` | prior history unchanged | prior leases unchanged | New notification generation and exactly one fresh transactional event |
| Replay / operator retry | Original stays `DEAD_LETTERED` with a DLQ entry | original unchanged; insert a new `QUEUED` job | original history unchanged | original leases unchanged | New job linked by `replayed_from_job_id`, replay identity record, fresh outbox event |

### Race semantics

Every one of these forms a transactionally ordered race, and exactly one
state-changing operation wins. Enforcement is row locks, transaction predicates,
constraints, and affected-row checks — never check-then-update application
logic.

- **Cancel versus success.** Success first makes a later cancel a stable
  conflict; cancel first makes a later success a state conflict.
- **Timeout versus success.** Whichever transaction reaches the authority rows
  first commits. A success that waited across the deadline is rejected against
  the fresh post-lock sample, with `attempt_timed_out` rather than
  `lease_expired`, because the deadline is the specific cause.
- **Failure versus renewal.** A failure under a freshly renewed lease commits;
  a renewal after the lease was released is rejected.
- **Cancellation versus renewal.** Once cancellation wins, renewal is refused —
  which is precisely what makes the lease lapse so reconciliation can finalize
  an uncooperative worker's attempt.
- **Promotion versus cancellation, and re-notification versus claim.** Both pairs
  serialize on the job row; the loser re-reads a state its predicate no longer
  matches and does nothing.

A job in `LEASED` or `RUNNING` is never marked terminally `CANCELED` while its
lease could still submit a valid completion. That is why cancellation of an
executing job produces `CANCEL_REQUESTED` rather than `CANCELED`, and why
finalization waits for the worker's acknowledgment or for the lease to lapse.

Precedence among terminal outcomes is stated rather than incidental:
cancellation, then a due deadline, then abandonment. See
[ADR-0010](adr/0010-durable-outcome-identity-and-terminal-precedence.md).

---

## 5. Domain model — [PARTIAL]

Everything below is implemented except result references, which are M5.

### Job
Id · auth scope · queue · job type · canonical immutable payload · status · priority
(0–100) · max attempts (total, including the first) · timeout · required capabilities
· scheduled time · next eligible time · cancellation-request time · timestamps ·
optional `replayed_from_job_id`.

### Attempt
Attempt id · job id · monotonically increasing attempt number (unique per job) ·
worker id · **worker-session id** · typed status · start and finish times ·
persisted execution deadline · retained outcome request identity · failure class
· bounded error code and safe message · persisted retry delay and `retry_at`.
The lease references the attempt through a constrained one-to-one binding. A
result reference and a trace id are planned.

Attempt history is preserved. The last error is never overwritten in place.

The execution deadline is stamped once, when the attempt's start transition
commits, and lease renewal never moves it. The outcome identity is unique for
the lifetime of attempt history, which is what makes an ambiguous failure or
cancellation report safe to retry. Recognizing a committed outcome is separate
from exercising live authority, so an exact replay still returns its stored
result after the session was replaced or the lease closed — the ordinary
consequences of the failure that lost the response — while a first-time outcome
from a fenced boot is still refused. All of this is
[ADR-0010](adr/0010-durable-outcome-identity-and-terminal-precedence.md).

Failure detail is bounded and safe by contract, in the schema as well as in Go:
a lowercase code of at most 64 bytes and a message of at most 512 bytes with no
control characters. Raw handler text, driver errors, panic values, stack traces,
and payload contents are never stored, returned, or logged.

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
loads eligibility from the locked session row. See
[ADR-0006](adr/0006-session-bound-worker-eligibility.md).

M3 adds periodic heartbeat. The request carries no timestamp: the control plane
locks the session row, samples `clock_timestamp()` afterwards, and advances
`last_heartbeat_at` monotonically. Only a current `HEALTHY` session is accepted,
and a late heartbeat never revives a fenced one. A session that misses the
configured threshold is marked `UNHEALTHY` with an `ended_at` stamp; `OFFLINE`
stays reserved for replacement and explicit shutdown, so an operator can tell a
crash from a restart.

Capacity is derived from active leases or a transactionally maintained reservation
ledger, and is reconcilable. A mutable `active_jobs` counter is never trusted as
unquestioned authority.

### Lease
Opaque lease id · job id · attempt id · worker id · worker-session id · acquired at ·
expires at · renewed at · typed status.

A partial unique index enforces **at most one active lease per job**. The window is
server-owned; start, renewal, and success are all rejected at or after expiry using
database time sampled after all authority locks.

M3 adds renewal. A lease also carries a monotonic `renewal_version` and the
identity of the request that produced it, so an ambiguous retry returns the
committed window instead of extending authority a second time, and a stale or
competing generation mutates nothing. Renewal never resurrects an expired,
completed, released, or reconciled lease, and it extends lease authority only —
never the job's `timeout_seconds` budget. See
[ADR-0008](adr/0008-fenced-idempotent-lease-renewal.md).

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

## 8. Scheduling and claim — [IMPLEMENTED]

`taskforge-scheduler` promotes due `PENDING` and `RETRY_WAIT` jobs using
PostgreSQL server time, creates the notification outbox event in the same
transaction as the promotion, and re-notifies stranded queued work under a
bounded, rate-limited, generation-aware policy. It holds no broker connection.
See [ADR-0011](adr/0011-notification-generations-and-bounded-renotification.md).

A job carries a monotonic notification generation identifying one eligibility
transition, and `last_notification_at`. Both are what let bounded re-notification
tell "the current transition still has an unpublished notification" from "a stale
event belonging to an attempt that is already over" — so an old
publish-before-mark event can never suppress the fresh event a new transition
requires.

Delayed and retry-waiting jobs deliberately carry no notification while they
wait. Advertising work no worker may claim yet is a wasted round trip a worker
must then decline to acknowledge.

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

## 9. Concurrency control — [IMPLEMENTED]

Submission idempotency, outbox publishing, notification consumption, queue
capacity, logical-worker capacity, claim, heartbeat, renewal, start, success,
failure, cancellation, cancellation acknowledgment, timeout, scheduler
promotion, bounded re-notification, dead-lettering, and replay are all
serialized in PostgreSQL.

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
queues; a hash collision only over-serializes. Fenced transitions, including renewal,
failure, and cancellation acknowledgment, use the same row order without the identity
lock. Queue capacity counts active leases by queue. Worker capacity counts by stable
logical worker, so restarting a process cannot evade a lease held by its old session.

M4's operations take the applicable prefix or subsequence of that same order,
never a different one:

- Public cancellation, scheduler promotion, bounded re-notification, and replay
  take `queue → job`. Both start with the queue row, so none can jump ahead of a
  fenced transition already holding it.
- Failure, cancellation acknowledgment, and timeout finalization take the full
  order. Reconciliation takes it without requiring a healthy session, because
  the whole point is repairing state whose worker is gone.
- `dlq_entries` and `dlq_replays` extend the order at the **end**, after every
  authority row is already held.

For each of these, a pre-read may supply immutable routing hints only — a job's
queue, a lease's binding — and every mutable field is re-read and revalidated
under the locks, against a `clock_timestamp()` sampled afterwards. A candidate
scan is never authority.

The identities each operation is idempotent under: submission uses scope plus
`Idempotency-Key`; claim uses the globally unique outbox event id; renewal uses
the full fence plus a renewal request id and expected generation; success uses
the full fence; failure and cancellation acknowledgment use the full fence plus
a lifetime-retained outcome request id; public cancellation uses scope plus job
id; timeout uses the attempt id, its persisted deadline, and the locked state,
with no client clock or client identity involved; scheduler promotion uses the
job id plus the locked expected status and generation; re-notification adds the
absence of a pending current-generation event; DLQ insertion uses the unique
terminal job id; and replay uses scope, original job id, and `Idempotency-Key`.

Heartbeat and stale-session marking lock only the worker-session row, which is a
prefix of that order. Lease reconciliation takes the full order and deliberately does
**not** require a healthy session, because an expired lease must be recoverable while
its process is still heartbeating. Its candidate scan reads lease ids without any
lock — inverting the order would be the one way to deadlock here — and every field
that matters is re-read and revalidated under the locks, against a `clock_timestamp()`
sampled afterwards.

---

## 10. Retry, timeout, cancellation, DLQ, replay — [IMPLEMENTED]

### Failure classes

`RETRYABLE` · `PERMANENT` · `TIMED_OUT` · `CANCELED` · `ABANDONED`.

A trusted handler may declare only the first two, through a typed error carrying
a stable code and a safe message. The other three are server-authoritative: a
worker that presents one is rejected. A plain error or a recovered panic becomes
a generic retryable failure whose raw text never travels.

`EXHAUSTED` is deliberately **not** an attempt status. The final attempt keeps
its truthful `FAILED`, `TIMED_OUT`, or `ABANDONED`, and why the job ended is
recorded once on its dead-letter entry.

### Retry

One policy governs worker-reported failures and reconciler-detected timeouts
alike, so a job cannot learn a different cadence depending on whether its worker
managed to report the failure:

```text
nominal = min(max_delay, base_delay * multiplier^(n-1))
factor  = 1 + jitter_fraction * (2r - 1)
delay   = clamp(nominal * factor, 0, max_delay)
retry_at = post-lock clock_timestamp() + delay
```

Both the chosen delay and the instant it produced are persisted on the terminal
attempt, and the job's `available_at` is set to the same `retry_at`. The job
moves to `RETRY_WAIT` and **no** notification is created until scheduler
promotion. The random source is injected: seeded in tests, independently
crypto-seeded in each API and reconciler process so replicas recovering from one
outage do not compute identical retry instants. An ambiguous failure response
returns the previously persisted decision and never recomputes jitter.

Overflow is handled before the conversion to a duration, not after: a large
attempt number makes the nominal delay `+Inf`, and with full jitter and the
lowest factor `+Inf * 0` is `NaN`, which compares false against every bound.

### Timeout

`timeout_seconds` is a **per-attempt execution budget**, not a whole-job
wall-clock deadline. It starts once when that attempt's start transition commits
and is persisted as `job_attempts.timeout_at`. Lease renewal never resets or
extends it.

Start returns a typed result: the start time, the persisted deadline, the
PostgreSQL-measured remaining milliseconds, and whether the response is an exact
replay. A replay returns the ORIGINAL deadline — recomputing it would hand a
worker a fresh budget every time a response was lost, which is the one way a
timeout could never fire. The worker converts the server-measured remaining
duration into a conservative monotonic local deadline; PostgreSQL stays
authoritative.

There is no worker-authoritative timeout endpoint. The worker cancels its
handler locally at the conservative deadline, and only a PostgreSQL transaction
driven by reconciliation may record `TIMED_OUT`. Every fenced operation that
could otherwise extend or finish the work checks the persisted deadline against
the same post-lock sample, so a success or failure that waited across it is
rejected rather than committed on a stale clock reading.

If an expired-lease scan finds a running attempt whose deadline is already due,
it uses the timeout path rather than misclassifying the attempt as `ABANDONED`.
That distinction is load-bearing: abandonment requeues immediately with no
backoff and no failure detail, so a job whose handler reliably takes too long
would otherwise loop through its whole budget at full speed.

### Cancellation

Public cancellation is keyed idempotently by scope plus job id and needs no
request identity: cancelling twice is one decision observed twice.

`PENDING`, `QUEUED`, and `RETRY_WAIT` go straight to terminal `CANCELED` with no
attempt created. `LEASED` and `RUNNING` go to `CANCEL_REQUESTED`, which
immediately stops start, success, failure, and renewal from committing while
leaving attempt and lease history intact. `SUCCEEDED` and `DEAD_LETTERED` return
a stable conflict.

Delivery rides the **heartbeat**, not a work notification. The heartbeat loop
already runs unconditionally — while idle and through a graceful drain — so
cancellation reaches a busy worker and one waiting on an empty broker queue
alike, with no broker delivery involved. The response carries a bounded list of
directives naming the job, attempt, and lease.

A cooperative worker acknowledges through a dedicated fenced operation carrying
its own retained outcome identity: job `CANCEL_REQUESTED → CANCELED`, attempt
`CANCELED`, lease `RELEASED`. An attempt canceled between claim and start
truthfully has no start time, which is why migration 0009 revised the timeline
constraint.

A cancellation that wins before Start reaches the control plane is refused with
its own code, `cancellation_requested`, rather than a generic state conflict.
The distinction is load-bearing: every other conflict Start can report means the
worker no longer holds the attempt, so dropping it is right, while this one
means the worker still holds it and is the only party that can end it promptly.
The directive may never have reached that process, so Start's answer has to be
sufficient on its own. If the worker is gone or uncooperative, renewal is already refused,
the lease lapses, and reconciliation finalizes the same transition with the
lease recorded `EXPIRED`.

Cancellation never produces a retry and never creates a dead-letter entry.

### Logical DLQ and replay

One `dlq_entries` row per dead-lettered job, unique by job id, inserted through
one shared transactional helper by every path that reaches `DEAD_LETTERED`.
Listing is scope-filtered, keyset-paginated on `(created_at DESC, id DESC)`
behind an opaque cursor, and carries bounded metadata but never a payload.

Replay creates a **new** job linked through `replayed_from_job_id` and leaves
the original job, attempts, leases, failure metadata, and DLQ entry exactly as
they are. `POST /v1/jobs/{job_id}/retry` and `POST /v1/dlq/{job_id}/replay` are
the same operation with one idempotency namespace, keyed by scope, original job
id, and `Idempotency-Key`. Different keys deliberately create different
replacement jobs. See
[ADR-0012](adr/0012-logical-dlq-and-replay-as-a-new-job.md).

## 11. Heartbeats, leases, and crash recovery — [IMPLEMENTED]

Lease issuance, renewal, and every expiry check use PostgreSQL time sampled after
all authority locks. A worker turns the server-measured remaining window into a
conservative monotonic execution deadline with completion margin; it never
compares its own wall clock with `expires_at`, and PostgreSQL remains
authoritative for every state transition.

**Heartbeat.** A worker heartbeats its process session on a fixed interval,
starting immediately after registration and continuing through a graceful drain.
Server receipt time is authoritative for staleness; the request carries no
timestamp. Development defaults, configurable and never hardcoded into domain
logic: heartbeat every 5 s, stale after 15 s. Configuration rejects a stale
threshold under three heartbeat intervals, so a single lost request can never look
like a dead process. A worker treats a definitive fence as fatal, retries
transient failures, and stops presenting itself as ready once it has gone a full
staleness threshold without confirming liveness. Because the loop runs while idle,
a replaced worker discovers it without waiting for a broker delivery.

**Renewal.** A running attempt renews its lease on a cadence that must leave room
for several attempts inside one window (configuration rejects a renewal interval
above one third of the lease duration). Renewal is fenced by job, attempt, lease,
worker, and session, and additionally by a renewal identity and generation, so an
ambiguous retry cannot extend authority twice. See
[ADR-0008](adr/0008-fenced-idempotent-lease-renewal.md). It extends lease
authority only; the job's `timeout_seconds` budget is measured once from execution
start. If renewal loses the fence, or cannot be confirmed before the conservative
deadline, the worker cancels its cooperative handler and reports nothing.

**Reconciliation.** Three distinct scans, in a deliberate order. A session can
stop heartbeating while its lease is still valid, and a lease can expire while
its session is perfectly healthy — the worker leaves a lease active after a
handler error and keeps heartbeating — so requiring several conditions at once
would strand exactly the cases this exists to recover.

1. Stale current sessions are marked `UNHEALTHY` with an `ended_at` stamp, from
   server time. `OFFLINE` stays reserved for replacement and explicit shutdown.
2. Running attempts whose persisted `timeout_at` is due are recorded
   `TIMED_OUT`, **while their leases may still be live**. Running this before the
   expired-lease scan is what makes a timed-out attempt a timeout rather than
   something that waits for its lease to lapse first.
3. For each expired `ACTIVE` lease, **one transaction**: acquire authority rows
   in the established order, sample PostgreSQL time afterwards, revalidate every
   mutable field, and then apply precedence —
   `CANCEL_REQUESTED` finalizes the cancellation (attempt `CANCELED`, lease
   `EXPIRED`); an already-due deadline finalizes a timeout; otherwise the
   attempt becomes `ABANDONED` with a `finished_at` (from `LEASED` or `RUNNING`
   alike) and the job either returns to `QUEUED` with a **fresh**
   `work.available` outbox event on a new generation, or becomes
   `DEAD_LETTERED` if that abandonment consumed the total attempt budget
   ([ADR-0009](adr/0009-abandoned-attempts-consume-the-attempt-budget.md)).

Repeated and concurrent passes never produce a duplicate timeout outcome, a
duplicate cancellation finalization, a duplicate retry decision, a duplicate
capacity release, a duplicate dead-letter entry, or a duplicate recovery event.

Capacity is released solely by the lease ceasing to be `ACTIVE`; there is no
counter to decrement. The recovery event gets a new id because the original event
id was already globally consumed as the claim identity.

A crash before commit leaves the old state intact, and rerunning repairs it.
Repeated and concurrent reconciliation never produces a second abandonment, a
second capacity release, or a duplicate recovery event. A returning old worker is
rejected by lease and session fencing; its stale heartbeat, renewal, and
completion are all refused and mutate nothing. Durable attempt and lease history
plus the transactional recovery event are the record of what happened; a dedicated
`audit_events` table remains planned.

---

## 12. Reliability invariants — [IMPLEMENTED]

Each of these must have an automated test. All eighteen are implemented and
tested as of M4. Invariants 13 and 14 were M4's to close: retry scheduling is
durable PostgreSQL state that no process holds, and a canceled job cannot later
become successful because `CANCEL_REQUESTED` stops success from committing and
terminal `CANCELED` is never left.

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

## 13. Failure model — [IMPLEMENTED]

| Failure | Response |
| --- | --- |
| Broker unavailable after DB commit | Job stays durable; outbox event stays pending and retries. No loss. |
| Broker duplicates a notification | Globally idempotent event-id consumption admits one claim; another session gets a safe duplicate outcome. |
| Broker loses a notification | The scheduler re-notifies a stranded queued job once the bounded interval elapses, using a new event id on the job's current notification generation. Reachability is repaired; nothing was ever at risk of corruption. |
| Publisher crashes mid-publish | Event republished after the claim timeout/visibility window lapses. Documented at-least-once window. |
| Worker crashes mid-execution | Its session goes stale and its lease expires on server time; reconciliation marks the attempt `ABANDONED`, releases capacity, and requeues the job with a fresh notification (or dead-letters it if the budget is gone). |
| Handler returns an error | The worker reports a fenced failure under a retained outcome identity. With budget remaining the job enters `RETRY_WAIT` with a persisted jittered delay; a permanent classification or an exhausted budget dead-letters it with exactly one DLQ entry. |
| Handler outlives its execution budget | The worker cancels it locally at a conservative deadline and reports nothing. Reconciliation records `TIMED_OUT` against the persisted deadline and applies the same retry policy. |
| Handler ignores cancellation entirely | Go cannot terminate it. Reconciliation finalizes the cancellation once the lease lapses, and every fenced operation refuses whatever the handler eventually produces. |
| Operator cancels a running job | The job moves to `CANCEL_REQUESTED`, which immediately stops start, success, failure, and renewal from committing. The worker learns of it on its next heartbeat and acknowledges; if it never does, reconciliation finalizes it. |
| Scheduler crashes mid-promotion | The transaction rolls back, so there is neither a promotion nor an event, and a later pass promotes it. An ambiguous commit is safe to rerun: the locked status and generation show it already happened. |
| Worker stops renewing but keeps heartbeating | The lease still expires on server time and is reconciled anyway. Reconciliation never requires both conditions. |
| Worker returns after expiry | Heartbeat, renewal, start, and completion are all rejected by session + lease fencing, and mutate nothing. |
| Reconciler crashes mid-repair | The transaction rolls back; the old state is intact and a later pass repairs it. Repeated and concurrent passes are idempotent. |
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

**Implemented** (`migrations/0001` through `0011`): `queues`, `jobs`,
`idempotency_records`, `outbox_events`, `workers`, `worker_sessions`,
`job_attempts`, `leases`, `dlq_entries`, and `dlq_replays`, plus
`schema_migrations` maintained by the runner.
M2 adds immediate eligibility time, worker-group routing, constrained session/
attempt/lease bindings, one current session per logical worker, one active lease per
job, globally unique notification claims, active-capacity indexes, and timeline-order
constraints. M3 adds renewal generation and identity on `leases`, with a check constraint
tying them together and a partial unique index ensuring no two leases hold the
same renewal identity at the same time, plus the two partial indexes the
reconciler's scans use. That index constrains live identities rather than every
identity ever used; the exact scope is recorded on the index itself and in
[ADR-0008](adr/0008-fenced-idempotent-lease-renewal.md).

M4 adds scheduling, cancellation, replay linkage, and notification-generation
columns to `jobs`; a persisted execution deadline, a lifetime-unique outcome
identity, and bounded typed failure detail to `job_attempts`; relational job and
generation metadata to `outbox_events`; and the two DLQ tables. It also replaces
the attempt timeline constraint forward-only, so a claimed-but-never-started
attempt may be `CANCELED` — inventing a start time to satisfy the old rule would
have put a lie in attempt history. Migration 0010 backfills every M3
`DEAD_LETTERED` job into `dlq_entries`, because ADR-0009 made that state
reachable one milestone before the DLQ that reads it.

Migration 0011 corrects three things 0009 and 0010 got wrong, forward-only,
because a published migration has been applied somewhere by definition and
editing one breaks the runner's checksum enforcement on every database that
already ran it (AGENTS.md section 6). It replaces the `outbox_events`
notification-metadata `CHECK`, which accepted the exact unpaired row it existed
to refuse because a `CHECK` rejects only `FALSE` and `NULL >= 1` is `NULL`. It
reconstructs notification generations and `last_notification_at` from the real
ordering of historical `work.available` events instead of from job creation
time, which was only correct for a job that was never requeued — and it does so
only on a database still carrying 0009's exact fingerprint, so it can never
rewrite generations M4 has since moved. And it makes the DLQ and replay
relationships database-enforced through composite foreign keys: a dead-letter
entry's terminal attempt must belong to that exact job, a replay's original and
replacement must both belong to the recorded scope, `replayed_from_job_id`
cannot cross scopes, and a `dlq_replays` row and its replacement job must name
the same original, so the two records of one replay cannot disagree.

**Planned:** `results`, `api_keys`, `audit_events`.

Tables are created in the milestone that puts working behavior on them, not in
advance. Every index exists because an implemented query orders by exactly its
columns and filters by exactly its predicate: eligibility and priority scans,
queue and status filters, idempotency lookup, pending outbox scans, active
queue/worker capacity, expiring active leases, stale current-session heartbeats,
due promotion of `PENDING` and `RETRY_WAIT` jobs, stranded queued
re-notification, due running-attempt timeouts, a session's executing attempts
for cancellation delivery, pending work events by job and generation, DLQ
scope/keyset listing, and replay identity lookup. Indexes for attempt history
and dashboard queries arrive only with the queries that justify them.
