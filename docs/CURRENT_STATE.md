# Current State

This document is the source of truth for what is runnable now and what remains
planned. Milestones M1, M2, and M3 are merged into `main`; this document records
the implemented state through M4, which is on the
`feat/m4-complete-job-lifecycle` branch and its draft pull request.

## Milestone status

- **M1 — durable ingress and transactional outbox:** complete.
- **M2 — worker sessions, atomic claims, and fenced execution:** complete.
- **M3 — heartbeat, lease renewal, and reconciliation:** complete.
- **M4 — retry, timeout, cancellation, DLQ, replay, delayed jobs:** complete.
- **M5 — API keys, result storage, CLI, Python SDK:** not started.

## Runnable system

Six binaries build and run:

- `taskforge-api` accepts idempotent immediate and delayed job submissions, job
  reads, cancellation, DLQ listing, replay and operator retry, plus the internal
  worker-control surface: session registration, heartbeat with cancellation
  delivery, atomic claims, fenced lease renewal, and the fenced start, success,
  failure, and cancellation-acknowledgment transitions.
- `taskforge-outbox` publishes durable work-availability events to ElasticMQ.
- `taskforge-scheduler` promotes due delayed and retry-waiting jobs and
  re-notifies stranded queued work. It holds no broker connection.
- `taskforge-worker` polls ElasticMQ only while it has capacity, heartbeats its
  process session, executes trusted handlers through the control plane, renews
  each running attempt's lease, reports classified failures, and acknowledges
  cancellation cooperatively.
- `taskforge-reconciler` marks stale sessions unhealthy, records due attempt
  timeouts, finalizes cancellations no worker acknowledged, expires lapsed
  leases, abandons their attempts, releases the capacity they held, and requeues
  or dead-letters the job.
- `taskforge-migrate` applies numbered PostgreSQL migrations.

The schema consists of migrations `0001` through `0011`. M4 adds three:

- `0009_job_lifecycle.sql` — scheduling, cancellation, replay linkage, and
  notification bookkeeping on `jobs`; a persisted execution deadline, a
  lifetime-unique outcome identity, and bounded typed failure detail on
  `job_attempts`; relational job and generation metadata on `outbox_events`; the
  six indexes M4's scans justify; and a forward-only replacement of the attempt
  timeline constraint so a claimed-but-never-started attempt may be `CANCELED`.
- `0010_logical_dlq_and_replay.sql` — `dlq_entries` (unique per job) and
  `dlq_replays` (keyed by scope, original job, and idempotency key), plus a
  backfill of every existing `DEAD_LETTERED` job into the DLQ.
- `0011_lifecycle_integrity_and_truthful_backfill.sql` — three corrections to
  what `0009` and `0010` shipped, carried forward rather than edited into them,
  because a published migration has been applied somewhere by definition and
  editing one breaks the runner's checksum enforcement on every database that
  already ran it:
  - the `outbox_events` notification-metadata `CHECK` is replaced with one that
    states `notification_generation IS NOT NULL` explicitly. A `CHECK` rejects
    only `FALSE`, so `notification_generation >= 1` evaluated to `NULL` and was
    accepted, leaving a job id paired with no generation.
  - `jobs.notification_generation` and `jobs.last_notification_at`, and every
    `work.available` event's generation, are reconstructed from the actual
    ordering of historical events rather than from job creation time. The
    reconstruction runs only on a database still carrying `0009`'s exact
    fingerprint, so it can never rewrite generations M4 has since moved.
  - composite foreign keys make the DLQ and replay relationships
    database-enforced: a dead-letter entry's terminal attempt must belong to
    that exact job, a replay's original and replacement must both belong to the
    recorded scope, a job's `replayed_from_job_id` cannot cross scopes, and a
    `dlq_replays` row and its replacement job must name the same original.

## Implemented behavior

Everything recorded for M1, M2, and M3 still holds. What M4 adds:

### Failure classification and retry

- A trusted handler may declare `RETRYABLE` or `PERMANENT` through a typed
  error carrying a stable lowercase code and an optional safe message.
  `TIMED_OUT`, `CANCELED`, and `ABANDONED` are server-authoritative and are
  rejected with `422` if a worker presents one.
- A plain Go error, a wrapped dependency error, and a recovered panic all become
  a generic retryable failure with a generic message. Their raw text is neither
  stored, returned, nor logged — that text is the one place payload fragments,
  credentials, driver output, and stack traces reliably appear.
- Bounds are enforced twice: in Go before the write, and by `CHECK` constraints
  in the schema. A code is a lowercase token of at most 64 bytes; a message is at
  most 512 bytes with no control characters and no line breaks.
- A retryable failure with budget remaining moves the job to `RETRY_WAIT` with a
  persisted jittered delay and `retry_at`, and sets `available_at` to the same
  instant. **No notification is created.** A scheduled job is durable but not
  claimable, and advertising it would wake a worker for work it cannot take.
- A permanent failure dead-letters immediately, deliberately without burning the
  remaining attempt budget. An exhausted retryable failure dead-letters too.
  Both create exactly one entry, through the same helper every other terminal
  path uses.
- Backoff is `min(max, base * multiplier^(n-1))` scaled by a jitter factor in
  `[1-j, 1+j]` and clamped back into `[0, max]`. Overflow is handled before the
  conversion to a duration: a large attempt number makes the nominal delay
  `+Inf`, and with full jitter and the lowest factor `+Inf * 0` is `NaN`, which
  compares false against every bound.
- The random source is injected. Tests seed it; `taskforge-api` and
  `taskforge-reconciler` each seed one independently from system entropy, so
  replicas recovering from the same outage do not compute identical retry
  instants.

### Durable outcome identity

- Failure reporting and cooperative cancellation acknowledgment each carry a
  client-generated `outcome_request_id`, retained on the attempt for the lifetime
  of history and made unique by a partial unique index.
- The decision is computed once and persisted on the attempt: classification,
  safe code and message, chosen delay, and `retry_at`. An exact replay returns
  those stored values unchanged — it does not consume budget again, redraw
  jitter, or create a second dead-letter entry.
- Reusing the identity for a different attempt, or replaying it with a different
  classification, code, or message, is a stable `outcome_conflict`, never a
  leaked uniqueness error.
- The values returned to the caller come from the `UPDATE`'s `RETURNING` clause
  rather than from what Go computed, and the retry instant is derived from the
  millisecond-truncated delay, so a first response and its own replay cannot
  disagree by rounding.
- This is deliberately stronger than ADR-0008's renewal identity, which is
  released when a lease renews again. An outcome identity is the permanent record
  of one terminal decision, so nothing releases it. See
  [ADR-0010](adr/0010-durable-outcome-identity-and-terminal-precedence.md).
- Recognizing committed history is separate from exercising live authority. An
  exact replay of a committed `Succeed`, `Fail`, or cancellation acknowledgment
  returns its stored result **after session replacement, after lease closure, and
  after lease expiry** — the ordinary consequences of the network failure that
  lost the response in the first place. The complete stored job, attempt, lease,
  worker, and session fence and the exact identity and body are verified before
  history is returned; a changed body is `outcome_conflict`, and a foreign
  identity or a different fence is `fence_rejected`.
- A first-time outcome still requires current authority. From a replaced boot,
  `Start`, `Succeed`, `Fail`, and cancellation acknowledgment are all
  `fence_rejected` when nothing has committed for that attempt yet.

### Per-attempt execution deadlines

- `timeout_seconds` is a per-attempt budget, not a whole-job wall-clock deadline.
  It is stamped **once**, as `job_attempts.timeout_at`, when that attempt's start
  transition commits.
- Start returns a typed result: the start time, the persisted deadline, the
  PostgreSQL-measured remaining milliseconds, and whether the response is an
  exact replay. A replay returns the ORIGINAL deadline. Recomputing it would hand
  a worker a fresh budget every time a response was lost, which is the one way a
  timeout could never fire.
- Lease renewal never moves the deadline, and renewal is itself refused once the
  deadline has passed: extending authority that can never be used would only
  delay reconciliation while the handler kept burning resources.
- The worker converts the server-measured remaining duration into a conservative
  monotonic local deadline. It never starts a fresh timer from `timeout_seconds`
  once the response lands, and it never compares its own wall clock with the
  server's.
- There is no worker-authoritative timeout endpoint. The worker cancels its
  handler locally with a distinguishable cause and reports **nothing**; only a
  PostgreSQL transaction driven by reconciliation may record `TIMED_OUT`.
- Every fenced operation that could otherwise extend or finish the work checks
  the persisted deadline against the same post-lock sample. A success or failure
  that waited across it is rejected with `attempt_timed_out` rather than
  committed on a stale clock reading, and rather than `lease_expired` — when a
  timeout wins it also releases the lease, so both are true, and the deadline is
  the specific cause.

### Cancellation

- Public cancellation is keyed by scope plus job id and needs no request
  identity: cancelling twice is one decision observed twice.
- `PENDING`, `QUEUED`, and `RETRY_WAIT` become terminal `CANCELED` immediately
  and **no attempt is created**. An advisory notification already on the broker
  stays harmless: the claim predicate simply finds no queued job.
- `LEASED` and `RUNNING` become `CANCEL_REQUESTED`. Attempt and lease history are
  untouched at that point; what changes immediately is that start, success,
  failure, and renewal all stop committing.
- `SUCCEEDED` and `DEAD_LETTERED` return a stable conflict. A terminal job never
  returns to a non-terminal state and never changes which terminal state it is
  in.
- Directives are delivered on the **heartbeat**, not on a work notification. The
  heartbeat loop already runs unconditionally — while idle and through a graceful
  drain — so cancellation reaches a busy worker and one waiting on an empty
  broker queue alike. Nothing about delivery depends on the broker.
- A directive names the job, attempt, and lease. A worker ignores one naming a
  lease it does not hold, and the control plane hands one only to the session
  actually executing that attempt, only while its lease is still active.
- The worker registers an attempt in a cancellation registry **before** Start, so
  a directive that wins the window between a claim committing and the handler
  being invoked is retained rather than dropped.
- A cancellation that wins before Start is refused by the control plane with its
  own stable code, `cancellation_requested`, distinct from `state_conflict`.
  Every other conflict Start can report means the worker no longer holds the
  attempt, so dropping it is right; this one means the opposite. The worker
  acknowledges with the full five-part fence and one reusable outcome identity,
  and only then unregisters the attempt. The directive may never have reached
  that process, so Start's own answer has to be sufficient on its own.
- A cooperative worker acknowledges through a dedicated fenced operation: job
  `CANCEL_REQUESTED → CANCELED`, attempt `CANCELED`, lease `RELEASED`. An attempt
  canceled between claim and start truthfully has no start time.
- If the worker is gone or uncooperative, renewal is already refused, the lease
  lapses, and reconciliation finalizes the same transition with the lease
  recorded `EXPIRED`. Cancellation produces neither a retry nor a DLQ entry.

### Delayed submission and the scheduler

- `POST /v1/jobs` accepts `scheduled_at` as RFC 3339, canonicalized to UTC.
  Equivalent offsets naming one instant are the same request; an omitted field
  and an explicit `null` remain equivalent.
- The idempotency fingerprint appends its scheduling component only when a
  schedule was requested, so an immediate submission hashes to exactly the byte
  stream M1 through M3 produced and a key recorded before this milestone still
  replays rather than conflicting.
- PostgreSQL decides whether the schedule is still in the future. A future value
  makes the job `PENDING` with `available_at = scheduled_at` and **no** outbox
  event; an absent, null, or already-due value is `QUEUED` with its event now.
- `taskforge-scheduler` promotes due `PENDING` and `RETRY_WAIT` jobs through one
  mechanism, creating the `work.available` event in the same transaction as the
  promotion. It exposes a database-backed `RunOnce` seam, loopback-only
  `/healthz` and PostgreSQL-backed `/readyz`, stops cleanly on SIGINT and
  SIGTERM, holds no row lock across network I/O, and reports bounded pass
  statistics with no payloads and no job ids.
- Safety with N replicas is structural: the candidate scans carry no authority,
  and each promotion's `UPDATE` names both the expected status and the expected
  notification generation.

### Bounded stranded-queue recovery

- A job carries a monotonic `notification_generation` identifying one eligibility
  transition, and `last_notification_at`. Both are incremented and stamped
  whenever the job newly becomes `QUEUED` — at submission, at promotion, and at
  crash-recovery requeue — in the same transaction as the event.
- `outbox_events` carries a real `job_id` and the generation the event
  advertises. Neither is serialized to the broker, so the published wire contract
  is unchanged and no schema version is bumped.
- A replacement notification is created only when the job is still `QUEUED`, the
  configured interval has elapsed, and **no pending event exists for the job's
  current generation**. That last condition is why generations exist: a stale
  event left by the publish-before-mark window belongs to an attempt that is
  already over, and a check on job id alone would let it suppress the
  notification a new transition requires.
- The replacement carries a new event id but the same generation, and
  `last_notification_at` advances in the same statement, so the job is
  rate-limited again immediately and N replicas cannot multiply events.
- See
  [ADR-0011](adr/0011-notification-generations-and-bounded-renotification.md).

### Logical DLQ, replay, and operator retry

- One `dlq_entries` row per dead-lettered job, unique by job id, inserted through
  one shared transactional helper by every path that reaches `DEAD_LETTERED` —
  permanent failure, exhausted retryable failure, exhausted timeout, and
  ADR-0009's exhausted abandonment.
- Migration 0010 backfills every existing M3 `DEAD_LETTERED` job as
  `ATTEMPTS_EXHAUSTED`, linked to its final attempt. Those jobs are real, and
  leaving them invisible after the upgrade would be a worse gap than the one
  ADR-0009 accepted.
- `GET /v1/dlq` is scope-filtered and keyset-paginated on
  `(created_at DESC, id DESC)` behind an opaque validated cursor, with bounded
  default and maximum page sizes. It carries operator metadata joined from the
  immutable job and terminal attempt, and **never a payload**.
- `POST /v1/dlq/{job_id}/replay` and `POST /v1/jobs/{job_id}/retry` are the same
  operation with one idempotency namespace. Both require `Idempotency-Key`; both
  accept only a `DEAD_LETTERED` job with a DLQ entry.
- Replay creates a distinct new job — copying queue, type, canonical payload,
  priority, attempt budget, timeout, and capabilities — linked through
  `replayed_from_job_id`, immediately eligible, with a fresh attempt budget and
  a fresh notification generation. The new job, the replay identity record, and
  the outbox event commit in one transaction.
- The original job, its attempts, its leases, its failure metadata, and its DLQ
  entry are left exactly as they are. Different idempotency keys deliberately
  create different replacement jobs; the entry's replay count says so. See
  [ADR-0012](adr/0012-logical-dlq-and-replay-as-a-new-job.md).

### Terminal-outcome precedence

Under the authority locks, against one post-lock `clock_timestamp()` sample:
cancellation first, then a due persisted deadline, then ADR-0009's abandonment.

The middle rule is load-bearing rather than cosmetic. Recording a genuine
timeout as `ABANDONED` would requeue it immediately with no backoff and no
failure detail, so a job whose handler reliably takes too long would loop
through its entire attempt budget at full speed and its history would say it was
interrupted rather than that it ran out of time.

### The attempt-budget decision is unchanged

ADR-0009 still governs abandonment. An `ABANDONED` attempt counts toward
`max_attempts`; recovery while budget remains is **immediate requeue** with no
backoff, no jitter, and no `RETRY_WAIT`; and an abandonment that consumes the
budget dead-letters the job. M4 shares only the budget arithmetic with retry,
through a policy that returns a zero delay for that class precisely so the two
cannot drift.

## Locking and time

The critical lock order is unchanged: `queue → worker session → job → attempt →
lease`. Claim additionally takes its claim-identity advisory lock first.

M4's operations take the applicable prefix or subsequence of that same order:

- Public cancellation, scheduler promotion, bounded re-notification, and replay
  take `queue → job`. Both start with the queue row, so none can jump ahead of a
  fenced transition already holding it.
- Failure, cancellation acknowledgment, and timeout finalization take the full
  order. Reconciliation takes it without requiring a healthy session, because
  repairing state whose worker is gone is the whole point.
- `dlq_entries` and `dlq_replays` extend the order at the end, after every
  authority row is already held.

For each of these, a pre-read supplies immutable routing hints only — a job's
queue, a lease's binding — and every mutable field is re-read and revalidated
under the locks against a `clock_timestamp()` sampled afterwards. A candidate
scan is never authority.

Every decision that can wait across an expiry, a deadline, or an eligibility
boundary uses `clock_timestamp()` sampled after the relevant locks, never
transaction-start `now()`.

## Configuration added in M4

All have documented defaults, are validated at startup, and are never hardcoded
into domain logic. See [.env.example](../.env.example).

| Variable | Default | Validated relationship |
| --- | --- | --- |
| `TASKFORGE_SCHEDULER_ADDR` | `127.0.0.1:8084` | must bind to loopback |
| `TASKFORGE_SCHEDULER_POLL_INTERVAL` | `2s` | must be positive |
| `TASKFORGE_SCHEDULER_BATCH_SIZE` | `50` | between 1 and 1000 |
| `TASKFORGE_SCHEDULER_RENOTIFY_AFTER` | `60s` | ≥ 3 × poll interval, and ≥ `TASKFORGE_OUTBOX_CLAIM_TIMEOUT` |
| `TASKFORGE_JOB_RETRY_BASE` | `1s` | must be positive |
| `TASKFORGE_JOB_RETRY_MAX` | `5m` | ≥ `TASKFORGE_JOB_RETRY_BASE` |
| `TASKFORGE_JOB_RETRY_MULTIPLIER` | `2.0` | finite, and ≥ 1 |
| `TASKFORGE_JOB_RETRY_JITTER` | `0.2` | finite, and between 0 and 1 |

The two re-notification rules exist so a repair cannot fire before an ordinary
delivery has had several chances, and cannot decide an event was lost while a
publisher is still in the middle of publishing it.

Every float setting is checked for finiteness separately from, and before, its
range. `strconv.ParseFloat` accepts `NaN`, `Inf`, `+Inf`, and `-Infinity`
without error, and a `NaN` compares false against every bound, so a range check
alone admits one. The same applies to `TASKFORGE_OUTBOX_BACKOFF_MULTIPLIER` and
`TASKFORGE_OUTBOX_BACKOFF_JITTER`. There are three defences: parsing falls back
to the documented default, `Config.Validate` and `RetryPolicy.Validate` both
reject a non-finite value by name, and `RetryPolicy.Delay` clamps a non-finite
sample from the injected jitter source. The last matters because converting a
`NaN` to a `time.Duration` is architecture-dependent — amd64 yields the most
negative `int64`, arm64 saturates to zero — so the same policy would otherwise
schedule differently on different machines.

## API surface added in M4

Public: `POST /v1/jobs` accepts `scheduled_at`; `GET /v1/jobs/{job_id}` returns
scheduling, eligibility, cancellation, and replay-link fields;
`POST /v1/jobs/{job_id}/cancel`; `POST /v1/jobs/{job_id}/retry`; `GET /v1/dlq`;
`POST /v1/dlq/{job_id}/replay`.

Internal: `POST /internal/v1/attempts/{attempt_id}/start` now returns a typed
timeout result instead of `204`; `POST /internal/v1/attempts/{attempt_id}/fail`
and `POST /internal/v1/attempts/{attempt_id}/cancel` are new; the heartbeat
response gains typed cancellation directives. There is deliberately no generic
"set status" endpoint and no worker-authoritative timeout endpoint.

New stable error codes: `attempt_timed_out`, `outcome_conflict`,
`job_not_cancelable`, `job_not_dead_lettered`, `invalid_cursor`,
`cancellation_requested`.
[api/openapi.yaml](../api/openapi.yaml) is version `0.4.0-m4` and documents only
implemented behavior, including a per-endpoint ambiguity contract that forbids a
fresh outcome identity after a `503`.

The three public mutating routes — `POST /v1/jobs/{job_id}/cancel`,
`POST /v1/jobs/{job_id}/retry`, and `POST /v1/dlq/{job_id}/replay` — classify a
deadline that elapsed inside the operation and answer a sanitized `503`
`service_unavailable` with endpoint-specific guidance, rather than a `500` that
tells an operator nothing about whether to try again. Only the error the
operation returned is classified; `ctx.Err()` is never consulted, so an
unrelated failure that merely finished after a deadline elapsed keeps its own
identity. Cancellation's guidance is to repeat the identical request for the
same job id, because scope plus job id is its whole identity. Retry and replay
share one identity namespace, so their guidance is to repeat the complete
identical request on the same path with the same `Idempotency-Key`; a fresh key
after an ambiguous response is forbidden, because it is a different replay
identity and silently creates a second replacement job. No message claims
nothing was committed — a deadline can land during COMMIT, and that is
genuinely ambiguous.

## Verification

The M4 tree passed these gates locally on 2026-09-01, against PostgreSQL 16 and
ElasticMQ started by `make up`:

- `make fmt` (no tracked Go file rewritten)
- `git diff --check origin/main...HEAD`
- `make lint`
- `make build` (six binaries)
- `make test-unit`
- `docker compose config --quiet`
- `make migrate` against a database created fresh by `make down && make up`
- `make test-integration`
- `make test-race` for both unit and integration packages
- `go test -v -count=1 -run '^TestOpenAPI_' ./internal/api/`

Exact commands and real output are recorded in the pull request.

New coverage beyond the M1/M2/M3 suites, all of which still pass unchanged:

**Migrations and upgrade.** Every M4 column, constraint, and index is checked
against the query or invariant that asked for it, with each index's *predicate*
asserted rather than just its existence — a full-table index would silently make
a bounded scan unbounded. Constraint tests pair every rejection with a positive
control differing only in the field under test, so a passing rejection is
attributable to the constraint rather than to some other column being wrong. The
revised timeline rule is pinned in both directions: a claimed-but-never-started
attempt may be `CANCELED`, while `SUCCEEDED`, `FAILED`, and `TIMED_OUT` still
require a start time.

Migrations `0009` and `0010` are pinned to the checksums of their first
published bytes, so an edit to a shipped file fails here rather than only on a
database that already applied it.

Every relationship migration `0011` adds is checked negatively as well as
positively: a dead-letter entry naming another job's attempt, or another
tenant's attempt; a job whose replay source lives in another scope; a replay row
whose original or replacement belongs to another scope; and lineage connecting
two unrelated jobs — a replacement that is nobody's, and one that is somebody
else's. Every rejection is paired with a positive control differing only in the
field under test, and the same invariants are checked once more against a replay
the production path actually wrote.

The upgrade rehearsal seeds a database at migration `0008` with what a running
M3 deployment actually holds — queued work, a running attempt under a renewed
lease, an abandoned attempt, an ADR-0009 dead-lettered job, and both a pending
and a published outbox event — records the `schema_migrations` rows a previous
release would have written, and then upgrades it with the real runner. It proves
the DLQ backfill creates exactly one correct entry linked to the last attempt,
that a mid-flight attempt gains no invented deadline, that outbox events resolve
from their payload hint or resolve to nothing rather than to something invented,
and that a rerun changes nothing. A separate test proves an idempotency
fingerprint recorded before this milestone still replays its original job.

A second upgrade rehearsal covers what one notification per job gets wrong. It
seeds jobs that were abandoned and requeued, with two and three historical
`work.available` events each and both pending and published states among them,
and proves that after the upgrade generations follow the real order the events
were created, the newest transition is the job's current generation, and
`last_notification_at` is when the job was last advertised rather than when it
was created. It then runs the real scheduler query against the upgraded
database: a job advertised thirty seconds ago is not re-notified and not
restamped; a stale pending event from a transition that is over no longer
suppresses the repair of the current one; a pending event **at** the current
generation still does; and a plainly stranded job is still repaired, which is
what makes the first result a real answer rather than an inert pass.

**Unit and contract.** Backoff grows exponentially, clamps at the maximum, and
stays bounded at attempt numbers where `math.Pow` returns `+Inf` — including the
corner where full jitter and the lowest factor turn that into `NaN`. Seeded
jitter is reproducible; two crypto-seeded sources are shown not to share a
schedule; the source is exercised concurrently under the race detector.
ADR-0009's zero-delay abandonment path is pinned against the most likely M4
regression. Bounded error detail is validated in Go with the same rules the
schema enforces.

Worker-runtime tests prove a handler's declared classification reaches the
control plane intact and an untyped failure does not: a credential-shaped string
planted in a plain error, a wrapped error, and a recovered panic appears nowhere
in what is reported. A handler cannot smuggle a server-owned classification
through the typed mechanism. Ambiguous reporting reuses one identity across
retries for failures and cancellation acknowledgments alike. The three ways an
attempt can stop without the job being canceled are kept apart: a timeout
reports nothing, authority loss reports nothing, and shutdown reports neither.
Client-side validation refuses a start result for another attempt or with no
deadline, an outcome naming another job or claiming a retry with no instant, and
a malformed cancellation directive.

OpenAPI parses with every implemented public and internal route, every stable
error code, a per-endpoint ambiguous-retry contract for all eight worker-control
operations and all three public mutating operations, the shared replay identity
namespace, the typed cancel-first Start refusal, and the two cancellation facts
a spec most easily loses.

Configuration and retry-policy tests cover every non-finite float: `NaN`, `Inf`,
`+Inf`, `-Inf`, and `Infinity` from the environment fall back to the documented
default; `Config.Validate` and `RetryPolicy.Validate` both reject one by name
rather than by range; and `Delay` stays inside `[0, Max]` for every sample an
injected jitter source can physically return, including `NaN`, both infinities,
and values outside the `[0, 1)` contract.

Worker-runner tests cover the cancel-first Start refusal with no directive
delivered at all — as the typed sentinel and as the `cancellation_requested`
code a DB-less worker actually receives over HTTP — and prove the acknowledgment
carries the full five-part fence, reuses one outcome identity across an
ambiguous retry, and unregisters the attempt only afterwards. Four unrelated
Start refusals are the control: none of them may produce an acknowledgment.

**PostgreSQL integration.** Retry enters `RETRY_WAIT` with both the chosen delay
and the instant it produced persisted, with no notification until promotion.
Permanent failure dead-letters without burning the remaining budget; exhaustion
dead-letters exactly once; the final attempt keeps its truthful status. Failure
and ADR-0009 abandonment are shown to land in the same DLQ table through the
same helper. A replayed failure returns the committed decision without moving a
stored field, consuming budget again, or creating a second entry; reusing an
outcome identity for another attempt, or replaying it with a changed body, is a
stable conflict.

Success, failure, and cancellation-acknowledgment replays are each proved to
return their stored result after the worker session was replaced **and** after
the lease expired, changing nothing durable. The same tests pin the boundary
from the other side: a changed classification, code, or message is
`outcome_conflict`; a fresh identity or any of the five fence parts being wrong
is refused; and from a replaced boot with nothing yet committed, `Start`,
`Succeed`, `Fail`, and acknowledgment are all `fence_rejected` with the attempt
left exactly as it was.

Start stamps a PostgreSQL-measured deadline once and an ambiguous retry returns
the original; renewal never moves it and is refused once it passes; a due
deadline becomes `TIMED_OUT` rather than `FAILED` or `ABANDONED`; the
expired-lease scan recognizes an already-due deadline instead of misreading it
as an abandonment; and an uncooperative handler cannot move a single stored
field afterwards.

Cancelling `PENDING`, `QUEUED`, and `RETRY_WAIT` is terminal with no attempt
created; cancelling `LEASED` and `RUNNING` produces `CANCEL_REQUESTED` and then
refuses success, failure, renewal, and start; cooperative acknowledgment
releases the lease; reconciliation finalizes it when nobody acknowledges;
cancellation takes precedence over a due timeout; and directives reach only the
session executing the attempt, keep arriving until it is finalized, and stop
once authority is gone.

A delayed job is durable, unclaimable by the claim predicate itself, and
unadvertised. Promotion writes exactly one fresh event transactionally for
delayed and retry-waiting jobs alike, four concurrent replicas promote each
transition exactly once, a fault before commit leaves neither promotion nor
event, and a rerun after an unobserved commit is a no-op. Bounded
re-notification is proven against a notification that was genuinely delivered
and lost: it fires once, is rate-limited again immediately, is batch-limited,
and is skipped while a current-generation event is still pending — and a stale
event from an earlier generation is shown not to suppress the fresh event a new
transition requires.

DLQ listing is scope-filtered and keyset-paginated, checked with every entry
sharing one timestamp so the id is the only tiebreak. Replay creates a distinct
eligible job and leaves the original byte-for-byte unchanged; concurrent
identical requests on separate connections create exactly one replacement and
leave no orphan; and `/retry` and `/replay` share one identity namespace through
the real HTTP surface.

Each of the three public mutating routes is driven into a genuine deadline —
its write parked behind an advisory-lock gate while the API's own request
timeout elapses — and must answer `503 service_unavailable` with its own
guidance rather than `500`. The control is the other direction: a live job that
is not replayable and a terminal job that is not cancelable keep their stable
`409`s, on the wire and in the store, and neither carries the deadline
sentinel. A committed-but-response-unknown test then follows each route's
printed guidance: repeating the identical cancellation returns the same decision
with the same instant and creates no attempt, and repeating a replay with the
same key through either route returns the replacement that already exists —
while a fresh key creates a second one, which is exactly why the guidance
forbids it.

**Contention.** Timeout versus success, cancel versus success, failure versus
renewal, cancellation versus renewal, cancellation versus start, promotion
versus cancellation, re-notification versus claim, and re-notification versus
reconciliation — each arranged deliberately with an advisory-lock gate on the
statement only one of the two operations executes, in both orderings where both
are meaningful. Two of those orderings are correctly *not* rejections: a failure
reported under a freshly renewed lease commits, and cancelling a job whose lease
was just renewed commits. Asserting only the rejections would have made the
suite agree with an implementation that refused valid work.

**End-to-end, against real PostgreSQL and real ElasticMQ.** Eight tests wire the
real API, the real outbox publisher, the real scheduler, the real reconciler, and
real DB-less workers, and assert durable state rather than status codes: retry
recovering through the scheduler and broker; a delayed job promoted and executed,
proven not to have started before its scheduled instant; a genuinely lost
notification repaired by bounded re-notification with no worker running at the
moment it was discarded; cancellation reaching a running handler on the heartbeat
with the broker drained first; a timeout recorded while an uncooperative handler
is still running, followed by that handler failing to commit anything when it
finally returns; and a permanent failure listed in the DLQ, replayed through the
public API, and completed as a new job.

The eighth is the cancel-first race, run through the whole chain. A transparent
proxy in front of the real API holds the worker's Start request — and its
heartbeats, so no directive can reach the worker that way — long enough for a
real operator cancellation to commit through the public API. It then asserts the
control plane answered `409 cancellation_requested`, that no heartbeat completed
in the window, and that the worker acknowledged: the attempt is `CANCELED` with
no start time, the handler never ran, and the lease is `RELEASED` rather than
left to lapse to `EXPIRED`. Nothing is faked; only the arrival time of one HTTP
request is controlled, because the window is otherwise too short to observe.

These tests consume from a broker queue of their own. Their workers legitimately
decline to acknowledge notifications for work another attempt already took, and
an unacknowledged message stays in flight for the queue's visibility timeout,
which `reset()` cannot drain — sharing a queue leaked those deliveries into
whichever test ran next. That is the same reason the process-crash test already
owned its queue.

**Binary smoke test.** `taskforge-scheduler` was run on its own from `./bin`, on
an isolated loopback port. `GET /healthz` returned `{"status":"alive"}` and
`GET /readyz` returned HTTP 200 with
`{"components":{"postgres":"ok"},"status":"ready"}`. It logged
`scheduler started` with the validated defaults (poll 2s, re-notify 60s, batch
50), then stopped cleanly on SIGTERM with exit status 0 and no error-level log
lines.

**Mutation evidence.** Four defects were temporarily reintroduced after a clean
checkpoint, and none was committed or pushed. Removing the deadline check from
`Succeed` failed the success-across-the-deadline test and the timeout-first
contention ordering. Downgrading the outcome-identity index from `UNIQUE` failed
the schema test; the behavioural test still passed, because the store also looks
the identity up under the same locks — the index is what makes that answer
authoritative under concurrency rather than a check-then-insert race, so the
schema assertion is the right guard for it. Removing the generation from the
pending-event check failed the stale-generation test. Removing the pre-multiply
clamp from the backoff policy failed nothing on this machine, which exposed a
real gap: the `NaN` corner had no test. One was added, and it is written as a
bounds assertion because an unguarded `NaN` converts to `-2^63` on amd64 and
saturates to 0 on arm64.

**No performance or recovery-time claim is made.** Every threshold in these tests
is deliberately short so a behavior is observable inside a test; they prove
correctness, not speed. The benchmark table in
[PROJECT_SPEC.md](PROJECT_SPEC.md) §7 remains unmeasured.

## Continuous integration

Unchanged from M3. The same three jobs —
`Format, lint, build, unit and OpenAPI tests`,
`Migrations and integration tests`, and `Race detector (unit and integration)` —
run on GitHub-hosted Linux runners for every pull request targeting `main` and
every push to `main`. See [`.github/workflows/ci.yml`](../.github/workflows/ci.yml).
The M4 pull request's run is recorded in that pull request.

The failure path — diagnostic capture and artifact upload — has still not been
exercised by a real hosted failure.

## Deliberately not implemented yet

- Result bodies and richer attempt-history APIs are M5.
  `GET /v1/jobs/{job_id}` returns lifecycle fields, not attempt history.
- Authentication and authorization are M5. Every request is still attributed to
  one configured development scope, and every service binds to loopback.
- The CLI, the Python SDK, and the operator dashboard are M5 and M6. The DLQ has
  a listing endpoint but no filtering beyond scope and no sorting beyond
  newest-first; operator search and bulk replay belong with the dashboard.
- Metrics and tracing are M6.
- Only `demo.echo` is registered as a production worker handler. Test-only
  handlers are injected through the existing registry seam and add no production
  surface.
- Recurring schedules, timezone handling, and misfire policy are post-V1. M4
  implements one-shot delayed submission, not cron.

## Known limitation: cooperative termination

Unchanged in principle from M3, and now load-bearing in three more places.

Go cannot forcibly terminate an arbitrary handler goroutine. An uncooperative
handler may keep running after its execution deadline, after cancellation, and
after its lease authority is gone, until it returns on its own or the process
exits. Hard cancellation needs isolated process or container execution, which is
post-V1.

What is guaranteed is durable, and it is guaranteed three independent ways: a
fenced transition is rejected once lease authority is gone, rejected once the
persisted deadline has passed, and rejected once cancellation has durably won.
The reconciler then records the truthful terminal outcome — `TIMED_OUT`,
`CANCELED`, or `ABANDONED` — and hands recoverable work to another worker. The
end-to-end suite demonstrates exactly this rather than describing it: a handler
that ignores its context is left running, its timeout is recorded while it runs,
and when it finally returns and tries to report success, not one stored field
moves.

## Local environment

- Go 1.25 or newer
- PostgreSQL 16 on `localhost:5442`
- ElasticMQ on `localhost:9324`
- Docker Compose and Make

Run `make bootstrap`, `make up`, `make migrate`, and `make build`, then start the
API, outbox publisher, scheduler, worker, and reconciler as shown in the
repository README.

## Next objective

M5 will add database-backed API keys with prefix lookup, scopes, and revocation;
result storage with a defined inline/external threshold; `taskforge-cli`; and a
typed, installable Python SDK — retiring the single development scope every
service currently runs under.
