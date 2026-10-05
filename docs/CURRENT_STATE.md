# Current State

This document is the source of truth for what is runnable now and what remains
planned. It records the implemented state through M8A. Each milestone's line
below says whether it is complete and names the pull request that holds its
review; none says anything about merge state, which a document cannot keep
current. M6 as a whole was already complete with M6D, its last slice; M6E is a
follow-up to it, not a fifth slice. M7 is split into M7A, the proof audit, and
M7B, the demonstration targets, and is complete with both. M8 is split into M8A,
the measured benchmarks, M8B and M8C; M8A is complete and the other two are
planned.

## Milestone status

- **M1 — durable ingress and transactional outbox:** complete.
- **M2 — worker sessions, atomic claims, and fenced execution:** complete.
- **M3 — heartbeat, lease renewal, and reconciliation:** complete.
- **M4 — retry, timeout, cancellation, DLQ, replay, delayed jobs:** complete.
- **M5A — database-backed API keys for the public surface:** complete.
- **M5B — worker/control authentication:** complete.
- **M5C — result storage:** complete.
- **M5D — CLI:** complete.
- **M5E — Python SDK:** complete.
- **M6A — operator read APIs:** complete.
- **M6B — OpenTelemetry tracing:** complete.
- **M6C — Prometheus metrics and the health residual:** complete.
- **M6D — operator dashboard:** complete. With it, **M6 is complete.**
- **M6E — browser-origin guard on the internal surface:** complete; see PR #17.
  A follow-up to M6 that closes the two exposures
  [ADR-0017](adr/0017-dashboard-toolchain-and-serving.md) recorded; M6's own
  objective was discharged by M6A–M6D and is not reopened.
- **M7A — invariant and scenario proof audit:** complete; see PR #18. Every
  reliability invariant and required scenario is mapped to a test that asserts
  durable state in [VERIFICATION_MATRIX.md](VERIFICATION_MATRIX.md); no
  production code changed.
- **M7B — `make demo` and `make demo-failure`:** complete; see PR #20. Two
  runnable demonstrations against the real binaries, and the two handlers they
  need, registered in the production worker
  ([ADR-0019](adr/0019-demo-handlers-are-trusted-built-ins.md)). With it, **M7 is
  complete.**
- **M8A — load generator and measured benchmarks:** complete; see PR #21. A
  harness that runs the real binaries, two recorded runs, and the first measured
  values for [PROJECT_SPEC.md](PROJECT_SPEC.md) §7: **two of its five targets are
  missed on the shipped defaults.** See "M8A" below. M8B (CI hardening) and M8C
  (deployment) are planned.

[ROADMAP.md](ROADMAP.md) records why M5 is split into five slices, M6 into four,
M7 into two, and M8 into three, and what each one owns, including why the CLI and the Python SDK — bundled under one M5D
name in the original roadmap — were split into M5D and M5E: independently
testable systems in two different language toolchains, the same reasoning
that split the original undivided M5 into M5A–M5D.

## Runnable system

Seven binaries build and run, plus one installable library, and two programs
that are run from source and are not among the seven:

- `scripts/bench` is the program behind `make bench` and `make bench-smoke`. It
  starts the same stack `scripts/demo` does, offers load over HTTP, kills workers
  on a seeded schedule, and measures by reading PostgreSQL. Like `scripts/demo` it
  is not built by `make build` and ships with nothing. See "M8A" below.

- `scripts/demo` is the program behind `make demo` and `make demo-failure`. It
  starts the real binaries from `./bin`, so it is not built by `make build`
  and ships with nothing. See "M7B — `make demo` and `make demo-failure`" below.

- `taskforge-sdk` (`sdk/python`, import `taskforge`) is a typed Python
  client over the same surface `taskforge-cli` covers. It is installed with
  `pip install ./sdk/python` from a clone and is deliberately not published
  to PyPI before M8 (ADR-0016). Like the CLI it contains no domain logic and
  no credential generation or verification of its own. Failures raise a
  `TaskForgeError` subclass carrying the matching `taskforge-cli` exit code.
  See "M5E — Python SDK" below.

- `taskforge-cli` is a command-line client for the public API and the
  loopback-only credential-management routes: job submission, read, result
  retrieval, cancellation, retry, DLQ listing and replay, and API-key /
  worker-key create, list, and revoke. It contains no domain logic and no
  credential generation or verification of its own (`internal/cli` only ever
  presents a key it was given, over HTTP, to routes already implemented by
  `taskforge-api`). A success response is written as JSON to stdout; a
  failure writes a JSON error object to stderr and prints nothing to stdout.
  The process exit code is a small, closed, individually tested set — see
  "M5D — CLI" below.

- `taskforge-api` accepts idempotent immediate and delayed job submissions, job
  reads, small and large result retrieval, cancellation, DLQ listing, replay
  and operator retry — all of which require an API key and take their scope
  from it — plus the loopback-only key management that mints those keys, plus
  the internal worker-control surface: session registration (which requires a
  worker key and takes its scope from it), heartbeat with cancellation
  delivery, atomic claims, fenced lease renewal, and the fenced start, success
  (optionally carrying a result), failure, and cancellation-acknowledgment
  transitions. Every worker-control route other than registration takes no
  credential on the request; it trusts the session identity registration
  already authenticated and checks that session's worker key has not since
  been revoked. The loopback-only worker-key management routes mint and revoke
  those credentials. `taskforge-api` never depends on the result object store
  being reachable at boot; only a request that needs an object-located result
  does. Every `/internal/v1` route also refuses a request that carries
  `Sec-Fetch-Site` or `Origin`, or is addressed to a non-loopback `Host`, with
  `403` `origin_refused` — see "M6E" below; that is not authentication. It also
  serves the read-only operator dashboard, compiled into the
  binary, same-origin under `/dashboard/` — see "M6D — operator dashboard"
  below. A binary built before `make dash-build` serves a page saying the
  dashboard has not been built, and logs `dashboard_built=false`.
- `taskforge-outbox` publishes durable work-availability events to ElasticMQ.
- `taskforge-scheduler` promotes due delayed and retry-waiting jobs and
  re-notifies stranded queued work. It holds no broker connection.
- `taskforge-worker` polls ElasticMQ only while it has capacity, heartbeats its
  process session, executes its trusted handlers (`demo.echo`, `demo.fail` and
  `demo.sleep`, pinned by a test; ADR-0019) through the control plane,
  classifies each handler's result and uploads a large one to the object
  store before reporting success, renews each running attempt's lease,
  reports classified failures, and acknowledges cancellation cooperatively.
  Its boot depends on the configured result object store and bucket being
  reachable, exactly as it already depends on the broker.
- `taskforge-reconciler` marks stale sessions unhealthy, records due attempt
  timeouts, finalizes cancellations no worker acknowledged, expires lapsed
  leases, abandons their attempts, releases the capacity they held, and requeues
  or dead-letters the job.
- `taskforge-migrate` applies numbered PostgreSQL migrations.

The schema consists of migrations `0001` through `0016`. M5A adds
`0014_api_keys.sql`, M5B adds `0015_worker_keys.sql`, and M5C adds
`0016_results.sql`, all described below. M4 adds five:

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
- `0012_per_job_notification_reconstruction.sql` — replaces `0011`'s
  database-wide reconstruction guard with a per-job one. `0011` asked whether
  **any** job's notification metadata deviated from the `0009` backfill and
  repaired nothing if one did. That is correct only for an upgrade that starts
  at `0008`; migrations `0009` and `0010` were published before `0011` existed,
  so a deployment can be running M4 code against a database at `0010`, and the
  moment that code promotes, requeues, or re-notifies one job, `0011` refuses to
  repair every other job — one advanced job silently cancels the whole repair.
  Eligibility is a property of a job, so `0012` asks per job: a job is repaired
  only if it still carries exactly the `0009` stamp (`notification_generation =
  1` and `last_notification_at = created_at`) and has at least one
  `work.available` event. Submission, promotion, crash-recovery requeue, and
  bounded re-notification each break one of those equalities, and every `UPDATE`
  is additionally guarded with `IS DISTINCT FROM`, so a row whose reconstructed
  value equals its current one is never written — which also makes the repair
  idempotent.
- `0013_restore_replay_notification_timestamps.sql` — corrects the one M4 write
  `0012`'s rule does **not** exclude. A DLQ replay stamps its replacement job's
  `created_at`, `updated_at`, `available_at`, and `last_notification_at` from one
  post-lock `clock_timestamp()` sample and sets generation 1, so the legacy
  fingerprint matches exactly; its `work.available` event, written in the same
  transaction, takes the column `DEFAULT now()` — the transaction *start* time,
  strictly earlier, and arbitrarily earlier when the replay waited on the queue
  row lock. `0012` therefore moved such a replacement's `last_notification_at`
  backward to that older instant, which matters because bounded re-notification
  measures staleness from it: a rewind past the re-notification interval makes
  the scheduler re-advertise a job that was just advertised. `0013` restores
  `last_notification_at` to the replacement's own `created_at`, which is a
  surviving copy of the instant the replay transaction assigned, and only for
  rows it can prove `0012` produced — see the eligibility rule below.

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
- The delay is quantized to whole milliseconds **once**, by the retry policy,
  before anything is decided from it. `retry_delay_ms` stores whole
  milliseconds, so a sub-millisecond delay used to be decided as a delayed retry
  from the unrounded duration and then persisted as `0`, making the first
  response say `RETRY_WAIT` and its own replay say `QUEUED`. The job transition
  now branches on the same integer that is stored. Rounding is upward, to at
  least `1ms`, because a configured positive backoff silently becoming an
  immediate retry is a different behavior under load rather than a rounding
  detail; a calculation that produces exactly zero stays zero, so ADR-0009's
  immediate requeue stays distinct.
- `TASKFORGE_JOB_RETRY_MAX` is a **strict upper bound on the stored value**, so
  it must be at least `1ms` and an exact whole-millisecond multiple. Both
  `RetryPolicy.Validate` and startup configuration validation enforce that, and
  a test pins them to the same answer so they cannot drift. A sub-millisecond
  maximum would leave no storable delay inside it and a non-whole one would be
  silently floored, so `delay <= maximum` would hold only after an unstated
  adjustment. `TASKFORGE_JOB_RETRY_BASE` carries no such rule — it is an input
  to the calculation, so any positive value is allowed and a sub-millisecond
  base rounds up to `1ms`.
- Saturation happens in **integer milliseconds, before any conversion**.
  `float64(math.MaxInt64)` rounds up to exactly `2^63`, one past what an `int64`
  holds, and that conversion is undefined in Go: arm64 saturated to
  `math.MaxInt64` while amd64 produced the most negative `int64`, which read as
  "no delay" and turned a maximal backoff into an immediate retry on one
  architecture and not the other. The ceiling is now the same number
  everywhere — `9223372036854ms`, or `2562047h47m16.854s`.
- Reusing the identity for a different attempt, or replaying it with a different
  classification, code, or message, is a stable `outcome_conflict`, never a
  leaked uniqueness error.
- The values returned to the caller come from the `UPDATE`'s `RETURNING` clause
  rather than from what Go computed, and the retry instant is derived from the
  stored delay, so a first response and its own replay cannot disagree by
  rounding.
- This is deliberately stronger than ADR-0008's renewal identity, which is
  released when a lease renews again. An outcome identity is the permanent record
  of one terminal decision, so nothing releases it. See
  [ADR-0010](adr/0010-durable-outcome-identity-and-terminal-precedence.md).
- A replay reports the decision that **committed**, never the job's current
  status. A retryable failure puts the job into `RETRY_WAIT`, and the job then
  moves on — promoted, claimed by a new attempt, and possibly `SUCCEEDED`
  minutes later — while the original attempt's outcome is unchanged and still
  replayable. The replayed job status is reconstructed from the attempt row
  alone, which is immutable once terminal: `retry_delay_ms` absent means
  `DEAD_LETTERED`, zero means `QUEUED` (ADR-0009's immediate requeue), and
  positive means `RETRY_WAIT`; a cancellation acknowledgment always produced
  `CANCELED`. Reading the live job row would report a value the `Outcome`
  contract does not even permit.
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
| `TASKFORGE_JOB_RETRY_MAX` | `5m` | ≥ `TASKFORGE_JOB_RETRY_BASE`, ≥ `1ms`, and an exact whole-millisecond multiple |
| `TASKFORGE_JOB_RETRY_MULTIPLIER` | `2.0` | finite, and ≥ 1 |
| `TASKFORGE_JOB_RETRY_JITTER` | `0.2` | finite, and between 0 and 1 |

The two re-notification rules exist so a repair cannot fire before an ordinary
delivery has had several chances, and cannot decide an event was lost while a
publisher is still in the middle of publishing it.

Every float setting is checked for finiteness separately from, and before, its
range. `strconv.ParseFloat` accepts `NaN`, `Inf`, `+Inf`, and `-Infinity`
without error, and a `NaN` compares false against every bound, so a range check
alone admits one. The same applies to `TASKFORGE_OUTBOX_BACKOFF_MULTIPLIER` and
`TASKFORGE_OUTBOX_BACKOFF_JITTER`.

**A documented default is used only when the variable is absent.** An
explicitly set value is carried into the configuration as written, so a
non-finite one reaches `Config.Validate`, is rejected by name, and the process
fails to start. Substituting the default instead would be a silent lie about
what is running: a deployment that set the retry multiplier to `NaN` through a
templating accident would come up quietly on `2.0` and behave in a way nothing
in its own configuration explains. A value that is present but not a number at
all is rejected the same way, because that is also what it is. A blank or
whitespace-only value counts as absent.

`RetryPolicy.Validate` applies the same rule to a policy built in code, and
`RetryPolicy.Delay` clamps a non-finite sample from the injected jitter source —
every non-finite sample to `0`, `+Inf` included, because a non-finite sample is
a broken source rather than a value that was too large. That last defence
matters because converting a `NaN` to a `time.Duration` is
architecture-dependent — amd64 yields the most negative `int64`, arm64 saturates
to zero — so the same policy would otherwise schedule differently on different
machines.

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
M4 published [api/openapi.yaml](../api/openapi.yaml) at version `0.4.0-m4`,
documenting only implemented behavior, including a per-endpoint ambiguity
contract that forbids a fresh outcome identity after a `503`. That contract is
unchanged; M5A moved the document to `0.5.0-m5a`.

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

## M5A — API-key authentication

### What changed

The public surface authenticates. `POST /v1/jobs`, `GET /v1/jobs/{job_id}`,
`POST /v1/jobs/{job_id}/cancel`, `POST /v1/jobs/{job_id}/retry`, `GET /v1/dlq`,
and `POST /v1/dlq/{job_id}/replay` each require
`Authorization: Bearer <key>` and resolve their scope from the authenticated
key. No scope-filtering logic changed anywhere: `jobs.Store` filters exactly as
it did in M1 through M4, and only where the scope value comes from is different.

`/healthz` and `/readyz` remain unauthenticated. They expose nothing
tenant-specific, and a liveness probe behind a credential would report a healthy
process as dead the moment that credential was revoked.

### The credential

A key is `tfk_<lookup>.<secret>`: a recognizable marker, 16 random bytes encoded
to a 22-character lookup segment, and 32 random bytes encoded to a 43-character
secret, both unpadded base64url. Only the lookup segment and a lowercase-hex
SHA-256 digest of the secret are stored, so the credential cannot be recovered
from anything TaskForge persists. It is returned once, by the creation endpoint,
and nowhere else.

The separator is `.` rather than `_` because base64url's alphabet contains `-`
and `_`; splitting on `_` would cut at whichever underscore a random lookup
segment happened to contain, so roughly a quarter of generated keys would fail
to parse. The round-trip test runs 500 keys for that reason — a single sample
passes three times in four.

A plain SHA-256 rather than a password KDF is justified by the generator: the
secret carries 256 bits from `crypto/rand`, so there is no guessable structure
for a work factor to protect. The `secret_hash` CHECK pins the stored shape to
`^[0-9a-f]{64}$`, so a future writer cannot put a raw key, a truncated digest,
or a different encoding in that column.

### One indistinguishable failure

A missing header, a malformed credential, an unknown prefix, a wrong secret, and
a revoked key all answer the same `401` with code `unauthorized` and a message
that names no cause. Verification runs before the revocation check so the two
cannot be separated by ordering, and uses `subtle.ConstantTimeCompare` so they
cannot be separated by timing. A distinguishing response would make a lookup
prefix an oracle for which prefixes exist, and would tell whoever holds a stolen
key that it has been revoked.

A deadline inside the credential lookup answers `503` `service_unavailable`,
never `401`: "I could not verify this" and "you are not authorized" are
different facts, and reporting the second for the first sends an operator
hunting for a revocation that never happened. Verification reads one row and
writes nothing, so that is the one `503` in this API whose guidance promises an
identical retry is unconditionally safe.

The `Authorization` header is length-bounded before the credential store is
consulted, so a caller cannot choose how much work a rejected request costs, and
authentication precedes body decoding, so the validation surface cannot be
probed anonymously. The `Bearer` scheme is matched case-insensitively per
RFC 7235.

### Fail-closed, with no test bypass

A `Server` built without `WithAuth` answers `401` on every public route and does
not register the key-management routes at all. A server that can authenticate
nobody has no authenticated caller to serve.

There is therefore no test-only authentication bypass anywhere in
`internal/api`: there is nothing to bypass, and a binary that forgets to wire a
credential store serves a closed API rather than an open one. The existing
validation-only unit tests present a real credential through a fake key store,
exactly as a client does.

### Key management

Three loopback-only routes: `POST /internal/v1/api-keys` mints a key and returns
it exactly once; `GET /internal/v1/api-keys` lists bounded metadata, newest
first, including revoked keys; `POST /internal/v1/api-keys/{key_id}/revoke`
revokes idempotently, reporting the original instant on a repeat rather than
moving it.

**These routes are themselves unauthenticated.** That is a real security
boundary, not an oversight: they are how the first credential comes into
existence, so requiring one would make the system unbootstrappable. Anyone who
can reach loopback can mint a credential for any scope — the same population
that can already drive the worker-control surface. It is why nothing under
`/internal/v1` may be exposed off loopback. See
[ADR-0013](adr/0013-database-backed-api-key-authentication.md) for the
alternatives considered.

Key creation deliberately carries no idempotency identity, against the grain of
every other mutating route here, and an `Idempotency-Key` sent to it is ignored
rather than honored. An idempotent create would have to return an existing
secret to a repeat. The cost is that its `503` is the one in this API that tells
a caller *not* to retry: with no request identity, a repeat mints a second
credential. The guidance is to list keys and revoke the one nobody received.

### Migration 0014

`api_keys` holds id, scope, operator name, a `UNIQUE` lookup prefix, the secret
digest, a creation instant from PostgreSQL server time, and a nullable
revocation instant, with a CHECK that a key cannot have been revoked before it
existed. One index, `api_keys_listing_idx`, matches the one query that justifies
it: the administrative listing's `created_at DESC, id DESC` keyset order, whose
id tiebreak makes a bounded page deterministic when two keys share a creation
instant.

`prefix` is unique across the whole table rather than only over live rows, so a
revoked key's prefix can never name a second credential and a log line naming it
stays unambiguous after revocation.

There is no backfill and nothing to reconstruct, because no earlier milestone
ever persisted a credential. This is the simplest migration in the repository,
deliberately and in contrast to `0011` through `0013`.

### Breaking change

**Every request that previously worked unauthenticated now answers `401`.**
There is no compatibility shim and none should be built. `TASKFORGE_DEV_SCOPE`
has been documented as a milestone-scoped, temporary mechanism since M1;
M5A narrowed its meaning to the internal worker-control surface, and **M5B
removes it entirely** — see below.

### Known limitation, closed by M5B: a key outside the worker-control scope
could not execute work

> **This limitation is closed.** It is kept here, struck through in spirit
> rather than deleted, because M5B's own section below states plainly what
> replaced it, and a reader who remembers this paragraph should find the
> answer next to the question. Worker claims used to filter on a single
> configured development scope; M5B replaced that with a worker key whose
> scope can be anything, so a job submitted under any authenticated API-key
> scope is now claimed by a worker registered under a matching worker key.
> See "M5B — worker-control authentication" below.

### API surface and contract

New stable error code: `unauthorized`.
[api/openapi.yaml](../api/openapi.yaml) was version `0.5.0-m5a`, now
`0.6.0-m5b`. It defines the `ApiKeyAuth` bearer scheme, declares it on all six
public operations, documents their `401`, and describes the three
key-management routes and their schemas.

Eight new `TestOpenAPI_*` contract tests landed with M5A. Every public
operation must declare exactly one `ApiKeyAuth` requirement and document a
`401` carrying code `unauthorized`; that `401` must name all four cases it
refuses to distinguish; the health probes must declare no security and can
never answer `401`; the documented scheme must be the one `bearerCredential`
actually accepts; only `ApiKeyCreated` may carry a credential field, and no
schema may expose a property named for a secret or a hash. M5B narrows the
"nothing under `/internal/v1` may declare security" assertion to name its one
now-authenticated exception — see below.

## M5B — worker-control authentication

### What changed

`PUT /internal/v1/worker-sessions/{worker_session_id}` — the one route that
registers a new worker process session — now requires
`Authorization: Bearer <worker key>` and resolves the session's scope from
that key, exactly as the public surface resolves a request's scope from an
API key. Every other worker-control route (heartbeat, claim, lease renewal,
and the fenced start/succeed/fail/cancel transitions) takes no credential on
the request itself: each resolves the calling session's scope from what
registration already recorded, and refuses the call if the worker key that
registered that session has since been revoked. See
[ADR-0014](adr/0014-worker-control-authentication.md) for the full design and
its rationale.

`TASKFORGE_DEV_SCOPE` is removed — from `internal/config`, from every binary,
and from `.env.example`. Nothing reads it anymore: the public surface has
taken its scope from an API key since M5A, and the internal surface now takes
its scope from a worker key. `taskforge-worker` gains a new required setting,
`TASKFORGE_WORKER_API_KEY`, presented on registration.

### The credential

A worker key has the identical `tfk_<lookup>.<secret>` format, storage shape,
and one-indistinguishable-`401` failure model API keys have had since M5A —
see that section above, which applies unchanged. It is a genuinely separate
credential, verified against its own table (`worker_keys`, migration 0015) by
its own package (`internal/workerauth`), sharing only the pure key-material
functions (`GenerateMaterial`, `ParseKey`, `HashSecret`, `VerifySecret`) that
know about neither table. A worker key can never authenticate where an API
key is checked, or the reverse.

### Two tiers: a credential once, a session forever after

Registration is the only worker-control call that verifies a secret. Every
later call from that session is authenticated by the session identity itself
(a worker id and a session id, neither forgeable, exactly as every
worker-control call has trusted since M2) plus a cheap, secret-free check —
`internal/workers.Store.SessionScope` reads the scope and worker-key id a
session was registered under, and `internal/workerauth.Store.IsRevoked`
checks whether that key is still live. Neither `internal/workers` nor
`internal/workerauth` depends on the other; `internal/api.Server.resolveWorkerControlScope`
is the only place their answers meet, and none of `internal/workers.Store`'s
eight original fenced methods changed signature or locking behavior to make
this possible.

**Revoking a worker key refuses the next call any session it registered
makes.** It does not force-expire a lease that session currently holds and
does not touch reconciliation: a cut-off session goes stale and its lease
expires through the exact same path a crashed worker's already does. A
request already in flight when revocation happens is not interrupted — the
identical in-flight boundary API-key revocation already has.

### Key management

`POST /internal/v1/worker-keys`, `GET /internal/v1/worker-keys`, and
`POST /internal/v1/worker-keys/{key_id}/revoke` mirror the M5A API-key admin
routes exactly: loopback-only, themselves unauthenticated (for the identical
bootstrapping reason — each surface is how its own credential type comes into
existence), non-idempotent creation with no `Idempotency-Key`, and idempotent
revocation reporting the original instant on a repeat.

### Migration 0015

`worker_keys` mirrors `api_keys` column for column: id, scope, operator name,
a `UNIQUE` lookup prefix, the secret digest, a creation instant from
PostgreSQL server time, and a nullable revocation instant. `worker_sessions`
gains a nullable `worker_key_id`, populated at registration and left `NULL`
— never fabricated — for any session registered before this migration or
through a path that never presented a worker key; such a session is simply
never treated as revoked, which is the same posture every session had before
this credential existed. `worker_keys` starts empty on every upgrade path,
for the identical reason `api_keys` did under M5A: no earlier milestone ever
persisted this credential.

### Breaking change

**`PUT /internal/v1/worker-sessions/{worker_session_id}` now requires a
worker key.** There is no compatibility shim. Every other worker-control
route's request shape is unchanged — the authentication they gained is
carried entirely by the session, not by a new required field or header.

### API surface and contract

[api/openapi.yaml](../api/openapi.yaml) is now `0.6.0-m5b`. It adds a
distinct `WorkerKeyAuth` security scheme — never `ApiKeyAuth` reused, because
the two guard different trust boundaries — declares it on the one route that
requires it, documents that route's `401`, and describes the three new
worker-key routes and their schemas. The Authentication section's "internal
surface" prose now names registration as the one deliberate exception to
"nothing under `/internal/v1` is authenticated," rather than stating the rule
without exception as it did through M5A.

`internal/api`'s `TestOpenAPI_*` contract tests gained coverage for the new
scheme and routes, plus an explicit carve-out in the test that walks the
internal surface asserting no security: it now skips exactly the one
authenticated operation, by name, rather than the assertion silently loosening.

## M5C — result storage

### What changed

A trusted handler's return value is no longer discarded. `demo.echo` always
produced one (`internal/worker/handler.go`'s `DemoEcho.Execute` has returned
an exact copy of the payload since M2), and every successful attempt since
then has thrown it away. `internal/worker/runner.go` now classifies it by
size and, before ever reporting success, either keeps it to send inline or
uploads it to an S3-compatible object store. `internal/workers.Store.Succeed`
records it in the `results` table in the same fenced transaction as the
`SUCCEEDED` transition, and `GET /v1/jobs/{job_id}/result` serves it back —
authenticated by the same `ApiKeyAuth` scope check every other public route
already uses, since M5A and M5B mean that check has somewhere real to land.
See [ADR-0015](adr/0015-result-storage.md) for the full design.

### Classification and the threshold

A result at or above `TASKFORGE_RESULT_INLINE_THRESHOLD_BYTES` (default
`65536`) is too large to store inline; everything smaller is
(`internal/results.Classify`, exercised at, just below, and just above the
boundary). The threshold lives on the shared `internal/config.Config`
struct and is validated to be positive and strictly less than
`TASKFORGE_MAX_REQUEST_BYTES` — an inline result travels inside the fenced
`/succeed` request body, which is itself subject to that same limit, so a
threshold at or above it would let a worker classify a result as "small
enough to inline" that its own reporting request could never actually
deliver.

### Upload before report

For a large result, `internal/worker/runner.go` uploads to the object store
*before* calling `Succeed` — never inside `Succeed`'s own PostgreSQL
transaction, which already holds the established
`queue → worker session → job → attempt → lease` authority lock order for
its duration. Holding those locks across a slow or unreachable network call
would block every other fenced operation on the same queue. The upload is
wrapped in the same bounded retry helper (`Runner.retry`) every other
worker-control call already uses; if it still fails, the worker reports
nothing at all and the attempt is abandoned to the ordinary crash-recovery
path (ADR-0009) — indistinguishable from any other reason a worker fails to
report in time, needing zero new control-plane failure-mode logic. The
object key is deterministic: `results/<scope>/<job_id>/<attempt_id>`.

### `Succeed`'s one new parameter, and why its replay branch stays safe

`internal/workers.Store.Succeed` gained one parameter, `result
*ResultRef` — the one fenced-method signature change this milestone makes.
Its replay branch (an exact replay of an already-committed success) returns
before `result` is ever looked at, so a replayed call cannot attempt a
second `results` insert; this is proven, not merely argued, by
`TestSucceedReplay_DoesNotInsertASecondResult`'s mutation evidence below.
This is not a breaking change: a worker that never sends `result` keeps
working exactly as it did before this milestone.

### The `results` table

Migration `0016_results.sql` adds `results`: at most one row per job,
`job_id` itself the primary key (not a surrogate, and not keyed by
attempt), `attempt_id NOT NULL` for operator traceability, a denormalized
`scope` tied to the real job's by a composite foreign key reusing migration
0011's `jobs_id_scope_key`, and a `results_location_shape` CHECK tying
`location` (`inline` or `object`) to exactly which of `inline_body` or
`object_bucket`/`object_key`/`checksum_sha256` is populated. Zero new
columns on `job_attempts`. There is no backfill, because no earlier
milestone ever persisted a result.

### Known limitation: an abandoned attempt's uploaded object is orphaned

The object key a worker uploads a large result to is attempt-scoped
(`results/<scope>/<job_id>/<attempt_id>`), not job-scoped — considered and
deliberately rejected in favor of keeping it attempt-scoped; see
[ADR-0015](adr/0015-result-storage.md)'s "Alternatives considered" for why a
job-scoped overwrite key was rejected. The accepted consequence: an attempt
whose object upload succeeds but whose process then dies or loses its
session before it ever calls `Succeed` leaves that uploaded object in the
object store permanently — nothing in PostgreSQL ever points at it, and a
replacement attempt (the ordinary ADR-0009 recovery path) uploads under its
own, different `attempt_id` if it also produces a large result, never
touching the first attempt's object.

This is a storage cost, not a correctness or invariant violation. Nothing
is ever observably `SUCCEEDED` with a broken or missing result reference,
because the `results` row is written in the same transaction as the
`SUCCEEDED` transition that makes the outcome observable at all — the
orphan is simply never referenced by anything, ever again. Garbage
collection of orphaned attempt-scoped objects is out of scope for this
milestone and is left for later; no specific future milestone is named for
it here.

### Breaking change

**None.** `POST /internal/v1/attempts/{attempt_id}/succeed`'s request body
gains `result` as a genuinely optional field; a worker that never sends it
is unaffected. This is the first M5 slice that ships without one.

### Local and CI infrastructure

MinIO was the original plan for the local S3-compatible object store,
matching what `docs/ARCHITECTURE.md` and `docs/ROADMAP.md` said before this
milestone. Its Docker Hub images are gone — `docker pull minio/minio` and
`docker pull minio/mc` both answer "repository does not exist", confirmed
against the real registry independent of any local network policy.
`compose.yaml` uses LocalStack's S3 provider (`SERVICES=s3`) instead;
`internal/objectstore` needed no code change, since it already speaks the
real AWS S3 API against whatever endpoint it is configured with. See
[ADR-0015](adr/0015-result-storage.md) for the full account.
`scripts/wait-for-infra.sh` gained a third `wait_for` block, probing
LocalStack's `/_localstack/health` endpoint from the host, the same way it
already probes ElasticMQ rather than trusting a Docker healthcheck. Both CI
jobs that start local infrastructure collect its logs alongside PostgreSQL's
and ElasticMQ's on failure.

### API surface and contract

`GET /v1/jobs/{job_id}/result` is new: `ApiKeyAuth`, registered
unconditionally like every other public route (not conditionally like the
worker-control surface), serving the exact JSON bytes a handler produced
regardless of where they are stored, with the same anti-oracle `404` shape
`GET /v1/jobs/{job_id}` already uses for a malformed id. `POST
.../attempts/{attempt_id}/succeed`'s request schema gains an optional
`result` field (a new `SucceedRequest` schema replaces its direct use of
`FenceRequest`, and a new `ResultPayload` schema mirrors the `results`
table's own columns). [api/openapi.yaml](../api/openapi.yaml) is now
`0.7.0-m5c`.

## M5D — CLI

### What changed

`taskforge-cli` (`cmd/taskforge-cli`, wiring only, AGENTS.md section 3) and
`internal/cli` (all logic) add a command-line client over every implemented
public route and the loopback-only credential-management routes. No backend
route, schema, or contract changed: this milestone is a pure new consumer of
`api/openapi.yaml` `0.7.0-m5c`, which is unchanged.

Commands: `jobs {submit,get,result,cancel,retry}`, `dlq {list,replay}`,
`api-keys {create,list,revoke}`, `worker-keys {create,list,revoke}`. Each
prints the server's JSON response bytes verbatim — to stdout on success, to
stderr on failure — rather than re-deriving or re-encoding them, the same
"proxy, don't re-derive" reasoning `GET /v1/jobs/{job_id}/result` already
applies (ADR-0015).

### The exit-code contract

ROADMAP.md's acceptance criterion, "CLI exit codes are stable," is treated
as a real contract with its own design section rather than an incidental
detail, because once a script depends on a code's meaning, repurposing it
is a breaking change to this CLI exactly as a response-shape change is to
the HTTP API.

Ten codes, `internal/cli/exitcode.go`, grouped by **remediation** — what a
caller's script should actually do differently — not by HTTP status or by
`api/openapi.yaml`'s `Error.code` enum one-for-one:

| Code | Name | Meaning |
| --- | --- | --- |
| 0 | `ExitSuccess` | The operation completed. |
| 1 | `ExitUsageError` | taskforge-cli itself rejected the invocation; no HTTP request was made. |
| 2 | `ExitRequestRejected` | The API rejected this specific request as malformed, invalid, or refused before it was looked at: `malformed_json`, `payload_too_large`, `validation_failed`, `invalid_cursor`, and (since M6E) `origin_refused`. A different request is needed. |
| 3 | `ExitUnauthorized` | `unauthorized` — the presented credential (or its absence) was refused. |
| 4 | `ExitNotFound` | `not_found`. |
| 5 | `ExitConflict` | `idempotency_conflict`, `job_not_cancelable`, `job_not_dead_lettered` — the operation cannot be applied given current state; retrying the identical request will not help. |
| 6 | `ExitInternalError` | `internal_error` — sanitized server-side failure. |
| 7 | `ExitServiceUnavailable` | `service_unavailable` — the request's own server-side deadline elapsed; endpoint-specific retry guidance is in the printed message. |
| 8 | `ExitTransportError` | The request never reached the API at all (DNS, connection refused, TLS, client-side timeout). |
| 9 | `ExitUnexpectedResponse` | A response this CLI does not recognize: an unexpected status, a non-JSON body, or an `Error.code` outside the reachable set below. Signals a version mismatch between this CLI and the server, and is deliberately never a fallback for a recognized failure that "wasn't handled." |

`apiErrorExitCodes` is exhaustive for exactly the `Error.code` values a route
this CLI calls can actually return — enumerated by hand against every
response documented for `POST`/`GET /v1/jobs`, `/v1/jobs/{job_id}`,
`/v1/jobs/{job_id}/result`, `/v1/jobs/{job_id}/cancel`,
`/v1/jobs/{job_id}/retry`, `GET /v1/dlq`, `POST /v1/dlq/{job_id}/replay`,
and the four `/internal/v1/{api-keys,worker-keys}*` routes — eleven values
in total (`malformed_json`, `payload_too_large`, `validation_failed`,
`invalid_cursor`, `unauthorized`, `not_found`, `idempotency_conflict`,
`job_not_cancelable`, `job_not_dead_lettered`, `internal_error`,
`service_unavailable`). The other eleven values in the API's 22-value
`Error.code` enum belong to the worker-control surface this CLI never
calls (`worker_session_conflict`, `worker_session_unavailable`,
`claim_conflict`, `fence_rejected`, `lease_expired`, `renewal_conflict`,
`attempt_timed_out`, `outcome_conflict`, `cancellation_requested`) or to
cases no CLI-invoked route documents (`unknown_queue`,
`method_not_allowed`).

**Bounded and mutually exclusive, checked mechanically, not only by
inspection.** `TestExitCodes_AreDistinct` asserts the named set has exactly
ten members and that no two share a numeric value; `TestExitCodes_OnlySuccessIsZero`
asserts `ExitSuccess == 0` and that no other named code, and no
`apiErrorExitCodes` value, is ever `0`. `TestApiErrorExitCodes_CoversExactlyTheReachableSet`
pins the eleven-entry map above so it cannot silently grow or shrink.

*Since M6E that map has twelve entries.* `origin_refused`, the `403` every
`/internal/v1` operation can now answer, is reachable from the key-administration
routes this CLI calls, so it joined `ExitRequestRejected`, and the Python SDK
maps it to `RequestRejectedError`. The counts in the paragraphs above and in
"M5E coverage" describe the state those milestones shipped in; the API's
`Error.code` enum now has 24 values, twelve of them unreachable from the CLI's
routes.

**Every code has its own test asserting that specific numeric value** — not
a shared "non-zero" assertion — driven through `Run()` against a real
`httptest.Server`, i.e. through the actual product surface rather than the
mapping table in isolation: `TestRun_ExitSuccess`,
`TestRun_ExitUsageError(_UnknownCommand/_MissingSubmitFlags)`,
`TestRun_ExitRequestRejected_{MalformedJSON,PayloadTooLarge,ValidationFailed,InvalidCursor}`,
`TestRun_ExitUnauthorized`, `TestRun_ExitNotFound`,
`TestRun_ExitConflict_{IdempotencyConflict,JobNotCancelable,JobNotDeadLettered}`,
`TestRun_ExitInternalError`, `TestRun_ExitServiceUnavailable`,
`TestRun_ExitTransportError`,
`TestRun_ExitUnexpectedResponse_{UnknownErrorCode,NonJSONBody}` — eighteen
tests for ten codes, because every multi-membership exit code (2, 5, 9) has
one test per member, proving the grouping is deliberate rather than
"whichever codes happened to be handled." `TestRun_FailurePrintsNothingToStdout`
additionally proves the stdout/stderr split holds across every failure
class in one place.

This was also proved against the real, running stack, not only against
fakes: minting a real API key, submitting a real job, then calling `jobs
get` with a wrong key (`unauthorized` → exit `3`), `jobs get` on a
nonexistent id (`not_found` → exit `4`), and `jobs retry` on a job that is
not dead-lettered (`job_not_dead_lettered` → exit `5`) each produced the
documented exit code against `taskforge-api` itself, not a stand-in.

### Credential handling

`taskforge-cli` obtains and presents credentials; it generates, parses, and
verifies none of them. `api-keys create`/`worker-keys create` call the
existing `POST /internal/v1/api-keys` / `POST /internal/v1/worker-keys`
routes and print the one-time secret the server returns — the CLI does not
store it anywhere itself. `--api-key` / `TASKFORGE_CLI_API_KEY` is sent as
`Authorization: Bearer` on the public (`/v1`) routes only: `Client.do`
(public routes) and `Client.doUnauthenticated` (the four key-management
routes) are two distinct code paths, because those routes are themselves
unauthenticated by design (ADR-0013, ADR-0014) and a client presenting an
unrelated credential to them — even one the server would silently ignore —
is still the wrong behavior. `TestCmdAPIKeysCreate_NeverSendsAuthorizationHeader`
and `TestClient_OmitsAuthorizationHeaderWhenAPIKeyEmpty` pin this.

### Which API address the CLI talks to

`internal/cli.ResolveBaseURL(flagValue, envValue)` resolves `--api-url`,
then `TASKFORGE_CLI_API_URL`, then the loopback default
`http://127.0.0.1:8080`. Whichever is chosen must be an absolute http(s) URL
with a host; anything else is a usage error (exit `1`) before any request is
made.

**`TASKFORGE_CLI_API_URL` is a purpose-built client-target variable, not a
reuse of `TASKFORGE_API_ADDR`.** An earlier revision of this milestone
reused `TASKFORGE_API_ADDR`; independent review found that a defect and it
was reversed. `TASKFORGE_API_ADDR` is `taskforge-api`'s own **bind**
address — read only by `cmd/taskforge-api` via `internal/config.go`, a bare
`host:port` with no scheme, validated by `isLoopbackBind`. A bind address
and a reachable client target are different shapes in general (a server can
bind a wildcard no client can dial, and a client needs a scheme), so one
name carrying both meanings would make each binary's reading of a shared
`.env` line depend on which binary is reading it. The repository's
established pattern for a client-facing address is `TASKFORGE_WORKER_API_URL`
(`internal/config.go`'s `LoadWorker`, default `http://127.0.0.1:8080`, "must
be an absolute http(s) URL"); `TASKFORGE_CLI_API_URL` follows that naming and
that validation shape, and `taskforge-cli` never reads `TASKFORGE_API_ADDR`
(`TestRun_IgnoresTheServersBindAddressVariable` sets it to a live server and
proves the CLI dials `TASKFORGE_CLI_API_URL` instead). Only the default
*value* (`127.0.0.1:8080`) is shared, pinned by
`TestResolveBaseURL_DefaultMatchesTaskforgeAPIsOwnDefault`.

**One deliberate difference from the worker's rule:** the CLI does not
additionally require a loopback host. The worker's `must use a loopback
host` rule is a permanent posture for a process that presents a worker key
and whose health endpoint is never exposed off-host. For the CLI, `--api-url`
exists so a later, non-local deployment milestone only has to change where
the CLI points, not how it talks to the API — a loopback-only validator
would defeat that. Loopback is the **default**, not a constraint, and it is
a safe default for a sourced reason: [PROJECT_SPEC.md](PROJECT_SPEC.md) §4
item 1 describes V1 as "Clone TaskForge and start it with Docker Compose
and Make," and §5's success criteria require "the full local stack starts
from a clean clone with only Git, Go, Docker, Docker Compose, and Make
installed" — V1 has no non-local deployment target — and the server side
enforces loopback itself (`TASKFORGE_API_ADDR`'s `isLoopbackBind` check, and
the loopback-only `/internal/v1` routes, ADR-0013). `internal/cli/config.go`'s
doc comment carries this same citation. Tests: `TestResolveBaseURL`,
`TestResolveBaseURL_RejectsAnythingButAnAbsoluteHTTPURL` (including the old
bind-address shape `127.0.0.1:8080`), `TestAPIURLEnv_IsNotTheServersBindAddressVariable`,
`TestRun_APIURLFromEnvWhenFlagAbsent`, `TestRun_IgnoresTheServersBindAddressVariable`,
and `TestRun_InvalidAPIURLIsAUsageErrorBeforeAnyRequest`.

### What is missing, and why it is not stubbed

`taskforge-cli` has no `jobs list`, `workers list`, or `queues list`
command. `GET /v1/jobs` (list), `GET /v1/workers`, and `GET /v1/queues` are
listed as V1-target routes in [PROJECT_SPEC.md](PROJECT_SPEC.md) §4, but
none is implemented anywhere in this API as of this milestone — confirmed
against `api/openapi.yaml`, which declares no such path. A CLI command with
no backend route to call would be exactly the fabricated functionality
[PROJECT_SPEC.md](PROJECT_SPEC.md) §5 forbids ("the dashboard, CLI, and SDK
read live data. Nothing is fabricated or hardcoded"). These commands land
whenever those routes do.

### Breaking change

**None.** This milestone adds a new binary and a new package; it changes no
existing route, schema, or file outside documentation and `.env.example`.

### API surface and contract

Unchanged. `api/openapi.yaml` stays `0.7.0-m5c`: this CLI is a consumer of
the existing contract, not a change to it.

## M5E — Python SDK

### What changed

`sdk/python` adds `taskforge-sdk` (distribution name) / `taskforge` (import
package): a typed, installable Python client over every implemented public
route and the loopback-only credential-management routes. It is the first
Python in this repository.

No backend route, schema, or contract changed. Like M5D, this milestone
adds a **consumer** of `api/openapi.yaml` `0.7.0-m5c`, not a change to it.

Surface: `jobs.submit/get/result/cancel/retry`,
`dlq.list/iter_entries/replay`, and `api_keys.create/list/revoke` plus
`worker_keys.create/list/revoke` — twelve methods over the eleven routes,
`dlq.iter_entries` being a client-side loop over `dlq.list` that follows the
documented `next_cursor`.

### The four infrastructure decisions this milestone owned

[ROADMAP.md](ROADMAP.md)'s M5E entry named four decisions as this
milestone's first work rather than a side effect of writing an SDK. All
four are decided and recorded in
[ADR-0016](adr/0016-python-sdk-toolchain-and-client-configuration.md):

| Decision | Choice | Rejected |
| --- | --- | --- |
| Dependency and build tooling | PEP 621 `pyproject.toml`, `hatchling`, stdlib `venv` + `pip`, `src/` layout, no lockfile, one runtime dependency (`httpx`) | `poetry` and `uv` — each a new *mandatory* installer against [PROJECT_SPEC.md](PROJECT_SPEC.md) §5's enumerated prerequisites, each a second source of truth beside `pyproject.toml`. `urllib.request` (zero-dependency) — no connection reuse, and its error surface would force hand-deriving the transport-vs-deadline distinction `httpx.TransportError` gives directly. `requests` — no inline types, no mock-transport seam. |
| Lint and format | `ruff format` authoritative, `ruff check` + `mypy --strict` clean, on their own `sdk-*` Make targets | `pyright` — a Node package, so npm would become a prerequisite for linting Python. Folding into `fmt`/`lint`/`test` — would make every Go contributor and the fast `checks` CI job depend on a provisioned virtualenv. |
| Packaging for V1 | Installable from this repository (`pip install ./sdk/python`). **Not planned for PyPI before M8** — a scope boundary, not a TODO | Publishing now — irreversible name claim, and a release workflow holding a publish credential would contradict `ci.yml`'s own "no job … publishes a package" least-privilege grant, against an API still short of three V1 routes. |
| CI placement | Its own `sdk` job, 3.11/3.13 matrix, SHA-pinned `setup-python` | Folding into `checks` (Go setup and Go caching; a Go failure and a Python failure would share one status line — the same reason `race` is already separate) or into `integration`/`race` (these tests need no PostgreSQL and no broker, so they must not wait on Compose). |

Python floor is 3.11 (`requires-python = ">=3.11"`), which is also what
makes `enum.StrEnum` and `typing.Self` available to the models.

### Which API address and credential the SDK uses

**`TASKFORGE_SDK_API_URL` and `TASKFORGE_SDK_API_KEY` — the SDK's own
variables.** Precedence is the constructor argument, then the environment
variable, then the loopback default `http://127.0.0.1:8080`, matching
`internal/cli.ResolveBaseURL`. A resolved value that is not an absolute
http(s) URL with a host raises `ConfigurationError` before any request.

It reads **neither** `TASKFORGE_API_ADDR` (the server's own *bind* address —
a bare `host:port`, validated as a loopback bind; the variable M5D
mistakenly reused and then reversed) **nor** `taskforge-cli`'s
`TASKFORGE_CLI_API_URL` / `TASKFORGE_CLI_API_KEY`.

The credential half of that second exclusion is the load-bearing part, and
it is the one place the SDK's reasoning genuinely differs from the CLI's
rather than merely echoing it. `taskforge-cli` is a process a developer
invokes deliberately. **An SDK is a library inside someone else's
process.** A developer who exported `TASKFORGE_CLI_API_KEY` for their shell
would otherwise have every Python process in that shell silently acquire
that credential — including a `TaskForgeClient()` whose author believed it
had none. Ambient credential pickup is defensible for a CLI and is not
defensible for a library. `test_config.py` sets all four foreign variables
to poison values and asserts the client still resolves to the loopback
default.

### The exception taxonomy

Nine classes, `sdk/python/src/taskforge/errors.py`, mapped one-to-one onto
the nine non-zero exit codes in `internal/cli/exitcode.go`. Exit `0` has no
exception: success is a normal return.

| Exception | `exit_code` | `Error.code` values |
| --- | --- | --- |
| `ConfigurationError` | 1 | none — raised before any request is made |
| `RequestRejectedError` | 2 | `malformed_json`, `payload_too_large`, `validation_failed`, `invalid_cursor` |
| `UnauthorizedError` | 3 | `unauthorized` |
| `NotFoundError` | 4 | `not_found` |
| `ConflictError` | 5 | `idempotency_conflict`, `job_not_cancelable`, `job_not_dead_lettered` |
| `InternalServerError` | 6 | `internal_error` |
| `ServiceUnavailableError` | 7 | `service_unavailable` |
| `TransportError` | 8 | none — the API was never reached |
| `UnexpectedResponseError` | 9 | anything outside the set above |

All inherit `TaskForgeError`, so `except TaskForgeError` catches everything
the SDK raises and nothing else. `APIError` subclasses carry `.code`,
`.message`, `.http_status`, `.request_id`, `.details` and `.raw`.

The grouping is by **remediation**, copied deliberately from
`exitcode.go` rather than re-derived, so one table reviews both clients.
`ConflictError` has no per-code subclasses for the same reason the CLI
groups those three codes: "retrying the identical request will not help" is
what they share, and a caller branches on `.code` for the rest.

`.exit_code` exists so a script wrapping the SDK can `sys.exit(exc.exit_code)`
and stay consistent with `taskforge-cli`. Python cannot import a Go
constant, so the two tables are hand-maintained copies of one mapping;
`test_errors.py::test_exit_codes_match_the_cli_contract` and
`::test_api_error_map_covers_exactly_the_reachable_set` are what stop them
drifting.

### The idempotency contract — M5E's acceptance criterion

ROADMAP.md's criterion is one sentence: "the SDK places an idempotency key
in the canonical header." Concretely, and tested:

- **Three methods take `idempotency_key`:** `jobs.submit`, `jobs.retry`,
  `dlq.replay` — exactly the three routes `api/openapi.yaml` marks
  `Idempotency-Key: required: true`.
- **Omitted → generated:** `str(uuid.uuid4())`, matching `taskforge-cli`'s
  `uuid.NewString()`. Asserted to be a version-4 UUID in canonical
  lowercase hyphenated form, within the header's documented 1..255 bound.
- **Supplied → verbatim**, including characters awkward in a URL.
- **Placed in the `Idempotency-Key` request header and nowhere else** —
  asserted absent from both the request body and the query string, which is
  [PROJECT_SPEC.md](PROJECT_SPEC.md) §4's sentence discharged literally.
- **No other method sends it**, asserted across `jobs.get`, `jobs.result`,
  `jobs.cancel`, `dlq.list` and all six key routes. Cancellation
  specifically has its own test: its identity is scope plus job id, so
  cancelling twice is one decision observed twice.
- **One generator, one setter.** `_jobs._key_or_new` is the only place a
  key is generated; `_transport.Transport._send` is the only place the
  header is set.

A caller-supplied key that could not survive being written into an HTTP
header — non-ASCII, a control character, or over 255 characters — raises
`ConfigurationError` before any request. This was found by the test suite
during implementation: without it, a non-ASCII key surfaced as a raw
`UnicodeEncodeError` from inside `httpx`, an exception that is not a
`TaskForgeError` and would escape a caller's `except TaskForgeError`.

**A stated limitation, documented where a caller will read it.** An
auto-generated key makes a submission *unique*, not *repeatable*: calling
`submit()` twice with identical arguments and no key creates two jobs. A
caller who needs a network-level retry to be safe must pass their own key
and reuse it. `taskforge-cli` has the identical property; the SDK says so
in the method docstring and in `sdk/python/README.md`.

### Credential handling

The SDK obtains and presents credentials and generates, parses, verifies,
and persists none of them — the model stays server-side in `internal/auth`
and `internal/workerauth` (ADR-0013, ADR-0014).

As in `internal/cli/client.go`, the authenticated and unauthenticated
request paths are **two distinct methods**: `Transport.request` presents
`Authorization: Bearer` on the public `/v1` routes;
`Transport.request_unauthenticated` never does, and is what all six
`/internal/v1` key-management routes go through. Those routes are
unauthenticated by design, and a client presenting a secret to a route that
does not check it is wrong even though the server would ignore it.
`test_auth.py` pins this, including the case that matters most — a
credential *is* configured and is still withheld from the internal routes,
in the same client that sends it to the public ones.

### Response models

Frozen dataclasses over the fields `api/openapi.yaml` marks required, each
carrying **`.raw`**, the untouched decoded response.

`.raw` exists because `taskforge-cli` gets a property for free that a typed
model would otherwise lose: it prints the server's bytes, so a field a
newer server adds is never dropped. Parsing here is therefore tolerant of
unknown keys and strict about missing required ones — a missing required
field, a wrongly typed field, or an unrecognized enum value raises
`UnexpectedResponseError` (exit `9`) rather than being guessed at.

`JobStatus`, `CancellationStatus` and `DLQReason` are `StrEnum`s, applying
[AGENTS.md](../AGENTS.md) §5's "domain types are typed, not `string`" rule
on the client side. The consequence is stated rather than hidden: **a server
that adds a tenth job status breaks an older SDK's parse of a job in that
status.** That is the same trade exit `9` already makes, and it is preferred
over handing a caller a state their code has never heard of.

`jobs.result()` deliberately returns the decoded JSON with no model:
`api/openapi.yaml` documents that body as "whatever JSON value the job's
`job_type` produces". A handler that produced a literal `null` is
distinguished from an empty body, which is not valid JSON and raises.

`WorkerKeySummary` / `WorkerKeyCreated` / `WorkerKeyRevoked` /
`WorkerKeyList` are distinct subclasses rather than aliases of their
`ApiKey*` counterparts, and a worker-key listing parses its entries into
`WorkerKeySummary` at runtime, not merely in the annotation. The wire
shapes are identical, but they are distinct credentials authenticating
distinct surfaces (ADR-0014), and an alias would let a caller pass one where
the other is meant and have it type-check.

### What is missing, and why it is not stubbed

The SDK has **no `jobs.list()`, `workers.list()`, or `queues.list()`**.
`GET /v1/jobs` (list), `GET /v1/workers`, and `GET /v1/queues` are V1-target
routes in [PROJECT_SPEC.md](PROJECT_SPEC.md) §4 and remain unimplemented
anywhere in this API — re-confirmed against `api/openapi.yaml` this
milestone, which still declares no such path and is still `0.7.0-m5c`. This
is unchanged since M5D. A method with no route to call would be exactly the
fabricated functionality [PROJECT_SPEC.md](PROJECT_SPEC.md) §5 forbids.
`test_jobs.py::test_no_list_workers_or_queues_methods_exist` asserts their
absence, so one cannot be added casually without a reviewer seeing it.

There is no async client. It is not in any milestone and was not added
speculatively.

### Breaking change

**None.** This milestone adds a new directory, four Make targets, and a CI
job. It changes no existing route, schema, Go file, or behavior. The only
edits outside `sdk/`, `Makefile` and `.github/` are documentation and
`.env.example`.

### API surface and contract

Unchanged. `api/openapi.yaml` stays `0.7.0-m5c`.

## M6A — operator read APIs

### What changed

Four authenticated public reads, the indexes that justify them, and the CLI and
SDK operations that consume them:

| Route | Shape |
| --- | --- |
| `GET /v1/jobs` | keyset page, newest first, filters `status` and `queue` |
| `GET /v1/jobs/{job_id}/attempts` | the full attempt timeline, oldest first, unpaginated |
| `GET /v1/workers` | keyset page by name, each worker joined to its latest session |
| `GET /v1/queues` | every queue with this scope's non-terminal depth per status |

This closes a gap the roadmap recorded twice. M5D and M5E each deferred the
CLI and SDK list operations with the same sentence — "these commands land
whenever those routes do" — because a command with no backend route would be
exactly the fabricated functionality [PROJECT_SPEC.md](PROJECT_SPEC.md) §5
forbids. The routes exist now, so the commands and methods do.

### Three things these responses deliberately do not carry

**No payloads.** A list endpoint that returned payloads would let one request
pull an unbounded amount of user data — the rule `DLQEntry` already stated. The
payload is not merely dropped after the scan; it is never selected out of
PostgreSQL for the listing at all, so a page of large payloads costs nothing
between the API and the database either. `GET /v1/jobs/{job_id}` still returns
it, one job at a time.

**No session, lease, or outcome identifiers.** Every worker-control route
except registration is unauthenticated and trusts the session identity its
request already carries (ADR-0014). Publishing a `worker_session_id` on an
authenticated public read would hand a caller an identifier that surface
treats as authority. `worker_id` is published: it names a logical worker,
matches what `GET /v1/workers` reports, and authorizes nothing.

**No queue-wide in-flight figure.** `queues.max_concurrency` is shared by every
scope while `depth` counts only the caller's own jobs, so the two are not
comparable and no ratio or combined total of them is reported — that would
disclose another scope's load.

### `GET /v1/workers` reports the latest session, not the current one

This is the design decision most easily gotten wrong, so it is stated plainly.
`worker_sessions_one_current_per_worker_idx` (migration 0002) covers only
`status IN ('STARTING','HEALTHY','DRAINING')`. Reconciliation moves a stale
session to `UNHEALTHY` (`internal/workers/reconcile.go`) and a replacement
registration moves the prior one to `OFFLINE` (`internal/workers/store.go`).
A listing built on that index would therefore omit precisely the workers an
operator opened the page to find: the one that just crashed and the one that
was replaced.

The implemented query instead takes the latest session per worker with a
`LATERAL` ordered by `(registered_at DESC, id DESC)` and **no status
predicate**, served by the new `worker_sessions_latest_per_worker_idx`. The
inner join cannot drop a worker, because registration writes the `workers` row
and its first session in one transaction.

`heartbeat_age_seconds` is computed by PostgreSQL and floored at zero. The
endpoint reports it and deliberately does not judge it: the staleness threshold
that decides whether a worker is dead lives in the reconciler's configuration,
and an API carrying its own copy would eventually disagree with the component
that acts on it.

### Pagination, and what it does not promise

Keyset on `(created_at DESC, id DESC)` for jobs and on `name` for workers, with
`LIMIT n+1` so the presence of a next page is a fact rather than a guess. The
cursor is opaque — a position, not an authorization — and a cursor minted by
one scope's walk presented by another returns only the second caller's own
rows.

One property is deliberately **not** promised, and is tested rather than merely
documented. `jobs.created_at` defaults to `now()`, which in PostgreSQL is
transaction-start time, so a submission that began before a page was read and
committed after it carries a timestamp already behind the cursor. That job is
not returned by the walk in progress and is returned by a fresh listing. What
keyset ordering guarantees is that no job present throughout a walk is
duplicated or skipped; it does not guarantee that a walk observes a job that
became visible during it.
`TestReadAPI_ConcurrentInsertsNeverDuplicateOrReorder` asserts both halves.

### Migration 0017

Index-only: no table, no column, no row changed.

- `jobs_scope_keyset_idx (scope, created_at DESC, id DESC)` — the unfiltered
  and queue-filtered listing.
- `jobs_scope_status_keyset_idx (scope, status, created_at DESC, id DESC)` —
  the status-filtered listing.
- `jobs_scope_queue_depth_idx (scope, queue, status)` partial on the six
  non-terminal statuses — queue depth.
- `worker_sessions_latest_per_worker_idx (worker_id, registered_at DESC, id DESC)`
  — the `LATERAL` above.
- `DROP INDEX jobs_scope_created_at_idx` — superseded.

**Why two job indexes rather than one.** PostgreSQL 16 has no index skip scan,
so `(scope, status, created_at DESC, id DESC)` cannot serve a query that does
not constrain `status`: with `status` between the equality column and the
ordering columns, the planner must scan every status and sort. Measured rather
than assumed — see the `EXPLAIN` evidence under "M6A coverage".

**Why the dropped index is safe.** `jobs_scope_created_at_idx
(scope, created_at DESC)` is a strict prefix of its replacement and was
referenced nowhere but its own creating migration;
`GET /v1/jobs/{job_id}` filters `id = $1 AND scope = $2` and is served by the
primary key. Keeping it would have left an index no query justifies
([AGENTS.md](../AGENTS.md) §6).

The attempts read needed **no** new index: `job_attempts` already carries
`UNIQUE (job_id, attempt_number)` from migration 0002, which serves the
timeline query in its natural order. That is recorded in the migration
alongside the indexes it does create, because "this query needs no index"
deserves the same evidence as the converse.

`jobs_scope_queue_depth_idx`'s partial predicate is fixed SQL text while
`jobs.NonTerminalStatuses()` is derived at runtime from `Status.Terminal()`.
`TestMigrations_M6ADepthPredicateMatchesTheDerivedStatusSet` asserts the two
name exactly the same statuses in both directions, so a tenth job status cannot
leave the predicate stale and silently turn the depth query into a full scan.

### Breaking change

None. Four routes were added, one index was replaced by a superset, and no
existing response shape, status code, or error code changed. `GET /v1/jobs` now
accepts `GET` as well as `POST`, so a request with neither verb answers `405`
with `Allow: GET, POST` rather than `Allow: POST`.

### API surface and contract

`api/openapi.yaml` is version `0.8.0-m6a` and documents all four operations
under `ApiKeyAuth` with the standard 401 and 503 responses. **No new error code
was added**: the four routes answer only `validation_failed`, `invalid_cursor`,
`unauthorized`, `not_found`, `internal_error` and `service_unavailable`, every
one of which was already reachable, so `internal/cli`'s `apiErrorExitCodes`
table is unchanged.

A cursor this API did not issue answers **422 `invalid_cursor`**, not 400 —
`writeFieldError` returns `StatusUnprocessableEntity`, the CLI maps
`invalid_cursor` to `ExitRequestRejected` (2), and the SDK raises
`RequestRejectedError`.

## M6B — OpenTelemetry tracing

### What changed

One trace id now follows a submission across every hop
[ROADMAP.md](ROADMAP.md)'s M6B names: the API request, the PostgreSQL
submission transaction, the persisted outbox row, the published broker
envelope, the worker's claim, and handler execution.

Tracing is **disabled by default**. `TASKFORGE_OTEL_EXPORTER` is `none`,
`stdout`, or `otlp`; with `none` no exporter is constructed, no goroutine
starts, and no endpoint is dialed. An unrecognized value is rejected at startup
rather than silently downgraded — a typo that quietly disabled tracing could
only be found by noticing an absence.

### The process boundary, which is the whole difficulty

`taskforge-outbox` publishes an event potentially long after the API process
that wrote it has exited (ADR-0004's publish-before-mark window). There is no
later moment at which it could ask the submitter for its trace context, so
migration 0018 commits that context to `outbox_events` in the same transaction
as the event itself — the same rule every other durable fact in this system
follows.

`traceparent` carries a `CHECK` pinning the exact W3C shape; `tracestate` is
length-bounded because it originates in a caller-supplied header. Neither is
backfilled and neither is indexed: a trace id is not derivable from anything
historical, a `NULL` is a complete answer meaning "start a new root", and
nothing queries by either column.

### One rule at every boundary

A value this system cannot parse is **dropped, never propagated and never
persisted**. It holds for an inbound HTTP `traceparent`, for the persisted
column, and for the envelope member. The next span becomes a fresh root
instead.

The propagator drops a malformed header before anything reaches PostgreSQL, so
the database `CHECK` never fires on that path. Both halves are asserted:
`TestTracing_MalformedInboundTraceparentIsNeverPersisted` proves the row is
written with a fresh root rather than the junk, **and** that the constraint
would in fact refuse the junk if anything wrote it directly.

### Span names are bounded; span attributes are not the same question

A server span is named for its matched route pattern (`GET /v1/jobs/{job_id}`),
never the raw path. Span *attributes* do carry job and attempt ids, and that is
correct: [ARCHITECTURE.md](ARCHITECTURE.md) §14's cardinality rule is about
**metric labels** specifically, and a span is exactly where a unique identifier
belongs. Conflating the two would have been a real mistake in the other
direction — dropping the one identifier that makes a trace useful.

No span carries SQL text, bind parameters, a job payload, a credential, or
handler error text. That last one is deliberate: handler error text is where
payload fragments, credentials and stack traces reliably appear, which is why
`FailureError` already refuses to store it, and a span is the same disclosure
with a different transport.

### Two decisions recorded in code rather than as an ADR

**The envelope is additive-only; the `data` member is versioned.** M6B's
optional `trace` member therefore did not bump `WorkAvailableSchemaVersion`. An
older consumer ignores an unknown JSON field, and a newer consumer reading an
older envelope sees the member absent — which is the same thing it sees when
the member is legitimately empty. That distinction was not written down
anywhere before this milestone; it is now stated in `internal/outbox/event.go`,
because a wire contract between two processes that can differ during a rolling
restart needs it stated rather than assumed.

**The tracing middleware is in two parts, and had to be.** `net/http` populates
`Request.Pattern` inside `ServeMux.ServeHTTP`, on the exact `*Request` pointer
the mux is handed — verified empirically, not assumed. So the pattern does not
exist when an outer middleware runs, and it is written to a pointer no outer
middleware holds, because `withTimeout` hands the mux its own copy. `withTracing`
therefore starts the span outermost, where the whole request is inside it and
where `withLogging` can read its trace id, and `withSpanRoute` renames it from
innermost once the mux has matched.

`withSpanRoute` renames in a `defer`, not sequentially. A handler that panics
unwinds straight past the call, and `withRecovery` sits outside it — so a
sequential rename would be skipped for exactly the requests an operator most
wants to find in a trace.

That defect surfaced through
`TestTracing_SpanIsNamedForTheRoutePatternNotTheRawPath`, but only
**incidentally**: that test panics because its harness passes a nil job store,
which is a property of the harness rather than anything the test states. Review
of this milestone correctly flagged that the coverage was therefore accidental
and would vanish silently the moment the harness gained a real store.
`TestTracing_SpanSurvivesAPanickingHandler` now covers it deliberately — and
asserts the recovery log line first, so the test fails loudly if it ever stops
exercising the panic path rather than passing while proving nothing.

### Execution nests under the claim

Both are children of the submitting trace when the envelope carried one. When
it did not, an earlier draft produced a claim root and a *separate* execution
root — two traces for one delivery. Making execution a child of the claim fixes
that, matches [ARCHITECTURE.md](ARCHITECTURE.md) §14's stated chain literally
("… → claim → worker execution"), and is semantically right besides: the lease
the claim acquired is held throughout execution, so execution really does
happen inside it.

### Breaking change

None. `outbox_events` gained two nullable columns; `InsertWorkAvailableTx`
gained a parameter, which is internal; the envelope gained an optional member
that older consumers ignore. No response shape, status code, or error code
changed, and no behavior changes at all with tracing left at its default.

One internal consequence worth naming: `InsertWorkAvailableTx` now writes
columns that exist only from 0018 onward, so a test that builds a database at
an older migration and then calls current code must apply 0018 as well.
`TestMigrations_RestoreReplayNotificationTimestampsRewoundBy0012` already did
this for 0014 and 0015 for the identical reason and now does it for 0018.

## M6C — Prometheus metrics and the health residual

### What changed

All five services expose `GET /metrics`. The metric set is the one
[ARCHITECTURE.md](ARCHITECTURE.md) §14 names, minus two families deliberately
left out and said so (below). The health residual M6's split identified is
closed: the worker's readiness now checks the object store, and all four
background services have tests where they previously had none.

### Cardinality is enforced, not asserted

The acceptance criterion is "no unbounded metric labels", and the point of this
milestone is that a reviewer can *check* that by running a test rather than by
reading every call site.

Three independent mechanisms, because one is not enough:

- **A manifest built by construction.** Every collector goes through one
  constructor that records its declared label names. Checking the manifest
  rather than walking `Gather()` matters: `Gather()` reports only families that
  have already emitted a sample, so a brand-new metric with a bad label — the
  exact case to catch — would be invisible to it.
- **An allowlist that demands a justification.** `AllowedLabels` maps each name
  to the one-line bound that makes it safe. A label whose bound cannot be stated
  in a line does not belong on a metric.
- **An independent denylist.** `ForbiddenLabels` is checked separately, so an
  allowlist edited carelessly still cannot readmit a forbidden name. A test also
  asserts the two lists never overlap, since a name in both would make the other
  two tests disagree and let running order decide.

The constructor applies the same rule at startup and panics, so a violation is a
process that will not start rather than something only a test would find.

### `job_type`, and why the bound is the worker's registry

`jobs.job_type` has no foreign key and no allowlist — only the regex
`^[a-z0-9][a-z0-9._-]{0,127}$`, enforced identically in the schema and at
submission validation. Any authenticated caller mints a new value per request.
That is cardinality explosion from ordinary use, not abuse.

It is bounded against `worker.Registry.Types()` — the emitting worker's own
handler set, which is already authoritative, already bounded, and already what
decides whether that process can run a job type at all. An earlier M6 draft
proposed a `TASKFORGE_METRICS_JOB_TYPES` environment variable; that would have
been a second copy of the registry kept in sync by hand across a process
boundary.

This is also why the label appears **only on worker-emitted metrics**. The API
has no registry, so it cannot bound the value, and its metrics therefore carry
no `job_type` at all rather than a poorly bounded one. A test pins which metrics
may carry it, so a future one cannot acquire it by accident.

Since M7B, `Registry.Types()` holds three entries (`demo.echo`, `demo.fail` and
`demo.sleep`), so the ceiling is 4. A test drives 10,000 distinct caller-supplied values through the bound and
asserts the series count stays there.

### Counters read from the database, not incremented in code

The job-lifecycle totals and every gauge are derived from PostgreSQL at scrape
time. Two reasons, both load-bearing.

They are facts about the **whole system**, not one replica. Every component here
is safe to run with N replicas, so an in-process counter would report one
replica's share — and summing across replicas would still be wrong, because a
job submitted by replica A and completed by replica B was counted by neither for
the transition it did not observe.

And it keeps instrumentation **out of fenced-transition code**. Incrementing at
the moment of a terminal transition would mean editing the paths that own
fencing and the transition matrix to add a side effect with nothing to do with
correctness.

Each is genuinely monotonic, which a Prometheus counter requires: jobs,
attempts and dlq_entries are never deleted (every foreign key to them is
`ON DELETE RESTRICT`), and a terminal job never returns to a non-terminal state
(invariant 2). A counter that could decrease would be silently wrong in every
`rate()` over it.

The cost is that a scrape runs real queries. Each is bounded by the same
two-second budget the readiness probes use, and a failure degrades that metric's
absence rather than the endpoint — `promhttp` is configured `ContinueOnError`,
so a database outage still serves the metrics an operator needs to diagnose it.
That is asserted against a deliberately-closed pool.

### Two families §14 named that this milestone did NOT build

`queue_wait_duration_seconds` and `end_to_end_duration_seconds` are absent, and
§14 has been corrected to say so rather than continuing to name them.

Both are histograms. Unlike the counters above they cannot be derived at scrape
time, because a histogram needs observations as they happen rather than a count
of current rows. Recording them means one of two things, and both are larger
than this milestone: observing inside the claim and terminal-outcome
transactions, which is fenced-transition code; or widening the claim and outcome
wire contracts to carry `available_at` and `created_at` outward so the boundary
can observe them. Each is a real change with its own review surface, and neither
belonged in a milestone whose subject is cardinality.

`execution_duration_seconds` — the third histogram §14 names — **is** built,
because the worker measures the handler itself and needs nothing from anyone.

### A defect this milestone's own tests caught

The HTTP metrics were first recorded in `withSpanRoute`, alongside the span
rename, since both need the route pattern at the same single point. That
reported **`code="200"` for a request that actually returned `500`**.

`withRecovery` sits *outside* `withSpanRoute`, so a panicking handler unwinds
past the inner recorder and the sanitized 500 is written to a writer that
recorder never sees. This is worse than a missing metric: an operator watching
error rates would have seen none at all.

The fix splits the measurement across the two points where each half is actually
knowable. `withSpanRoute` — still the only place that can know the route —
publishes it outward through a pointer in the request context, and
`withHTTPMetrics`, placed outside `withRecovery`, observes the real final
status. A mutation moving the recording back inside `withRecovery` fails
`TestMetrics_PanickingHandlerIsStillCounted`.

### The health residual

**`objectstore.Ping`** uses `HeadBucket`, not `CreateBucket`. Readiness must be
a read; `EnsureBucket` exists to create the bucket once at startup, and a probe
that created infrastructure as a side effect would make "is this process ready"
a mutating question.

**The worker now checks it.** M5C made this process depend on the object store
— a large result is uploaded before the attempt reports success — but readiness
never checked it, so a worker that could not reach the bucket reported itself
ready and then failed every large-result job.

**The four background health servers are testable, which required a refactor.**
Each took its dependencies concretely (a `*pgxpool.Pool` it pinged inline), so
exercising a failing dependency meant having real infrastructure that was
genuinely down. That is why none of the four had a single test file. They now
take named `func(context.Context) error` checks — the shape
`internal/api.ReadinessCheck` already established — so every branch is reachable
from `make test-unit` with nothing running. This is a deliberate design change,
not an incidental one.

Each service now asserts: liveness is unconditionally 200 with every dependency
failing; readiness is 200 when all pass; readiness is 503 naming the right
component when each dependency is failed **one at a time**, with the others
still reporting `ok`; and `/metrics` parses as valid exposition format through a
real parser rather than being string-matched.

### `/metrics` placement: resolved, not deferred

An earlier M6 draft treated this as an open decision needing a loopback-only
admin listener for `taskforge-api`. It is not open. `TASKFORGE_API_ADDR` is
**already** rejected unless it binds loopback (`internal/config.Validate`),
identically to the four background addresses, so there is no live
trust-boundary difference between the API's mux and a background service's
health mux — and `/healthz` and `/readyz` are already unauthenticated on that
same mux.

So `/metrics` sits on the existing listener, unauthenticated, with no new
address and no new env var. §14 records the one thing that must be revisited
before this API ever sits behind a real load balancer, which is post-V1 work.

### Breaking change

None. No schema change and no migration — M6C is the first milestone since M5B
to add neither. The four background `newHealthServer` signatures changed, but
they are `package main` in their own binaries and have no callers outside them.
Every component's `WithMetrics` is optional: a nil one records nothing and
changes no behavior, which is what leaves every pre-M6C test unchanged.

## M6D — operator dashboard

### What changed

Six read-only views — Overview, Jobs, Job detail with its attempt timeline,
Workers, Queues, and DLQ — reading only the public `/v1` routes M6A shipped,
with the operator's own API key. The frontend is React + TypeScript built by
Vite in `dashboard/`; its whole Node toolchain is `dashboard/Dockerfile`, pinned
by version and index digest. `internal/dashboard` embeds the build and
`internal/api`'s optional `WithDashboard` serves it same-origin under
`/dashboard/`. [ADR-0017](adr/0017-dashboard-toolchain-and-serving.md) records
the toolchain, serving, credential, and CI decisions and the alternatives each
rejected.

No migration, no new API endpoint, no new query, and no new credential type. The
dashboard is a client of routes that already existed.

### No new host prerequisite — by containment, not by amendment

[PROJECT_SPEC.md](PROJECT_SPEC.md) §5's list — Git, Go, Docker, Docker Compose,
Make — is unchanged. Every `dash-*` Make target builds a stage of
`dashboard/Dockerfile`; `node_modules/` never exists on the host, and only
`dash-build` writes anything back (the static output, into the ignored
`internal/dashboard/dist/`). This was verified rather than inferred, twice:

- the Go gates (`make fmt` with no diff, `make lint`, `make build`, `make test`
  including integration) ran green in a `golang:1.25` container in which
  `command -v node` fails, on a fresh clone;
- a fresh clone was taken from clean infrastructure to a served, built
  dashboard with `PATH` restricted to symlinks for Git, Go, Docker, and Make
  plus `/usr/bin:/bin`, where `node`, `npm`, and `npx` are absent. See "M6D
  gates" below.

### Mounted under `/dashboard/`, not `/` — and why that is the fix

`taskforge-api` already answered every unrouted path with a structured JSON 404
from a catch-all at `"/"`. A single-page app at the root would need its entry
document as the fallback for every non-file path, which would turn a mistyped
`/v1/...` into HTML unless the fallback carried a list of API-shaped
exclusions — a second copy of the route table that would drift. Under a prefix
there is nothing to exclude: with the dashboard enabled, every path outside
`/dashboard/` except the bare root (which redirects to it) is answered exactly
as before, including `/v1/nonexistent`'s JSON 404.

`WithDashboard` is optional in exactly the way `WithMetrics` is. Unset, no
pattern is registered, so every pre-M6D test — including M6B's
`TestTracing_UnroutedRequestsShareOneBoundedSpanName` — passes unmodified. One
subtree pattern means every client route shares one span name and one metric
`route` label.

Inside the mount: a real file is served; a missing file under `assets/` is a
JSON 404, never HTML the browser would execute as script; dotfiles (the
committed `.gitkeep` is embedded) are never served; any other path gets the
entry document, so reloads and deep links work. Hashed assets are cached
immutably and the entry document is `no-cache`.

### The credential, and what protects it

The operator pastes a key minted exactly as for the CLI. It lives in
`sessionStorage` only — tested: after entering one, `localStorage` is empty and
no cookie exists — and every read sends it as a bearer token with
`credentials: "omit"`.

**What a script in the dashboard's origin could reach is more than that key.**
The dashboard shares an origin with routes that take no credential at all: key
administration (`GET`/`POST /internal/v1/api-keys` and
`/internal/v1/worker-keys`, and their `.../{key_id}/revoke` routes), and the
worker-control routes after registration, which trust a session id. Those
routes do not check the request's `Content-Type`. So such a script could list
every key, mint an API key or worker key for **any** scope, and revoke keys —
full credential administration, not "this key's scope". The dashboard's own
reads are scope-limited; its origin is not.

That is an **accepted, loopback-only limitation**, recorded in
[ADR-0017](adr/0017-dashboard-toolchain-and-serving.md). It rests on the same
facts the unauthenticated routes already rested on: `TASKFORGE_API_ADDR` is
validated as a loopback bind, and anyone on loopback can already call them
directly. What the dashboard adds is a path to them through script injection
in its page. A guard — refusing browser-originated `/internal/*` requests by
`Origin` or `Sec-Fetch-Site`, or serving the dashboard from its own listener —
moves a trust boundary, so it is the owner's decision and is deferred as a
bounded follow-up; it had to be settled before `taskforge-api` listens on
anything but loopback. **M6E built that guard**; see "M6E" below for what it
closed and what remains open.

The mitigation against such a script running at all: a Content-Security-Policy
on every dashboard response allowing only same-origin script, style, and fetch
(quoted exactly in ADR-0017), with no `unsafe-inline` or `unsafe-eval`, no
third-party script, and text-only rendering of payloads and error messages.
`TestAssets_BuiltOutputIsSelfConsistent` fails if the built `index.html` ever
contains an inline or off-origin script, so the build and the policy cannot
quietly disagree. Live, the browser logged no CSP violation. The CSP makes such
a script unlikely; it does not narrow what one could reach.

### Four states per view, and the request id

Every view renders loading, empty, error, and loaded as four distinct DOM
states, and each view has a test for each — 24 state tests. "Empty" is a
successful response with zero rows, never a spinner or an error. The error
state renders the server's HTTP status, `error.code`, `error.message`, and
`request_id`; each view's error test asserts all four. Removing the request id
from the component fails exactly those six tests. Live, the unknown-job error
state rendered request id `41db1461-ae47-447e-b78a-ff13c9a7f01c`, and that
exact id is on `taskforge-api`'s log line for the 404.

The client never invents a server code: a non-TaskForge error body keeps
`code` null, and an unreachable API has no status at all rather than a
fabricated one.

### "Nothing is hardcoded", as a check

`hardcoded.test.ts` scans every shipped source file and fails on a UUID, an
RFC 3339 timestamp, or any string from the test fixtures (whose free-text
values all carry a `fixture` marker); and it forbids every numeric literal but
0 and 1 in views and components, so any number shown came from the API.
Writing a queue's limit into the Queues view as the literal `64` fails it.

### Type drift against `api/openapi.yaml`

`types.ts` is hand-written. Each interface has a `FieldSpec` constant that
`tsc` forces to list exactly its fields with their presence and nullability;
`types.test.ts` compares those, every enum, and every route and query
parameter the client sends with the real `api/openapi.yaml`. Removing
`worker_name` from `Attempt`'s required list in the OpenAPI file fails it; so
does changing a `FieldSpec` entry, at `tsc` time.

Its boundary: it does **not** compare scalar types (`integer` against
`number`, `date-time` against `string`) or array item schemas (that
`JobPage.jobs` holds `JobSummary`). Those rest on the hand-written interfaces
and review.

### Two things the Queues view refuses to imply

`depth` counts this key's scope; `max_concurrency` is queue-wide across every
scope. They are separate, labeled columns with a caption saying they are not
comparable, and a test asserts no depth-over-limit ratio is rendered.

Workers are listed by whatever status `GET /v1/workers` reports, in text, not
only color, and heartbeat age is shown as the API measured it without a
client-side verdict. A crashed worker appears `UNHEALTHY`. A worker whose boot
was replaced appears with its **newest** session's status — `HEALTHY` in
practice, as M6A's own integration test pins — not `OFFLINE`; the test fixture
models exactly that. `OFFLINE` is in the enum, so its label has a separate,
explicitly rendering-only test. A status the dashboard does not know (a newer
server during a rolling deploy) is counted on the Overview as unrecognized
rather than dropped.

### Deliberate scope boundaries

These are boundaries, not TODOs:

- **No write from the browser** — no cancel, retry, replay, or bulk replay.
  Each such route requires a caller-chosen `Idempotency-Key`, and how a browser
  should mint and reuse one across a double click or an ambiguous timeout is a
  design question of its own; doing it here would also stack a destructive
  action on a brand-new browser credential story. The DLQ view names
  `taskforge-cli dlq replay` instead.
- No free-text search, no auto-refresh, websockets, or SSE — every view has an
  explicit Refresh control.
- No server-side credential proxy. It is the stronger posture and the natural
  next step if the dashboard ever leaves loopback; ADR-0017 records why it is
  deferred.
- No `dash-dev` hot-reload target: a containerized dev server must reach
  `taskforge-api` on host loopback, which does not work portably. The loop is
  `make dash-test`, then `make dash-build`, `make build`, and a restart: the
  dashboard is embedded at compile time.

### A defect this milestone's own checks caught

The clean-clone run found that `dashboard/Dockerfile`'s
`# syntax=docker/dockerfile:1` line made every build resolve a Dockerfile
frontend image **by floating tag** — an unpinned dependency beside the
digest-pinned Node image, in a file claiming to pin the whole toolchain. It was
removed; the per-Dockerfile `.dockerignore` allowlist still holds under the
builtin frontend (363 kB of build context against a 193 MB `bin/` beside it).

### Breaking change

None. No schema change, no migration, no API change. `WithDashboard` is
additive and optional.

## M6E — browser-origin guard on the internal surface

A follow-up to M6, not a fifth slice of it: M6's objective was discharged in full
by M6A–M6D. [ADR-0018](adr/0018-browser-origin-guard-on-the-internal-surface.md)
records the decision, the alternatives it rejected, and its consequences. This
section keeps three things apart: what is implemented, what evidence exists for
it, and what is still open.

### Implemented behavior

Every registered `/internal/v1` route — all 14 documented operations and the 12
method-less `405` fallbacks — refuses a request with **`403` and
`Error.code` `origin_refused`** when any of these holds, checked in this order:

1. it carries a `Sec-Fetch-Site` header, with any value, including `none` and an
   empty value (`sec_fetch_site`);
2. it carries an `Origin` header, with any value, including `null` and the API's
   own origin (`origin`);
3. its `Host` is empty, cannot be parsed, or is not loopback (`host`). Parsing
   fails closed: an unbracketed IPv6 literal, an unclosed bracket, text after a
   `]`, extra colons, and an empty or non-numeric port are all refused.

The refusal runs before authentication, before the `405` answer, and before any
handler reads the body. The response message is fixed and never echoes a header
value; the log line carries the request id and the rule name and never a header
value. The guard is stateless: no migration, no mixed-version state, and no
change to any `/v1` route, the dashboard, `/metrics`, or the health probes.

It is registered per route through one helper, `handleInternal`, as the
outermost layer, so a refused request keeps its route pattern as its span name
and metric label. The loopback predicate moved from `internal/config` to
`internal/loopback` with identical behavior; the bind rule, the worker's API URL
check, and the Host rule all call it.

Contract: `origin_refused` is in the `Error.code` enum; all 14 `/internal`
operations document the `403`; the `info` Authentication section describes the
guard and says it is not authentication. The CLI maps `origin_refused` to
`ExitRequestRejected` and the Python SDK to `RequestRejectedError`.

### Evidence

All of this ran on code commit `c90b269`; later commits on the branch change
documentation only. Gate outputs are under "M6E gates", mutation results under
"M6E coverage".

- **Refusal table, driven by `api/openapi.yaml`** — for every `/internal`
  operation the document declares, each of 18 browser markings (five
  `Sec-Fetch-Site` values, three `Origin` values, ten malformed or foreign
  `Host`s) returns `403`, `origin_refused`, the fixed message, the JSON error
  shape, and the echoed request id, **and** a recording fake behind the API-key
  store, the worker-key store, and the worker-control service shows nothing was
  called, including the worker-key credential check. Each operation also has a
  positive control proving the same request, unmarked, does reach those fakes —
  without it, a request built wrongly would be "refused" by validation and the
  empty-fake assertion would prove nothing.
- **Pass-through** — with no browser headers, each of eight loopback `Host`
  spellings (`127.0.0.1:8080`, `127.0.0.2:8080`, `localhost:8080`,
  `LOCALHOST.:8080`, `[::1]:8080`, `[::1]`, `127.0.0.1`, `localhost`) reaches the
  handler on every operation with the same status as the canonical request.
- **Ordering** — a browser-marked registration carrying a *valid* worker key is
  refused `403`, neither served (`200`) nor `401`, calls neither the credential
  check nor registration, and its unmarked twin does register; a browser-marked
  `DELETE /internal/v1/api-keys` is `403`, not `405`, and advertises no `Allow`.
- **Scope** — with a valid key, `Sec-Fetch-Site`, `Origin`, and a non-loopback
  `Host` all set, `/v1/jobs`, `/dashboard/`, `/metrics` and `/healthz` answer
  exactly as without them, and an unregistered `/internal/v1/nonexistent` stays
  the JSON `404`.
- **Observability** — a refused request's span name and metric `route` label are
  the route pattern (`POST /internal/v1/api-keys`; the fallback's own
  `/internal/v1/api-keys` for a `DELETE`), never `unmatched`; the log lines carry
  `rule` and `request_id`; and the captured log contains neither `evil.example`
  nor `cross-site`.
- **A real listener and database** — a raw HTTP `POST /internal/v1/api-keys` with
  an `Origin` header (also with `Sec-Fetch-Site`, and with a foreign `Host`)
  returns `403`; a following `GET` with no browser headers shows no key was
  created, and the identical unmarked request then creates one.
- **Registration cannot drift** — a test reads `server.go` and fails on any
  `/internal` pattern registered around the helper, and pins the count of 26
  guarded registrations.
- **Contract** — every `/internal` operation declares the `403` with the fixed
  message the server sends; no `/v1` operation does; and
  `TestOpenAPI_TheInternalSurfaceIsDocumentedAsUnauthenticated` passes
  unmodified.
- **Existing tests** — the only existing tests changed are the Host edits in the
  mechanical commit (19 direct `httptest.NewRequest` sites plus the shared
  `doJSON` helper, through one helper, no assertion touched) and the CLI and SDK
  exit-code tables that must now name `origin_refused`.

### Upgrade note

A client that reaches the API through a hostname alias for loopback — an
`/etc/hosts` entry mapping `tf.local` to `127.0.0.1`, say — sent a non-loopback
`Host` that was accepted before and gets `403` on `/internal` now. Address the
API as `127.0.0.1`, `[::1]` or `localhost`. This is the one visible behavior
change. Every default base URL (the CLI's, the SDK's, the worker's) is already
`http://127.0.0.1:8080`, and none sends `Origin` or `Sec-Fetch-Site`.

### What a worker does on a `403` at registration

It exits; it does not loop. Observed with the real worker client and runner
against the guarded API, with the request's `Host` rewritten to an alias:
`RemoteError.Retryable` is false for a `403` (`internal/worker/client.go:612`),
`Runner.retry` returns on the first attempt (`internal/worker/runner.go:839`),
`Run` returns `register worker session: control plane returned HTTP 403
(origin_refused)` (`runner.go:174`), and `taskforge-worker` logs it and exits `1`
(`cmd/taskforge-worker/main.go:181`). With `RetryAttempts=5` exactly one request
was sent. A worker also cannot be configured into this by accident:
`TASKFORGE_WORKER_API_URL` must already use a loopback host, by the same
predicate. That check is a throwaway observation and is not a committed test.
Worker logic is unchanged.

### Limitations — what this does not do

- **It is not authentication.** Any non-browser process that can reach loopback
  can still mint a credential. Authentication on `/internal` remains absent by
  design.
- **DNS rebinding can still read `/metrics`, `/healthz` and `/readyz`,** and load
  the dashboard's static HTML, because the guard covers `/internal` only. They
  carry no tenant data; the dashboard HTML is inert because the key lives in the
  real origin's `sessionStorage`; `/v1` still needs a key the attacker lacks.
  Guarding the whole listener would close the `/metrics` read and is left for
  M8.
- **A browser that sends no Fetch Metadata is covered by the `Origin` and `Host`
  rules alone.** Such a browser also omits `Origin` on a same-origin `GET`, so a
  same-origin script there can still list keys. Writes carry `Origin` and are
  refused in every engine. Current Chromium, Firefox and Safari send Fetch
  Metadata.
- **The Host rule is coupled to the loopback bind.** M8's ECS workers will reach
  the API under a non-loopback name; the bind and the Host rule have to be
  revisited together there.
- **Fail-closed coverage for a *new* route depends on two tests, not on
  structure.** An `/internal` route registered around `handleInternal` is caught
  by the source-scan test, which also pins the count of registrations. One
  registered through it but absent from the OpenAPI document is not covered by
  the refusal table, which is driven by the document; that is the pre-existing
  gap, recorded above, that nothing checks the route table and the document
  against each other.
- The API version string in `api/openapi.yaml` (`0.8.0-m6a`) was not bumped for
  the new error code; it has not been bumped since M6A.

### Breaking change

One new error code and one new `403` on every `/internal` operation. A client
that branches on `Error.code` and treats an unknown code as a protocol error sees
`origin_refused` only if it sends a browser-marked or non-loopback request, which
no shipped client does.

## M7A — invariant and scenario proof audit

M7 is split into M7A, this milestone, and M7B; [ROADMAP.md](ROADMAP.md) records
why. M7A asks one question of every reliability invariant in
[ARCHITECTURE.md](ARCHITECTURE.md) §12 and every required scenario in M7: which
test proves it, and does that test read durable PostgreSQL state after the
operation? The answer is [VERIFICATION_MATRIX.md](VERIFICATION_MATRIX.md).

**No production code changed.** Nothing under `cmd/`, nothing in `internal/`
outside `_test.go` files, no migration, no change to `api/openapi.yaml`, nothing
in `sdk/` or `dashboard/`, and no Make target. This section keeps four things
apart: what the audit found, what was added, what evidence exists for it, and
what is still limited.

### Audit results

A row counts as covered only if its test asserts **durable state**: a SQL query
or a store read made after the operation. A test that checks only an HTTP
status, a response body, or in-memory state does not count, whatever its name
says. Of the thirty rows, **twenty-two were already covered** and **eight were
closed** by a new or extended test. None is `STOPPED`: no gap test exposed a
production defect, so nothing was stopped on and the matrix claims every row.

What the audit found about the candidates it was given, and about the rows it
closed:

| Row | Finding |
| --- | --- |
| S1 | Covered, but `TestE2E_APIKeyDrivesTheWholeLifecycle` is weaker than its name: it reads the job's status, its attempt count, and its scope, never the attempt's or the lease's status. `TestWorker_EndToEndAndDuplicateBrokerDeliveryCreateOneAttempt` joins attempt and lease status after a real publisher, broker, and worker, and carries the row. |
| S2 | Covered. The real-binary crash test already asserts attempt history, lease history, and the two attempts' session ids from PostgreSQL, not only the final status, so it needed no strengthening. It was changed only for hermeticity. |
| S3 | **Closed.** No candidate did what the row says. `TestFail_IsFencedByEveryIdentifier` presents random identifiers, not a stale attempt's own. `TestStaleSession_CanDoNothingAtAll` asserts error values and the session fence, not that any row is unchanged. `TestReconcile_ReplacementClaimsAttemptTwoAndPreservesHistory` sends attempt 1's late success only after the job has already succeeded and never sends a failure, so it cannot tell a stale report from a finished job. |
| S11, I17 | **Closed.** The API and the outbox publisher had restart tests and the worker had a real `SIGKILL` test. The scheduler and the reconciler had none: the existing "unobserved commit" tests rerun a pass, which is not a process dying mid-scan. |
| I18 | **Closed.** `TestHeartbeat_UsesPostgreSQLTimeAndIsMonotonic` shows the stored time is PostgreSQL's, but the request has no time field, so that is equally true if worker time is merely absent. It passes under a mutation that makes a worker-supplied header authoritative (see the coverage table). |
| I2 | **Closed.** One operation at a time against one terminal status, and the scheduler only ever against a job canceled out of `PENDING`: never against `SUCCEEDED` or `DEAD_LETTERED`, and never through re-notification. |
| I5 | **Closed.** An attempt number was seen only as a value `Claim` returned, and only 1 and 2. |
| I13 | **Closed.** The retry decision was shown persisted, but nothing restarted the scheduler across a `RETRY_WAIT`. |
| I15 | **Closed.** Capacity is not a counter, so a literal negative is impossible; no test checked the property that matters, that a release reported twice frees one slot. |
| I3 | Covered, with a limitation (below): the index definition and contested claims, not a refused insert. |
| I8 | Covered by the two `WaitingAcrossExpiry` tests, which read the rows back. `TestStaleSession_CanDoNothingAtAll` and `TestExpiredLeaseCannotStart` assert refusals but not unchanged rows, and are not cited. |

### What was added

All in `tests/` and `docs/`:

- `tests/integration/late_completion_test.go` — `TestLateCompletion_AfterReassignmentIsRejectedAndOnlyTheReplacementCommits` (S3, I8, I9). Over the worker's real HTTP client: attempt 1's lease expires, reconciliation abandons it, a second session claims attempt 2, and attempt 1's success and failure are both refused with `lease_expired` while attempt 2 is still running. The job's rows in the six tables `durableSnapshot` reads (`jobs`, `job_attempts`, `leases`, `results`, `dlq_entries`, `outbox_events`) are compared before and after. Attempt 2 then succeeds: the job is `SUCCEEDED`, attempt 1 `ABANDONED`, exactly one result exists and it is attempt 2's, and the `SUCCEEDED` attempt and `COMPLETED` lease rows carry attempt 2's attempt, worker, session, and lease identifiers. (The `jobs` row has no fence columns, so "the final row carries attempt 2's fence values" is asserted on those rows and on `results.attempt_id`.)
- `tests/integration/restart_durability_test.go` — `TestSchedulerRestart_RetryWaitSurvivesAndIsPromotedExactlyOnce` and `TestReconcilerRestart_MidScanCrashLeavesNoPartialRepairAndTheReplacementFinishesIt` (S11, I13, I16, I17). The reconciler is crashed **mid-scan**, not between scans, and deterministically: a barrier parks the second of two repairs inside its transaction and the server terminates that connection. The scheduler's backoff is made due with PostgreSQL's clock, and passes are counted rather than slept on.
- `tests/integration/time_authority_test.go` — `TestWorkerSuppliedTimeIsNeverAuthoritative` (I18). Offers a worker-supplied time to the heartbeat, claim, start, renewal, success, failure, and cancellation-acknowledgment requests, in the body and in headers.
- `tests/integration/invariant_proofs_test.go` — `TestInvariant_TerminalJobsStayTerminalUnderEveryMutator` (I2), `TestInvariant_AttemptNumbersIncreaseWithoutGapsAndAreUniquePerJob` (I5), and `TestInvariant_ReleasingCapacityNeverAdmitsPastTheLimit` (I15), plus `durableSnapshot`, a before/after comparison of one job's rows in exactly six tables: `jobs`, `job_attempts`, `leases`, `results`, `dlq_entries`, and `outbox_events`. It does not read `idempotency_records`, `dlq_replays`, or `worker_sessions`.
- `tests/integration/worker_process_crash_test.go` — changed. The environment is built by functions and now pins `TASKFORGE_RESULTS_ENDPOINT`; `startService` asserts, on the exact environment it hands the operating system, that every endpoint the binary dials is set to where the suite reaches that service; and `TestWorkerProcessCrash_EnvironmentPinsEveryLoopbackDefault` derives the variables with a loopback default from `internal/config` with `go/parser` and requires each to be pinned or named, with a reason, as one no spawned binary reads. Which endpoints each binary dials was read off `cmd/*/main.go`: the API takes the database and the object store, the publisher the database and the broker, the reconciler and scheduler the database, and the worker the broker, the object store, and the API URL. `TASKFORGE_RESULTS_ENDPOINT` was the only one missing.
- `tests/verification/matrix_test.go` and `docs/VERIFICATION_MATRIX.md` — the matrix and its drift check, which runs inside `make test-unit` and so in CI's first job.

### Evidence

All of this ran on code commit `0d5bb05`; later commits on the branch change
documentation only. Gate outputs are under "M7A gates".

**Every new or changed test was caught by a production-code mutation**, each
applied by a script that asserted the edit landed exactly once, shown by
`git diff`, run, reverted, and confirmed with a clean `git status`. The diffs and
failing output are in the pull request's handoff.

| Test | Mutation | Observed |
| --- | --- | --- |
| `TestLateCompletion_…` | `fenceState.leaseUsable()` returns `true` | Fails on the late success: expected `lease_expired`, got `state_conflict`. The late call is **still refused** — the status precondition is a second layer, and the `UPDATE` predicates a third — so this mutation changes the reason, not the outcome, and the test pins the reason. |
| `TestSchedulerRestart_…` | the promotion scan excludes `RETRY_WAIT` | "the restarted scheduler promotes the due retry" never became true within 15s |
| `TestReconcilerRestart_…` | the abandonment path's `Commit` becomes a `Rollback` | "the repair that committed before the crash is durable": `QUEUED/ABANDONED/EXPIRED` expected, `RUNNING/RUNNING/ACTIVE` found |
| `TestWorkerSuppliedTimeIsNeverAuthoritative` | (a) `decodeControlJSON` drops `DisallowUnknownFields` | a body time is accepted: `400` expected, `200` found. By itself this only breaks the documented contract, since the time would still be ignored |
| | (b) the heartbeat honors a worker-supplied `Date` header, across `types.go`, `heartbeat.go`, `worker_control.go` | the stored heartbeat is in 2099 and the assertion fails. `TestHeartbeat_UsesPostgreSQLTimeAndIsMonotonic` **passes** under the same mutation |
| `TestInvariant_TerminalJobs…` | the promotion scan and its in-lock recheck admit `SUCCEEDED` | "a terminal job is never promoted": zero expected, one found |
| `TestInvariant_AttemptNumbers…` | `Claim` allocates `max(attempt_number) + 2` | caught, but earlier than the numbers assertion: the budget arithmetic consumes `attempt_number`, so the job exhausts its budget and the test stops at `makeRetryDue` |
| `TestInvariant_ReleasingCapacity…` | the worker-capacity check `>=` becomes `>` | all five `worker limit` subtests fail (`CAPACITY_EXHAUSTED` expected, `CLAIMED` found); none of the `queue limit` subtests did, which is why the two ledgers are exercised apart |
| the drift check | (a) delete the `S9` row | "S9 is missing from the matrix" |
| | (b) rename `TestSubmit_SameKeyDifferentRequestIsAConflict` | the rows that name it, I11 and S5, each report it is no longer a test function |
| the crash test's hermeticity | remove the `TASKFORGE_RESULTS_ENDPOINT` pin (the pre-fix environment; a test-code revert, since the fix is in test code) | `startService` fails before spawning: "`taskforge-api` would take `TASKFORGE_RESULTS_ENDPOINT` from its loopback default", and the loopback-default test fails for the same variable |

**Flake evidence.** `go test -tags integration -race -count=10` over
`TestLateCompletion_`, `TestSchedulerRestart_`, `TestReconcilerRestart_`,
`TestWorkerSuppliedTimeIsNeverAuthoritative`, `TestInvariant_` and
`TestWorkerProcessCrash_EnvironmentPinsEveryLoopbackDefault`: each of the eight
top-level tests passed 10 of 10, with no `DATA RACE` and no failure. The
real-binary crash test, which this milestone changed, passed 10 of 10 under
`-race` in 129.2s.

**Hermeticity.** The assertions above are the evidence that the spawned binaries
are pinned. A run of the suite inside a **bridged container** against
infrastructure on the host was **not reproduced** here, so the original failure
was not re-observed and its fix was not observed passing there.

### Limitations

- **The drift check proves existence, not that a test is load-bearing.** A test
  that still exists and no longer proves its row passes it. The mutation results
  above are a point-in-time record; nothing in CI re-runs them. It also proves a
  cited line lies inside the named test, not that the line is the assertion, and
  the AST fix for that is deferred to M8.
- **A "restart" of the scheduler and the reconciler is a connection pool, an
  engine, and a loop, stopped and replaced in one test process.** Only the worker
  has a real-binary kill test. The reconciler's replacement runs one `RunOnce`
  pass rather than its timer loop, because the loop holds no state between
  passes.
- **I3's database enforcement is evidenced by the index definition,** not by an
  insert PostgreSQL refuses. I5's is stronger: the test asks for a duplicate and
  gets `23505`.
- **I15 is tested as "a release is worth one slot",** because capacity is derived
  from `ACTIVE` leases and a literal negative cannot occur.
- **The late-completion rejection is layered,** so a mutation of any one layer
  changes the error code but not the outcome. The test pins the code; see the
  table.
- **The route table and `api/openapi.yaml` are still not checked against each
  other.** That is deferred to M8 and recorded under "Deliberately not
  implemented yet". M7A did not touch the hand-maintained maps.
- **Hosted CI on the final head is not recorded here,** because a commit cannot
  contain its own CI result: the pull request's checks are the record.
- **M7B was out of scope for M7A.** It is the next section.

### Breaking change

None. No production code, schema, API, CLI, SDK, dashboard, or configuration
changed.

Post-merge review corrections (B1: I17 citation; B2: durableSnapshot scope wording; ARCHITECTURE §12 wording): see PR #19.

## M7B — `make demo` and `make demo-failure`

M7 is split into M7A and M7B, this milestone; [ROADMAP.md](ROADMAP.md) records why.
M7B puts two handlers in the production worker, adds a Go program that runs the
real binaries, and adds two Make targets and their CI steps. This section keeps
three things apart: what it does, what evidence exists, and what is still
limited.

**What changed in production code:** `internal/worker/handler.go` gains
`DemoSleep`, `DemoFail` and a strict payload decoder, and
`cmd/taskforge-worker` registers its handlers in one function,
`registerTrustedHandlers`, instead of inline. **Nothing else.** The runner, the
migrations, `api/openapi.yaml`, the SDK and the dashboard are unchanged.
OpenAPI constrains `job_type` with a pattern (`^[a-z0-9][a-z0-9._-]{0,127}$`),
never an enumeration, and both new types match it, so no schema moved; `demo.sleep`
was already the example in the submission schema.

### Behavior

**The handlers.** `demo.sleep` takes exactly `{"duration_ms": n}` with
`1 <= n <= jobs.MaxTimeoutSeconds * 1000`, waits on a timer inside a `select` with
the context, and returns `{"slept_ms": n}`. `demo.fail` takes exactly
`{"class": "retryable"}` or `{"class": "permanent"}` and fails in that class with
the fixed code `demo_failure` and a fixed message per class. Any other payload is a
`Permanent` `invalid_payload`, and no part of a payload is ever echoed into a
failure. They are registered unconditionally, with no environment gate and no
separate build; the reasoning, the bounds and the abuse analysis are
[ADR-0019](adr/0019-demo-handlers-are-trusted-built-ins.md).

**How the runner treats a `demo.sleep` that returns `ctx.Err()`.** It does not
look at the returned error first. It reads the cause it recorded on the handler's
context, and a recorded cause outranks the return value
(`internal/worker/runner.go:569-604`): operator cancellation reports a fenced
acknowledgment (`:570`); the attempt deadline reports nothing and leaves
`TIMED_OUT` to reconciliation (`:576`); loss of lease authority reports nothing
and leaves recovery to the reconciler (`:586`); worker shutdown reports nothing
(`:594`). A handler error with none of those causes is a failure (`:601`).

**`make demo`** is `build up migrate` and then `go run ./scripts/demo success`. One
worker runs three jobs. An echo job succeeds after one attempt and its result,
read back, equals its payload. A retryable `demo.fail` job with `max_attempts=3`
fails three times, with a retry time recorded on the first two attempts and none
on the third, and is dead-lettered with `ATTEMPTS_EXHAUSTED`. A permanent
`demo.fail` job, with the same budget, fails once and is dead-lettered at once
with `PERMANENT_FAILURE`. The retry is shown by a retryable failure spending its
whole budget; no handler fails and then succeeds, because that would need the
attempt number in `Execution`.

**`make demo-failure`** is `build up migrate` and then
`go run ./scripts/demo failure`, in two phases.

1. *Crash.* Only worker A runs, and a `demo.sleep` job is submitted. When the API
   shows the job `RUNNING` on A, worker B starts and A is sent `SIGKILL`. A's
   attempt is `ABANDONED` and bound to A's session; a second attempt on B, under a
   different session, is `SUCCEEDED`, and so is the job.
2. *Frozen worker.* B is stopped, so only worker C runs, and another `demo.sleep`
   job is submitted. When it is `RUNNING` on C, C is sent `SIGSTOP`. It stays
   stopped until the API shows attempt 1 `ABANDONED`. Worker D then starts and
   finishes attempt 2. Only then is C sent `SIGCONT` and given at least four
   seconds to act. The pass condition is the stored outcome after that: the job
   still `SUCCEEDED`, attempt 1 still `ABANDONED`, exactly two attempts, exactly one
   result and it is attempt 2's, and all of it identical to what was stored just
   before C resumed.

**Timings.** Both profiles are values `internal/config` accepts, and a unit test
loads each through `config.Load`, `config.LoadWorker` and
`config.ValidateWorkerTimings` and asserts the loaded value is the chosen one,
because a duration the loader cannot parse is silently replaced by its default.

| Setting | `demo` | `demo-failure` | Rule it satisfies |
| --- | --- | --- | --- |
| lease duration | 30s | 5s | between 1s and 24h |
| heartbeat interval | 5s | 1s | positive |
| session stale after | 15s | 3s | at least 3x heartbeat |
| lease renew interval | 10s | 1.5s | 3x renew (4.5s) at most the lease |
| worker request timeout | 5s | 1s | at most stale, and at most renew |
| reconciler and scheduler poll | 250ms | 250ms | positive |
| outbox poll, claim timeout | 200ms, 1s | 200ms, 1s | claim timeout at least the poll |
| scheduler re-notify after | 5s | 5s | at least 3x poll (750ms) and at least the outbox claim timeout (1s) |
| worker poll wait | 1s | 1s | between 1s and 20s |
| job retry base, max | 500ms, 5s | 500ms, 5s | max at least base, whole milliseconds |

The success profile keeps the shipped lease and liveness values, because nothing
in it depends on a worker dying; only the retry settings and the poll intervals
are shortened, so the whole run takes seconds rather than minutes.

**What the program does and does not do.**

- Every service and worker is started from `./bin` with an explicit environment
  that pins every endpoint that binary dials, found from `cmd/<binary>/main.go`,
  and an empty working directory so no `.env` reaches it. A test derives the
  variables with a loopback default from `internal/config` by parsing it, and
  requires each to be pinned.
- Infrastructure comes from the same `TASKFORGE_*` variables the services read
  (and the same `.env`), falling back to the compose defaults. CI points it at its
  own services the same way.
- Ports are free loopback ports chosen at run time. The run has its own broker
  queue, so a worker already running elsewhere cannot receive its notifications
  and a message a killed worker was holding cannot surface in a later run. Keys
  are created in a scope unique to the run, and everything is asserted by this
  run's job ids.
- It never deletes or truncates anything in the database. It deletes the broker
  queue it created, and it **revokes**, not deletes, its two keys, after its
  workers have stopped.
- Every child is in its own process group, and every exit stops all of them.
  Every wait has a timeout.
- It uses `taskforge-cli` for key creation, submission, and every read the CLI
  gives as JSON. **Two facts come from PostgreSQL instead**, read-only and only for
  this run's job ids and scope: which process session each attempt is bound to,
  and which attempt a result belongs to. `api/openapi.yaml` withholds both from the
  public API on purpose.

**Output.** Trimmed from `make demo-failure` at code commit `68b693e`; the whole of
both transcripts is in the pull request.

```
[   1.2s] The API shows the job RUNNING on worker A (session 09dfbe7f).
[   1.3s] Started worker B ... Now SIGKILL worker A: no signal handler, no cleanup ...
[  14.6s] Final timeline: #1 ABANDONED on worker-a, #2 SUCCEEDED on worker-b.
[  14.6s] Read from PostgreSQL (the API withholds session ids): attempt 1 is bound to session 09dfbe7f, attempt 2 to session b9ea4417.
[  16.6s] SIGSTOP worker C: frozen, not dead. ...
[  21.7s] The API shows attempt 1 ABANDONED. Worker C is still frozen.
[  30.2s] Attempt 2 SUCCEEDED on worker D. Stored state: SUCCEEDED, attempts [1:ABANDONED:...-c 2:SUCCEEDED:...-d].
[  30.2s] SIGCONT worker C. Every timer it had is now overdue ...
[  34.4s] Stored state after C resumed: SUCCEEDED, attempts [1:ABANDONED:...-c 2:SUCCEEDED:...-d].
           WARN  lease authority deadline reached without a confirmed renewal
           WARN  lease authority lost before an outcome could be reported
           ERROR worker stopped with error worker session liveness could not be confirmed before the stale threshold after 3s
  PASS  crash: attempt 1 is bound to worker-a's session       session 09dfbe7f
  PASS  crash: the two attempts ran under different sessions  09dfbe7f vs b9ea4417
  PASS  fence: results stored for the job                     1
  PASS  fence: the result belongs to attempt 2                result attempt e4be2483, attempt 2 is e4be2483
  PASS  fence: nothing C did changed the stored state         job, attempts and result are identical before and after C resumed
=== RESULT: PASS (25 of 25 expectations met) ===
```

and from `make demo`, in the same run: `retry job attempts: #1 FAILED, #2 FAILED, #3 FAILED`,
`attempt 1 failed; the control plane scheduled attempt 2 after 465ms`,
`PASS  retry: dead-letter reason   ATTEMPTS_EXHAUSTED`,
`PASS  permanent: dead-letter reason   PERMANENT_FAILURE`, and
`=== RESULT: PASS (21 of 21 expectations met) ===`.

### Evidence

All of this ran on code commit `68b693e`, except the mutation record below, which
ran on `fa0b420`; the two commits between them change two comments and the
narration of one phase, and no expectation. Later commits on the branch change
documentation only. Gate outputs are under "M7B gates".

**Five mutations were applied and each was caught**, one per row below, each by a
script that asserted the edit landed exactly once, shown by `git diff`, run,
reverted, and confirmed with a clean `git status`. They are a spot check, not one
per test or per expectation. The diffs and failing output are in the pull request's
handoff.

| Check | Mutation | Observed |
| --- | --- | --- |
| `demo.sleep` honors its context | the `select` on the timer and `ctx.Done()` becomes a bare `<-timer.C` | Four unit tests fail, each in the 10s watchdog ("it is ignoring its context"), instead of waiting out an hour. The integration test fails on the lease: expected `RELEASED`, found `EXPIRED`. The job still reached `CANCELED`, but through the reconciler rather than the worker's own acknowledgment |
| `demo.fail` classes | the two `case` arms return each other's class | `TestDemoFail_MapsEachClass…` fails both ways (`RETRYABLE` against `PERMANENT`). The retryable integration test sees `[FAILED]` instead of `[FAILED FAILED]`, and the permanent one `[FAILED FAILED FAILED]` instead of `[FAILED]` |
| the registered set | `demo.fail` is dropped from `registerTrustedHandlers` | The pinned-set test fails (`[demo.echo demo.sleep]` for `[demo.echo demo.fail demo.sleep]`), and so does the binding test (`demo.fail must be registered`). With the real binary rebuilt, `go run ./scripts/demo success` leaves the retry and permanent jobs `QUEUED` until its 60s wait ends, and exits 1 |
| a `make demo` expectation | the permanent job is expected `SUCCEEDED` | `FAIL permanent: job status  expected SUCCEEDED, got DEAD_LETTERED`; `RESULT: FAIL (1 of 21 expectations failed)`; the program exits 1 and `make` reports `Error 1`, exiting 2 |
| a `make demo-failure` expectation | phase 2 expects three attempts | `FAIL fence: attempts  expected 3, got 2`; `RESULT: FAIL (1 of 25 expectations failed)`; exit 1, `make` exit 2 |

**Flake evidence.** `make demo` five times back to back: **5 of 5** exit 0, each
`PASS (21 of 21)`, 4 to 9 seconds each including build and migrate.
`make demo-failure` five times back to back: **5 of 5** exit 0, each
`PASS (25 of 25)`, 34 to 36 seconds. `go test -tags integration -race -count=10`
over the three new integration tests: each **10 of 10**, no `DATA RACE`,
`ok … 16.008s`. `go test -race -count=10` over `scripts/demo`,
`cmd/taskforge-worker` and the new handler tests in `internal/worker` passed.

**Interruption.** Verified by experiment, not by reading: `SIGINT` during phase 1,
and `SIGTERM` while worker C was frozen, each exit 130 with no process left and
none left stopped; and a `SIGINT` sent to the whole `make demo-failure` process
group, which is what a terminal's Ctrl-C does, mid-phase-1, ended with
`exit status 130` and no process left.

### Limitations

- **The frozen worker is not refused by the control plane in `make demo-failure`;
  it stops itself.** In all eleven failure-mode runs whose output was kept, C's log shows its own
  lease deadline passing during the freeze, then "lease authority lost before an
  outcome could be reported", then exit. `SIGSTOP` stops a process but not the
  monotonic clock, and the real worker checks its conservative local deadline
  before it reports, so it never sends the stale call. No run printed a control-plane
  refusal code. The stored state is unchanged either way, which is what the
  demonstration asserts, but a server-side refusal is what
  `TestLateCompletion_AfterReassignmentIsRejectedAndOnlyTheReplacementCommits`
  proves, by sending the stale calls itself. Whether
  [PROJECT_SPEC.md](PROJECT_SPEC.md) §5's "stale-attempt fencing" is satisfied by
  that, or wants the refusal shown in the demonstration, is the owner's call.
- **Two facts are read from PostgreSQL, not the API.** The demonstration therefore
  needs the database URL and depends on `job_attempts.worker_session_id`,
  `worker_sessions`, `workers` and `results.attempt_id`. They are the only
  schema-coupled reads in it.
- **Queue capacity is shared across scopes.** `max_concurrency` counts active
  leases across every scope, so a scope's own workers running long `demo.sleep` jobs
  can occupy slots of that shared limit for up to `timeout_seconds` each. ADR-0019
  records this. V1 has no multi-tenancy, so it is recorded, not mitigated.
- **Phase 2 waits a fixed time, not an observed condition, before it submits.**
  Stopping worker B can leave its last long poll open at the broker, which can hand
  the next notification to a connection nobody reads. The program waits one poll
  wait plus half a second. Without it, one run was observed to recover only after
  the scheduler re-notified about five seconds later (`renotified_jobs: 1`); the
  re-notification is still the fallback if the wait is ever not enough.
- **Timing assumptions.** Each `demo.sleep` runs eight seconds so that it is still
  running while a second worker starts and the first is killed. A runner slow
  enough to take longer than that to start a worker would fail the first
  expectation visibly rather than pass. None was observed, and **hosted CI on the
  final head is not recorded here**, because a commit cannot contain its own CI
  result: the pull request's checks are the record. The workflow was parsed as
  YAML locally, and GitHub has not run it from this document.
- **It creates its broker queue with an unsigned SQS query request,** which
  ElasticMQ, the local broker, accepts (ADR-0005). Against real SQS it would need
  signing; V1 has no such target.
- **`SIGKILL` of the program itself leaves its children.** Each is in its own
  process group, so it does not receive a signal sent to the program's group.
  `SIGINT` and `SIGTERM` are handled and were tested; `SIGHUP` and a closed
  terminal were not.
- **The database keeps what a run created.** The jobs, attempts, workers, sessions,
  results and dead-letter entries stay, under a scope no later run shares; only
  `make down` removes them. The two keys are revoked.
- **`make demo` and `make demo-failure` are not in the root `README.md`.** Pull
  request #10 rewrites that file wholesale and owns it, so that is where they belong.
- **No performance claim is made.** The run times above are what happened on one
  machine, not a benchmark.

### Breaking change

The production worker now declares three job types, which `GET /v1/workers` reports
as `supported_job_types` and which bound the worker's `job_type` metric label to
four values. No schema, API, CLI, SDK, dashboard or configuration changed.

## M8A — load generator and measured benchmarks

M8 is split into M8A, this milestone, M8B and M8C; [ROADMAP.md](ROADMAP.md)
records why. M8A adds a harness that runs the real binaries, records one measured
run of [PROJECT_SPEC.md](PROJECT_SPEC.md) §7's targets, and carries three small
items from M7. This section keeps three things apart: what it does, what evidence
exists, and what is still limited.

**What changed in production code:** `internal/worker/handler.go` only. The two
demonstration handlers, `demo.sleep` and `demo.fail`, now match their payload key
exactly and refuse a repeated key. **Nothing else.** The runner, the migrations,
`api/openapi.yaml`, the SDK and the dashboard are unchanged. Everything else is
under `scripts/`, `tests/`, `docs/`, the Makefile, `AGENTS.md` and CI.

### Behavior

**The harness** is `scripts/bench`, with three modes: `throughput`, `faults` and
`smoke`. A full run, `make bench`, is `throughput faults --record`. It starts the
stack in `scripts/internal/stack`, the hermetic stack `scripts/demo` now shares
(extracted without changing `make demo` or `make demo-failure`), creates keys in a
scope of its own, submits `demo.sleep` for 50 ms over `net/http` at 1,000 jobs a
minute to 12 workers of 4 slots, and reads everything back from PostgreSQL through
`scripts/readdb`, a read-only handle (`default_transaction_read_only=on`) whose
queries `tests/integration` also imports. It never deletes or truncates anything.
How each figure is defined, how a target is judged Met or MISSED, and why the
headline run uses the shipped defaults are in
[ADR-0020](adr/0020-benchmark-methodology.md) and are not restated here.

**What it refuses.** A recorded run refuses to start unless `git status
--porcelain` is empty and every binary in `bin/` was stamped by `go build` with
`HEAD` and an unmodified tree. Every mode refuses while another TaskForge service
binary is running on the machine, and a run aborts, naming the cause, if
PostgreSQL's clock and the harness's monotonic clock drift apart by more than two
seconds, or if a worker the fault injector did not kill exits. A record is never
overwritten and an invalid run is never recorded.

**`make bench-smoke`** runs a small throughput run and a small fault run on the
short-lease profile with a kill that has to hit an attempt, asserts thirteen
things about the harness's own output, and records nothing. CI runs it in the
integration job, after the demonstrations; **CI never records a number.**

**The results** are in [PROJECT_SPEC.md](PROJECT_SPEC.md) §7, which is their
canonical home, and in the two records under [benchmarks/](benchmarks/). On the
shipped defaults, throughput and the fault-injection volume and completion targets
are met and the dispatch-latency and worker-failure-recovery targets are
**missed**. A second, labelled run on a tuned profile met all five.

**Three carried items.** The stack opens PostgreSQL read-only. ADR-0019 now says
its capacity bound is per attempt and that one job can hold a slot for up to
`max_attempts × timeout_seconds` across separate attempts. The demonstration
handlers decode their payload token by token into a map of exact key to value, so
`{"DURATION_MS": 5}`, `{"Class": "retryable"}` and a repeated key, all accepted
before, are now permanent `invalid_payload` failures.

### Evidence

The measured commit is **`d796722`**: both the headline run and its record say so,
and `git diff --stat d796722 HEAD -- ':!docs'` is empty, so no file outside
`docs/` has changed since. The gate outputs under "M8A gates" ran on `18601e0`,
which differs from `d796722` only in a paragraph of `AGENTS.md`. The tuned run was
measured on `c1764d7`, the commit that added the headline record, which differs from
`d796722` only under `docs/`.

**Four mutations were applied and each was caught**, each shown by `git diff`, run,
reverted, and confirmed with a clean `git status` (they ran on `6a908aa`):

| Mutation | Caught by | What it said |
| --- | --- | --- |
| (a) the dispatch query reads `started_at` instead of the claim's `created_at` | `TestBenchQueries_DispatchLatencyIsTheClaimMinusTheSubmission` | expected 40ms, actual 90ms |
| (b) the fault schedule ignores the seed | `TestSchedule_ADifferentSeedGivesADifferentSchedule` | the two schedules are equal |
| (c1) the recorded path no longer calls the dirty-tree guard | `TestRun_ARecordedRunOnADirtyTreeIsRefusedBeforeAnythingStarts` | the refusal was about `bin/`, not the tree |
| (c2) the guard function accepts any tree | `TestRequireCleanTree_RefusesAModifiedFile`, `…RefusesAnUntrackedFileAndStagedWorkToo`, and the one above | three tests failed |
| (d) recovery is measured from the replacement's `started_at` | `TestBenchQueries_RecoveryIsTheReplacementsClaimMinusTheKill` | expected 31s, actual 33s |

The percentile method was checked the same way: a floating-point `ceil(0.95 × n)`
fails the exact-rank test (it gives 951 for n = 1000), which is why the rank is
computed in integers.

**What this milestone's own checks caught, and what was wrong about my reading of
them.** This is recorded because the fixes and one retraction are part of why the
numbers can be trusted.

- **A real harness bug.** The first five `make bench-smoke` runs passed two. One
  failed with `conn busy` and one crashed the process with `fatal error:
  concurrent map writes`: the benchmark shared a single `pgx.Conn` between its
  watchdog, its fault injector and its main flow. `readdb.Open` now returns a
  read-only pool, and `TestReaddb_OpenIsSafeForConcurrentUse` fails with a data
  race on the old single connection and passes ten times under `-race` on the
  pool. The recorded run would have crashed the same way.
- **A wrong diagnosis, retracted.** One of those runs left 38 jobs `QUEUED` after
  its drain, and I attributed it to a slow broker and raised the smoke's wait
  bounds (commit `2b9931c`). That was wrong. The crash above had left a whole
  stack running as an orphan, and its outbox and scheduler were taking other runs'
  events and publishing them to its own dead queue. The same orphan then broke
  eight outbox and scheduler integration tests. `2b9931c` is reverted
  (`bfdd35a`), and the harness now refuses to start while any TaskForge service is
  running on the machine (`18601e0`), with a test and a check against a real
  process.
- **The environment can invalidate a run, and the harness now says so.** With the
  laptop's lid closed on battery, macOS entered Maintenance Sleep during runs
  even under `caffeinate`, and the Docker VM's clock was stepped on each wake: a
  "12 second" window measured 61 seconds, and a "4 second" one measured 14
  minutes. One `make demo-failure` run failed on this (1 of 13 expectations; a
  worker was fenced the second the machine slept) and was rerun under `caffeinate`
  with the lid open. The harness's clock watchdog exists because of it.

### Limitations

- **One machine, one run, and a busy one.** A laptop (Apple M3, 8 logical cores,
  8 GiB) running the load generator, five services, twelve workers, Docker (a 3.8
  GiB VM) and the Claude desktop app, whose renderer was using roughly a third of a
  core when checked just before the runs. Both
  recorded runs were on AC power with Low Power Mode off, after the owner plugged
  in and switched it off. There is no variance estimate.
- **The throughput target is met inside a tolerance.** 999.83 jobs/min is 0.17
  below 1,000. ADR-0020 fixed a one-sided 1% tolerance before any run, and the
  verdict follows from it; read strictly, 999.83 is below 1,000.
- **The 50.04 s recovery has not been explained.** Fourteen of the sixteen
  recovered attempts took 31.96 to 33.98 s, which is the 30 s lease plus the 2 s
  reconciler scan plus the outbox poll. The last kill's two attempts took 50.04 s
  each, and the tuned run had no such outlier. A guess is a notification handed to
  the killed worker's open long poll and redelivered later; it was not
  investigated, and it is not asserted.
- **Eleven of the headline run's 24 kills hit nothing.** Each chose a worker that
  held an attempt, but a 50 ms attempt can finish between the choice and the
  signal, so recovery is measured on 16 attempts, not 24.
- **Dispatch latency leans slightly in the system's favor by definition.** It reads
  `job_attempts.created_at`, the start of the claim transaction. The supplementary
  lease-issuance figure is 583.3 ms median and 973.1 ms p95 on the headline run,
  against 552.2 ms and 963.7 ms, and does not change the verdict.
- **The quiet-host check sees this machine only.** A stack on another host pointed
  at the same database is invisible to it.
- **The time estimates in `AGENTS.md` and the Makefile help are wrong.** They say
  about 25 minutes; each recorded run took about 16. They are outside `docs/` and
  were left alone so that nothing outside `docs/` changed after the measured commit.
- **The benchmark uses the demonstration handlers.** If M8C gates them in a
  deployed worker, the benchmark has to run against one that registers them.
- **Hosted CI on the final head is not recorded here,** because a commit cannot
  contain its own CI result: the pull request's checks are the record.

### Breaking change

`demo.sleep` and `demo.fail` match payload keys exactly. A payload with a wrong-case
or a repeated key, which both handlers accepted before, is now a permanent
`invalid_payload` failure. No schema, API, CLI, SDK, dashboard or configuration
changed.

## Verification

### M5A gates

Every gate below was run on the branch head, on 2026-09-13, against PostgreSQL 16
and ElasticMQ started by `make up`. Durations are wall-clock from that run.

| Command | Result | Real output |
| --- | --- | --- |
| `gofmt -l .` after `make fmt` | PASS | empty — no tracked Go file rewritten |
| `make lint` | PASS | `go vet ./...`, exit 0 |
| `make build` | PASS | six binaries in `./bin` |
| `make test-unit` | PASS | every package `ok`, including `internal/auth` |
| `go test -v -count=1 -run '^TestOpenAPI_' ./internal/api/` | PASS | 17 top-level contract tests, exit 0 |
| `docker compose config --quiet` | PASS | exit 0 |
| `make migrate` on a database from `make down && make up` | PASS | `"migrations complete" applied=14`, `0014_api_keys.sql` last |
| `make test-integration` | PASS | `ok .../tests/integration 83.990s` |
| `make test-race` | PASS | every unit package `ok`; `ok .../tests/integration 94.587s` |

Exact commands and complete output are recorded in the pull request.

### M5A coverage

`internal/auth` carries 16 top-level unit tests and `internal/api` 59; the
integration package carries 191, of which 16 are new for M5A.

**Unit.** Key generation is pinned to exact encoded lengths and to a hash vector
verified independently against `sha256sum`, so a change to either entropy
constant or to what is hashed fails here rather than silently invalidating
stored keys. `ParseKey` rejects seventeen malformed shapes — including an
underscore used as the separator and a whole `Bearer` header passed verbatim —
each paired with the valid key as a control. `auth.Store`'s malformed-credential
and validation paths run against a **nil connection pool**: if one of them
reached PostgreSQL it would panic rather than quietly pass, which is the
strongest available proof that an unauthenticated caller cannot turn a header
into database load.

**Constant-time verification** is guarded by an AST assertion over
`VerifySecret` rather than a timing measurement, which would be flaky and prove
nothing on a loaded runner. It requires `subtle.ConstantTimeCompare` and forbids
comparing the digests with `==` or `!=`. The refactor it exists to stop is
someone simplifying the call into a string comparison, which reads as equivalent
and hands an attacker who knows a valid prefix a way to walk the stored digest
one character at a time.

**Integration.** Scope isolation is driven by two real minted credentials over
real HTTP rather than by a scope string written into the database: a job created
under key A answers `404` to key B on read, cancel, and retry alike, and the
durable row carries the key's scope rather than `TASKFORGE_DEV_SCOPE`. Every
public route refuses an anonymous caller over the wire and writes nothing — no
job, no idempotency record, no outbox event. Revocation takes effect on the very
next request. Every authentication failure returns the same error *value*,
asserted by equality rather than by non-nil.

Schema constraints pair each rejection with a positive control, including at the
boundary lengths, so a CHECK that rejected everything would fail rather than look
like a guard. Prefix uniqueness asserts the constraint *name*, because that is
what `auth.Store` matches on to decide a collision is retryable: a rename would
silently turn a retry into a leaked unique violation.

Concurrency runs on separate connections. Twelve concurrent creations produce
twelve distinct, independently authenticatable credentials, each resolving to
its own scope; eight concurrent revocations of one key produce exactly one
winner and one instant every caller agrees on; sixteen concurrent
authentications against one key are clean under the race detector.

The upgrade rehearsal takes a database migrated through `0013`, seeds real
M1–M4 rows across ten tables, and compares a **per-table content digest** — not
a row count, which a rewrite-in-place would preserve — before and after. The
upgrade applies exactly one migration, `api_keys` starts empty, not one digest
changes, a re-run applies nothing, `0014` is recorded with the checksum of its
own file, and existing jobs keep their original scope.

**Mutation evidence.** Five defects were reintroduced and reverted. Four
confirmed the guard written for them: dropping `security` from an operation in
the spec; falling back to `DevScope` when no scope is in the request context;
replacing `subtle.ConstantTimeCompare` with `==`; and expecting an end-to-end
status the job never reaches.

The fifth found a real gap rather than confirming a guard. Removing
`requireAPIKey` from a public route did **not** fail the original test:
`scopeOrUnauthorized` is a second line of defense inside every public handler,
so an unwrapped route still answered `401` — the right status reached the wrong
way, which that test could not distinguish. It matters, because without the
wrapper the handler runs, so an unauthenticated caller reaches body decoding and
validation, and the next public handler written without that internal guard
would simply be open.
`TestAuth_EveryPublicRouteConsultsTheCredentialStore` closes it by counting
calls into the credential store, which an unwrapped route never makes, and a
companion test pins that authentication precedes body decoding. Removing the
wrapper from either `/v1/dlq` or `/v1/jobs` now fails.

A sixth mutation checked the limitation above rather than a guard: minting the
"stranded" key in the executable scope makes the scope-boundary test fail, which
is what proves that test is observing the boundary and not a coincidence.

### M5B gates

Every gate below was run on the branch head, on 2026-09-14, against PostgreSQL 16
and ElasticMQ started by `make up`. Durations are wall-clock from that run.

| Command | Result | Real output |
| --- | --- | --- |
| `gofmt -l .` after `make fmt` | PASS | empty — no tracked Go file rewritten |
| `make lint` | PASS | `go vet ./...`, exit 0 |
| `make build` | PASS | six binaries in `./bin` |
| `make test-unit` | PASS | every package `ok`, including `internal/workerauth` |
| `go test -v -count=1 -run '^TestOpenAPI_' ./internal/api/` | PASS | 18 top-level contract tests, exit 0 |
| `docker compose config --quiet` | PASS | exit 0 |
| `make migrate` on a database from `make down && make up` | PASS | `"migrations complete" applied=15`, `0015_worker_keys.sql` last |
| `make test-integration` | PASS | `ok .../tests/integration 73.636s` |
| `make test-race` | PASS | every unit package `ok`; `ok .../tests/integration 83.810s` |

Exact commands and complete output are recorded in the pull request.

### M5B coverage

`internal/workerauth` carries 10 top-level unit tests, mirroring
`internal/auth`'s structure exactly. `internal/api` grew from 59 to 69
top-level tests (the ten new ones drive `requireWorkerKey` and
`resolveWorkerControlScope` directly, against fakes, with no database). The
integration package grew from 191 to 204 top-level tests: twelve new in
`worker_keys_test.go` (schema, store, concurrency, and the two headline
end-to-end tests), plus revisions to existing worker-control and API-key
tests to thread a worker key through the real HTTP stack.

**Unit.** `requireWorkerKey` is proven to refuse no credential, an unknown or
revoked worker key, and a deadline during verification (`503`, not `401`) —
the identical shape `requireAPIKey`'s own tests already established.
`resolveWorkerControlScope` is proven to refuse a non-register call whose
session's worker key has since been revoked, succeed for a live one, fail
closed when no worker-key store exists to check a real key id, never treat a
`nil` worker-key id as revoked, and propagate an unknown session as the
existing `ErrSessionUnavailable` conflict rather than a new error shape.

**Integration.** `TestE2E_AWorkerRegisteredUnderAMatchingWorkerKeyExecutesTheJob`
is the headline test: a job submitted under an API key for one scope is
claimed and executed by a worker registered under a worker key for that same
scope — the real, closed form of the limitation M5A recorded.
`TestE2E_RevokingAWorkerKeyRefusesTheNextCallItsSessionsMake` drives the
confirmed Option B semantics over real HTTP: a live session heartbeats fine,
revoking its worker key makes the *next* heartbeat `401` and every one after,
and the session's own row stays `HEALTHY` throughout — proving revocation is
discovered on the next call, not enforced by mutating the session.
`TestWorkerKeyStore_IsDistinctFromAPIKeyStore` proves the two credential
stores actually refuse each other's keys, not merely that they are
implemented as different Go types. Schema constraints, prefix uniqueness
across revocation, and concurrent create/revoke/authenticate all mirror the
M5A API-key tests' structure and pass under the race detector.

The upgrade rehearsal seeds real M1–M4 data on a database migrated through
`0010`, applies `0014` and `0015` together through the real runner (both are
now pending from an M4 baseline), and checks: `worker_keys` exists and starts
empty; every pre-existing `worker_sessions` row gets a `NULL worker_key_id`
and no other column changes, checked by an explicit content digest restricted
to the columns that existed before `0015`; a second run applies nothing; and
both `0014` and `0015` are recorded with the checksum of their own files.
`TestMigrations_RestoreReplayNotificationTimestampsRewoundBy0012` — which
deliberately runs current control-plane code against a database frozen at
migration `0010` to reproduce a real historical upgrade boundary — needed
`0014` and `0015` applied immediately after reaching `0010`, since
`workers.Store.Register`'s INSERT now names `worker_key_id` unconditionally;
neither migration touches the notification-history tables that test is
actually about, so this changes nothing the test asserts.

**Mutation evidence.** Two guards were confirmed by deliberately breaking
them and watching the corresponding test fail, then reverting. Removing
`requireWorkerKey` from the registration route's mux registration made
`TestHandleRegisterWorkerSession_PersistsThePrincipalsScopeAndKeyID` fail
(expected `200`, got `401`) — caught by the handler's own defensive
`workerPrincipalFrom` check, the same defense-in-depth pattern M5A's
`scopeOrUnauthorized` uses, so this also proves the second line of defense
works. Removing the `IsRevoked` check from `resolveWorkerControlScope` made
`TestResolveWorkerControlScope_RefusesANonRegisterCallWhoseWorkerKeyWasRevoked`
fail (expected `401`, got `200`) — the guard that makes Option B revocation
real, not merely documented.

A third finding came from `make test-race` itself, not from a deliberate
mutation: the first version of the headline end-to-end test started a second,
wrong-scope worker sharing the stack's broker queue to demonstrate isolation
inline. Under the race detector it occasionally received the job's broker
notification, found nothing it was eligible to claim, and held the message
invisible for the queue's visibility timeout — starving the worker that
could actually claim it, and timing out at 30s. Removed; isolation is already
proven elsewhere, and three repeated `-race` runs of the test as fixed
complete in ~0.15s each.

### M5C gates

`gofmt`, `lint`, `build`, `test-unit`, the OpenAPI contract tests, and
`docker compose config` were run locally on the branch head, on
2026-09-16, after every fix below was in place. `test-integration` and
`test-race` could not be run locally in this sandbox at all — its Docker
daemon can pull only already-cached images, and LocalStack's image is not
cached here; every pull attempt (LocalStack itself, and two alternative
S3-compatible test doubles) failed at blob fetch from a blocked registry
host, independent of this project's own code. Hosted CI, which has
unrestricted network access, is the real gate for those two, and its
result below is from commit `2506222` — the actual final head, after the
three real fixes this section's "Debugging arc" recounts.

| Command | Result | Real output |
| --- | --- | --- |
| `gofmt -l .` after `make fmt` | PASS | empty — no tracked Go file rewritten |
| `make lint` | PASS | `go vet ./...`, exit 0 |
| `make build` | PASS | six binaries in `./bin`, including `taskforge-worker` |
| `make test-unit` | PASS | every package `ok` (or `[no test files]`), including `internal/results` and `internal/worker` |
| `go test -v -count=1 -run '^TestOpenAPI_' ./internal/api/` | PASS | 18 top-level contract tests, exit 0 |
| `docker compose config --quiet` | PASS | exit 0 |
| `make test-integration` (hosted CI) | PASS | `ok github.com/co-rtex/TaskForge/tests/integration 93.398s`, commit `2506222`, run [35145694272](https://github.com/co-rtex/TaskForge/actions/runs/35145694272) |
| `make test-race` (hosted CI) | PASS | every unit package `ok`; `ok github.com/co-rtex/TaskForge/tests/integration 78.732s` under `-race`, same commit and run |

Exact commands and complete output are recorded in the pull request.

**Debugging arc.** Getting `test-integration`/`test-race` green took six
pushes and three real, independent fixes, in this order:

1. **Unbounded object-store HTTP client.** `internal/objectstore.New` used
   the AWS SDK's default HTTP client, whose overall request `Timeout` is
   the zero value — unbounded — unless a caller sets one. Against this
   project's own CI, LocalStack accepted a `PutObject` connection and never
   answered it, hanging the upload forever with no error. Fixed by setting
   an explicit timeout (`internal/objectstore`'s `localRequestTimeout` /
   `remoteRequestTimeout`, split by destination once the first, uniform
   10-second bound proved too generous relative to the test's own
   15-second patience for its three retries to ever surface within it).
   The first diagnosis on the way to this one — aws-sdk-go-v2's
   trailing-checksum default — was wrong, and is kept in
   [ADR-0015](adr/0015-result-storage.md) alongside the real cause,
   because the wrong turn is exactly what a future reader hitting the same
   symptom needs.
2. **Message starvation on a shared broker queue.** Checkpoint logging
   added across three pushes (and removed once its job was done) proved
   the worker's own `Receive()` calls were polling correctly the entire
   time — the message the test published was simply never returned to
   either of the test's own two worker slots. `deploy/local/elasticmq.conf`
   sets `defaultVisibilityTimeout = 30 seconds` on the shared
   `taskforge-work-available` queue every integration test uses unless it
   opts out; a message left invisible there by some other test's abandoned
   receive outlives this test's 15-second patience twice over. Fixed by
   giving the results tests their own isolated queue via
   `createIsolatedBrokerQueue`, the same helper `lifecycle_e2e_test.go`
   already used for exactly this failure mode.
3. **`scope` silently dropped from the claim response.** Fixing the queue
   let the pipeline finally run far enough, in milliseconds, to hit a
   third, genuinely different bug: `api.AssignmentResponse` never had a
   `scope` field, so the uploaded object key came out
   `results//<job_id>/<attempt_id>` — the scope segment empty — instead of
   `results/<scope>/<job_id>/<attempt_id>`. Nothing before M5C ever needed
   a worker to know its own claimed job's scope, so the gap was invisible
   until this milestone's object-store key became the first consumer.
   Fixed on both sides of the wire contract, with a regression test per
   side, each confirmed to fail with its fix reverted before being
   restored.

None of the three was hypothetical: each was confirmed by reverting the
fix and watching the corresponding test or assertion fail for exactly the
reason claimed, not merely by writing a test that passed against the fix
already in place.

### M5C coverage

`internal/results` carries 5 top-level unit tests covering `Classify`'s
threshold (inclusive on the object side), an independently-verified SHA-256
test vector, and `Result.Validate`'s two well-formed shapes plus every
malformed one. `internal/worker` grew from 55 to 61 top-level tests: five
drive `prepareResult` directly against a fake object store — an empty
handler result produces no result at all, a result below the threshold
never touches the object store, one at or above it uploads before
`prepareResult` returns, a retry-exhausted upload failure and a missing
object store both refuse rather than proceed — and one,
`TestParseAssignment_PreservesScope`, is the client-side half of the
scope-wire-contract regression covered below. `internal/api` grew from 69
to 77 top-level tests: seven drive `GET /v1/jobs/{job_id}/result` against
fakes — an inline result served as its exact bytes, an object-located
result proxied through a fake object store, a missing object store on an
object-located row answering `500` rather than `404`, the shared
not-found/malformed-id shape, authentication, a missing `Results` store
answering `500` (the route is registered unconditionally, like every other
public route, not conditionally like worker-control), and `405` on the
wrong method — and one, `TestWorkerControl_ClaimResponseIncludesAssignmentScope`,
is the server-side half of that same regression. The integration package
grew from 204 to 207 top-level tests: three new in `results_test.go` — the
small-result and large-result end-to-end round trips, and the
no-result-recorded `404` — each driving a real worker process against real
PostgreSQL, a real broker, and a real object store, plus signature-ripple
updates to existing worker-control and outcome tests for `Succeed`'s new
parameter.

**Mutation evidence.** Three guards were each confirmed by deliberately
breaking them and watching the corresponding test fail for the claimed
reason, then reverting:

- `Store.Succeed`'s replay guard: duplicating the `results.InsertResultTx`
  call into the replay branch (the one that returns before ever reaching
  the real insert) made `TestSucceedReplay_DoesNotInsertASecondResult` fail
  with a real PostgreSQL `duplicate key value violates unique constraint
  "results_pkey"` error. This proves the replay branch is safe by
  construction — it returns before `result` is ever looked at — not merely
  safe because the primary key would reject a second attempt at the same
  `job_id` if the code ever reached it.
- The server-side scope fix: removing `Scope: assignment.Scope` from
  `toClaimResponse` made `TestWorkerControl_ClaimResponseIncludesAssignmentScope`
  fail with `expected: "acme-corp", actual: ""`.
- The client-side scope fix: removing `Scope: response.Scope` from
  `parseAssignment`'s return made `TestParseAssignment_PreservesScope` fail
  the identical way. Together these prove the fix closes the gap on both
  sides of the wire, not just one.

### M5D gates

`gofmt`, `lint`, `build`, `test-unit` (whole repository, including
`internal/cli`), the OpenAPI contract tests, and `docker compose config`
were run locally on the branch head, on 2026-09-16/17, against PostgreSQL
16, ElasticMQ, and LocalStack started by `make up`.

| Command | Result | Real output |
| --- | --- | --- |
| `gofmt -l .` after `make fmt` | PASS | empty — no tracked Go file rewritten |
| `make lint` | PASS | `go vet ./...`, exit 0 |
| `make build` | PASS | seven binaries in `./bin`, including `taskforge-cli` |
| `make test-unit` | PASS | every package `ok` (or `[no test files]`), including `internal/cli` (47 top-level tests) |
| `go test -v -count=1 -run '^TestOpenAPI_' ./internal/api/` | PASS | 18 top-level contract tests, exit 0 — unaffected, run as a regression check since this milestone touches no API code |
| `docker compose config --quiet` | PASS | exit 0 |
| `make test-integration` | PASS | `ok github.com/co-rtex/TaskForge/tests/integration 75.726s`, re-run on the final head after the `TASKFORGE_CLI_API_URL` change |
| `make test-race` | PASS | every unit package `ok`; `ok github.com/co-rtex/TaskForge/tests/integration 113.273s` under `-race`, on the final head, once the sandbox clock agreed with the host again |

**An earlier local `test-race` run failed, for an environmental reason, and
is kept on record.** Before the final head, a `make test-race` run failed two
tests this milestone does not touch:
`TestScheduler_PromotesDueJobsWithExactlyOneFreshEvent/a_delayed_job` and
`TestWorkerProcessCrash_SigkillRecoversThroughTheRealBinaries`. `git diff`
touches nothing outside `internal/cli`, `cmd/taskforge-cli`, and
documentation. The scheduler test computes an "already due" `available_at`
from the **test process's** `time.Now()` with a one-second margin
(`tests/integration/scheduler_test.go`), while `PromoteDueJobs` decides from
**PostgreSQL's** `clock_timestamp()` — server time is authoritative by design
(AGENTS.md section 6) — so it needs the two clocks to agree within about a
second. Measured at the time, the Postgres container's clock was about 7
seconds behind the host, and about 5 minutes 36 seconds behind after a full
`make down && make up` (the skew was the Docker Desktop VM's clock, not a
container's). Nothing was changed to make it pass. On the later follow-up
the same two clocks agreed to the second (`04:18:54` host, `04:18:54.748`
Postgres), and the identical `make test-race` passed on the first attempt.
Hosted CI, whose runners keep correct time, was green on the original head
as well — see the pull request.

### M5D coverage

`internal/cli` carries 47 top-level tests across four files. There is
deliberately no server-side test change: this milestone adds a consumer,
not a contract.

**Exit-code contract** (`exitcode_test.go`): `TestExitCodes_AreDistinct` and
`TestExitCodes_OnlySuccessIsZero` check the bounded/mutually-exclusive/
zero-means-success properties mechanically over the named constant set
rather than by inspection. `TestApiErrorExitCodes_CoversExactlyTheReachableSet`
pins the eleven-entry reachable-code map. Eighteen `TestRun_Exit*` tests
each assert one specific numeric exit value through `Run()` against a real
`httptest.Server`, including one test per member of every
multi-membership code (2, 5, 9), and `TestRun_FailurePrintsNothingToStdout`
checks the stdout/stderr split across five representative failure classes
in one table.

**Client** (`client_test.go`): `Authorization` is sent when an API key is
set and withheld when it is not; a wrong-route case
(`TestCmdAPIKeysCreate_NeverSendsAuthorizationHeader`, in `run_test.go`)
additionally proves a *configured* key is still withheld from the
unauthenticated key-management routes specifically — this is the test that
caught the one real product bug this milestone's own review process found
(see below). `Idempotency-Key` is sent on job submission. IDs containing
characters special to a URL (`?`, `=`, a space) are proved to survive as one
literal path segment rather than being parsed as a query string.
`TestClient_TransportErrorOnUnreachableHost` proves a `*TransportError` is
returned, not a `*Response`, when nothing answers. `TestClient_ReturnsResponseForHTTPLevelFailures`
proves the reverse: a 4xx/5xx is a `Response` like any other, for the
caller to classify.

**Configuration** (`config_test.go`): `TestResolveBaseURL` is a table
covering flag-over-env precedence, the default, `https`, a non-loopback host
being accepted (loopback is a default, not a constraint), and trailing-slash
trimming. `TestResolveBaseURL_RejectsAnythingButAnAbsoluteHTTPURL` pins the
validation shape shared with `TASKFORGE_WORKER_API_URL` — including the old
bind-address shape (`127.0.0.1:8080`, `localhost:8080`), a missing scheme, a
non-http scheme, and an empty host — for both the flag and the environment
variable, asserting the error names whichever source was wrong.
`TestResolveBaseURL_DefaultMatchesTaskforgeAPIsOwnDefault` pins that this
CLI's fallback address is exactly `internal/config.Config`'s own default
`127.0.0.1:8080`, so the two cannot silently drift apart, and
`TestAPIURLEnv_IsNotTheServersBindAddressVariable` pins the variable name.

**Commands** (`run_test.go`): `TestCommands_HappyPaths` drives all
thirteen commands to their documented success status and asserts the
server's JSON body reaches stdout unmodified. `TestCmdJobsSubmit_SendsExactRequestBody`
asserts every field of the wire request, including repeated `--capability`
flags collecting into an ordered slice and an omitted `--scheduled-at`
producing no field at all rather than a null. `TestCmdJobsSubmit_GeneratesIdempotencyKeyWhenOmitted`
and the malformed-payload test
(`TestCmdJobsSubmit_RejectsMalformedPayloadWithoutCallingTheAPI`) prove a
CLI-side validation failure never reaches the network — the recording
server's own path field stays empty. `TestRun_APIKeyFlagOverridesEnv` and
`TestRun_APIKeyFromEnvWhenFlagAbsent` cover the same precedence
`TestResolveBaseURL` covers for the address, but for the credential.
`TestRun_APIURLFromEnvWhenFlagAbsent` proves `TASKFORGE_CLI_API_URL` is read
correctly end to end through `Run`, and `TestRun_IgnoresTheServersBindAddressVariable`
proves `TASKFORGE_API_ADDR` is not (mutation-checked: switching `Run` back to
reading `TASKFORGE_API_ADDR` fails it, `TestRun_APIURLFromEnvWhenFlagAbsent`,
and `TestRun_InvalidAPIURLIsAUsageErrorBeforeAnyRequest`).

**This milestone's own review found one real product bug before it ever
reached the pull request, not after**: an early implementation set
`Authorization` on every request whenever an API key was configured,
including the four key-management routes that are unauthenticated by
design. `TestCmdAPIKeysCreate_NeverSendsAuthorizationHeader` was written to
prove the intended behavior, failed against that implementation, and the
fix — a distinct `Client.doUnauthenticated` code path, never sharing a
branch with the public-route path that reads `APIKey` — made it pass.
This is recorded here rather than silently fixed and left unmentioned,
because the failure was caught by a test whose entire purpose was to prove
this exact property, which is the evidence this milestone's own exit-code
and credential-handling design sections claim to have.

**Real end-to-end evidence, against `taskforge-api` itself, not only
fakes.** Beyond the unit-test suite above, this milestone's exit-code
mapping was independently confirmed against a real running server: minting
a real API key and worker key through `taskforge-cli api-keys create` /
`worker-keys create`; submitting a real job and reading it back; a wrong
key on `jobs get` answering `unauthorized` (exit `3`); a nonexistent job id
on `jobs get` answering `not_found` (exit `4`); `jobs retry` on a job that
is not dead-lettered answering `job_not_dead_lettered` (exit `5`); and
`jobs cancel` called twice on the same job returning `200`
`already_requested: true` the second time (exit `0`, not a conflict — a
cancel-twice on the same job is one decision observed twice, not the
`job_not_cancelable` case, which requires a job already `SUCCEEDED` or
`DEAD_LETTERED`).

### M5E gates

Run locally on the branch head on 2026-09-21, against PostgreSQL 16,
ElasticMQ, and LocalStack started by `make up`, on Python 3.14.2 (the host
interpreter; CI additionally covers 3.11 and 3.13).

| Command | Result | Real output |
| --- | --- | --- |
| `gofmt -l .` after `make fmt` | PASS | empty — no tracked Go file rewritten |
| `make lint` | PASS | `go vet ./...`, exit 0 |
| `make build` | PASS | seven binaries in `./bin` |
| `make test-unit` | PASS | every package `ok` or `[no test files]` |
| `go test -count=1 -run '^TestOpenAPI_' ./internal/api/` | PASS | `ok github.com/co-rtex/TaskForge/internal/api 0.513s` — run as a regression check; this milestone touches no Go |
| `docker compose config --quiet` | PASS | exit 0 |
| `make test-integration` | PASS | `ok github.com/co-rtex/TaskForge/tests/integration 74.605s` |
| `make test-race` | PASS | every unit package `ok`; `ok github.com/co-rtex/TaskForge/tests/integration 89.426s` under `-race` |
| `make sdk-lint` | PASS | `19 files already formatted`; `All checks passed!`; `Success: no issues found in 18 source files` |
| `make sdk-test` | PASS | `139 passed in 0.12s` |

**One integration run failed first, for a reason this milestone caused but
did not introduce, and it is kept on record.** The first `make test-integration`
failed two tests — `TestOutbox_SurvivesPublisherRestart`
("should have 5 item(s), but has 6") and
`TestWorkerCrash_RecoversThroughTheRealOutboxAndBrokerPath`. The cause was
the real-stack verification below: it submitted real jobs and ran
`taskforge-outbox` against the same PostgreSQL and the same ElasticMQ queue
the integration suite drains, so leftover `work.available` notifications
were still on the queue when the suite counted them. Nothing in the product
was changed. After `docker compose down -v --remove-orphans`, `make up` and
`make migrate` rebuilt the infrastructure from empty, the identical command
passed on the first attempt. The lesson is about test-environment hygiene,
not about outbox behavior: manual verification against the shared local
broker must be followed by a teardown before the integration suite is
trusted.

### M5E coverage

**139 test cases from 81 test functions across eight files**, every one
driven through the real `TaskForgeClient` against an `httpx.MockTransport`
— the product surface, never the mapping tables in isolation. This is the
Python equivalent of `internal/cli`'s own decision to drive every exit-code
test through `Run()` against a real `httptest.Server`.

| File | Functions | Cases | Covers |
| --- | --- | --- | --- |
| `test_idempotency.py` | 8 | 26 | The acceptance criterion: the header is sent on exactly the three routes that require it, generated as a UUIDv4 when omitted, verbatim when supplied, absent from body and query, absent everywhere else, and unusable keys rejected before any request |
| `test_errors.py` | 17 | 23 | One test per exception class through a public method, one per member of every multi-membership class, the exit-code table, the eleven-entry reachable-code map, and the transport-vs-service-unavailable distinction |
| `test_models.py` | 12 | 31 | `.raw` preservation, one case per required field missing, unknown enum values, wrongly typed fields, frozen-ness |
| `test_jobs.py` | 14 | 19 | Request shape, documented defaults, both success statuses, URL-hostile ids, result values including literal `null` |
| `test_config.py` | 11 | 15 | Precedence, URL validation, and the poison-value test proving no foreign variable is read |
| `test_dlq.py` | 8 | 9 | Listing, nullable entry fields, and cursor iteration to exhaustion |
| `test_keys.py` | 7 | 7 | Both key surfaces, runtime type distinction, URL-hostile key ids |
| `test_auth.py` | 4 | 9 | `Authorization` present on public routes, absent without a key, and withheld from all six internal routes even when configured |

**One real product defect was found by the tests during implementation and
fixed before this PR.** A caller-supplied non-ASCII `idempotency_key`
escaped as a raw `UnicodeEncodeError` from inside `httpx` — not a
`TaskForgeError`, so it would have broken the SDK's own
`except TaskForgeError` contract. It is now a `ConfigurationError` raised
before any request, alongside the length and control-character checks, with
three parametrized cases per affected route.

Two further failures during implementation were **test** bugs, not product
bugs, and are recorded because the distinction matters: `httpx.Response(json=None)`
means "no body", not "a body of literal `null`" (the two are genuinely
different responses, and the SDK distinguishes them correctly), and httpx's
`URL.path` percent-*decodes*, which would have hidden the very path
escaping under test. Both assertions were corrected; no product code
changed for either.

**Real end-to-end evidence, against `taskforge-api` itself.** Every call
below went through the installed SDK, over HTTP, to the real server — with
`taskforge-api`, `taskforge-outbox` and a real `taskforge-worker` running:

| Scenario | Observed |
| --- | --- |
| Mint an API key (`api_keys.create`, unauthenticated loopback route) | id `c4bbccb2-…`, scope `m5e-verify`, one-time secret returned |
| Submit a job | `8260f16e-…`, `JobStatus.QUEUED` |
| Read it back | same id, `JobStatus.QUEUED`, `default/demo.echo` |
| **Full round trip with a real worker** | submitted `b604a183-…` → worker executed it → `JobStatus.SUCCEEDED` → `jobs.result()` returned `{'message': 'round trip through the python sdk'}` |
| Idempotent resubmit (same key, same request) | the same job id returned both times |
| Idempotency conflict (same key, different request) | `ConflictError`, `.code == "idempotency_conflict"`, `exit_code 5` |
| Result before success | `NotFoundError`, `.code == "not_found"`, `exit_code 4` |
| Cancel | `CancellationStatus.CANCELED`, `already_requested False`, job then `JobStatus.CANCELED` |
| Wrong key | `UnauthorizedError`, `exit_code 3`, `request_id` present |
| Unknown job id | `NotFoundError`, `exit_code 4` |
| `jobs.retry` on a non-dead-lettered job | `ConflictError`, `.code == "job_not_dead_lettered"`, `exit_code 5` |
| `dlq.list` / `iter_entries` | empty page parsed, `next_cursor None`, iteration terminated |
| Worker key mint / list / revoke | `WorkerKeyCreated` / `WorkerKeyList` / `WorkerKeySummary` at runtime; second revoke reported `already_revoked True` |
| Revoke the API key, then reuse it | `UnauthorizedError`, `exit_code 3` — revocation effective on the next request |

**What the real-stack run could not cover, and why.** `dlq.replay` against a
genuinely dead-lettered job was **not** exercised live. Producing a real DLQ
entry requires a handler that *fails*, and only `demo.echo` is registered as
a production worker handler — a job with an unregistered `job_type` is never
claimed at all (session-bound eligibility, ADR-0006) and simply stays
`QUEUED`, which is what was observed when attempted. Failing handlers are
injected through the registry seam by the integration suite, not by the
production binaries. `dlq.replay` is therefore covered by unit tests and by
its live `job_not_dead_lettered` conflict path only. This is a gap in *this
milestone's manual evidence*, not in the SDK, and M7 owns the automated
end-to-end failure suites.

### M5E known risks

Recorded as risks, not worked around silently.

- **Strict parsing is enforced, not tolerated.** `JobStatus` /
  `CancellationStatus` / `DLQReason` reject unrecognized values, and the
  `Error.code` map is exhaustive for the reachable set. A newer server that
  adds a job status or an error code breaks an older SDK's parse rather than
  degrading quietly. Deliberate, and the same trade exit `9` already makes;
  `.raw` limits the blast radius to the enums and the required-field checks,
  since everything the server sent is still reachable there.
- **`httpx` is this repository's first third-party Python dependency**, with
  its own release cadence and a 1.0 release ahead of it. The `<1.0` ceiling in
  `pyproject.toml` is intentional and will need a considered bump rather than
  an automatic one.
- **The Go and Python error contracts are two hand-maintained copies of one
  mapping.** Python cannot import a Go constant.
  `test_exit_codes_match_the_cli_contract` and
  `test_api_error_map_covers_exactly_the_reachable_set` pin the numbers and the
  membership, but a reviewer changing `internal/cli/exitcode.go` has to know to
  look here; the two files reference each other by name for that reason.
- **`make lint` and `make test` stay Go-only.** A contributor can therefore
  push Python that fails CI without a local signal unless they run
  `make sdk-lint`. Mitigated by [AGENTS.md](../AGENTS.md) §4 and the `sdk` CI
  job, not eliminated. The alternative — making every Go contributor provision
  a virtualenv — was judged worse.
- **`make sdk-venv` needs network access** on first run to fetch `httpx` and
  the dev extras, unlike the Go modules already in the module cache. An offline
  first run of that target fails.
- **`dlq.replay` has no live-stack evidence** against a genuinely dead-lettered
  job, for the reason given under "M5E coverage": no production handler can
  fail. Unit-tested, and its conflict path is live-verified.
- **A pre-existing inaccuracy in `internal/cli/exitcode.go`'s comment was
  noticed and deliberately not fixed here.** That comment calls
  `api/openapi.yaml`'s `Error.code` enum "the full 22-value enum" and lists
  eleven unreachable values; the enum actually has 23 values and twelve are
  unreachable from the CLI's routes (`state_conflict` is missing from the
  list). The mapping itself — which codes the CLI handles and how — is
  correct; only the count in the prose is wrong. It is left alone because this
  milestone adds a consumer and touches no Go, and a comment-only Go edit here
  would cross that boundary for no behavioral gain. `errors.py`'s equivalent
  comment states 23 and 12 correctly.

### M8A gates

Run locally on the branch, 2026-10-04, on commit `18601e0` (see "Evidence" for how
it relates to the measured commit `d796722`), from a machine with no other
TaskForge process running, on AC power with the lid open. PostgreSQL 16, ElasticMQ
and the object store from `make up`, on Go 1.27. A first round of these gates
failed because of an orphaned stack and a harness bug, both described under
"Evidence"; the table is the round after they were fixed.

| Command | Result |
| --- | --- |
| `make fmt` | PASS — `gofmt -w .` changed nothing (0 diff lines) |
| `make lint` | PASS — `go vet ./...` silent |
| `make build` | PASS — `go build -o bin/ ./cmd/...`; `scripts/bench` is not among the outputs |
| `make test-unit` | PASS — 24 packages `ok`, up from 22: `scripts/bench` and `scripts/internal/stack` (`scripts/demo` is a test package already). `TestVerificationMatrix_EveryRowResolves` prints "30 rows (I1-I18, S1-S12), 41 distinct tests resolved to real test functions". 77 `--- PASS` lines across the three script packages, no `--- FAIL`. |
| `make test-integration` | PASS — `ok  github.com/co-rtex/TaskForge/tests/integration  78.297s` |
| `make test-race` | PASS — 24 unit packages `ok` under `-race`; `ok  …/tests/integration  96.580s`; no `DATA RACE` |
| `go test -tags integration -race -count=10 -run 'TestReaddb_\|TestBenchQueries_' ./tests/integration/` | PASS — each of the six tests 10 of 10, `ok … 3.981s`, no `DATA RACE` |
| `make demo` | PASS — 21 of 21 expectations, the same as the baseline taken before the stack was extracted |
| `make demo-failure` | PASS — 25 of 25 expectations, the same as the pre-extraction baseline |
| `make bench-smoke`, five consecutive runs | PASS — 5 of 5, each "13 of 13 checks met". The round before the fixes passed 2 of 5. |
| `go run -race ./scripts/bench smoke` | PASS — 13 of 13, no `DATA RACE` |
| `make bench` (the recorded run) | PASS — exit 0, about 16 minutes, wrote `docs/benchmarks/2026-10-05-d796722.{md,json}`; run once |
| `make bench BENCH_ARGS="--profile tuned"` | PASS — exit 0, about 16 minutes, wrote `…-c1764d7-tuned.{md,json}`; run once |
| `make sdk-lint`, `make sdk-test`, `make dash-lint`, `make dash-test` | NOT RUN — M8A changes nothing in `sdk/` or `dashboard/` |
| `docker compose config --quiet` | NOT RUN — M8A changes no Compose file |

The mutation results are under "Evidence" in the M8A section.

### M7B gates

Run locally on the branch, 2026-10-02, against code commit `68b693e`.
PostgreSQL 16, ElasticMQ and the object store from `make up`, on Go 1.27.

| Command | Result |
| --- | --- |
| `make fmt` | PASS — `gofmt -w .` changed no Go file (`git status -- '*.go'` empty) |
| `make lint` | PASS — `go vet ./...` silent |
| `make build` | PASS — `go build -o bin/ ./cmd/...`; `scripts/demo` is not among the outputs |
| `make test-unit` | PASS — 22 packages `ok`, up from 21: `scripts/demo`. With `GOFLAGS='-count=1 -v'` the output carries `--- PASS: TestRegisterTrustedHandlers_PinsTheExactSetTheWorkerDeclares` and `--- PASS: TestVerificationMatrix_EveryRowResolves` with "verification matrix: 30 rows (I1-I18, S1-S12), 41 distinct tests resolved to real test functions" |
| `make test-integration` | PASS — `ok  github.com/co-rtex/TaskForge/tests/integration  92.107s` |
| `make test-race` | PASS — 22 unit packages `ok` under `-race`; `ok  .../tests/integration  95.727s`; no `DATA RACE` |
| `make demo` | PASS — 21 of 21 expectations; five consecutive runs, 5 of 5 |
| `make demo-failure` | PASS — 25 of 25 expectations; five consecutive runs, 5 of 5 |
| `go test -tags integration -race -count=10 -run 'TestDemoFail_\|TestDemoSleep_' ./tests/integration/` | PASS — each of the three tests 10 of 10, `ok … 16.008s`, no `DATA RACE` |
| `go test -race -count=10 ./scripts/demo/ ./cmd/taskforge-worker/`, and the same over `-run 'TestDemoSleep_\|TestDemoFail_\|TestDemoHandlers_' ./internal/worker/` | PASS |
| `make sdk-lint`, `make sdk-test`, `make dash-lint`, `make dash-test` | NOT RUN — M7B changes nothing in `sdk/` or `dashboard/` |
| `docker compose config --quiet` | NOT RUN — M7B changes no Compose file |

The mutation results are under "Evidence" in the M7B section. Hosted CI on the
final head is not recorded here, because a commit cannot contain its own CI
result: the pull request's checks are the record.

### M7A gates

Run locally on the branch, 2026-10-02, against code commit `0d5bb05`.
PostgreSQL 16, ElasticMQ and the object store from `make up`, on Go 1.27.

| Command | Result |
| --- | --- |
| `make fmt` | PASS — `gofmt -w .` changed no Go file (`git status -- '*.go'` empty) |
| `make lint` | PASS — `go vet ./...` silent |
| `make build` | PASS — `go build -o bin/ ./cmd/...` |
| `make test-unit` | PASS — 21 packages `ok`, up from 20: `tests/verification`. With `GOFLAGS='-count=1 -v'` the output carries `--- PASS: TestVerificationMatrix_EveryRowResolves` and its line "verification matrix: 30 rows (I1-I18, S1-S12), 41 distinct tests resolved to real test functions" |
| `make test-integration` | PASS — `ok  github.com/co-rtex/TaskForge/tests/integration  105.740s`. The suite took 85.255s on the untouched base, and 88.712s on the previous commit |
| `make test-race` | PASS — 21 packages `ok` under `-race`; `ok  .../tests/integration  98.400s`; no `DATA RACE` |
| `go test -tags integration -race -count=10 -run '<new tests>' ./tests/integration/` | PASS — `TestLateCompletion_`, `TestSchedulerRestart_`, `TestReconcilerRestart_`, `TestWorkerSuppliedTimeIsNeverAuthoritative`, `TestInvariant_` (three tests) and `TestWorkerProcessCrash_EnvironmentPinsEveryLoopbackDefault`: each 10 of 10, no `DATA RACE` |
| `go test -tags integration -race -count=10 -run TestWorkerProcessCrash_SigkillRecoversThroughTheRealBinaries ./tests/integration/` | PASS — 10 of 10, `ok … 129.229s`, no `DATA RACE` |
| `make sdk-lint`, `make sdk-test`, `make dash-lint`, `make dash-test` | NOT RUN — M7A changes nothing in `sdk/` or `dashboard/` |

The mutation results are under "Evidence" in the M7A section. Hosted CI on the
final head is not recorded here, because a commit cannot contain its own CI
result: the pull request's checks are the record.

### M6E gates

Run locally on the branch, 2026-10-01, against code commit `c90b269`.
PostgreSQL 16, ElasticMQ and the object store from `make up`; Python in
`sdk/python/.venv`; Node only inside `dashboard/Dockerfile`.

| Command | Result |
| --- | --- |
| `make fmt` | PASS — `gofmt -w .` left no diff (`git status` showed only the documentation files edited afterwards) |
| `make lint` | PASS — `gofmt -l` empty, `go vet ./...` silent |
| `make build` | PASS — `go build -o bin/ ./cmd/...` |
| `make test-unit` | PASS — 20 packages `ok`, none `FAIL` |
| `make test-integration` | PASS — `ok  github.com/co-rtex/TaskForge/tests/integration  91.594s` |
| `make test-race` | PASS — 21 packages `ok` under `-race`; `ok  .../tests/integration  98.621s`; no `DATA RACE` |
| `make sdk-lint` | PASS — `ruff format --check` clean, `ruff check` "All checks passed!", `mypy --strict` "no issues found in 21 source files" |
| `make sdk-test` | PASS — `179 passed` |
| `make dash-lint` | PASS — `biome ci`: "Checked 44 files ... No fixes applied", `tsc --noEmit` silent |
| `make dash-test` | PASS — `Test Files 10 passed (10)`, `Tests 79 passed (79)` |

The dashboard did not change; `dash-lint` and `dash-test` only confirm it did not
regress. No Make target and no CI job was added. Hosted CI on the final head is
not recorded here, because a commit cannot contain its own CI result: the pull
request's checks are the record.

### M6E coverage

Six mutations, each applied by a script that asserts the edit landed exactly
once, shown by `git diff`, run, and reverted. Each is caught by named tests in
`internal/api`:

| Mutation | Failing subtests in total | Caught by |
| --- | --- | --- |
| (a) delete the `sec_fetch_site` rule | 76 | `TestInternalGuard_RefusesBrowserRequestsOnEveryDocumentedOperation`, `..._EveryBrowserMarkingNamesItsRule`, `..._FirstRuleToFireNamesTheRefusal`, `..._RefusalIsObservable`, `..._RunsBeforeAuthenticationAndBefore405` |
| (b) delete the `origin` rule | 47 | the same five tests |
| (c) delete the `host` rule | 150 | the refusal table, `..._EveryBrowserMarkingNamesItsRule`, `..._FirstRuleToFireNamesTheRefusal`, `..._RefusalIsObservable` |
| (d) a `Host` split error falls back to accepting | 60 | the refusal table (the unparseable Hosts) and `..._EveryBrowserMarkingNamesItsRule` |
| (e) register `POST /internal/v1/claims` without the helper | 18 | the refusal table, for exactly that route, and `TestServer_EveryInternalPatternIsRegisteredThroughTheGuard`, which names `server.go:256` |
| (f) swap the guard and `requireWorkerKey` | 19 | the refusal table, `..._RunsBeforeAuthenticationAndBefore405`, and the source-scan test |

The mutated source diffs and failing output are in the pull request's handoff.

### M6D gates

Run locally on the branch, 2026-09-30. PostgreSQL 16, ElasticMQ, and LocalStack
from `make up`; Go 1.27 on the host and Go 1.25.14 in the clean container;
Node 24.21.0 inside `dashboard/Dockerfile` only; Python 3.14 in
`sdk/python/.venv`.

| Command | Result |
| --- | --- |
| `make lint` | PASS — `gofmt` clean, `go vet ./...` silent |
| `make build` | PASS — seven binaries into `./bin` |
| `make test-unit` | PASS — 19 packages `ok`, up from 18: `internal/dashboard` |
| `make test-integration` | PASS — `ok github.com/co-rtex/TaskForge/tests/integration 116.679s` |
| `make test-race` | PASS — integration `ok … 99.543s`, no DATA RACE |
| `make dash-lint` | PASS — `Checked 44 files … No fixes applied.`; `tsc --noEmit` silent |
| `make dash-test` | PASS — `Test Files 10 passed (10)`, `Tests 79 passed (79)` after review fixes (74 at first review) |
| `make dash-build` | PASS — `index.html` plus one hashed JS and one hashed CSS asset; `git status` clean afterwards |
| `TASKFORGE_REQUIRE_BUILT_DASHBOARD=1 go test ./internal/dashboard/` | PASS — including `TestAssets_BuiltOutputIsSelfConsistent` against the real build |
| `make sdk-lint` / `make sdk-test` | PASS — mypy clean in 21 files; 178 passed; unchanged by this milestone |
| `docker compose config --quiet` | PASS |
| Go gates in `golang:1.25`, no Node, fresh clone | PASS — `command -v node` fails; `make fmt` changed nothing; `make lint`, `make build`, `make test` (integration `ok … 82.709s`) green |
| Clean clone, `PATH` without Node | PASS — `go build ./cmd/...` with only `dist/.gitkeep`; placeholder served and `dashboard_built=false`; `make dash-build` left the tree clean; rebuilt API served the real dashboard (`dashboard_built=true`); `GET /v1/queues` 200 same-origin; `GET /v1/nonexistent` the JSON 404 |

One gate needed a second run, recorded rather than smoothed over: in a
**bridged** container, `TestWorkerProcessCrash_SigkillRecoversThroughTheRealBinaries`
failed because it spawns real binaries without passing
`TASKFORGE_RESULTS_ENDPOINT`, so the spawned worker dialed the container's own
loopback for the object store. That is the test harness meeting container
networking, not Node; with `--network host` the same fresh clone passed `make
test` in full, and the test passed on the host in both `test-integration` and
`test-race`. M7A fixed the harness: the test now pins
`TASKFORGE_RESULTS_ENDPOINT` and asserts it on the spawned environment. A
bridged run was not repeated; see "M7A".

**Mutation evidence**, each run in a throwaway container so the tree never
changed:

| Mutation | Caught by | Observed |
| --- | --- | --- |
| Drop `worker_name` from `Attempt.required` in `api/openapi.yaml` | `types.test.ts` | `× Attempt: same fields, same presence, same nullability` |
| Render `{64}` in place of `queue.max_concurrency` | `hardcoded.test.ts` | `views/Queues.tsx: expected [ '64' ] to deeply equal []` |
| Stop rendering `request_id` in `ErrorState` | the six views' error tests | `Tests 6 failed | 21 passed` |
| Call `decodeURIComponent` unguarded in `parseRoute` | `App.test.tsx` router cases | `× /dashboard/jobs/%E0`, `× /dashboard/jobs/%zz` |
| `payload: "optional"` in `JOB_FIELDS` | `tsc --noEmit` | `TS2322: Type '"optional"' is not assignable to type '"required"'` |

**Live stack.** All five services plus a scripted worker driving only the real
internal control routes (register, heartbeat, claim, start, fail). Seeded
through `taskforge-cli` and that worker: five `demo.echo` jobs a real worker
completed; a job failed `RETRYABLE`, retried after the server's 1150 ms
backoff, then failed `PERMANENT` into the DLQ; a `max_attempts=1` job whose
worker stopped heartbeating, which the reconciler timed out into the DLQ as
`ATTEMPTS_EXHAUSTED`; a replay of the permanent failure; a job needing a
capability no worker has; a delayed job; and a canceled one. Each view was
then read in a browser from the running `taskforge-api` and matched the CLI's
answers:

- **Overview** — "1 queue, 3 non-terminal jobs visible to this key"; PENDING 1,
  QUEUED 2; "4 workers, by status", HEALTHY 1, UNHEALTHY 3; the ten most recent
  jobs.
- **Jobs** — all eleven jobs, newest first, with status, priority, and the
  delayed job's future `available_at`.
- **Job detail** — the dead-lettered job's two attempts oldest first:
  `FAILED`/`RETRYABLE`/`upstream_unavailable` with "Retry delay 1.1s", then
  `FAILED`/`PERMANENT`/`invalid_recipient`. The replacement job links back
  through "Replayed from" and shows "No attempts yet."
- **Workers** — the healthy echo worker, a gracefully stopped one, and the two
  scripted workers, the last three `UNHEALTHY` with `ended_at` set.
- **Queues** — `default`: PENDING 1, QUEUED 2, depth 3, max concurrency 100 in
  its own column.
- **DLQ** — both entries with reason, "#1 of 1 TIMED_OUT" / "#2 of 3 FAILED",
  error code and message, and replay counts 0 and 1.

Before any job existed, the same views rendered their empty states live
("No jobs have been submitted.", "No workers have registered.", "The
dead-letter queue is empty."), and `default` with all-zero depth — a real
migration-seeded row, correctly shown as data rather than as empty. `OFFLINE`
could not be produced as a worker's latest status live (a graceful stop leaves
the session to go stale; `OFFLINE` marks a session a newer boot replaced, which
is then not the latest), so its rendering rests on the Workers view test. All
browser requests were same-origin and returned 200.

### M6D coverage

**`internal/api/dashboard_test.go`.** Unset: `/`, `/dashboard`, `/dashboard/`,
client routes, and assets are the JSON 404. Set: the entry document with its
security headers; client routes fall back to it; hashed assets are immutable
and top-level files are not; a missing asset and dotfiles are JSON 404s; seven
unrouted API-shaped paths stay JSON 404s; a client route with invalid UTF-8 is
a JSON 404 before the router ever sees it; API routes behave identically; the
root redirects only when enabled; writes are 405; HEAD has no body; every
dashboard path shares the span name `GET /dashboard/`; and a missing entry
document is a sanitized 500.

**`internal/dashboard`.** The committed state serves the placeholder; a build
is preferred when present; the placeholder names `make dash-build` and has no
script; and, against a real build, every referenced asset is under
`/dashboard/`, exists in the embedded tree, and no script is inline.

**`dashboard/src`.** Four states for each of six views; a replaced worker
shown by its newest session, and `OFFLINE` as a rendering-only case; an
unrecognized worker status counted rather than dropped; the client's URL
building, headers, and error mapping; the drift test; the hardcoded-values
scan; key storage, forgetting, and routing in `App.test.tsx`, including
malformed escapes answered as not-found instead of a thrown `URIError`.

### M6C gates

All run locally on this branch. PostgreSQL 16, ElasticMQ, and LocalStack from
`make up`; Go 1.27; Python 3.13 in `sdk/python/.venv`.

| Command | Result |
| --- | --- |
| `make lint` | PASS — `gofmt` clean, `go vet ./...` silent |
| `make build` | PASS — seven binaries into `./bin` |
| `make test-unit` | PASS — 18 packages `ok`, up from 13: `internal/metrics` plus the four `cmd/` binaries that had no tests at all |
| `make migrate` | N/A — M6C adds no migration, the first milestone since M5B to add none |
| `make test-integration` | PASS — `ok github.com/co-rtex/TaskForge/tests/integration 89.337s` |
| `make test-race` | PASS — `ok … 96.478s`, no DATA RACE |
| `make sdk-lint` | PASS — mypy success in 21 source files, unchanged by this milestone |
| `make sdk-test` | PASS — 178 passed |
| `docker compose config --quiet` | PASS |

### M6C coverage

**`internal/metrics`.** The enforcement tests check the manifest against the
allowlist and, independently, against the denylist; a third asserts the two
never overlap. `TestNew_PanicsOnAForbiddenLabel` proves the constructor applies
the same rule at startup, which also proves the enforcement tests are not
vacuous. `TestBoundJobType_CapsCardinalityAtTheRegistrySize` drives 10,000
distinct caller-supplied job types through the bound.
`TestJobTypeAppearsOnlyOnWorkerEmittedMetrics` pins which metrics may carry the
one caller-influenced label.

**`internal/api`.** Route-label correctness (pattern, never raw path), one
bounded value for every unmatched path, the status code reaching the counter,
a panicking handler still counted with its real 500, `/metrics` needing no
credential, no forbidden label or leaked query parameter on any exposed series,
and a server without instrumentation behaving exactly as before.

**`cmd/taskforge-{outbox,scheduler,reconciler,worker}`.** The first tests these
binaries have ever had. Liveness unconditional; readiness per-dependency,
failed one at a time, asserting the other components still report `ok`;
`/metrics` parsed by a real exposition-format parser. The worker additionally
asserts the object-store failure in isolation — the specific residual this
milestone closed.

**`tests/integration`.** Durable counters checked against direct row counts;
a live scrape carrying no forbidden label and no uuid-shaped label *value*; the
worker gauge counting a reconciliation-marked `UNHEALTHY` session rather than
only current ones; and `/metrics` still serving against a deliberately-closed
pool, proving a database outage degrades gauges rather than the endpoint.

**Mutation checks.** Each applied, observed to fail the named test, reverted,
and the files confirmed byte-identical by `diff`.

| Mutation | Test that caught it | Observed failure |
| --- | --- | --- |
| Add a `job_id` label to `http_requests_total` | `TestNoUnboundedLabels` | panic at construction: "label job_id is not in AllowedLabels; label job_id is forbidden" |
| Add `job_id` to `AllowedLabels` to defeat the allowlist | `TestAllowedAndForbiddenDoNotOverlap` | the independent denylist still caught it |
| Record HTTP metrics inside `withRecovery` | `TestMetrics_PanickingHandlerIsStillCounted` | the 500 was recorded as 200 |

The second is the one worth noting: it is the attack the two-list design exists
for. An allowlist alone would have accepted it.

### M6B gates

All run locally on this branch. PostgreSQL 16, ElasticMQ, and LocalStack from
`make up`; Go 1.27; Python 3.13 in `sdk/python/.venv`.

| Command | Result |
| --- | --- |
| `make lint` | PASS — `gofmt` clean, `go vet ./...` silent |
| `make build` | PASS — seven binaries into `./bin` |
| `make test-unit` | PASS — 13 packages `ok` (`internal/telemetry` now has tests) |
| Review follow-up | `TestTracing_SpanSurvivesAPanickingHandler` added; verified to fail when the `defer` is reverted |
| `make migrate` | PASS — `migration applied version=18 name=0018_outbox_trace_context.sql`, `migrations complete applied=1` |
| `make test-integration` | PASS — `ok github.com/co-rtex/TaskForge/tests/integration 76.149s` |
| `make test-race` | PASS — `ok … 86.639s`, no DATA RACE, provider shutdown clean |
| `make sdk-lint` / `make sdk-test` | PASS — unchanged by this milestone |
| `docker compose config --quiet` | PASS — and `jaeger` is absent from the default service list |

### M6B coverage

**New unit tests.** `internal/telemetry/tracing_test.go` covers exporter
selection (including that `none` registers **no** provider at all, rather than
merely dropping spans), rejection of an unrecognized exporter, `otlp` requiring
an endpoint, the stdout exporter actually emitting valid JSON carrying the
service name, shutdown being safe when disabled / repeated / on a nil receiver,
shutdown still flushing when handed an already-canceled context — which is the
only way production ever calls it — the W3C round trip, and that every
unparseable `traceparent` is dropped while a well-formed one is adopted. It
also pins that propagation does **not** depend on the global propagator.

`internal/api/tracing_test.go` covers route-pattern span naming, the bounded
name for unrouted paths, attribute boundedness (asserting no query parameter or
credential reaches an attribute), inbound-trace continuation, malformed-header
rejection, and that a disabled server behaves identically.

**New integration tests.** `tests/integration/tracing_test.go`, five tests. The
acceptance criterion is `TestTracing_OneTraceIDReachesEveryHop`, which reads the
persisted `outbox_events.traceparent` **from PostgreSQL** and the published
envelope **off the broker**, rather than inferring either from in-process
context — those two reads are what make it a statement about the system rather
than about Go's context propagation. It also asserts the boundary in the other
direction: worker *registration* continues no client request and must not join
the submission's trace.

**Mutation checks.** Each applied, observed to fail the named test, reverted,
and all three files then confirmed byte-identical to their originals by `diff`.

| Mutation | Test that caught it | Observed failure |
| --- | --- | --- |
| Persist no trace context on submission | `TestTracing_OneTraceIDReachesEveryHop`, `…PublishedEnvelopeCarriesTheTrace` | outbox row and envelope both carried no trace |
| Worker ignores the envelope's trace | `TestTracing_OneTraceIDReachesEveryHop` | "the claim must continue the submitting trace, not start its own" |
| Name the server span from the raw path | `TestTracing_SpanIsNamedForTheRoutePatternNotTheRawPath` | span named `GET /v1/jobs/11111111-…`, and every unrouted path got its own name |
| Rename the span sequentially instead of in a `defer` | `TestTracing_SpanSurvivesAPanickingHandler` | a panicking handler left the span named `GET` |

**Two defects this milestone's own tests caught before it shipped**, both
recorded because neither would have been visible by inspection:

- `withSpanRoute` renamed the span *after* `next.ServeHTTP` rather than in a
  `defer`, so a panicking handler — unwinding past it to `withRecovery` — left
  the span with its provisional method-only name. **Caught incidentally**, by a
  test that panics only because its harness passes a nil store; review flagged
  that as fragile, and `TestTracing_SpanSurvivesAPanickingHandler` now covers it
  on purpose, asserting the recovery log line so the coverage cannot evaporate
  unnoticed.
- With no inbound trace, the claim and execution spans were two separate roots
  for one delivery, because the execution span was started from the
  pre-claim context.

### A test-isolation property worth knowing before writing more tracing tests

`otel.SetTracerProvider` binds package-level delegating tracers through a
`sync.Once` (`otel/internal/global/state.go`). Once any test installs a
recording provider, every package-level tracer in that binary stays bound to it;
swapping the global back to a no-op afterwards does **not** un-bind them.

An earlier draft of the "tracing disabled" integration test did exactly that and
passed in isolation while failing in the suite. It was replaced with
`TestTracing_AnUntracedEventStillPublishesAndExecutesUnderAFreshRoot`, which
produces an untraced event the way production does — through a server-initiated
scheduler promotion — and is therefore order-independent. Production calls
`SetTracerProvider` exactly once at startup, so this is purely a test property,
but it is the kind that produces a flake rather than a failure.

The "zero spans exported with the default" criterion is covered instead by
`TestStartTracing_DisabledRegistersNoProvider` at the unit level, plus the fact
that the entire existing suite runs with tracing at its default and is unchanged
by this milestone.

### M6A gates

All run locally on this branch. PostgreSQL 16, ElasticMQ, and LocalStack from
`make up`; Go 1.27; Python 3.13 in `sdk/python/.venv`.

| Command | Result |
| --- | --- |
| `make lint` | PASS — `gofmt` clean, `go vet ./...` silent |
| `make build` | PASS — seven binaries into `./bin` |
| `make test-unit` | PASS — every package `ok` |
| `make migrate` | PASS — `migration applied version=17 name=0017_operator_read_indexes.sql`, `migrations complete applied=1` |
| `make test-integration` | PASS — three consecutive runs: `ok ... 125.863s`, `120.056s`, `116.461s` |
| `make test-race` | PASS — three consecutive runs: `ok ... 80.991s`, `78.490s`, `120.954s` |
| `make sdk-lint` | PASS — `ruff format --check` 22 files, `ruff check` all passed, `mypy` success in 21 source files |
| `make sdk-test` | PASS — 178 passed |
| `docker compose config --quiet` | PASS — no output |

### M6A coverage

**New Go unit tests.** `internal/jobs/list_test.go` covers the cursor codec
(round trip, UTC normalization, and every malformed shape it must refuse), the
page-size clamp, filter validation including the deliberate choice that a
nonexistent queue is not an invalid filter, and that `NonTerminalStatuses()` is
exactly the complement of `Status.Terminal()`. `internal/workers/list_test.go`
covers the worker cursor and pins that `UNHEALTHY` and `OFFLINE` lie outside
the current-only index's predicate. `internal/api/read_handlers_test.go`
covers validation ordering, the 422 codes, the 404 anti-oracle on the attempts
route, the sanitized 500 for an unwired store, and — asserted on serialized
JSON rather than on Go structs — that a job summary carries no `payload` key,
that an attempt carries no session/lease/outcome identifier, that queue depth
is zero-filled and excludes terminal statuses, and that an empty page omits
`next_cursor` entirely.

**New integration tests.** `tests/integration/read_api_test.go` (17 top-level
tests) and `tests/integration/migrations_m6a_test.go` (4). Scope isolation is
asserted for all three listings with two live credentials and durable row
counts proving both tenants have data. Keyset pagination is exercised with
every timestamp collapsed onto one instant so the id is the only tiebreak.
Worker listing is asserted against a fixture containing one healthy, one
reconciliation-marked `UNHEALTHY`, and one registration-replaced `OFFLINE`
session. Queue depth is compared against a direct `SELECT count(*)` per
`(scope, queue, status)` over a fixture spanning all nine statuses with another
scope's jobs in the same queue.

**Mutation checks.** Each mutation was applied, observed to fail the named
test, and reverted; both mutated files were then confirmed byte-identical to
their originals by `shasum -a 256` before proceeding.

| Mutation | Test that caught it | Observed failure |
| --- | --- | --- |
| Drop `scope = $1` from `ListJobs` | `TestReadAPI_EveryReadIsScopeFiltered` | saw 5 jobs, expected 2 |
| Drop the `id` keyset tiebreak | `TestReadAPI_KeysetPaginationHasNoDuplicatesOrOmissions` | 2 of 7 jobs returned |
| Drop `LIMIT n+1` | `TestReadAPI_NextCursorIsAbsentOnlyOnTheLastPage` | a cursor advertised on a genuinely last page |
| Join current-only sessions in `ListWorkers` | `TestReadAPI_WorkersIncludeCrashedAndReplacedSessions` | 2 workers listed, expected 3 — the crashed one vanished |
| Count terminal statuses in queue depth | `TestReadAPI_QueueDepthMatchesDirectCounts` | `SUCCEEDED` present in `depth` |

**Index usage — manual evidence, not a CI guarantee.** Measured once by hand
against 12,024 seeded jobs across two scopes and three queues, after `ANALYZE`,
with default planner settings. It is recorded because a reviewer should be able
to see the indexes are used; it is **not** an automated assertion, because a
plan over a small test table is a sequential scan regardless of how an index is
defined. No performance claim is made from these numbers.

| Query | Plan | Buffers |
| --- | --- | --- |
| `GET /v1/jobs`, unfiltered | `Index Scan using jobs_scope_keyset_idx` | 4 |
| `GET /v1/jobs?status=QUEUED` | `Index Scan using jobs_scope_status_keyset_idx` | 8 |
| `GET /v1/queues` depth | `Index Only Scan using jobs_scope_queue_depth_idx`, `Heap Fetches: 0` | 8 |
| `GET /v1/workers` | `Index Scan using worker_sessions_latest_per_worker_idx` inside the `LATERAL` | — |

The two-index decision was measured rather than assumed. With
`jobs_scope_keyset_idx` dropped inside a rolled-back transaction, the
unfiltered listing does **not** fall back to
`jobs_scope_status_keyset_idx` — PostgreSQL 16 has no skip scan, so it runs a
`Seq Scan` over 12,024 rows plus a top-N heapsort, touching 280 buffers instead
of 4.

One honest limitation in that evidence: the per-worker active-lease subquery
planned as `Index Scan using leases_active_expiry_idx` with a filter on
`worker_id` rather than using `leases_active_worker_idx`, because the `leases`
table was empty in the measured fixture. No new index was added for it, since
no measurement justifies one yet.

### A flake this milestone introduced and fixed

`make test-race` initially failed intermittently, twice in three full runs, on
a **different** pre-existing test each time
(`TestReconcile_NoDeadlockUnderEveryConcurrentOperation`, then
`TestE2E_DelayedJobIsPromotedAndExecuted`). Neither is touched by M6A, so the
tempting conclusion was an environment flake.

It was not. The cause was M6A's own
`TestReadAPI_ConcurrentInsertsNeverDuplicateOrReorder`, whose inserter
goroutines originally ran an unbounded tight loop for as long as the page walk
took, saturating the shared pgx pool; under the race detector that starved
later timing-sensitive tests into failing. Established by elimination rather
than by inspection: the base commit passed, and the full suite passed twice out
of two with only that one test skipped.

The fix bounds the load — three inserters with a fixed budget each and a
one-millisecond yield between commits — which still interleaves commits with
the walk, which is the property under test. With it, `make test-race` passed
three consecutive times and `make test-integration` three consecutive times.
Recorded here rather than quietly fixed, because a test that destabilizes its
own suite is worth knowing about.

One residual honesty note: during an earlier combined gate sweep,
`make test-integration` failed once and the failing test name was not captured
before the next run passed. Six consecutive clean full runs followed (three
race, three integration), so the suite is stable as it stands, but that single
failure has no recorded cause and is reported rather than omitted.

### M4 gates

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

`0013`'s eligibility rule is stated narrowly, and every clause exists to prove a
row was produced by `0012` rather than to guess that it might have been: the
`dlq_replays` row names the job as a replacement in the same scope; the job's own
`replayed_from_job_id` agrees with that record; `notification_generation` is
still 1, so nothing has promoted or requeued it since; exactly one
`work.available` event references it, which is the one the replay wrote; the
current `last_notification_at` equals that event's `created_at`, which is the
value `0012` writes; and `last_notification_at` is earlier than the job's own
`created_at`, which nothing in M4 ever produces — submission, promotion,
requeue, and re-notification all sample server time at or after the row exists.
A replacement that has since been promoted, requeued, or re-notified fails at
least one clause and is untouched, and once restored the last clause is false,
so the repair is idempotent.

The repair is proven through the real replay path, not hand-written SQL. A
database is taken to `0010`, a job is submitted, claimed, started, and
permanently failed through the control plane so its DLQ entry is written by the
production helper, and the replacement is then created by
`jobs.Store.Replay` — made to wait on the `queues` row lock first, so the
transaction-start event timestamp is observably older than the post-lock job
timestamp rather than merely usually so. Alongside it sits an M4-advanced job
that makes `0011`'s global guard skip, and an untouched three-event M3 history.
The test proves `0012` rewinds the replacement to the event timestamp, `0013`
restores it to exactly what the replay transaction wrote, the advanced job is
unchanged, the legacy history is still reconstructed to generations `1, 2, 3`,
re-running `0013` changes nothing, and a fresh database still applies `0001`
through `0013`.

The per-job reconstruction is proven against mixed state, which is the only
state that distinguishes it from `0011`'s guard. A database is taken to
migration `0010`, seeded with a job M4 legitimately promoted, a job M4
re-notified, an untouched M3 job with three historical notifications, and an
untouched M3 job with one, and then upgraded with the real runner. The two
M4-authored jobs come out byte-identical, generations and event labels
included; the three-event legacy job comes out with generations `1, 2, 3` and
`last_notification_at` set to its newest event rather than its creation time;
and the one-event legacy job, whose stored values were already right, is not
written at all. Re-executing `0012`'s body afterwards changes nothing, and a
fresh database still applies every migration and produces an M4-created job that
provably cannot match the legacy stamp.

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

Configuration and retry-policy tests cover every non-finite float. Each spelling
Go's parser accepts — `NaN`, `nan`, `Inf`, `inf`, `+Inf`, `-Inf`, `Infinity`,
`-Infinity`, `+infinity` — is first asserted to parse to a genuinely non-finite
value, so the cases pin real parser behavior rather than a guess about it, and
each is then shown to fail `Load()` with an error naming the variable. A value
that is present but not a number fails the same way, while an absent or blank
one still takes the documented default. `Config.Validate` and
`RetryPolicy.Validate` both reject a non-finite value by name rather than by
range.

`Delay` is pinned to the exact clamped duration, not to a range: a `NaN` sample
on attempt 1 of a `1s`/`×2`/`0.5`-jitter policy must be exactly `500ms`. A range
assertion could not do this job — "between 0 and Max" is satisfied by the arm64
answer of an entirely unguarded implementation, so the same test would pass on
one machine and fail on another for the same bug. Every non-finite sample is
shown to clamp to the same delay as a sample of `0` (including `+Inf`, because
the finiteness check precedes the range checks), every finite sample at or above
the range to the same delay as the largest float below `1`, and the two clamps
to genuinely different delays so a guard that collapsed everything to one value
would be caught.

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

The sub-millisecond boundary is proved end to end for `1ns`, `500µs`, and
exactly `1ms` bases against a valid `1ms` maximum: each records a retryable
failure, is promoted, is claimed by a second attempt that succeeds, and is then
replayed — and the replay must report the same `RETRY_WAIT` the first response
did, with the whole response equal field for field. The other side of the
boundary is proved the same way: a retryable failure whose injected jitter
reduces the calculation to exactly zero commits as `QUEUED` with a stored delay
of `0`, a later attempt succeeds, and the replay still answers `QUEUED`.

The maximum-duration boundary is tested through the **public** `Delay`, not a
helper, because the unsafe conversion lived on the public path. Both halves use
`Base` and `Max` at `math.MaxInt64` and attempt numbers up to 1000, and they
prove different things:

- **Jitter disabled.** Every case saturates at exactly `2562047h47m16.854s` and
  never zero — this is the case that returned `0s` on amd64 and the ceiling on
  arm64. The jitter source is passed but irrelevant here: with `Jitter: 0` the
  policy never draws a sample, so the matrix of sources (`nil`, the finite
  extremes, out-of-range values, `NaN`, `+Inf`, `-Inf`) is exercising the
  conversion rather than the clamp.
- **Jitter enabled.** The result legitimately depends on the sample, so what is
  asserted is that every case is deterministic, positive, whole-millisecond, and
  within the ceiling — not that each returns the ceiling. The extremes are then
  pinned exactly: the largest finite sample returns the ceiling, while the
  smallest returns half of it, `4611686018428ms`. `NaN`, `+Inf`, and `-Inf` all
  return that same halved value, because a non-finite sample clamps to `0` —
  the finiteness check precedes the range checks, since a non-finite sample is a
  broken source rather than a value that was too large. Full jitter at a sample
  of `0` zeroes the calculation itself and correctly yields an immediate retry.

A table test additionally proves every valid policy returns a whole-millisecond
delay within its own maximum, and the validation tests pin `RetryPolicy.Validate`
and startup configuration validation to the same accept/reject answer across a
shared table of cases.

A replay is proved to answer the decision that committed even after the job has
moved on: a retryable failure is recorded and its whole response captured, the
job is promoted by the real scheduler, a second attempt claims it and succeeds,
and the original failure identity is then replayed — returning the original
`RETRY_WAIT` decision and a response equal to the first one field for field,
with only `replayed` differing. Exhausted, permanent, and cancellation outcomes
are covered the same way, each after the job has advanced as far as its terminal
state allows, so the reconstruction is pinned on every branch rather than only
the one that motivated it.

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
[PROJECT_SPEC.md](PROJECT_SPEC.md) §7 was unmeasured then; M8A measured it (see "M8A").

## Continuous integration

**Five jobs as of M6D.** The three that have run since M3 —
`Format, lint, build, unit and OpenAPI tests`,
`Migrations and integration tests`, and `Race detector (unit and integration)` —
plus `Python SDK (format, lint, types, unit tests)`, added by M5E, and
`Dashboard (format, lint, types, tests, build, embed)`, added by M6D. All run on
GitHub-hosted Linux runners for every pull request targeting `main` and every
push to `main`. See [`.github/workflows/ci.yml`](../.github/workflows/ci.yml).
Each milestone's run is recorded in its own pull request.

The `sdk` job runs a 3.11/3.13 matrix with `fail-fast: false`, pins
`actions/setup-python` by commit SHA like every other action in the file, and
needs neither PostgreSQL nor the broker. It installs and runs the SDK but
publishes nothing, so the workflow's `contents: read` grant is still the whole
grant.

The `dashboard` job has no `actions/setup-node`: it runs `make dash-lint`,
`make dash-test`, and `make dash-build`, so the pinned image in
`dashboard/Dockerfile` stays the one declaration of the Node version. It then
asserts the build left the tree clean and embeds it with
`TASKFORGE_REQUIRE_BUILT_DASHBOARD=1`, which turns the built-output test's skip
into a failure.

The M7A drift check (`tests/verification`) runs inside `make test-unit`, so it
runs in the first job; no job was added.

M8A adds one step to the integration job, after the two demonstrations:
`go run ./scripts/bench smoke`, pointed at the compose services the job already
started with the same `TASKFORGE_*` endpoints. It does not call `make up`, and it
never records a number. The failure-diagnostics step also collects the harness's
`taskforge-bench-*` process logs. No job was added.

The failure path — diagnostic capture and artifact upload — has still not been
exercised by a real hosted failure.

## Deliberately not implemented yet

- `GET /v1/jobs/{job_id}` still returns lifecycle fields and not attempt
  history. That is deliberate and unchanged by M6A: attempt history is served
  by its own route, `GET /v1/jobs/{job_id}/attempts`, for the reason
  `JobResponse` records in code and M5C applied to results — reading a job's
  status must not pull an unbounded body along with it.
- Worker registration authenticates, but every other worker-control route and
  both key-management surfaces (`/internal/v1/api-keys` and
  `/internal/v1/worker-keys`) remain unauthenticated by design, and every
  service still binds to loopback. Anyone who can reach loopback with a
  non-browser client can still mint a credential of either kind for any scope;
  since M6E a web page can no longer do so on their behalf.
- **Closed by M6E, with what remains open.** M6D put a browser origin beside
  the unauthenticated routes, so script in the dashboard's page could have
  administered credentials for every scope, and those routes had always checked
  neither `Host` nor the request's origin, so a website the operator visits could
  send a cross-site request that minted or revoked a key (CSRF), and DNS
  rebinding could read the response. M6E's guard (ADR-0018) now refuses, on every
  registered `/internal/v1` route, a request carrying `Sec-Fetch-Site` or `Origin`
  or addressed to a non-loopback `Host`. Still open, and recorded in M6E below:
  DNS rebinding can still read `/metrics`, `/healthz` and `/readyz`, because the
  guard covers `/internal` only; a browser that sends no Fetch Metadata is covered
  by the `Origin` and `Host` rules alone, so a same-origin `GET` from one still
  reaches the routes; and the guard is not authentication, so any non-browser
  process that can reach loopback can still mint a credential.
- An `index.html` and its hashed assets come from one binary, so several
  `taskforge-api` replicas behind a load balancer during a rolling deploy could
  serve an entry document naming assets another replica lacks. Impossible on
  today's single loopback process.
- Authorization beyond scope is post-V1. A key carries exactly one scope and no
  permission set; there is no RBAC, no per-route permission, and no rate
  limiting.
- Key rotation and expiry are not implemented for either credential type. A
  key lives until it is revoked.
- The operator dashboard is read-only by design — no cancel, retry, replay, or
  bulk replay from the browser, and no search. See "M6D — operator dashboard"
  above for why each is a boundary rather than a TODO. The DLQ listing still
  has no filtering beyond scope and no sorting beyond newest-first.
- `README.md` line 35 ("What does not exist yet: the Python SDK and the
  dashboard") is stale: the SDK shipped in M5E. It is deliberately not edited
  here, because PR #10 rewrites that file wholesale and an edit in this branch
  would conflict with the repository owner's own work.
- The Python SDK is installable from this repository only. Publishing it to
  PyPI is **not planned before M8** — a deliberate scope boundary recorded in
  [ADR-0016](adr/0016-python-sdk-toolchain-and-client-configuration.md), not an
  outstanding task. There is also no async client; it is in no milestone and
  was not added speculatively.
- The operator read surface is no longer missing. `GET /v1/jobs`,
  `GET /v1/jobs/{job_id}/attempts`, `GET /v1/workers` and `GET /v1/queues`
  landed in M6A, together with `taskforge-cli jobs list` / `jobs attempts` /
  `workers list` / `queues list` and the SDK's `jobs.list()`,
  `jobs.attempts()`, `workers.list()` and `queues.list()`. What remains absent
  there is narrower and deliberate: no write operation, no search, no sorting
  beyond keyset order, no cross-scope listing, no lifetime queue totals, and no
  cursor-following iterator helpers on the SDK's new listings.
- There is no automated check that the route table and `api/openapi.yaml`
  describe the same set of routes. `publicRoutes` (in
  `internal/api/auth_test.go`) and `publicOperations` (in
  `internal/api/auth_contract_test.go`) are hand-maintained lists derived from
  two different sources, and they are what makes them agree — but nothing walks
  the mux, so a route added without being added to both is uncovered rather
  than failing. A real completeness gate needs a route registry inside
  `Handler()`; it is a small, separate change and is not claimed here.
  **M7A recorded this as deferred to M8** and did not touch the maps. Two
  hand-maintained, one-directional maps in
  `internal/api/deadline_contract_test.go` are what keeps the document from
  falling behind the handlers — `TestOpenAPI_DocumentsEveryImplementedRouteAndErrorCode`
  (line 236) and `TestOpenAPI_DocumentsEveryImplementedPublicRoute` (line 364) —
  and each walks from its list into the document, never from the mux into the
  list. [ROADMAP.md](ROADMAP.md) carries the deferral under M8.
- Tracing shipped in M6B, but three parts of it are
  deliberately absent rather than pending: the worker's own control-plane calls
  after the claim (start, renew, succeed, fail) are not traced, the Python SDK
  has no tracing, and server-initiated notifications — replay, scheduler
  promotion, abandonment requeue — carry no trace context, because none of them
  continues a client request.
- Three handlers are registered as production worker handlers: `demo.echo`,
  `demo.fail` and `demo.sleep` (M7B, ADR-0019). The set is pinned by
  `TestRegisterTrustedHandlers_PinsTheExactSetTheWorkerDeclares`, and there is
  still no real workload handler. Test-only handlers are still injected through
  the existing registry seam and add no production surface.
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
- Python 3.11 or newer — **only** to build or test the Python SDK in
  `sdk/python` (`make sdk-venv`). Running TaskForge itself never needs one.
- No Node, ever. The dashboard's `make dash-*` targets run it inside Docker.

Run `make bootstrap`, `make up`, `make migrate`, `make dash-build`, and
`make build`, then start the API, outbox publisher, scheduler, worker, and
reconciler as shown in the repository README. The dashboard is at
`http://127.0.0.1:8080/dashboard/`; it asks for an API key created with
`taskforge-cli api-keys create`.

`make demo` and `make demo-failure` build, start the infrastructure, migrate, and
run the two demonstrations; they start and stop their own services, so none of
the processes above needs to be running. They are not yet described in the root
`README.md`: pull request #10 rewrites that file wholesale and owns it, so the
two targets belong in its version.

## Next objective

M8B: CI hardening. See [ROADMAP.md](ROADMAP.md)'s M8B entry. It is not started. It
is Dockerfiles, image builds in CI, and secret and dependency scanning, plus the two
gates M7A deferred: a route registry inside `Handler()` checked against
`api/openapi.yaml`, and the verification-matrix drift check's AST fix. M8C
(deployment) follows it and **needs its own owner decision before any work
starts**: the non-loopback bind, which M6E's note says has to be revisited together
with the Host rule; protection of `/internal`; and the decision ADR-0019 leaves,
whether to gate the demonstration handlers in a deployed worker. Every M7 scenario
can also be watched from the dashboard.

Two follow-ups M8A leaves, neither blocking M8B: the 50.04 s recovery outlier on the
headline run is unexplained, and whether the shipped lease and outbox interval
should change is a question the two records answer only as numbers, not as a
decision.

Smaller items available to whoever wants them, none blocking M8:

- `queue_wait_duration_seconds` and `end_to_end_duration_seconds`, which M6C
  deliberately did not build. See its section above for exactly what each would
  take; both are larger than they look because the clean recording points are
  inside fenced-transition code.
- `README.md` line 35 is still stale ("the Python SDK and the dashboard" do not
  exist yet), and it does not mention `make demo` or `make demo-failure`.
  Untouched through M7B because PR #10 rewrites that file wholesale.
