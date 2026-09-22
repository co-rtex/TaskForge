# TaskForge — Roadmap

Canonical owner of: milestone sequence, per-milestone acceptance criteria, and the
V1/post-V1 boundary.

Actual implementation and verification status lives in
[CURRENT_STATE.md](CURRENT_STATE.md). This file says what to build and in what
order; it does not report progress evidence.

**One milestone per session.** Do not roll forward into the next one.

---

## M1: Durable job ingress and recoverable outbox

**Status:** complete — see [CURRENT_STATE.md](CURRENT_STATE.md) for the evidence.

**Objective.** Accept job submissions durably and idempotently, and deliver a
recoverable work-availability notification to a real local SQS-compatible broker.
Stop before any worker execution.

This is the right first slice because it establishes the authoritative PostgreSQL
model, migrations, API validation, submission idempotency, transactional outbox
behavior, the broker abstraction, broker-outage recovery, and the first real
cross-process integration path — without needing leases or workers.

### Deliverables

- Docker Compose local infrastructure: PostgreSQL + an SQS-compatible broker, with
  health checks, bound to loopback only.
- Migrations for `queues`, `jobs`, `idempotency_records`, `outbox_events`.
- `taskforge-api`: `POST /v1/jobs`, `GET /v1/jobs/{job_id}`, `GET /healthz`, `GET /readyz`.
- Idempotent submission in a single transaction (job + idempotency record + outbox event).
- `taskforge-outbox`: a separately runnable publisher with claim, backoff, retry, and
  restart safety.
- Provider-neutral broker interface with a real SQS-compatible implementation.
- OpenAPI covering only the implemented endpoints.
- Unit tests plus integration tests against real PostgreSQL and a real broker.
- Make targets for the commands that actually work.

### Acceptance criteria

**Database**
- Migrations apply cleanly to a fresh database and are deterministic and versioned.
- Required constraints and indexes exist.
- A failed submission transaction leaves no partial job, idempotency record, or outbox event.
- Concurrent publisher scans never claim the same event twice.

**API**
- A valid immediate submission returns a durable job.
- `GET /v1/jobs/{id}` returns the same persisted state.
- Job and outbox event are created atomically.
- Restarting the API loses nothing.

**Idempotency**
- Concurrent identical submissions (separate connections) create exactly one job, and
  every successful response references it.
- The same key with a different canonical request returns a deterministic conflict.
- Missing key, malformed JSON, unknown fields, invalid queue, invalid priority,
  invalid attempt count, invalid timeout, and oversized payload are all handled
  consistently.
- Scheduled/delayed execution is rejected with an explicit, truthful
  "not implemented in this milestone" error.

**Outbox and broker**
- A real local broker receives the versioned notification.
- The event is marked delivered only after publication succeeds.
- With the broker down after commit, the job stays durable and the event stays
  pending/retryable; restoring the broker publishes it without resubmission.
- Restarting the publisher loses no pending events.
- The publish-before-mark duplicate window is documented and tested.
- A notification never carries the authoritative full job payload.

**Quality and security**
- Local-only; no public unauthenticated exposure.
- No secrets committed; logs carry no full sensitive payloads.
- Formatting, vet, unit, integration, race, and migration checks pass.
- No fabricated performance or reliability claims.

### Explicitly out of scope for M1

Worker registration · worker sessions · claims · worker pools · handlers · attempts ·
leases · heartbeats · scheduler promotion of delayed jobs · retry execution · DLQ ·
cancellation · result storage · API-key persistence · Python SDK · full CLI · React
dashboard · Prometheus/Grafana · tracing beyond minimal seams · failure-injection
harness · load generator · performance claims · Terraform · AWS · Kubernetes · DAGs ·
recurring jobs · autoscaling.

These are documented as planned work. They are **not** scaffolded as empty code.

---

## M2 — Workers, sessions, and atomic claim
**Objective.** A worker registers a process session, claims a job atomically under
priority and capability rules, and executes one trusted handler to completion.
**Deliverables.** `workers`, `worker_sessions`, `leases`, `job_attempts` tables;
claim transaction with `FOR UPDATE SKIP LOCKED`; queue and worker capacity
enforcement; `taskforge-worker` with a bounded pool; the `demo.echo` handler.
**Acceptance.** Exactly one worker wins a contested claim; concurrency limits hold
under load; a job runs end to end; duplicate broker delivery produces at most one
active lease.
**Depends on.** M1 (complete).
**Status:** complete — see [CURRENT_STATE.md](CURRENT_STATE.md) for the evidence.

---

## M3 — Heartbeats, lease renewal, and crash recovery
**Objective.** A killed worker's job is recovered and completed by another worker,
and the dead worker can never commit an outcome afterward.
**Deliverables.** Heartbeat endpoint using server time; lease renewal with fencing;
`taskforge-reconciler`; attempt abandonment; capacity release.
**Acceptance.** Kill a worker mid-job → lease expires → attempt `ABANDONED` →
replacement attempt succeeds; a late completion from the dead process is rejected;
reconciliation is idempotent under repeated and concurrent runs.
**Depends on.** M2.
**Status:** complete — see [CURRENT_STATE.md](CURRENT_STATE.md) for the evidence.

One boundary decision M3 could not avoid is recorded in
[ADR-0009](adr/0009-abandoned-attempts-consume-the-attempt-budget.md): an
abandoned attempt consumes the attempt budget, so recovery requeues while budget
remains and dead-letters when it is gone. That is the minimum needed to avoid a
job no worker could ever claim. Everything else about failure — classification,
backoff, `RETRY_WAIT`, timeouts, the DLQ API, and replay — stays in M4 below.

---

## M4 — Retry, timeout, cancellation, DLQ, replay, delayed jobs
**Objective.** Complete the job lifecycle.
**Deliverables.** Failure classification; exponential backoff with injected jitter;
`RETRY_WAIT`; timeouts; `CANCEL_REQUESTED` delivery and the cancel-vs-complete race;
logical DLQ; replay via `replayed_from_job_id`; `taskforge-scheduler` promoting
delayed jobs and re-notifying stranded work.
**Acceptance.** Retries survive a restart; exhaustion dead-letters; cancel and
success race resolves to exactly one winner; replay preserves terminal history.
**Depends on.** M3 (complete).
**Status:** complete — see [CURRENT_STATE.md](CURRENT_STATE.md) for the evidence.

Three decisions M4 could not avoid are recorded as ADRs. A terminal outcome
carries a lifetime-unique identity, and cancellation, timeout, and abandonment
have a stated precedence
([ADR-0010](adr/0010-durable-outcome-identity-and-terminal-precedence.md)). A
notification generation identifies one eligibility transition, so a stale event
left by the publish-before-mark window cannot suppress the notification a new
transition requires
([ADR-0011](adr/0011-notification-generations-and-bounded-renotification.md)).
Replay creates a linked new job rather than resurrecting a terminal one, and
operator retry is the same operation
([ADR-0012](adr/0012-logical-dlq-and-replay-as-a-new-job.md)).

ADR-0009's boundary held: an abandoned attempt still consumes the attempt budget
and still requeues immediately with no backoff. Only the budget arithmetic is
shared with retry.

---

## M5 — API keys, result storage, CLI, Python SDK

M5 as originally written bundles four independently testable systems:
authentication, result storage with a new object-store dependency, a CLI, and an
SDK. It is split below so each ships on its own evidence, in the order that makes
the earlier ones useful to the later ones. The objective and acceptance criteria
of the original M5 are preserved across M5A–M5D, not reduced: scope is regrouped
here, never silently expanded or shrunk.

### M5A — Database-backed API keys for the public surface
**Objective.** Replace the single configured development scope on the public API
with real, revocable, database-backed credentials, so a caller's scope-isolated
view of jobs and the DLQ is enforced by a credential rather than a constant.
**Deliverables.** `api_keys` with prefix lookup and a hashed secret; scoped
authentication on `POST /v1/jobs`, `GET /v1/jobs/{job_id}`, the cancel, retry,
DLQ listing, and replay routes; a loopback-only mint/revoke/list surface;
constant-time verification; OpenAPI and contract tests covering all of it.
**Acceptance.** Every public route refuses an unauthenticated caller; a key for
one scope cannot see another scope's job; revocation takes effect on the next
request; missing, malformed, unknown, and revoked credentials are
indistinguishable; the migration adds an empty table and changes no M1–M4 row.
**Depends on.** M4 (complete).
**Status:** complete — see [CURRENT_STATE.md](CURRENT_STATE.md) for the evidence.

The decision, its alternatives, and the trust boundary it deliberately does not
move are recorded in
[ADR-0013](adr/0013-database-backed-api-key-authentication.md). One consequence
is load-bearing for what follows: worker claims still filter on
`TASKFORGE_DEV_SCOPE`, so a job submitted with a key minted for any other scope
stays `QUEUED` forever. Multi-tenant keys isolate reads today, not execution.

### M5B — Worker/control authentication
**Objective.** Give the internal worker-control surface its own credential, so
`TASKFORGE_DEV_SCOPE` can be retired and an authenticated scope can actually
execute work.
**Deliverables.** Worker credentials separable from user keys
([PROJECT_SPEC.md](PROJECT_SPEC.md) §6), tied to the process-session lifecycle
that already carries fencing and replacement semantics; authentication on
worker registration, with every later worker-control call trusting the
registered session and a cheap revocation check instead of a re-presented
credential; removal of `TASKFORGE_DEV_SCOPE`. Worker-key management stays
loopback-only and unauthenticated, mirroring M5A's own bootstrapping
precedent for `api_keys` — it is how the first worker credential comes into
existence.
**Acceptance.** Registration authenticates and every other worker-control
route is refused once its session's worker key is revoked; the dev scope from
M1 is gone; a job submitted under any authenticated scope is claimed and
executed by a worker registered for that scope, which closes M5A's recorded
limitation.
**Depends on.** M5A (complete).
**Status:** complete — see [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence and [ADR-0014](adr/0014-worker-control-authentication.md) for the
decision.

### M5C — Result storage
**Objective.** Small and large results round-trip.
**Deliverables.** Inline results in PostgreSQL with a defined threshold, large
results in an S3-compatible object store, and the retrieval endpoint.
**Acceptance.** Small and large results round-trip; the threshold is explicit and
tested on both sides of the boundary.
**Depends on.** M5B, so result retrieval is authenticated on arrival rather than
retrofitted onto an endpoint that serves job output.
**Status:** complete — see [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence and [ADR-0015](adr/0015-result-storage.md) for the decision.

The object key a worker uploads a large result to is attempt-scoped, not
job-scoped, by deliberate decision — see ADR-0015. An attempt whose object
upload succeeds but whose process then dies before it reports success
leaves that object permanently orphaned: a storage cost, never a
correctness problem, and left for later rather than fixed here. See
ADR-0015's "Known limitation" section.

M5D as originally written bundles two genuinely independent deliverables in
two different languages: `taskforge-cli` (a new Go binary in the same
module, needing no new toolchain) and a Python SDK (a first language this
repository has never contained, needing its own dependency-management,
lint/format, and CI-job decisions). Their own acceptance sentence already
separates them by clause — "CLI exit codes are stable and output is
machine-readable" is a fact about the CLI alone; "the SDK places an
idempotency key in the canonical header" is a fact about the SDK alone —
the same signal that justified M5A/M5B shipping independently. It is split
below the same way M5 itself was split into M5A–M5D: each half ships on its
own evidence, and the combined objective and acceptance criteria of the
original M5D are preserved across M5D/M5E, not reduced.

### M5D — CLI
**Objective.** Make the TaskForge public API and its loopback-only
credential-management routes usable by an outside developer from the
command line, without reimplementing the credential model.
**Deliverables.** `taskforge-cli` (`cmd/taskforge-cli`, `internal/cli`):
commands for every implemented public route (job submission, read, result
retrieval, cancel, retry, DLQ list and replay) and the API-key/worker-key
management routes; a stable, documented, individually-tested exit-code
contract; JSON output on stdout for success and on stderr for failure.
**Acceptance.** CLI exit codes are stable and output is machine-readable.
**Depends on.** M5C.
**Status:** complete — see [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence.

`GET /v1/jobs` (list), `GET /v1/workers`, and `GET /v1/queues` were listed as
V1-target routes in [PROJECT_SPEC.md](PROJECT_SPEC.md) §4 but were not
implemented anywhere in this API when M5D shipped, so `taskforge-cli` had no
`jobs list` / `workers list` / `queues list` command: a CLI command with no
backend route to call would be exactly the fabricated functionality
[PROJECT_SPEC.md](PROJECT_SPEC.md) §5 forbids. **M6A implemented those routes
and those commands**, closing this gap.

### M5E — Python SDK
**Objective.** Make TaskForge usable by an outside Python developer, via a
typed, installable SDK that obtains and presents API keys rather than
reimplementing the credential model.
**Deliverables.** A typed, installable Python SDK.
**Acceptance.** The SDK places an idempotency key in the canonical header.
**Depends on.** M5D.
**Status:** complete — see [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence and [ADR-0016](adr/0016-python-sdk-toolchain-and-client-configuration.md)
for the decision.

Introducing Python was a first for this repository, and the four
infrastructure decisions it forced — dependency and build tooling, a
lint/format story, a packaging story for V1, and its CI placement — were
this milestone's first work rather than an implicit side effect of "write
an SDK". All four are recorded in
[ADR-0016](adr/0016-python-sdk-toolchain-and-client-configuration.md):
PEP 621 with stdlib `venv`/`pip` and one runtime dependency (`httpx`),
`ruff` plus `mypy --strict` on their own `sdk-*` Make targets, installable
from this repository with PyPI publication deferred past V1, and its own CI
job rather than a fold into an existing one.

The SDK reads `TASKFORGE_SDK_API_URL` and `TASKFORGE_SDK_API_KEY`, its own
variables — not `TASKFORGE_API_ADDR` and not the CLI's pair. A CLI is
invoked deliberately; an SDK is a library inside someone else's process, so
ambient credential pickup is a different risk there.

`GET /v1/jobs` (list), `GET /v1/workers`, and `GET /v1/queues` were still
unimplemented when M5E shipped, so the SDK had no `jobs.list()` /
`workers.list()` / `queues.list()` method, for the same reason `taskforge-cli`
had no such command. **M6A implemented those routes and those methods**,
closing this gap.

---

## Remaining V1 milestones

## M6 — Observability and health

M6 as originally written bundles four independently testable systems across
three technical domains: OpenTelemetry tracing, Prometheus metrics,
liveness/readiness per service, and a full operator dashboard — the last of
which is a frontend in a language this repository has never contained, and
which cannot read live data at all until four public routes exist that do not.
It is split below the same way M5 was split into M5A–M5E: each slice ships on
its own evidence, in the order that makes the earlier ones useful to the later
ones. The objective and acceptance criteria of the original M6 are preserved
across M6A–M6D, not reduced.

Its acceptance sentence already separates by clause — "a submission is
traceable end to end" is a fact about tracing alone, "no unbounded metric
labels" about metrics alone, and "the dashboard reads live APIs ... nothing is
hardcoded" about a dashboard that has live APIs to read. That last clause is
what forces the first slice.

One correction the split records rather than inherits. M6's original
deliverable list says "real liveness/readiness per service", but every
long-running service has exposed a distinct `GET /healthz` and `GET /readyz`
pair since M1–M4 — see [ARCHITECTURE.md](ARCHITECTURE.md) §14, which is marked
`[PARTIAL]` for exactly this reason and states that tracing and metrics are the
parts that remain. The deliverable as written was stale. What is genuinely
outstanding is narrower and moves to M6C: the four non-API health servers have
no test coverage at all, and `taskforge-worker`'s readiness does not check the
object store M5C made it depend on.

### M6A — Operator read APIs
**Objective.** Make job state, attempt history, worker capacity and health, and
queue depth readable through the authenticated public API, the CLI, and the
Python SDK.
**Deliverables.** `GET /v1/jobs` (keyset-paginated, scope-filtered, no
payloads); `GET /v1/jobs/{job_id}/attempts` (the full attempt timeline,
unpaginated); `GET /v1/workers` (each worker joined to its most recent session
whatever its status); `GET /v1/queues` (non-terminal depth per status);
migration 0017's index set; `taskforge-cli jobs list` / `jobs attempts` /
`workers list` / `queues list`; and the SDK's `jobs.list()`,
`jobs.attempts()`, `workers.list()` and `queues.list()`.
**Acceptance.** Every read is scope-isolated; keyset pagination neither
duplicates nor omits a job present throughout a walk; list responses carry no
payload and no session, lease, or outcome identifier; queue depth matches a
direct count and excludes terminal statuses; a crashed or replaced worker is
still listed.
**Depends on.** M5E.
**Status:** complete — see [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence.

This slice is a genuine prerequisite rather than part of "the dashboard", and
is named as one. `GET /v1/jobs`, `GET /v1/workers` and `GET /v1/queues` are
V1-target routes in [PROJECT_SPEC.md](PROJECT_SPEC.md) §4 that M5D and M5E each
deferred with the same sentence — "these commands land whenever those routes
do" — and the dashboard's own acceptance clause ("reads live APIs ... nothing
is hardcoded") is unsatisfiable without them. A fourth route,
`GET /v1/jobs/{job_id}/attempts`, is required by M6D's "Job detail with attempt
timeline" and by [PROJECT_SPEC.md](PROJECT_SPEC.md) §4 item 4; it is a separate
route rather than an expansion of `GET /v1/jobs/{job_id}`, following the
precedent M5C set with `GET /v1/jobs/{job_id}/result` and the reason
`JobResponse` already records in code: reading a job's status must not pull an
unbounded body along with it.

Two decisions M6A could not avoid are recorded in migration 0017 itself rather
than as an ADR, because each constrains one query rather than future work.
`GET /v1/jobs` needs **two** keyset indexes, not one: PostgreSQL 16 has no
index skip scan, so an index with `status` between the equality column and the
ordering columns cannot serve the unfiltered listing. And `GET /v1/workers`
deliberately does **not** use `worker_sessions_one_current_per_worker_idx`,
whose predicate is `status IN ('STARTING','HEALTHY','DRAINING')`:
reconciliation marks a crashed worker's session `UNHEALTHY` and a replacement
registration marks the prior one `OFFLINE`, so a listing built on that index
would silently omit exactly the workers an operator opened the page to find.

### M6B — OpenTelemetry tracing
**Objective.** One trace id follows a submission from the API through the
PostgreSQL transaction, across the process boundary into `taskforge-outbox`,
through the broker message, into the claim, and onto worker execution.
**Deliverables.** `internal/telemetry` tracing provider with three exporter
modes and a bounded shutdown; migration 0018's `outbox_events.traceparent` /
`tracestate`; an optional envelope `trace` member; server spans on every route,
named for the route pattern; a submission-transaction span; worker claim and
execution spans; and the ARCHITECTURE §3 correction below.
**Acceptance.** A submission is traceable end to end.
**Depends on.** M6A, so a complete route surface is instrumented once.
**Status:** complete — see [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence.

The note this entry carried for its successor is now discharged.
[ARCHITECTURE.md](ARCHITECTURE.md) §3 was marked `[IMPLEMENTED]` while claiming
a broker message carries "trace metadata"; it did not, and now does. The trace
context is persisted in the same transaction as the event, because
`taskforge-outbox` is a separate process that publishes later and can recover
it no other way.

Two decisions M6B could not avoid are recorded in the code rather than as an
ADR, each constraining one contract rather than future work. The **envelope is
additive-only while the `data` member is versioned** — M6B's optional `trace`
member therefore did not bump `WorkAvailableSchemaVersion`, and that
distinction, previously unwritten, is now stated in `internal/outbox/event.go`.
And the **server span is named from the matched route pattern**, which forced
the tracing middleware into two parts: `net/http` populates `Request.Pattern`
only inside `ServeMux.ServeHTTP`, on the exact pointer the mux is handed, so
the name cannot be known where the span must start.

Tracing is disabled by default and every trace boundary drops a value it cannot
parse. Deliberately **not** in scope: the worker's own control-plane calls after
the claim, tracing in the Python SDK, and trace context on server-initiated
notifications (replay, scheduler promotion, abandonment requeue), none of which
continues a client request.

### M6C — Prometheus metrics and the health residual — **planned**
**Objective.** Expose the metric set [ARCHITECTURE.md](ARCHITECTURE.md) §14
specifies, with an enforcement mechanism a reviewer can run rather than trust.
**Acceptance.** No unbounded metric labels.
**Depends on.** M6A. Independent of M6B.

`job_type` is the one caller-reachable cardinality hole: `jobs.job_type` has no
foreign key and no allowlist, only the regex `^[a-z0-9][a-z0-9._-]{0,127}$`, so
an authenticated caller can mint a new label value per submission. `queue` is
FK-constrained and safe; `scope` is bounded but is a tenancy identifier on an
unauthenticated `/metrics`. This slice also carries the health residual
described above.

### M6D — Operator dashboard — **planned**
**Objective.** Overview, Jobs, Job detail with attempt timeline, Workers,
Queues, and DLQ, reading only M6A's live routes.
**Acceptance.** The dashboard reads live APIs and handles loading, empty, and
error states; nothing is hardcoded.
**Depends on.** M6A for data; M6B and M6C for anything it links to.

A frontend toolchain is a first for this repository in the way Python was for
M5E, and deserves the same treatment [ADR-0016](adr/0016-python-sdk-toolchain-and-client-configuration.md)
gave that one: dependency and build tooling, a lint/format story, and CI
placement each decided and recorded, not left as an implicit side effect of
"build the dashboard". The binding constraint is
[PROJECT_SPEC.md](PROJECT_SPEC.md) §5's prerequisite list, which ADR-0016 could
discharge for Python by noting that running TaskForge never needs an
interpreter — an escape that does not exist for a dashboard that is part of the
running stack.

### M7 — Full concurrency, restart, failure, and race suites
**Objective.** Prove the invariants.
**Deliverables.** Automation for all twelve required scenarios: end-to-end success;
worker crash and replacement; late-completion rejection; concurrent duplicate
submission; idempotency conflict; outbox recovery after broker outage; duplicate
broker delivery; concurrent worker claims; cancel-vs-success race; timeout and retry
exhaustion; process-restart durability; stranded-notification recovery. Plus
`make demo` and `make demo-failure`.
**Acceptance.** Every invariant in [ARCHITECTURE.md](ARCHITECTURE.md) §12 has a test
that asserts durable state; the race detector is clean.
**Depends on.** M6 (M6A complete; M6B-M6D planned).

### M8 — Load generator, measured benchmarks, CI hardening, ECS Terraform
**Objective.** Measure reality and make deployment credible.
**Deliverables.** Load generator; reproducible benchmark harness recording SHA,
environment, command, and limitations; CI covering fmt, vet, unit, race, migrations,
integration, e2e, SDK, frontend, Docker builds, secret and dependency scanning;
validated (not applied) Terraform for ALB, ECS services, RDS, SQS, S3, secrets, logs.
**Acceptance.** Benchmark numbers replace the "unmeasured target" table in
[PROJECT_SPEC.md](PROJECT_SPEC.md) §7 with recorded results; CI is green and no
workflow is permanently failing; `terraform validate` passes without applying.
**Depends on.** M7.

---

## Post-V1

Recurring schedules with timezone and misfire policy · workflow DAGs · fan-out and
fan-in · weighted fairness · aging and quotas · CPU/memory/GPU/architecture resource
classes · affinity and anti-affinity · autoscaling · gRPC · multi-tenancy and RBAC ·
Helm and Kubernetes · isolated process or container execution (the prerequisite for
hard cancellation of uncooperative handlers).

V1 scope may be regrouped here, but never silently expanded or reduced.
