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

## Current milestone — M5C: result storage

### M5C — Result storage
**Objective.** Small and large results round-trip.
**Deliverables.** Inline results in PostgreSQL with a defined threshold, large
results in MinIO/S3, and the retrieval endpoint.
**Acceptance.** Small and large results round-trip; the threshold is explicit and
tested on both sides of the boundary.
**Depends on.** M5B, so result retrieval is authenticated on arrival rather than
retrofitted onto an endpoint that serves job output.
**Status:** not started.

### M5D — CLI and Python SDK
**Objective.** Make TaskForge usable by an outside developer.
**Deliverables.** `taskforge-cli`; a typed, installable Python SDK. Both obtain
and present API keys rather than reimplementing the credential model.
**Acceptance.** CLI exit codes are stable and output is machine-readable; the SDK
places an idempotency key in the canonical header.
**Depends on.** M5C.
**Status:** not started.

---

## Remaining V1 milestones

### M6 — Observability and health
**Objective.** Make behavior visible.
**Deliverables.** OpenTelemetry tracing across the full path; Prometheus metrics with
bounded label cardinality; real liveness/readiness per service; the operator dashboard
(Overview, Jobs, Job detail with attempt timeline, Workers, Queues, DLQ).
**Acceptance.** A submission is traceable end to end; no unbounded metric labels; the
dashboard reads live APIs and handles loading, empty, and error states; nothing is
hardcoded.
**Depends on.** M5D.

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
**Depends on.** M6.

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
