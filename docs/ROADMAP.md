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

### M6C — Prometheus metrics and the health residual
**Objective.** Expose the metric set [ARCHITECTURE.md](ARCHITECTURE.md) §14
specifies, with cardinality enforced by a mechanism a reviewer can run rather
than a rule trusted by inspection; and close the health residual this split
identified.
**Deliverables.** `internal/metrics` with an allowlist, an independent denylist,
a startup-time check and two enforcement tests; `GET /metrics` on all five
services; HTTP request metrics reusing M6B's route-pattern computation;
`objectstore.Ping` wired into the worker's readiness; the four background
services' health servers refactored to named closure checks, and their first
tests.
**Acceptance.** No unbounded metric labels.
**Depends on.** M6A (the latest-session query shape). Independent of M6B.
**Status:** complete — see [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence.

Three decisions M6C could not avoid are recorded in code rather than as an ADR,
each constraining one contract.

`job_type` is the one caller-reachable label — `jobs.job_type` has no foreign
key and no allowlist — and is bounded against the **worker's own handler
registry** rather than a configured allowlist. The registry is already
authoritative and already bounded; a configured list would be a second copy of
it to keep in sync by hand. That is also why the label appears only on
worker-emitted metrics: the API has no registry to bound it against.

The job-lifecycle totals and every gauge are **derived from PostgreSQL at scrape
time** rather than counted in process. They are facts about the whole system
rather than one replica, and deriving them keeps instrumentation out of the
fenced-transition code that owns correctness.

`/metrics` sits on each service's **existing** listener rather than a new admin
one. `TASKFORGE_API_ADDR` is already validated as a loopback bind exactly like
the four background addresses, so there is no live trust-boundary difference to
protect — and §14 records what must be revisited before this API sits behind a
real load balancer.

Two families §14 named are deliberately **not** built:
`queue_wait_duration_seconds` and `end_to_end_duration_seconds`. Both are
histograms, so unlike the counters they cannot be derived at scrape time, and
recording them means either observing inside fenced-transition code or widening
a wire contract. §14 and [CURRENT_STATE.md](CURRENT_STATE.md) both say so and
say what each would take.

### M6D — Operator dashboard
**Objective.** Overview, Jobs, Job detail with attempt timeline, Workers,
Queues, and DLQ, reading only M6A's live routes.
**Deliverables.** `dashboard/` (React + TypeScript + Vite) with a hand-written
client and a drift test against `api/openapi.yaml`; `dashboard/Dockerfile` as
the whole Node toolchain; `internal/dashboard`'s `go:embed` with a committed
not-built placeholder; `internal/api`'s optional `WithDashboard`, serving
same-origin under `/dashboard/`; `make dash-fmt` / `dash-lint` / `dash-test` /
`dash-build`; a `dashboard` CI job; and
[ADR-0017](adr/0017-dashboard-toolchain-and-serving.md).
**Acceptance.** The dashboard reads live APIs and handles loading, empty, and
error states; nothing is hardcoded.
**Depends on.** M6A for data; M6B and M6C for anything it links to.
**Status:** complete — see [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence. **With it, M6 as a whole is complete:** every clause of the original
M6 objective and acceptance sentence is now discharged by one of M6A–M6D.

The binding constraint this entry named — [PROJECT_SPEC.md](PROJECT_SPEC.md)
§5's prerequisite list, with no ADR-0016-style "running never needs it" escape
available — is met by containment rather than by amendment: Node runs only
inside a digest-pinned build image, so §5's list is unchanged and the host needs
no Node for anything. ADR-0017 records that and the other decisions M6D could
not avoid: mounting under `/dashboard/` rather than `/`, so the existing JSON
404 catch-all is untouched; the operator's key in `sessionStorage` behind a
same-origin-only CSP, with the origin it shares with the unauthenticated
loopback key-administration routes recorded as an accepted limitation whose
guard was left as the owner's decision, and has since been built as M6E; Biome as the single lint/format tool; and a CI job that
runs the Make targets rather than `actions/setup-node`.

Deliberately **not** in scope, as boundaries rather than TODOs: every write from
the browser (cancel, retry, replay, bulk replay), because each requires a
caller-chosen `Idempotency-Key` whose browser-side minting is its own design
question and because it would stack on a new credential story in the same
milestone; free-text search; auto-refresh, websockets, and SSE; and a
server-side credential proxy.

### M6E — Browser-origin guard on the internal surface (follow-up to ADR-0017)
**Label.** A follow-up to M6, **not** a fifth slice of it. M6's objective and
acceptance sentence were already discharged in full by M6A–M6D, and this entry
does not reopen them. It exists because [ADR-0017](adr/0017-dashboard-toolchain-and-serving.md)
recorded two exposures it deliberately did not fix and named a bounded
follow-up for them.
**Objective.** Close the two exposures ADR-0017 recorded: a script in the
dashboard's origin reaching the unauthenticated `/internal` key administration,
and cross-site CSRF and DNS rebinding against those routes, which dates from
M5A.
**Deliverables.** A stateless guard on every registered `/internal/v1` route
that refuses, with `403` and the new `Error.code` `origin_refused`, a request
carrying `Sec-Fetch-Site` or `Origin` (any value) or addressed to a non-loopback
`Host`, before authentication, before the `405`, and before any handler reads
the body; the shared `internal/loopback` predicate the bind rule and the Host
rule both call; the `403` documented on all 14 `/internal` operations and in the
`Error.code` enum; the CLI and Python SDK mappings; and
[ADR-0018](adr/0018-browser-origin-guard-on-the-internal-surface.md).
**Acceptance.** Every operation `api/openapi.yaml` documents under `/internal`
refuses each browser marking with `403`, without reaching a handler or a
dependency; loopback Hosts pass untouched; the guard runs before authentication
and before the `405`; `/v1`, `/dashboard/`, `/metrics` and the health probes are
unchanged; and a refused request keeps its route pattern as its span name and
metric label.
**Depends on.** M6D, whose dashboard origin made the exposure larger.
**Status:** complete; see PR #17 and [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence and for what remains open.

Deliberately **not** in scope: authentication on `/internal`, CORS headers, a
separate listener, a configurable Host allowlist, and guarding the whole
listener, which would also close DNS rebinding's read of `/metrics`. The last two
belong with M8's deployment work, which has to revisit the loopback bind and the
Host rule together.

### M7 — Full concurrency, restart, failure, and race suites

M7 as originally written bundles two different things: proving the invariants,
and two demonstration targets (`make demo`, `make demo-failure`). The first is an
audit of what the tests already prove and the closing of what they do not; the
second is new runnable behavior whose handlers raise a trust-boundary question
the first never touches. It is split the same way M5 and M6 were, so each slice
ships on its own evidence. The original objective, deliverables, and acceptance
sentence are preserved across M7A and M7B, not reduced.

**Status:** complete — both slices are; see M7A and M7B below.

#### M7A — Invariant and scenario proof audit
**Objective.** Prove the invariants: establish, row by row, which test proves
each reliability invariant and each required scenario, and close every row whose
test does not assert durable state.
**Deliverables.** An audit of all eighteen invariants in
[ARCHITECTURE.md](ARCHITECTURE.md) §12 and all twelve required scenarios, recorded
in [VERIFICATION_MATRIX.md](VERIFICATION_MATRIX.md); a new or extended test in
`tests/integration` for every gap the audit found; a drift check over the matrix
that needs no infrastructure and runs under `make test-unit` and in CI; and the
real-binary crash test's environment made hermetic. The twelve required
scenarios are: end-to-end success; worker crash and replacement; late-completion
rejection; concurrent duplicate submission; idempotency conflict; outbox recovery
after broker outage; duplicate broker delivery; concurrent worker claims;
cancel-vs-success race; timeout and retry exhaustion; process-restart
durability; stranded-notification recovery.
**Acceptance.** Every invariant in [ARCHITECTURE.md](ARCHITECTURE.md) §12 has a
test that asserts durable state; the race detector is clean.
**Depends on.** M6 (complete) and M6E.
**Status:** complete; see PR #18 and [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence.

Deliberately **not** in scope: any production code, any demo handler, any new
Make target, and the route-table-versus-OpenAPI completeness check, which is
deferred to M8 (see below).

#### M7B — `make demo` and `make demo-failure`
**Objective.** Two runnable demonstrations of the system's behavior, one of
success and one of failure and recovery.
**Deliverables.** `make demo` and `make demo-failure`, a Go program at
`scripts/demo` that runs the real binaries; `demo.sleep` and `demo.fail`
registered in the production worker, with strict and bounded payloads;
[ADR-0019](adr/0019-demo-handlers-are-trusted-built-ins.md); three integration
tests over real PostgreSQL; and both demonstrations run in CI's integration job.
**Acceptance.** Each target exits zero only if every one of its expectations
holds, against the real binaries, and non-zero if any expectation fails or any
wait times out.

- `make demo`: an echo job is `SUCCEEDED` after one attempt and its result read
  back equals its payload; a retryable `demo.fail` job with `max_attempts=3`
  fails three times, with a retry time recorded on the first two, and is
  `DEAD_LETTERED` with the exhausted-budget reason; a permanent `demo.fail` job
  fails once and is `DEAD_LETTERED` at once with the permanent-failure reason.
- `make demo-failure`: a worker is `SIGKILL`ed while its attempt is `RUNNING`;
  that attempt is `ABANDONED` and bound to that worker's session, a second attempt
  on a second worker, under a different session, is `SUCCEEDED`, and so is the
  job. A second worker is `SIGSTOP`ped mid-attempt, its attempt is abandoned and
  finished by another worker, and after the frozen worker resumes and has had time
  to act, the job is still `SUCCEEDED`, the first attempt still `ABANDONED`,
  exactly one result exists and it is the second attempt's, and no third attempt
  exists. The pass condition is the stored outcome, not anything a worker logs.
- Both assert only on the jobs they submitted, in a scope of their own, never
  delete or truncate anything, and leave no process behind on any way out.

**Depends on.** M7A.
**Status:** complete; see PR #20 and [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence and for what remains limited.

The demo-handler trust decision this entry carried is made and recorded in
[ADR-0019](adr/0019-demo-handlers-are-trusted-built-ins.md): the handlers are
compiled into the production worker, unconditionally, with the bounds and the
abuse analysis stated there. [AGENTS.md](../AGENTS.md) §10 is unchanged.

Deliberately **not** in scope: a handler that fails and then succeeds, which
would need the attempt number in `Execution` (the retry is shown by a retryable
failure spending its whole budget instead); any change to the runner, a
migration, the API, OpenAPI, the SDK, or the dashboard; the two M8 deferrals
below; and the root `README.md`, which pull request #10 rewrites wholesale and
which is where `make demo` belongs.

### M8 — Load generator, measured benchmarks, CI hardening, ECS Terraform

M8 as originally written bundles three different things: measuring (a harness
and a methodology, and a machine to run them on), hardening CI (images, scanners,
and two gates M7A deferred), and deployment (a trust-boundary decision and
infrastructure that costs money). It is split into slices so each ships on its own
evidence, the way M5, M6 and M7 were: M8A measures, M8B is the supply chain
(images and scanning), M8D carries the two gates M7A deferred together with four
items the M8A review raised, and M8C is deployment. The owner split M8D in two
when M8D began: M8D1 is the route registry and nothing else, and M8D2 is the rest.
When M8D2 began the owner split it again: the 50.04 s recovery-outlier investigation
moved to M8D3, which needs an experiment and not a documentation change. The original
objective, deliverables, and acceptance sentence are preserved across them, not
reduced.

**Order: M8B, then M8D1, then M8D2, then M8D3, then M8C.** M8C keeps its name.

**Status:** M8A is complete; see PR #21. M8B is complete; see PR #22. M8D1 is
complete; see PR #23. M8D2 is complete; see PR #24. M8D3 is complete; see PR #25. M8C is
planned.

#### M8A — Load generator and measured benchmarks
**Objective.** Measure reality: replace PROJECT_SPEC §7's unmeasured targets with
recorded results.
**Deliverables.** A load generator and benchmark harness at `scripts/bench`
(`make bench`, `make bench-smoke`) that runs the real binaries with the stack
`scripts/demo` shares (`scripts/internal/stack`); the measurement queries the
harness and `tests/integration` share (`scripts/readdb`);
[ADR-0020](adr/0020-benchmark-methodology.md); a smoke step in CI's integration
job that records nothing; and a record under [docs/benchmarks](benchmarks/) naming
the commit, the environment, the command, the settings and the limitations. Three
carried items: the stack's PostgreSQL connection is read-only, ADR-0019's capacity
bound is stated per attempt, and the demonstration handlers match payload keys
exactly.
**Acceptance.** [PROJECT_SPEC.md](PROJECT_SPEC.md) §7 carries a measured value and
a Met or MISSED verdict for each target, linked to a record that a reproducible
run produced on a clean tree; a target that was missed is recorded as missed next
to the settings that decided it; and CI never records a number.
**Depends on.** M7.
**Status:** complete; see PR #21 and [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence and for what remains limited.

Deliberately **not** in scope: Dockerfiles, scanners, the route-table registry,
the drift-check fix, Terraform, the non-loopback bind, and any change to
production behavior, the schema, the API, OpenAPI, the SDK, the dashboard or the
runner other than the demo handlers' payload decoding. A saturation search is not
in scope either: the owner fixed the workload and the offered load.

#### M8B — Supply chain (container images and scanning)
**Objective.** Make the repository build container images and scan its supply
chain, with scanners that block CI.
**Deliverables.** One root `Dockerfile` with a builder stage and one final target
per service (api, outbox, scheduler, reconciler, worker, migrate; no CLI image) on
a digest-pinned distroless nonroot base, built after `make dash-build`; `make
images` and `make images-smoke` (`scripts/imagesmoke`); a drift check between the
Dockerfile's Go version and `go.mod`; `scripts/scan` and `make scan`, which run
govulncheck, gitleaks, pip-audit and npm audit at pinned versions (pip-audit with its
whole dependency tree hash-locked) and apply two
acceptance mechanisms with distinct meanings: `security/scan-exceptions.yaml`, a dated
risk acceptance for govulncheck, pip-audit and npm audit only, and `.gitleaks.toml`, the
only place a gitleaks finding is accepted, as a fixture (a fake value) or a revoked
secret pinned to its commit; the `images` and `scan` jobs in CI; and
[ADR-0021](adr/0021-container-images-and-supply-chain-scanning.md).
**Acceptance.** CI builds the six images and checks that each runs as a non-root
user, rejects an invalid configuration with the specific message, and that the
migrate image applies every embedded migration into an empty database and leaves it
at the embedded schema version, and the api image serves the real dashboard build; the
four scanners block CI on any finding not accepted by its tool's mechanism (an
unexpired dated entry for the three dependency scanners; a fixture or revoked entry
for gitleaks); and no workflow is permanently failing.
**Depends on.** M8A.
**Status:** complete; see PR #22 and [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence and for what remains limited.

Deliberately **not** in scope: any change to production code, the schema, the API,
OpenAPI, the SDK or the dashboard; a registry push or registry credentials;
multi-architecture builds in CI; an image vulnerability scanner; Dependabot; and the
items that moved to M8D.

#### M8D1 — The route registry
**Objective.** Close the first of the two gates M7A deferred: make every route
`taskforge-api` registers one entry of a table, and prove the table agrees with
`api/openapi.yaml`.
**Deliverables.** One route table in `internal/api` (method, path, surface, wrapper
chain, feature group, handler, and for an unlisted route a reason) from which
`Handler()` registers every route, every derived `405` fallback and the `/` catch-all,
and nothing else; an AST check over the package's non-test files that nothing
registers around it; table-versus-spec, unlisted-entry, wrapper, fallback and
feature-group tests that replace the hand-maintained `publicRoutes` and
`publicOperations` lists and the test that counted `s.handleInternal(mux,` lines; a
behavior golden generated from the hand-registered server and left byte-identical by
the refactor; and [ADR-0022](adr/0022-the-route-table-is-the-single-source-of-routes.md).
Routes the spec does not document (`/metrics`, `/dashboard/`, `/{$}`) stay out of it
as explicit unlisted entries with reasons; the spec is not edited.
**Acceptance.** Every route is registered from the table and the AST check proves
nothing registers around it; the table equals the spec in both directions and
unlisted entries are enforced; the golden was generated before the refactor and the
refactor leaves it unchanged; and no status, `Allow` header, error code, wrapper
order, matched pattern, span name or metric route label changed.
**Depends on.** M8B.
**Status:** complete; see PR #23 and [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence and for what remains limited.

Deliberately **not** in scope: the verification-matrix drift check, the four M8A
review items, the `make bench-smoke` flake, any change to `api/openapi.yaml`,
`scripts/bench`, the SDK, the CLI, the dashboard, migrations or the schema, and a
runtime route listing.

#### M8D2 — The matrix drift check, the bench-smoke flake, and the M8A documentation items
**Objective.** Close the other gate M7A deferred, the one flake M8B recorded, three of
the four items the M8A review raised, and the gap M8D1 recorded in the route rule.
**Deliverables.**
- A verification-matrix drift check that finds the cited assertion in the syntax tree
  instead of trusting a line number: a cited line must be covered by an assertion call
  on the test's own goroutine (see [VERIFICATION_MATRIX.md](VERIFICATION_MATRIX.md)).
- `make bench-smoke`'s flake: its fault phase submits 3 s jobs and aims its kill at an
  attempt that PostgreSQL shows has time left, so a kill can no longer land on an
  attempt that is about to finish, and a miss fails its own check and is not read as a
  recovery failure. Recorded runs are untouched.
- The benchmark record renderer's extra newline.
- The run-time estimate in [AGENTS.md](../AGENTS.md) and the Makefile, from the
  committed records.
- [ADR-0023](adr/0023-the-throughput-tolerance-derived-from-the-headline-record.md):
  the throughput tolerance derived from the headline record, and
  [PROJECT_SPEC.md](PROJECT_SPEC.md) §7's wording for that target: "kept pace with the
  offered load; headroom not measured". The coded 1% and the recorded verdict are
  unchanged.
- The route rule's wildcard gap: a route or fallback whose first path segment is a
  wildcard, other than `/{$}`, is refused, so the guard and the API key cannot be
  stepped around by a pattern that does not carry the prefix.
**Acceptance.** The drift check fails on a cited line that is inside the named test
but is not an assertion, and all 54 citations are inventoried; the smoke kills only a
targetable attempt and passed ten consecutive runs; the renderer ends a record in one
newline and a test holds every committed record to it; the estimate is the measured
one; ADR-0023 and §7 carry the derivation with the code and the verdict unchanged;
and a wildcard first segment is refused with the golden byte-identical.
**Depends on.** M8D1.
**Status:** complete; see PR #24 and [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence and for what remains limited.

Deliberately **not** in scope: the 50.04 s recovery outlier (M8D3), any change to a
recorded run's workload, victim selection, rules or record format, to
`JudgeThroughput` or any other `Judge*` function, to either committed record, to
`api/openapi.yaml`, the SDK, the CLI, the dashboard, migrations or the schema.

#### M8D3 — Investigate the 50.04 s worker-failure recovery outlier
**Objective.** Find out why one worker-failure recovery in the headline run took 50.04 s
against a median of 32.02 s, when the shipped lease is 30 s.
**Deliverables.** A reproduction of the outlier, with a kill timed after submission
ends on an otherwise idle system, under the benchmark harness or in an integration
test; and then one of three outcomes, which the owner fixed when M8D3 began. Either the
outlier is **explained with evidence**, a mechanism that the reproduction shows and that
puts the extra time in a named segment of the recovery; or a **defect is found, and the
work stops and reports it without fixing it**, because a fix to recovery is a change to
the control plane and is the owner's decision; or it is **not reproduced**: the planned
trials (10 loaded, 20 at the tail, 10 idle) ran with no recovery above the lease plus
5 s, and the report records the trial count, the 95% upper bound on the rate (about 3/N
for N clean trials) and the per-segment distributions, and leaves the outlier listed as
unexplained.
**Acceptance.** The report names which of the three it is, shows the reproduction, and,
for an explanation, shows the evidence that the mechanism and not chance produced the
figure: a controlled variation of a harness setting that moves or removes the excess as
the mechanism predicts.
**Depends on.** M8D2.
**Status:** complete; see PR #25 and [CURRENT_STATE.md](CURRENT_STATE.md) for the
evidence and for what remains limited. The outcome is **explained**
([ADR-0024](adr/0024-the-50s-recovery-is-a-held-notification-at-the-tail.md)), and the
owner accepted the behavior as intended on 2026-10-10: no control-plane change follows.

#### M8C — Deployment
**Objective.** Make deployment credible without applying it.
**Deliverables.** Validated (not applied) Terraform for ALB, ECS services, RDS,
SQS, S3, secrets and logs, together with the three decisions that deployment
forces: the non-loopback bind, which
[ADR-0018](adr/0018-browser-origin-guard-on-the-internal-surface.md)'s shared
loopback predicate ties to the `Host` rule and which must be revisited with it;
protection of `/internal`, which ADR-0018 deliberately did not provide; and
whether the demonstration handlers are gated in a deployed worker
([ADR-0019](adr/0019-demo-handlers-are-trusted-built-ins.md) ships them and leaves
a gate open).
**Acceptance.** `terraform validate` passes without applying.
**Depends on.** M8B; follows M8D3 in the owner's order (M8B, M8D1, M8D2, M8D3, M8C). **Needs its
own owner decision before any work starts:** the bind, `/internal` and the handler
gate are trust-boundary decisions, and the infrastructure costs money.
**Status:** planned.

**Open product decision, not part of M8C's deliverables: the shipped default
timings.** The M8A benchmark missed the dispatch-latency and worker-failure-recovery
targets on the shipped defaults (`TASKFORGE_OUTBOX_POLL_INTERVAL=1s`,
`TASKFORGE_LEASE_DURATION=30s`) and met all five on a tuned, labelled profile
([PROJECT_SPEC.md](PROJECT_SPEC.md) §7). Which timings a deployment ships is the
owner's decision. Changing the defaults would require re-measuring, because the
recorded results describe the defaults as they are.

---

## Post-V1

Recurring schedules with timezone and misfire policy · workflow DAGs · fan-out and
fan-in · weighted fairness · aging and quotas · CPU/memory/GPU/architecture resource
classes · affinity and anti-affinity · autoscaling · gRPC · multi-tenancy and RBAC ·
Helm and Kubernetes · isolated process or container execution (the prerequisite for
hard cancellation of uncooperative handlers).

V1 scope may be regrouped here, but never silently expanded or reduced.
