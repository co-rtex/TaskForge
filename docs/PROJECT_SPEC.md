# TaskForge — Product Specification

Canonical owner of: product purpose, users, V1 scope, success criteria, guarantees,
security boundaries, benchmark targets, and non-goals.

This document describes **what TaskForge must become**. It is not a status report —
see [CURRENT_STATE.md](CURRENT_STATE.md) for what is actually built.

---

## 1. Purpose

TaskForge is an open-source distributed job-processing, scheduling, and reliability
platform. It takes design inspiration from Celery, Sidekiq, Temporal, AWS SQS, and
Kubernetes Jobs, but it does not wrap them. It implements the difficult
control-plane behavior directly: durable job lifecycle, explicit state machines,
idempotent submission, transactional database-to-broker delivery, worker sessions,
priority-aware scheduling, bounded capacity, leases, heartbeats, retries,
cancellation, dead-lettering, stale-worker detection, crash recovery,
duplicate-delivery safety, late-completion fencing, and reconciliation.

Two audiences must both be served:

- **An experienced backend or infrastructure engineer** should read the repository
  and conclude that it demonstrates real understanding of why production job
  systems are difficult.
- **A serious student contributor** should be able to learn from it. Architecture
  docs, ADRs, failure tests, and examples exist to teach the underlying
  engineering, not to hide it behind a framework.

## 2. Delivery guarantee

> **Durable at-least-once job execution with idempotent control-plane transitions,
> fenced stale attempts, and application-level idempotency support for external
> handler side effects.**

TaskForge **does not** provide exactly-once execution and must never claim to.

What TaskForge guarantees:

- A job accepted by the API is durable before the caller receives a success response.
- A job is never silently lost, and never stops making progress because a broker
  message was lost, duplicated, or delayed.
- Duplicate or stale workers cannot corrupt TaskForge's own control-plane state.
- A terminal job never returns to a non-terminal state.
- Exactly one state-changing operation wins every race.

What TaskForge cannot guarantee:

- That an arbitrary external side effect inside a handler happens exactly once. A
  handler may run more than once. Handlers that touch external systems must be
  idempotent; TaskForge supplies stable job and attempt identifiers so they can be.
- That an uncooperative in-process handler goroutine can be forcibly killed. Go
  cannot do this. Hard cancellation requires process or container isolation, which
  is post-V1.

## 3. Target users

- Backend engineers who need durable background execution with real operational
  visibility.
- Platform engineers evaluating job-system design tradeoffs.
- Students and contributors learning distributed-systems reliability engineering.

## 4. V1 requirements

A completed V1 lets a developer do all of the following on a local machine:

1. Clone TaskForge and start it with Docker Compose and Make.
2. Create or obtain a local API key.
3. Submit immediate and delayed jobs.
4. Inspect job state and full attempt history.
5. Run multiple workers.
6. Observe priority-aware dispatch.
7. Inspect worker capacity and health.
8. Kill a worker mid-execution.
9. Observe heartbeat staleness and lease expiration.
10. Observe the abandoned attempt and its replacement attempt.
11. See a different worker complete the job.
12. Exercise retry, timeout, cancellation, DLQ, and replay behavior.
13. Retrieve small and large results.
14. Inspect structured logs, metrics, and traces.
15. Use a CLI, a Python SDK, and an operator dashboard.
16. Run automated concurrency, restart, and failure-recovery tests.

### Submission contract

```json
{
  "queue": "default",
  "job_type": "demo.sleep",
  "payload": { "duration_ms": 5000 },
  "priority": 50,
  "max_attempts": 3,
  "timeout_seconds": 30,
  "scheduled_at": null,
  "required_capabilities": ["cpu"]
}
```

Submission idempotency uses the **`Idempotency-Key` request header**. If an SDK also
exposes the key as a method argument, the SDK places it in that canonical header.

`max_attempts` counts **total** attempts, including the first.

### Public API surface (V1 target)

```
POST /v1/jobs
GET  /v1/jobs/{job_id}
GET  /v1/jobs
POST /v1/jobs/{job_id}/cancel
POST /v1/jobs/{job_id}/retry
GET  /v1/dlq
POST /v1/dlq/{job_id}/replay
GET  /v1/workers
GET  /v1/queues
```

Plus authenticated internal operations for worker registration, process sessions,
heartbeat, claim, attempt start, lease renewal, terminal outcomes, and cancellation
delivery. Endpoint-by-endpoint semantics live in
[ARCHITECTURE.md](ARCHITECTURE.md); implementation status lives in
[CURRENT_STATE.md](CURRENT_STATE.md).

## 5. V1 success criteria

- The full local stack starts from a clean clone with only Git, Go, Docker, Docker
  Compose, and Make installed.
- `make demo` runs real jobs that succeed, retry, and dead-letter.
- `make demo-failure` demonstrates a worker crash, lease expiration, attempt
  abandonment, stale-attempt fencing, reassignment, and eventual success.
- Every reliability invariant in [ARCHITECTURE.md](ARCHITECTURE.md) has an
  automated test.
- All twelve required end-to-end and failure scenarios in that document are
  automated and assert durable database state, not just HTTP status codes.
- The dashboard, CLI, and SDK read live data. Nothing is fabricated or hardcoded.
- Documentation distinguishes implemented behavior from planned behavior everywhere.

## 6. Security boundaries

- API keys are high-entropy, returned exactly once, stored as a lookup prefix plus a
  cryptographic hash, revocable, and scoped. Worker/control scopes are separable
  from user scopes.
- All SQL is parameterized. Every handler enforces a payload-size limit and a
  timeout. Errors returned to clients are sanitized.
- Logs never contain secrets or unbounded payloads.
- **TaskForge executes only trusted handlers compiled into the worker binary.** It
  never accepts uploaded scripts, shell commands, containers, dynamic plugins, or
  any other form of remote code execution. This is a permanent product boundary,
  not a V1 limitation.
- Local development binds to loopback only. No unauthenticated API is deployed
  publicly.
- Secrets are never committed. `.env.example` contains names and safe placeholders.

## 7. Benchmark targets — **UNMEASURED**

These are goals used to shape design. **None has been measured.** They must never
appear as achieved results in the README, in a commit message, or in a handoff
until a reproducible run records them under
[docs/](.) with SHA, environment, command, and limitations.

| Target | Value | Status |
| --- | --- | --- |
| Sustained throughput | 1,000 jobs/minute across 12 workers | Not measured |
| Dispatch latency | p95 < 500 ms | Not measured |
| Fault-injection volume | 10,000 jobs | Not measured |
| Completion under fault injection | ≥ 99.7% | Not measured |
| Worker-failure recovery | < 30 s | Not measured |

## 8. Non-goals

TaskForge V1 is **not**:

- a CRUD dashboard with fake workers;
- an in-memory queue with a database bolted on;
- a background-thread manager;
- a thin SQS or Lambda wrapper;
- a serverless demonstration;
- a generic workflow or DAG engine;
- an arbitrary code, shell, container, or remote-execution service;
- an LLM or AI-agent project;
- a Kubernetes-first platform;
- a multi-region system;
- an exactly-once system;
- a complete cron platform;
- a multi-tenant billing system;
- a collection of empty services and directories;
- a project that presents targets as measured results.

## 9. Explicitly deferred past V1

Recurring schedules with timezone and misfire policy; workflow DAGs; fan-out and
fan-in; weighted fairness; aging and quotas; CPU/memory/GPU/architecture resource
classes; affinity and anti-affinity; autoscaling; gRPC; multi-tenancy and RBAC;
Helm and Kubernetes; isolated process or container execution.

Do not create speculative services, tables, or packages for any of these.
