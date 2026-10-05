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

## 7. Benchmark targets and measured results

These targets shaped the design. Each has now been **measured once, on one
machine**, by a reproducible run whose record names the commit, the environment,
the command, the settings in effect and the limitations:
[docs/benchmarks/2026-10-05-d796722.md](benchmarks/2026-10-05-d796722.md), the
headline run, on the shipped default timings. Every figure is read from
PostgreSQL's clock, and the definition of each figure and the rule for when a
target is met were fixed before the run in
[ADR-0020](adr/0020-benchmark-methodology.md). A figure below is never rounded in
the system's favor, and a target that was missed says so.

| Target | Value | Measured ([record](benchmarks/2026-10-05-d796722.md)) | Met? |
| --- | --- | --- | --- |
| Sustained throughput | 1,000 jobs/minute across 12 workers | 999.83 jobs/min completed over a 5 minute window, with 1000.23 offered | **Met**, within the 1% tolerance ADR-0020 fixed before the run. It is 0.17 jobs/min below 1,000. |
| Dispatch latency | p95 < 500 ms | p95 963.7 ms (p50 552.2 ms, p99 1009.4 ms, max 1046.6 ms; n = 5,001) | **MISSED**, beside `TASKFORGE_OUTBOX_POLL_INTERVAL=1s` |
| Fault-injection volume | 10,000 jobs | 10,000 jobs, with 24 workers killed | **Met** |
| Completion under fault injection | ≥ 99.7% | 10,000 of 10,000 `SUCCEEDED` (100.000%), 5 minutes after the last submission | **Met** |
| Worker-failure recovery | < 30 s | worst 50.04 s, median 32.02 s, over 16 abandoned attempts, none left unreplaced | **MISSED**, beside `TASKFORGE_LEASE_DURATION=30s` |

Read the table with these in mind.

- **Two targets are missed on the shipped defaults, and the record says which
  settings sit beside each miss.** A killed worker's attempt is only abandoned when
  its lease expires, so with a 30 s lease recovery cannot be much under 30 s; the
  outbox publishes once a second, so a job waits for the next pass.
- **A second run, labelled TUNED, met all five**
  ([record](benchmarks/2026-10-05-c1764d7-tuned.md)): a 10 s lease, tighter
  liveness windows, and scans every 250 ms with the outbox every 200 ms gave a
  dispatch p95 of 208.5 ms and a worst recovery of 10.98 s. It shows what those
  settings buy. It does not replace the headline run, and the shipped defaults have
  not changed.
- **These are one machine and one run.** A laptop running the load generator, the
  services, twelve workers and Docker, with PostgreSQL on tmpfs in a Docker VM and
  ElasticMQ for SQS. There is no variance estimate. A measured value here says
  nothing about another machine or a deployment, and the targets remain the targets
  for those. The record's Limitations section lists the rest.
- **A result may be quoted only with a link to a record.** The rule this section
  was written under stands for any future number: it appears in the README, a
  commit message or a handoff only with the record that produced it.

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
