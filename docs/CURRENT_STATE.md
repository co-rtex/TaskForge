# TaskForge — Current State

Canonical owner of: what is actually implemented, what was actually verified, known
gaps, and the recommended next milestone.

**Update this file at the end of every implementation session. It must match the
branch head. Never mark planned, scaffolded, or compiled-but-untested work as complete.**

---

## Snapshot

| Field | Value |
| --- | --- |
| Branch | `feat/durable-job-ingress` |
| Base commit | `8d340104f8c43261567f1a518adaf43ac20eca37` (`docs: establish TaskForge repository context`) |
| Milestone | M1 — Durable job ingress and recoverable outbox |
| Milestone status | **Complete.** All acceptance criteria in [ROADMAP.md](ROADMAP.md) are met. |
| Next milestone | M2 — Workers, sessions, and atomic claim. **Not started.** |

## What is actually implemented

### Runnable components

| Binary | What it does |
| --- | --- |
| `taskforge-migrate` | Applies embedded SQL migrations. Advisory-locked, checksum-verified, idempotent. |
| `taskforge-api` | `POST /v1/jobs`, `GET /v1/jobs/{job_id}`, `GET /healthz`, `GET /readyz`. |
| `taskforge-outbox` | Drains `outbox_events` to the broker. Own `/healthz` and `/readyz`. |

### Schema (`migrations/0001_initial_job_ingress.sql`)

`queues`, `jobs`, `idempotency_records`, `outbox_events`, plus `schema_migrations`
maintained by the runner. Invariants are enforced by database constraints: the
composite primary key on `(scope, idempotency_key)` is what makes duplicate
submissions collapse to one job, and a partial index on pending events serves the
publisher's only scan.

`jobs` is **insert-only** in this milestone — nothing transitions a job yet, so
every row is `QUEUED`. The `CHECK` constraint already covers the whole V1 state
machine so later milestones do not have to rewrite it.

### Behavior

- **Idempotent submission.** Job, idempotency record, and outbox event commit in one
  transaction. Requests are canonicalized (object keys sorted, capabilities reduced
  to a sorted set, defaults applied) and hashed into a length-prefixed SHA-256
  fingerprint. Same key + equivalent request replays with `200`; same key +
  different request conflicts with `409`.
- **Transactional outbox.** Claim (commit) → publish (no transaction held) → mark
  published (commit). `FOR UPDATE SKIP LOCKED` makes publisher replicas safe; the
  advanced `available_at` acts as a visibility timeout. Publish failures back off
  exponentially with injected jitter and are never marked terminally failed.
- **Broker abstraction.** Publish, long-poll receive, acknowledge. One implementation
  serves ElasticMQ locally and AWS SQS in a deployment. Notifications carry
  identifiers and routing hints only — never the authoritative job payload.
- **Validation.** Unknown fields rejected, body size capped, all field problems
  reported at once, `scheduled_at` refused with an explicit "not implemented"
  message rather than silently ignored.
- **Errors.** One structured shape with stable machine-readable codes, including 404
  and 405. Internal detail never reaches a client; a request id threads back to logs.
- **Health.** Liveness checks nothing by design; readiness checks real dependencies
  under a bounded timeout and returns `503` when one is down.

### Not implemented

Workers · sessions · claims · attempts · leases · heartbeats · scheduler · retries ·
timeouts · cancellation · DLQ · replay · result storage · API keys · CLI · Python SDK ·
dashboard · metrics · tracing · Terraform · benchmarks.

Nothing executes a job. No empty package, table, or service has been created for any
of the above.

## What was actually verified

Verified on macOS (darwin/arm64), Go 1.27.0, Docker 28.5.2, PostgreSQL 16-alpine,
ElasticMQ 1.6.11, at commit `1c25d69`.

| Command | Result | Summary |
| --- | --- | --- |
| `make lint` | **PASS** | `gofmt` clean; `go vet ./...` clean. |
| `make build` | **PASS** | Three binaries produced in `./bin`. |
| `make test-unit` | **PASS** | 5 packages ok; `internal/{api,config,database,jobs,outbox}`. |
| `make test-integration` | **PASS** | `ok ... 30.9s` against real PostgreSQL and real ElasticMQ. |
| `make test-race` | **PASS** | Unit clean; integration `ok ... 41.1s`. No data races. |
| `docker compose config --quiet` | **PASS** | Compose file valid. |
| `go run ./cmd/taskforge-migrate` (fresh DB) | **PASS** | `migrations complete applied=1`; re-run reports `schema already up to date`. |
| End-to-end smoke with real binaries | **PASS** | See below. |

The smoke run submitted a job over HTTP, replayed the key (`200`), conflicted a
different request on the same key (`409`), read the job back, and observed the real
broker message:

```json
{"event_id":"bea09fbf-...","event_type":"work.available","schema_version":1,
 "occurred_at":"2026-08-29T22:57:13.360367Z",
 "data":{"queue":"default","job_id":"1e1f8b9b-..."}}
```

with the outbox row settling to `status=PUBLISHED, attempts=1`. Both services
reported `{"status":"ready"}` with every component `ok`.

### A real bug this work caught

`TestMigrations_AreSafeToRunConcurrently` failed on first run with a duplicate key
on `pg_type_typname_nsp_index`. `CREATE TABLE IF NOT EXISTS schema_migrations` ran
**before** the advisory lock, and `IF NOT EXISTS` is not atomic against a concurrent
creation of the same table. The lock now precedes any DDL. This is why concurrency
tests exist.

## Known gaps, limitations, and debt

- **No authentication.** Every request is attributed to the single configured
  `TASKFORGE_DEV_SCOPE`. The scoping model is correct and already enforced in the
  schema and every query, but there is no identity behind it. **The services must
  stay bound to loopback until database-backed API keys land in M5.**
- **Broker outage is injected at the network layer**, not by stopping the container.
  This exercises the real AWS SDK client, real error handling, and real recovery
  deterministically, but it does not cover behavior when the broker's TCP listener
  disappears entirely, nor ElasticMQ-versus-real-SQS differences in throttling and
  quota errors. Closing that gap needs a deployment smoke test (M8).
- **Outbox retention is unbounded.** `outbox_events` keeps published rows forever.
  A retention or archival policy is needed before any load testing. Debt.
- **`queues.max_concurrency` is stored but not enforced.** It is part of a queue's
  definition; enforcement belongs to the claim path in M2.
- **No metrics or tracing.** Structured logs with correlation ids exist; the
  observability stack is M6.
- **No performance data.** The targets in [PROJECT_SPEC.md](PROJECT_SPEC.md) §7
  remain **unmeasured**, and no benchmark has been run.
- A stale `CLAUDE.md` for an unrelated project (`Cadence`) sits in the parent
  directory `~/CLAUDE.md` and may be auto-loaded into sessions started from this
  path. It does not contradict TaskForge, but it is irrelevant context; this
  repository's own `CLAUDE.md` and `AGENTS.md` govern this project.

## Environment notes

- Go 1.27.0 (`darwin/arm64`), installed via Homebrew during bootstrap. `go.mod`
  targets 1.24 so contributors are not forced onto the newest toolchain.
- PostgreSQL is published on host port **5442**, not 5432: 5432 and 5433 were both
  occupied by other local projects on the development machine.
- Local PostgreSQL data lives on tmpfs and is destroyed by `make down`.

## Recommended next bounded milestone

**M2 — Workers, sessions, and atomic claim.** Objective, deliverables, and
acceptance criteria are in [ROADMAP.md](ROADMAP.md). Not started.
