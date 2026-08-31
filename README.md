# TaskForge

[![CI](https://github.com/co-rtex/TaskForge/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/co-rtex/TaskForge/actions/workflows/ci.yml?query=branch%3Amain)

**Durable distributed job processing, scheduling, and reliability platform.**

TaskForge implements the hard parts of a production job system directly — durable job
lifecycle, explicit state machines, idempotent submission, transactional
database-to-broker delivery, leases and fencing, and crash recovery — rather than
wrapping an existing queue framework.

> ### Status: early development — milestone 2 of 8
>
> **What works today:** durable, idempotent job submission and a recoverable
> transactional outbox; durable logical workers and process sessions; atomic,
> priority- and capability-aware claims with queue and worker capacity limits;
> fenced start/success transitions; and bounded execution of the trusted
> `demo.echo` handler through a real SQS-compatible broker.
>
> **What does not exist yet:** heartbeats, lease renewal, crash reconciliation,
> retries, cancellation, DLQ, results, the CLI, the SDK, and the dashboard.
>
> See [docs/CURRENT_STATE.md](docs/CURRENT_STATE.md) for exactly what is implemented
> and verified at this commit.

## Execution guarantee

> **V1 target:** durable at-least-once job execution with idempotent control-plane
> transitions, fenced stale attempts, and application-level idempotency support for
> external handler side effects.

TaskForge does not provide exactly-once execution and never claims to. A handler may
run more than once; handlers with external side effects must be idempotent. See
[ADR-0002](docs/adr/0002-at-least-once-execution-semantics.md).

M2 proves the successful path and rejects expired or replaced-session fences. It
does **not** yet recover a worker crash or failed handler: heartbeat, renewal, lease
expiration, attempt abandonment, and capacity repair arrive in M3. See
[docs/CURRENT_STATE.md](docs/CURRENT_STATE.md) for the exact current boundary.

## Design in one paragraph

PostgreSQL is the single authoritative store for job, attempt, lease, worker-session,
idempotency, and outbox state. The broker carries only small advisory
work-availability notifications — never authoritative job state — and every state
change that requires a notification writes it in the same transaction that changes
the state.
Workers **pull**: a worker with a free slot asks the control plane, and one SQL
transaction enforces capacity, matches capabilities, picks the highest-priority
eligible job, and creates the attempt and lease. Correctness never depends on queue
ordering or process memory. A lost broker notification cannot corrupt state or cause
duplicate execution, but M2 does not yet recreate a lost sole notification; that
eventual-progress mechanism is planned for M4.

## Try what exists

Needs Git, Go 1.25+, Docker, Docker Compose, and Make.

```bash
make bootstrap   # create .env from the example, download dependencies
make up          # start PostgreSQL and ElasticMQ, wait until both are ready
make migrate     # apply the schema
make build       # compile ./bin/taskforge-{api,outbox,migrate,worker}
```

Run the API, publisher, and worker in three terminals:

```bash
./bin/taskforge-api
./bin/taskforge-outbox
./bin/taskforge-worker
```

Submit a job:

```bash
curl -X POST http://127.0.0.1:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: my-first-job' \
  -d '{"queue":"default","job_type":"demo.echo","payload":{"message":"hello"}}'
```

Sending the same key again returns the same job with `200` instead of `201`; sending
it with a different body returns `409`. The publisher delivers a `work.available`
notification, and the worker claims the authoritative job from PostgreSQL, runs
`demo.echo`, and commits `SUCCEEDED`. `GET /v1/jobs/{job_id}` shows the durable state;
M2 intentionally stores no handler result body.

Run the tests:

```bash
make test        # unit and integration
make test-race   # both, under the race detector
```

`make test-integration` runs against the real PostgreSQL and the real broker started
by `make up`. It fails rather than skipping when they are not running.

The same gates run on GitHub-hosted runners for every pull request targeting `main`
and every push to `main` — see [.github/workflows/ci.yml](.github/workflows/ci.yml).

## Documentation

| Read this | For |
| --- | --- |
| [docs/PROJECT_SPEC.md](docs/PROJECT_SPEC.md) | What TaskForge must become; V1 scope and non-goals |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | How it works; invariants and failure model |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Milestone sequence |
| [docs/CURRENT_STATE.md](docs/CURRENT_STATE.md) | What is actually built and verified |
| [docs/adr/README.md](docs/adr/README.md) | Why decisions were made |
| [AGENTS.md](AGENTS.md) | Engineering rules for contributors |
| [api/openapi.yaml](api/openapi.yaml) | The implemented HTTP endpoints |

## Performance

No performance numbers are published. Targets exist in
[docs/PROJECT_SPEC.md](docs/PROJECT_SPEC.md) §7 and are explicitly marked
**unmeasured**; they will be replaced by recorded results only when a reproducible
benchmark run produces them.

## License

[Apache-2.0](LICENSE)
