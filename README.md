# TaskForge

**Durable distributed job processing, scheduling, and reliability platform.**

TaskForge implements the hard parts of a production job system directly — durable job
lifecycle, explicit state machines, idempotent submission, transactional
database-to-broker delivery, leases and fencing, and crash recovery — rather than
wrapping an existing queue framework.

> ### Status: early development — milestone 1 of 8
>
> **What works today:** durable, idempotent job submission and a recoverable
> transactional outbox that publishes work-availability notifications to a real
> SQS-compatible broker.
>
> **What does not exist yet:** workers, claims, leases, retries, cancellation, DLQ,
> the CLI, the SDK, and the dashboard. Nothing executes a job.
>
> See [docs/CURRENT_STATE.md](docs/CURRENT_STATE.md) for exactly what is implemented
> and verified at this commit.

## Execution guarantee

> Durable **at-least-once** job execution with idempotent control-plane transitions,
> fenced stale attempts, and application-level idempotency support for external handler
> side effects.

TaskForge does not provide exactly-once execution and never claims to. A handler may
run more than once; handlers with external side effects must be idempotent. See
[ADR-0002](docs/adr/0002-at-least-once-execution-semantics.md).

## Design in one paragraph

PostgreSQL is the single authoritative store for job, attempt, lease, worker-session,
idempotency, and outbox state. The broker carries only small advisory
work-availability notifications — never authoritative job state — and every state
change writes its notification in the same transaction that changes the state.
Workers **pull**: a worker with a free slot asks the control plane, and one SQL
transaction enforces capacity, matches capabilities, picks the highest-priority
eligible job, and creates the attempt and lease. Correctness never depends on queue
ordering, broker delivery, or process memory.

## Try what exists

Needs Git, Go 1.24+, Docker, Docker Compose, and Make.

```bash
make bootstrap   # create .env from the example, download dependencies
make up          # start PostgreSQL and ElasticMQ, wait until both are ready
make migrate     # apply the schema
make build       # compile ./bin/taskforge-{api,outbox,migrate}
```

Run the API and the publisher in two terminals:

```bash
./bin/taskforge-api
./bin/taskforge-outbox
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
notification to the broker — which nothing consumes yet, because workers are
milestone M2.

Run the tests:

```bash
make test        # unit and integration
make test-race   # both, under the race detector
```

`make test-integration` runs against the real PostgreSQL and the real broker started
by `make up`. It fails rather than skipping when they are not running.

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
