# TaskForge

[![CI](https://github.com/co-rtex/TaskForge/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/co-rtex/TaskForge/actions/workflows/ci.yml?query=branch%3Amain)

**Durable distributed job processing, scheduling, and reliability platform.**

TaskForge implements the hard parts of a production job system directly — durable job
lifecycle, explicit state machines, idempotent submission, transactional
database-to-broker delivery, leases and fencing, and crash recovery — rather than
wrapping an existing queue framework.

> ### Status: early development — milestone 5E of 8
>
> **What works today:** the complete durable job lifecycle. Idempotent immediate
> and delayed submission with a recoverable transactional outbox; durable logical
> workers and process sessions; atomic, priority- and capability-aware claims with
> queue and worker capacity limits; fenced start with a persisted per-attempt
> execution deadline; fenced success, failure, and cooperative cancellation under
> a retained outcome identity; server-timed heartbeats that also deliver
> cancellation; fenced, idempotent lease renewal; retry with bounded exponential
> backoff and injected jitter; server-authoritative timeouts; cancellation; the
> logical DLQ with listing, replay, and operator retry; a scheduler that promotes
> due work and re-notifies queued jobs whose notification was lost; crash
> recovery; scoped, revocable API-key authentication on the public API; scoped,
> revocable worker-key authentication of registration, with every later
> worker-control call trusting that session and a cheap revocation check
> instead of a re-presented credential; and small and large result storage,
> with a large result uploaded to an S3-compatible object store before the
> attempt reports success and retrieved through the same scoped API key. A job
> submitted under any authenticated scope is claimed and executed by a worker
> registered for that same scope; and a command-line client, `taskforge-cli`,
> over the public API and the loopback-only credential-management routes,
> with a stable, individually-tested exit-code contract.
>
> **What does not exist yet:** the Python SDK and the dashboard. Both
> key-management surfaces stay unauthenticated and loopback-only, because each
> is how its own credential type comes into existence.
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

M4 completes the lifecycle: a handler failure is classified and retried with
bounded backoff or dead-lettered, an attempt that outlives its budget is recorded
`TIMED_OUT` by reconciliation rather than by the worker, cancellation reaches a
running handler over the heartbeat, and a dead-lettered job can be listed and
replayed as a new job that leaves the original's history intact. A killed worker
is still recovered exactly as M3 established. What TaskForge cannot do is
forcibly terminate an uncooperative handler — Go offers no such thing — so what
it guarantees instead is that nothing such a handler produces afterwards can be
committed. See [docs/CURRENT_STATE.md](docs/CURRENT_STATE.md) for the exact
current boundary.

## Design in one paragraph

PostgreSQL is the single authoritative store for job, attempt, lease, worker-session,
idempotency, and outbox state. The broker carries only small advisory
work-availability notifications — never authoritative job state — and every state
change that requires a notification writes it in the same transaction that changes
the state.
Workers **pull**: a worker with a free slot asks the control plane, and one SQL
transaction enforces capacity, matches capabilities, picks the highest-priority
eligible job, and creates the attempt and lease. A running attempt renews that lease
under a fence that makes an ambiguous retry safe, and reports its outcome under an
identity retained for the lifetime of attempt history, so an ambiguous failure
report cannot consume a second attempt. A scheduler promotes work that becomes
eligible later and re-notifies queued work whose notification was lost, and a
reconciler repairs what no live process will: stale sessions, attempts past their
deadline, unacknowledged cancellations, and leases that expired with work
unfinished. Correctness never depends on queue ordering, process memory, or a
worker's own clock, and a lost broker notification costs reachability rather than
correctness — which is exactly what bounded re-notification restores.

## Try what exists

Needs Git, Go 1.25+, Docker, Docker Compose, and Make.

```bash
make bootstrap   # create .env from the example, download dependencies
make up          # start PostgreSQL, ElasticMQ, and the object store, wait until all are ready
make migrate     # apply the schema
make build       # compile ./bin/taskforge-{api,outbox,scheduler,migrate,worker,reconciler,cli}
```

First, mint an API key and a worker key for the same scope. Both are returned
**exactly once** and cannot be recovered afterwards.

```bash
curl -X POST http://127.0.0.1:8080/internal/v1/api-keys \
  -H 'Content-Type: application/json' \
  -d '{"scope":"local-dev","name":"my-laptop"}'

export TASKFORGE_API_KEY=tfk_...   # the "key" field of that response

curl -X POST http://127.0.0.1:8080/internal/v1/worker-keys \
  -H 'Content-Type: application/json' \
  -d '{"scope":"local-dev","name":"my-worker"}'

export TASKFORGE_WORKER_API_KEY=tfk_...   # the "key" field of THAT response
```

A worker key's scope decides which jobs a worker started with it can claim: a
job submitted under an API key for a *different* scope is durable and
readable but will never be claimed by that worker — it stays `QUEUED`. Mint
both keys for the same scope for work that must run.

Both key-management surfaces live under `/internal/v1` and are themselves
unauthenticated, because each is how its own credential type comes into
existence. That is why nothing under `/internal/v1` may be exposed off
loopback. List and revoke with:

```bash
curl http://127.0.0.1:8080/internal/v1/api-keys
curl -X POST http://127.0.0.1:8080/internal/v1/api-keys/<key_id>/revoke

curl http://127.0.0.1:8080/internal/v1/worker-keys
curl -X POST http://127.0.0.1:8080/internal/v1/worker-keys/<key_id>/revoke
```

Now run the API, publisher, scheduler, worker, and reconciler in five
terminals. `TASKFORGE_WORKER_API_KEY` must be set (or present in `.env`)
before starting the worker — it presents that credential when it registers.

```bash
./bin/taskforge-api
./bin/taskforge-outbox
./bin/taskforge-scheduler
./bin/taskforge-worker
./bin/taskforge-reconciler
```

The reconciler is what makes a crash recoverable, and what records a timeout.
Without it, a killed worker's lease stays active and its job never runs again.
The scheduler is what makes a delayed or retry-waiting job eventually run, and
what repairs a queued job whose notification was lost.

Submit a job:

```bash
curl -X POST http://127.0.0.1:8080/v1/jobs \
  -H "Authorization: Bearer $TASKFORGE_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: my-first-job' \
  -d '{"queue":"default","job_type":"demo.echo","payload":{"message":"hello"}}'
```

A missing, malformed, unknown, or revoked key answers `401` with code
`unauthorized`, and the message deliberately does not say which: a response that
distinguished them would let a caller probe for which key prefixes exist.

Sending the same key again returns the same job with `200` instead of `201`; sending
it with a different body returns `409`. The publisher delivers a `work.available`
notification, and the worker claims the authoritative job from PostgreSQL, runs
`demo.echo`, and commits `SUCCEEDED`. `GET /v1/jobs/{job_id}` shows the durable state.

Read its result once it has succeeded:

```bash
curl http://127.0.0.1:8080/v1/jobs/<job_id>/result \
  -H "Authorization: Bearer $TASKFORGE_API_KEY"
```

`demo.echo` returns an exact copy of the submitted payload, so this answers
`{"message":"hello"}` — served identically whether the result was small
enough to store inline in PostgreSQL or large enough to have been uploaded
to the object store first; the caller never needs to know which.

Submit one for later by adding `"scheduled_at": "2030-01-01T00:00:00Z"`. It stays
`PENDING` and unadvertised until PostgreSQL says it is due, and the scheduler
promotes it then.

Cancel a job:

```bash
curl -X POST http://127.0.0.1:8080/v1/jobs/<job_id>/cancel \
  -H "Authorization: Bearer $TASKFORGE_API_KEY"
```

A job that has not been claimed becomes `CANCELED` immediately with no attempt
ever created. One that is running becomes `CANCEL_REQUESTED`, its worker learns
of it on the next heartbeat, and the attempt is finalized when the handler stops
— or by the reconciler if it never does.

List what failed for good, and run one again:

```bash
curl http://127.0.0.1:8080/v1/dlq \
  -H "Authorization: Bearer $TASKFORGE_API_KEY"
curl -X POST http://127.0.0.1:8080/v1/dlq/<job_id>/replay \
  -H "Authorization: Bearer $TASKFORGE_API_KEY" \
  -H 'Idempotency-Key: replay-once'
```

Replay creates a **new** job linked back to the original. The original stays
dead-lettered, with its attempts and failure detail exactly as they were.

To watch recovery, kill the worker while a job is running. Its session goes stale,
its lease expires, the reconciler abandons attempt 1 and requeues the job, and a
worker started afterwards completes it as attempt 2.

Run the tests:

```bash
make test        # unit and integration
make test-race   # both, under the race detector
```

`make test-integration` runs against the real PostgreSQL, the real broker, and the
real object store started by `make up`. It fails rather than skipping when they
are not running.

The same gates run on GitHub-hosted runners for every pull request targeting `main`
and every push to `main` — see [.github/workflows/ci.yml](.github/workflows/ci.yml).

## Try it with `taskforge-cli`

Everything above through raw `curl` has a `taskforge-cli` equivalent. It talks to
the same address `taskforge-api` binds to by default
(`TASKFORGE_API_ADDR`, `--api-url` to override) and reads its credential from
`TASKFORGE_CLI_API_KEY` or `--api-key`.

```bash
./bin/taskforge-cli api-keys create --scope local-dev --name my-laptop
# {"id":"...","scope":"local-dev","name":"my-laptop","prefix":"...","created_at":"...","key":"tfk_..."}

export TASKFORGE_CLI_API_KEY=tfk_...   # the "key" field above

./bin/taskforge-cli worker-keys create --scope local-dev --name my-worker
export TASKFORGE_WORKER_API_KEY=tfk_...   # the "key" field of THAT response; taskforge-worker reads this one

./bin/taskforge-cli jobs submit --queue default --job-type demo.echo --payload '{"message":"hello"}'
./bin/taskforge-cli jobs get <job_id>
./bin/taskforge-cli jobs result <job_id>
./bin/taskforge-cli jobs cancel <job_id>
./bin/taskforge-cli dlq list
./bin/taskforge-cli dlq replay <job_id>
```

A success response is JSON on stdout; a failure is a JSON error object on stderr
and nothing on stdout. The process exit code is a small, stable, individually
tested set documented in [internal/cli/exitcode.go](internal/cli/exitcode.go):
`0` success, `1` CLI usage error, `2` the request was rejected as invalid, `3`
unauthorized, `4` not found, `5` conflict (the operation cannot be applied given
current state), `6` internal server error, `7` the server's own deadline elapsed,
`8` could not reach the API at all, `9` a response this CLI version does not
recognize. `taskforge-cli --help` lists every command and flag.

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
