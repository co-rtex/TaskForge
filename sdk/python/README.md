# `taskforge-sdk` — Python client for TaskForge

A typed, installable Python client for the [TaskForge](https://github.com/co-rtex/TaskForge)
public API and its loopback-only credential-management routes.

It is a **pure consumer** of `api/openapi.yaml`. It contains no domain logic,
and it obtains and presents API keys rather than reimplementing a credential
model that lives server-side — it never generates, parses, verifies, or
persists key material (see
[ADR-0013](../../docs/adr/0013-database-backed-api-key-authentication.md) and
[ADR-0014](../../docs/adr/0014-worker-control-authentication.md)).

## Install

Not published to PyPI. Install it from a clone of this repository:

```bash
pip install ./sdk/python          # or -e for an editable install
```

Requires Python 3.11+. One runtime dependency: `httpx`.

## Configure

```bash
export TASKFORGE_SDK_API_URL=http://127.0.0.1:8080   # optional; this is the default
export TASKFORGE_SDK_API_KEY=tfk_...                 # mint one, see below
```

Precedence is **constructor argument → environment variable → default**.

These are the SDK's **own** variables. It does not read `TASKFORGE_API_ADDR`
(that is `taskforge-api`'s *bind* address — a bare `host:port`, not a client
target), and it does not read `taskforge-cli`'s `TASKFORGE_CLI_API_URL` /
`TASKFORGE_CLI_API_KEY`. The credential half matters most: this is a library
inside your process, and a key exported for your shell's CLI should not be
silently picked up by every Python process in that shell. See
[ADR-0016](../../docs/adr/0016-python-sdk-toolchain-and-client-configuration.md).

## Use

```python
from taskforge import TaskForgeClient

# Minting a credential needs no credential: the /internal/v1 routes are
# loopback-only and unauthenticated by design.
with TaskForgeClient() as bootstrap:
    created = bootstrap.api_keys.create(scope="local-dev", name="my-laptop")
    print(created.key)  # returned exactly once, never recoverable

with TaskForgeClient(api_key=created.key) as client:
    job = client.jobs.submit(
        queue="default",
        job_type="demo.echo",
        payload={"message": "hello"},
    )
    print(job.id, job.status)  # JobStatus.QUEUED

    print(client.jobs.get(job.id).status)
    print(client.jobs.result(job.id))  # whatever JSON the handler produced

    client.jobs.cancel(job.id)

    for entry in client.dlq.iter_entries():
        print(entry.job_id, entry.reason)
```

### The full surface

| Method | Route |
| --- | --- |
| `jobs.submit(queue=, job_type=, payload=, …, idempotency_key=None)` | `POST /v1/jobs` |
| `jobs.get(job_id)` | `GET /v1/jobs/{job_id}` |
| `jobs.result(job_id)` | `GET /v1/jobs/{job_id}/result` |
| `jobs.cancel(job_id)` | `POST /v1/jobs/{job_id}/cancel` |
| `jobs.retry(job_id, idempotency_key=None)` | `POST /v1/jobs/{job_id}/retry` |
| `dlq.list(limit=, cursor=)` | `GET /v1/dlq` |
| `dlq.iter_entries(limit=)` | follows `next_cursor` over `GET /v1/dlq` |
| `dlq.replay(job_id, idempotency_key=None)` | `POST /v1/dlq/{job_id}/replay` |
| `api_keys.create(scope=, name=)` / `.list(limit=)` / `.revoke(key_id)` | `/internal/v1/api-keys…` |
| `worker_keys.create(scope=, name=)` / `.list(limit=)` / `.revoke(key_id)` | `/internal/v1/worker-keys…` |

**There is no `jobs.list()`, `workers.list()`, or `queues.list()`.**
`GET /v1/jobs`, `GET /v1/workers` and `GET /v1/queues` are V1 targets in
[PROJECT_SPEC.md](../../docs/PROJECT_SPEC.md) §4 but are not implemented in
this API. A method with no route to call would be fabricated functionality.
They arrive when the routes do.

## Idempotency

`jobs.submit`, `jobs.retry` and `dlq.replay` are the three routes the API
marks as requiring an `Idempotency-Key`. Each takes `idempotency_key=`; when
you omit it the SDK generates a random UUIDv4, matching `taskforge-cli`'s own
default. **The key is placed in the canonical `Idempotency-Key` request
header, and nowhere else** — never in the body, never in the query string.

> An auto-generated key makes a submission *unique*, not *repeatable*.
> Calling `submit()` twice with identical arguments and no key creates two
> jobs. If a network-level retry of the call must be safe, pass your own key
> and reuse it.

No other method sends the header. Cancellation needs no request identity —
its identity is scope plus job id, so cancelling twice is one decision
observed twice.

## Errors

Every failure raises a subclass of `TaskForgeError`. The taxonomy is the same
grouping-by-remediation that `taskforge-cli`'s exit codes use, and each class
carries the matching `exit_code`, so a script wrapping this SDK can
`sys.exit(exc.exit_code)` and stay consistent with the CLI.

| Exception | `exit_code` | Raised for |
| --- | --- | --- |
| `ConfigurationError` | 1 | The SDK rejected its own configuration; no request was made. |
| `RequestRejectedError` | 2 | `malformed_json`, `payload_too_large`, `validation_failed`, `invalid_cursor`. Send a different request. |
| `UnauthorizedError` | 3 | `unauthorized`. |
| `NotFoundError` | 4 | `not_found`. |
| `ConflictError` | 5 | `idempotency_conflict`, `job_not_cancelable`, `job_not_dead_lettered`. Retrying identically will not help. |
| `InternalServerError` | 6 | `internal_error`. Use `.request_id` to find the cause in the server's logs. |
| `ServiceUnavailableError` | 7 | `service_unavailable` — the server's own deadline elapsed. |
| `TransportError` | 8 | The API was never reached (DNS, refused, TLS, client-side timeout). |
| `UnexpectedResponseError` | 9 | A response this SDK version does not recognize. Signals version skew. |

Every `APIError` carries `.code`, `.message`, `.http_status`, `.request_id`,
`.details` and `.raw`. **Branch on `.code`, not on `.message`.**

## Models

Responses parse into frozen dataclasses (`Job`, `DLQPage`, `Replay`, …), each
carrying `.raw` — the untouched decoded response. Unknown fields a newer
server adds are preserved there rather than dropped. Missing required fields
and unrecognized enum values raise `UnexpectedResponseError` instead of being
guessed at, which means a server that adds a tenth job status breaks an older
SDK's parse of a job in that status. That is deliberate: reporting version
skew beats handing you a state your code has never heard of.

`jobs.result()` returns the decoded JSON as-is, with no model — the API
documents that body as "whatever JSON value the job's `job_type` produces".

## Develop

From the repository root:

```bash
make sdk-venv    # create sdk/python/.venv and install with dev extras
make sdk-fmt     # ruff format
make sdk-lint    # ruff format --check + ruff check + mypy --strict
make sdk-test    # pytest
```

Tests drive the real client through `httpx.MockTransport`, which is this
package's equivalent of the `httptest.Server` the CLI's own exit-code tests
run against — the product surface, not the mapping tables in isolation.
