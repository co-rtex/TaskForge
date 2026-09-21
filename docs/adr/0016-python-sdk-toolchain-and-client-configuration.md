# ADR-0016: Python SDK toolchain, packaging, and client configuration

- **Status:** Accepted
- **Date:** 2026-09-21

## Context

[`docs/ROADMAP.md`](../ROADMAP.md)'s M5E delivers "a typed, installable Python
SDK". Introducing Python is a first for this repository: before this milestone
there was no `.py` file, no `pyproject.toml`, and no `setup.py` anywhere in the
tree. The roadmap named the consequence explicitly — dependency and build
tooling, a lint and format story, a packaging story for V1, and whether the SDK
gets its own CI job — and recorded that none of it was decided.

Two constraints bound every answer.

[`PROJECT_SPEC.md`](../PROJECT_SPEC.md) §5 fixes the prerequisite set: "The full
local stack starts from a clean clone with only Git, Go, Docker, Docker Compose,
and Make installed." [`AGENTS.md`](../../AGENTS.md) §2 repeats it. A Python SDK
unavoidably adds a Python interpreter to that list; everything beyond it is a
cost this decision has to justify.

[`AGENTS.md`](../../AGENTS.md) §3 forbids scaffolding: "Create a package,
service, or directory when the current milestone puts working behavior in it.
Do not scaffold empty placeholders." So the toolchain could not ship as its own
preliminary milestone with nothing to lint, type-check, or test — its gates
would pass vacuously, which is precisely the failure the existing OpenAPI CI
step guards against by name.

## Decision

### Dependency and build tooling: PEP 621, stdlib `venv` and `pip`

`sdk/python/pyproject.toml` with a `hatchling` backend, a `src/` layout, and
`requires-python = ">=3.11"`. No lockfile.

`venv` and `pip` ship inside CPython. Choosing them means the only new
prerequisite is the interpreter itself — the irreducible minimum for a Python
SDK. A lockfile is deliberately absent: a lockfile pins an application's whole
environment, while a library declares ranges and must stay installable beside
whatever else is in a consumer's environment. This is a library.

### One runtime dependency: `httpx`

`httpx>=0.27,<1.0`, and nothing else at runtime.

The alternative was zero runtime dependencies via `urllib.request`. It was
rejected because it opens a new connection per call and because its error
surface (`URLError`, `HTTPError`, `socket.timeout`) forces the SDK to hand-derive
the one distinction [`internal/cli/exitcode.go`](../../internal/cli/exitcode.go)
treats as load-bearing: `ExitTransportError` (the API was never reached) versus
`ExitServiceUnavailable` (the API was reached and reported that *its own*
deadline elapsed). `httpx.TransportError` maps onto that directly. `httpx` also
ships `py.typed`, separates connect and read timeouts, and provides
`httpx.MockTransport` — the seam every test drives the real client through, the
Python equivalent of the `httptest.Server` the CLI's exit-code tests use.

`requests` was considered and rejected: no inline type hints (a "typed SDK"
would then depend on a separate stub package) and no mock-transport seam.

### Lint and format: `ruff` and `mypy --strict`

`ruff format` is authoritative, `ruff check` and `mypy --strict` must be clean.
This maps one-to-one onto [`AGENTS.md`](../../AGENTS.md) §5's existing Go rule,
"`gofmt` is authoritative; `go vet` must be clean". One tool replaces black,
isort and flake8; one config block; one dev dependency.

`pyright` was rejected for the type check: it is a Node package, so it would add
npm as a prerequisite for linting Python. `mypy` installs from the same
ecosystem as everything else here.

Its gates are **separate Make targets** (`sdk-venv`, `sdk-fmt`, `sdk-lint`,
`sdk-test`), not additions to `fmt`, `lint` and `test`. Those stay Go-only, so a
Go contributor and the fast `checks` CI job never need a provisioned
virtualenv. The accepted cost is that a contributor gets no local Python signal
unless they run `make sdk-lint`; [`AGENTS.md`](../../AGENTS.md) §4 lists the
targets, and the CI job below is the backstop.

### Packaging for V1: installable from this repository, not published

`pip install ./sdk/python` from a clone. Distribution name `taskforge-sdk`,
import package `taskforge`. **Not published to PyPI before M8, and not as a
deferred TODO inside this milestone — as a scope boundary.**

Publishing is outward-facing and irreversible: it claims a global name, and a
released version can be yanked but never truly unpublished. It would require a
release workflow holding a publish credential or a trusted-publishing grant,
which contradicts the standing comment at the top of
[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) — "No job writes to
the repository, publishes a package, or comments on a pull request, so the read
grant is the whole grant." And it would pin a public versioned artifact against
an API surface that is still short of V1: `GET /v1/jobs`, `GET /v1/workers` and
`GET /v1/queues` are V1 targets in [`PROJECT_SPEC.md`](../PROJECT_SPEC.md) §4 and
are not implemented.

[`PROJECT_SPEC.md`](../PROJECT_SPEC.md) §4 item 15 asks that a developer "use a
CLI, a Python SDK, and an operator dashboard" on a local machine. A repo-local
install satisfies that in full. Release engineering belongs with M8's CI
hardening.

### CI: its own job

A fourth job, `sdk`, in the existing workflow, on a 3.11/3.13 matrix, with
`actions/setup-python` pinned by commit SHA like every other action there.

Not folded into `checks`, because that job sets up Go and caches by `go.mod`,
and a Go failure and a Python failure sharing one status line reads badly — the
same reason `race` is already separate ("its failures read very differently").
Not in `integration` or `race` either: the SDK's tests run against a mock
transport and need neither PostgreSQL nor the broker, so putting them there
would make a pure-unit gate wait on Compose. Jobs run in parallel, so a fourth
costs no wall-clock time.

### Client configuration: the SDK's own environment variables

`TASKFORGE_SDK_API_URL` and `TASKFORGE_SDK_API_KEY`. Precedence is constructor
argument, then environment variable, then the loopback default
`http://127.0.0.1:8080` — the same order
[`internal/cli.ResolveBaseURL`](../../internal/cli/config.go) applies.

**Not `TASKFORGE_API_ADDR`.** That is `taskforge-api`'s own *bind* address: a
bare `host:port` with no scheme, validated to be a loopback bind. A bind address
and a reachable client target are different shapes — a server can bind a
wildcard no client can dial, and a client needs a scheme. M5D reused it, review
found that a defect, and it was reversed; this record does not reintroduce it.

**Also not `taskforge-cli`'s `TASKFORGE_CLI_API_URL` / `TASKFORGE_CLI_API_KEY`.**
Those are documented in `internal/cli/config.go` as the variables
*`taskforge-cli`* reads; a second, different-language consumer reading them
would falsify that sentence, which is the same one-name-two-readers problem in a
new costume.

The credential half is the stronger half of this argument. A CLI is a process a
developer invokes deliberately. **An SDK is a library inside someone else's
process.** A developer who exported `TASKFORGE_CLI_API_KEY` for their shell
would otherwise have every Python process in that shell silently acquire that
credential — including a `TaskForgeClient()` whose author believed it had none.
Ambient credential pickup is defensible for a CLI and is not defensible for a
library.

The repository already establishes one client-target variable per consumer:
`TASKFORGE_WORKER_API_URL` for `taskforge-worker`, `TASKFORGE_CLI_API_URL` for
`taskforge-cli`. The SDK is the third consumer and takes the third pair.

## Alternatives considered

**Poetry or uv for dependency management.** Both are better tools than `pip` for
an application, and either would have been defensible. Both were rejected for
the same reason: each is a *new mandatory installer* a contributor must acquire
before touching the SDK, against a spec whose §5 success criterion enumerates
the prerequisites, and each brings its own lockfile format as a second source of
truth beside `pyproject.toml`. Nothing forbids an individual developer using
`uv` locally; nothing in the repository requires it.

**Publishing to PyPI in V1.** Rejected above. Revisit at M8 with the rest of the
release story, if a published artifact is wanted at all.

**Sharing the CLI's environment variables.** Rejected above. The cost of the
decision taken is that a developer using both the CLI and the SDK sets two
pairs of variables; that is explicit, documented in `.env.example` and both
READMEs, and cheap.

**A separate toolchain-only milestone before the SDK.** Rejected as scaffolding
under [`AGENTS.md`](../../AGENTS.md) §3: a lint gate with nothing to lint and a
type gate with nothing to type-check are not independent evidence, and the CI
job would pass vacuously.

## Consequences

- Building and testing the SDK needs a Python 3.11+ interpreter. Running
  TaskForge itself still does not.
- `make lint` and `make test` remain Go-only. Python has its own four targets.
- The SDK is installable from a clone and is not on PyPI. An outside developer
  clones the repository — which they must do to run the stack anyway.
- `httpx` has its own release cadence and a 1.0 ahead of it. The `<1.0` ceiling
  is deliberate and will need a considered bump.
- The Go and Python error contracts are two hand-maintained copies of one
  mapping; Python cannot import a Go constant. `tests/test_errors.py` pins the
  exit codes and the reachable-code map so the copies cannot drift silently.
- Any future Python in this repository inherits these four decisions, or
  supersedes this record.
