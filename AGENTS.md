# AGENTS.md — TaskForge Engineering Rules

Stable, repository-wide rules for any human or agent contributing to TaskForge.
These rules change rarely. They are not a status report.

- **What TaskForge is:** [docs/PROJECT_SPEC.md](docs/PROJECT_SPEC.md)
- **How it is built:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **What is actually implemented right now:** [docs/CURRENT_STATE.md](docs/CURRENT_STATE.md)
- **What comes next:** [docs/ROADMAP.md](docs/ROADMAP.md)
- **Why decisions were made:** [docs/adr/README.md](docs/adr/README.md)

---

## 1. Purpose

TaskForge is an open-source distributed job-processing, scheduling, and reliability
platform. It implements the hard control-plane behavior itself — durable job
lifecycle, explicit state machines, idempotent submission, transactional
database-to-broker delivery, leases, fencing, and crash recovery — rather than
wrapping an existing queue framework. Correctness, explicit semantics, and truthful
evidence matter more than feature count.

## 2. Toolchain

| Concern | Choice |
| --- | --- |
| Services | Go (see `go.mod`, which names an exact patch release; the `Dockerfile`'s builder image must match it, and a test enforces that) |
| Database | PostgreSQL 16, accessed with `pgx/v5` and explicit SQL |
| Broker | SQS-compatible; ElasticMQ locally, AWS SQS Standard as the cloud direction |
| Local orchestration | Docker Compose |
| Migrations | Plain `.sql` files in `migrations/`, applied by the embedded runner in `internal/database` |
| Tests | `go test`, `testify` assertions, real PostgreSQL + real broker for integration |
| Python SDK | Python 3.11+, PEP 621 `pyproject.toml`, stdlib `venv` + `pip`, one runtime dependency (`httpx`) |
| Python tooling | `ruff format` (authoritative), `ruff check`, `mypy --strict` |
| Operator dashboard | React + TypeScript built by Vite, embedded into `taskforge-api` with `go:embed`; Node runs **only** inside the digest-pinned image in `dashboard/Dockerfile` |
| Dashboard tooling | Biome (`biome ci`, authoritative formatter and linter), `tsc --noEmit`, Vitest |
| Container images | One root `Dockerfile`: a digest-pinned `golang` builder and a digest-pinned `distroless/static-debian12:nonroot` base, six targets, no CLI image |
| Supply-chain scanning | `scripts/scan` driving govulncheck, gitleaks (pinned container), pip-audit and npm audit (in the dashboard's pinned Node). Dated risk acceptance for the three dependency scanners lives in `security/scan-exceptions.yaml`; a gitleaks finding is accepted only in `.gitleaks.toml` |

Required to build and run: Git, Go, Docker, Docker Compose, GNU Make.
Additionally required to build or test the Python SDK in `sdk/python`: a
Python 3.11+ interpreter. Running TaskForge itself never needs one. See
[ADR-0016](docs/adr/0016-python-sdk-toolchain-and-client-configuration.md).
The dashboard adds **no host prerequisite at all** — not to build, test, lint,
format, or run it. Every `dash-*` target runs Node inside Docker, which is
already required, and `node_modules/` never exists on the host. See
[ADR-0017](docs/adr/0017-dashboard-toolchain-and-serving.md).

## 3. Directory conventions

```
cmd/<binary>/          Process entry points. Wiring only — no domain logic.
internal/<domain>/     Library code. Not importable outside this module.
migrations/            Versioned, forward-only SQL. Never edit an applied file.
api/                   OpenAPI description of implemented endpoints only.
scripts/               Developer scripts. scripts/demo is the Go program behind
                       `make demo` and `make demo-failure`, and scripts/bench the
                       one behind `make bench` and `make bench-smoke`: deliberately
                       not under cmd/, so `make build` neither builds nor ships
                       them. scripts/internal/stack is the hermetic stack of real
                       processes both run; scripts/readdb is the read-only database
                       access and the measurement queries they and
                       tests/integration share (it is not under internal/ because
                       Go would then keep tests/integration out).
tests/integration/     Tests requiring real PostgreSQL and/or a real broker.
tests/verification/    Infrastructure-free checks over the repository's own
                       documentation: the verification-matrix drift check.
sdk/python/            The Python SDK. Its own toolchain; see ADR-0016.
dashboard/             The operator dashboard's frontend source. Its own
                       toolchain, containerized; see ADR-0017.
internal/dashboard/    go:embed of the dashboard build. Only dist/.gitkeep and
                       the not-built placeholder page are committed.
docs/                  Canonical project context. See the links above.
docs/adr/              Architecture Decision Records.
```

Create a package, service, or directory when the current milestone puts working
behavior in it. Do not scaffold empty placeholders to make the tree look complete.

## 4. Commands

Every target must fail loudly and exit non-zero on failure. Never add a target that
swallows an error or prints success after a failed command.

```bash
make help              # list targets
make up                # start local infrastructure (PostgreSQL, broker)
make down              # stop infrastructure
make logs              # tail infrastructure logs
make migrate           # apply migrations to the local database
make fmt               # gofmt -w
make lint              # gofmt check + go vet
make test              # unit + integration
make test-unit         # no external dependencies
make test-integration  # requires `make up`
make test-race         # race detector
make build             # compile all binaries into ./bin
make demo              # success demonstration: succeed, retry, dead-letter
make demo-failure      # failure demonstration: a killed and a frozen worker
make bench             # the recorded benchmark: throughput, then faults (clean tree, ~25 min)
make bench-smoke       # the benchmark harness in miniature: ~1 min, records nothing
make images            # build the dashboard, then the six service images as taskforge-<service>:dev
make images-smoke      # build the images, then inspect and run them (needs `make up`)
make scan              # govulncheck, gitleaks, pip-audit and npm audit; blocks on any unexcepted finding
```

`make demo` and `make demo-failure` build the binaries, start the infrastructure,
migrate, and then run `go run ./scripts/demo success|failure`. The program starts
its own services on free loopback ports, with a broker queue and a key scope of
its own, asserts only on the jobs it submitted, never deletes or truncates
anything, stops everything it started on every way out, and exits non-zero if any
expectation fails. They leave the infrastructure up; `make down` is separate.

`make bench` builds the binaries, starts the infrastructure, migrates, and runs
`go run ./scripts/bench throughput faults --record`. It refuses to start unless
`git status --porcelain` is empty and every binary in `bin/` was built from
`HEAD`; refuses, in every mode, while another TaskForge service is running on the
machine (a stray outbox or scheduler takes other runs' work); measures every
instant on PostgreSQL's clock; uses the shipped default
timings unless a labelled `--profile tuned` run is asked for; and writes
`docs/benchmarks/<date>-<sha>.md` and `.json`, never overwriting one. A missed
target is recorded as missed, not re-run. `make bench-smoke` asserts that the
harness measured validly and records nothing; it is the only part CI runs, and CI
never records numbers. The definitions are in
[ADR-0020](docs/adr/0020-benchmark-methodology.md).

`make images` builds `internal/dashboard/dist` first (the api embeds it; a clean
clone has only a placeholder) and then one image per service from the root
`Dockerfile`, each labelled with the commit. `make images-smoke` runs
`go run ./scripts/imagesmoke`: every image must run as a non-root user, hold only its
own binary, and reject an invalid configuration with the specific message; the
migrate image must **apply** every embedded migration into an empty database the
smoke creates for it (`taskforge_imagesmoke_<pid>` on the same server, dropped on
every way out, so the database role needs `CREATEDB`) and leave it at the embedded
schema version; and the api image must serve the dashboard build it was built with.
Nothing is pushed anywhere.

`make scan` runs `go run ./scripts/scan`: govulncheck (reachable findings only, under
the Go that `go.mod` declares), gitleaks over the full git history, pip-audit over
the SDK's runtime tree, and npm audit over the dashboard's production dependencies.
A govulncheck, pip-audit or npm audit finding without a valid, unexpired entry in
`security/scan-exceptions.yaml` fails it, as does a malformed or expired entry; an entry
for gitleaks there is an error. A gitleaks finding is accepted only in `.gitleaks.toml`,
as a fixture (a fake value) or a revoked secret pinned to its commit. pip-audit and its
whole dependency tree are installed from the hash-locked
`security/pip-audit.requirements.txt`. It needs Docker, Python 3, the network and a full
git history (a shallow clone is refused). **A live secret is never allowlisted anywhere:
revoke it first, then pin it as revoked.** The decisions are in
[ADR-0021](docs/adr/0021-container-images-and-supply-chain-scanning.md).

The Python SDK has its own targets. They are deliberately **not** folded into
`fmt`, `lint` and `test`, which stay Go-only so a Go contributor — and the fast
CI job that runs them — never needs a provisioned virtualenv.

```bash
make sdk-venv          # create sdk/python/.venv and install with dev extras
make sdk-fmt           # ruff format
make sdk-lint          # ruff format --check + ruff check + mypy --strict
make sdk-test          # pytest
```

The dashboard's targets follow the same rule for the same reason, and each
runs inside the pinned Node image rather than on the host.

```bash
make dash-fmt          # biome format --write, in place
make dash-lint         # biome ci + tsc --noEmit
make dash-test         # vitest
make dash-build        # build into internal/dashboard/dist for go:embed
```

A `taskforge-api` built before `make dash-build` serves a page saying the
dashboard has not been built. Rebuild the binary after `make dash-build` to
embed the real one.

Targets are added only when the behavior behind them actually works.

## 5. Coding conventions

- Idiomatic Go. `gofmt` is authoritative; `go vet` must be clean.
- Explicit SQL. Do not hide transactions, locks, or state transitions behind an
  abstraction that stops a reviewer from reasoning about correctness.
- Domain types are typed (`jobs.Status`, not `string`) with validated transitions.
  No generic "set any status" function and no generic status-update endpoint.
- `context.Context` is the first parameter of every function that does I/O.
- Inject clocks and random sources into domain logic so tests are deterministic.
  Never call `time.Now()` or `rand` directly inside a decision function.
- Wrap errors with `%w` and enough context to locate the failure. Do not log and
  return the same error; do one or the other.
- Errors crossing the HTTP boundary become a stable, structured error body. Internal
  detail (SQL text, driver errors, payload contents) never reaches a client.

## 6. Database rules

- PostgreSQL is the authoritative store for all control-plane state.
- Migrations are forward-only and numbered `NNNN_description.sql`. Once a migration
  has been applied anywhere, it is immutable — add a new one instead.
- Express invariants in the schema: primary keys, foreign keys, `CHECK`
  constraints, unique and partial-unique indexes. Do not rely on application code
  alone to enforce an invariant the database can enforce.
- Use PostgreSQL server time (`now()`) for anything that affects eligibility,
  expiry, or staleness. Client- or worker-supplied wall-clock time is never
  authoritative.
- Every index must have a query that justifies it. Every lock must have a comment
  explaining what race it prevents and in what order it is acquired.
- Multi-instance safety is the default assumption: every scan or claim loop must be
  correct with N replicas running concurrently.

## 7. Testing rules

- Unit tests cover domain decisions: state transitions, validation, fingerprinting,
  backoff. They use fake clocks and seeded randomness, never real sleeps.
- Integration tests cover anything whose correctness depends on the database or the
  broker: migrations, transaction boundaries, locking, idempotency, outbox delivery,
  concurrency, and recovery. Mocks may isolate domain logic but must never be the
  only evidence that these work.
- Concurrency tests must use separate database connections, not one shared
  connection, or they prove nothing.
- Integration tests poll with a deadline. They do not sleep for an arbitrary
  duration when a condition can be observed.
- Assert durable state and history, not only HTTP status codes.
- Never weaken an assertion, delete a valid test, or raise a timeout to make a
  suite go green.

## 8. Documentation rules

- Each fact has exactly one canonical owner (see the table at the top of
  [CLAUDE.md](CLAUDE.md)). Other files link to it instead of restating it.
- Planned behavior is labeled planned. Never describe unbuilt behavior in the
  present tense.
- `docs/CURRENT_STATE.md` is updated at the end of every implementation session and
  must match the branch head.
- Record an architectural decision as an ADR when it constrains future work or has
  a real tradeoff. Do not write an ADR for a small coding choice.
- A milestone's status line must not depend on merge state. Write "complete; see
  PR #N", never "on its own branch and draft pull request": a document cannot keep
  merge state current, and the pull request is the record of it.

## 9. Verification honesty

**Never state that a build, test, migration, benchmark, or deployment succeeded
unless the command actually ran and actually passed.**

For each verification item record the exact command, the result
(PASS / FAIL / NOT RUN), and a concise summary of real output. If something cannot
be verified, say so and name the residual risk. Performance targets are targets
until a reproducible run measures them — never present a target as an achieved
result, in the README or anywhere else.

## 10. Security rules

- No secrets in the repository. `.env` is ignored; `.env.example` carries names and
  safe placeholder values only.
- All SQL is parameterized.
- Every HTTP handler enforces a body-size limit and a timeout.
- Logs never contain secrets or unbounded request payloads.
- TaskForge executes only trusted handlers registered in its own binary. It does not
  and will not run uploaded scripts, arbitrary shell commands, or untrusted plugins.
  The registered set is pinned by a test, and
  [ADR-0019](docs/adr/0019-demo-handlers-are-trusted-built-ins.md) records why the
  demonstration handlers are part of it.
- Local services bind to loopback. Nothing is exposed publicly without authentication.

## 11. Git rules

- Work on a feature branch. Never commit directly to `main`.
- Small, coherent commits at points where the tree builds and the relevant checks pass.
- Conventional-commit style subjects: `feat(scope):`, `fix(scope):`, `docs:`,
  `test(scope):`, `chore:`.
- Stage deliberately. Never `git add -A` in a dirty worktree.
- Preserve unrelated changes. Never stash, revert, or commit work you did not author
  in this session.
- Never force-push, amend a pushed commit, rewrite shared history, or delete branches.
- Every commit is authored **and** committed by `Christian Cortez` using an email
  verified for the GitHub account `co-rtex`, configured with `git config --local`.
  Never modify global Git configuration.
- Never attribute a commit to an AI, add AI co-author trailers, or mention AI
  authorship in a commit message.
- Push after every meaningful checkpoint, and verify the remote SHA before claiming
  a push succeeded.
