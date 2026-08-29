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
| Services | Go (see `go.mod` for the pinned language version) |
| Database | PostgreSQL 16, accessed with `pgx/v5` and explicit SQL |
| Broker | SQS-compatible; ElasticMQ locally, AWS SQS Standard as the cloud direction |
| Local orchestration | Docker Compose |
| Migrations | Plain `.sql` files in `migrations/`, applied by the embedded runner in `internal/database` |
| Tests | `go test`, `testify` assertions, real PostgreSQL + real broker for integration |

Required to build and run: Git, Go, Docker, Docker Compose, GNU Make.

## 3. Directory conventions

```
cmd/<binary>/          Process entry points. Wiring only — no domain logic.
internal/<domain>/     Library code. Not importable outside this module.
migrations/            Versioned, forward-only SQL. Never edit an applied file.
api/                   OpenAPI description of implemented endpoints only.
tests/integration/     Tests requiring real PostgreSQL and/or a real broker.
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
```

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
