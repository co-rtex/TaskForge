# ADR-0017: Dashboard toolchain, serving, and credential handling

- **Status:** Accepted
- **Date:** 2026-09-30

## Context

[`docs/ROADMAP.md`](../ROADMAP.md)'s M6D delivers an operator dashboard —
Overview, Jobs, Job detail with attempt timeline, Workers, Queues, and DLQ —
reading only the public routes M6A shipped. A frontend is a first for this
repository in the way Python was for M5E, and the roadmap asked for the same
treatment [ADR-0016](0016-python-sdk-toolchain-and-client-configuration.md)
gave Python: build tooling, a lint and format story, and CI placement, each
decided rather than left as a side effect.

Three constraints bound every answer.

[`PROJECT_SPEC.md`](../PROJECT_SPEC.md) §5: "The full local stack starts from a
clean clone with only Git, Go, Docker, Docker Compose, and Make installed."
ADR-0016 discharged this for Python by observing that running TaskForge never
needs an interpreter; only building or testing the SDK does. **That escape does
not exist here.** A dashboard a developer uses is part of the running stack, so
whatever builds it is on the path from clean clone to working system.

[`PROJECT_SPEC.md`](../PROJECT_SPEC.md) §5 again: "The dashboard, CLI, and SDK
read live data. Nothing is fabricated or hardcoded."

Every `/v1` route requires `Authorization: Bearer <key>`, and
`internal/api/server.go` already registers a catch-all `"/"` that answers every
unrouted path with the structured JSON 404 — which M6B's bounded-span-name test
relies on.

## Decision

### Build: React, TypeScript, and Vite, run only inside a pinned container

`dashboard/` holds a React + TypeScript app built by Vite. Its **entire Node
toolchain lives in `dashboard/Dockerfile`**, `FROM` an image pinned by version
and by multi-platform index digest. `make dash-lint`, `make dash-test`, and
`make dash-build` each build one stage of that file; only `dash-build` copies
anything back to the host — the static output, into
`internal/dashboard/dist/`. `make dash-fmt` runs the formatter in the same
image against a bind mount. `npm ci --ignore-scripts` installs exactly the
committed lockfile and runs no dependency's install hook.

Docker is already a required prerequisite, so §5's list is **unchanged**. This
is a stronger property than ADR-0016 achieved: building or testing the Python
SDK still needs a host interpreter, while the dashboard needs no Node on the
host for anything — not to build, test, lint, format, or run it.
`node_modules/` never exists on the host.

The Dockerfile deliberately has no `# syntax=` directive: that would resolve a
Dockerfile frontend image by floating tag on every build, an unpinned
dependency beside the pinned one.

### Serving: embedded in `taskforge-api`, same-origin, under `/dashboard/`

`internal/dashboard` embeds the build with `//go:embed all:dist`. Only
`dist/.gitkeep` is committed, which is what lets every binary compile on a clean
clone before the frontend has been built (an empty embed pattern is a compile
error). Until then the binary serves a committed placeholder page, kept outside
`dist/` so a build never dirties a tracked file, that says the dashboard has not
been built, names `make dash-build`, and carries no script.

`internal/api`'s `WithDashboard(fs.FS)` is optional in exactly the way
`WithMetrics` is. Unset, nothing is registered and every path answers exactly
as before. Set, it registers one subtree pattern, `GET /dashboard/`, plus
`GET /{$}` redirecting the bare root there.

**The mount is `/dashboard/`, not `/`.** A single-page app at the root needs a
fallback to its entry document for every path that is not a file, and that
fallback would turn a mistyped `/v1/...` path into an HTML page unless it
carried a list of API-shaped exclusions — a list that would drift from the
route table. Under a prefix the question never arises: everything outside it
except the bare root is answered exactly as before M6D, including every
unrouted API path's JSON 404. One subtree pattern also means every client route
shares one span name and one metric `route` label, however many views exist.

Inside the mount a real file is served, a missing file under `assets/` is a
real 404 rather than HTML the browser would try to execute, dotfiles are never
served, and every other path gets the entry document so deep links and reloads
work. Content-hashed assets are cached immutably; the entry document is
`no-cache`.

**Same-origin is load-bearing, not a preference.** It removes CORS entirely:
no `Access-Control-Allow-Origin` decision, no preflight, and no trust boundary
moves. The dashboard adds no endpoint, no credential, and no server-side
session; it is a static client of routes that already exist.

### Credential: the operator's own key, in `sessionStorage`, behind a strict CSP

The operator pastes an API key minted exactly as for the CLI
(`taskforge-cli api-keys create`). It is held in `sessionStorage` only — never
`localStorage`, never a cookie, never a URL — so its lifetime in the browser is
bounded by the tab. Every read presents it as a bearer token with
`credentials: "omit"`.

The trade-off is recorded rather than hidden: a script running in the
dashboard's origin could read that key. The mitigation is concrete. Every
dashboard response carries a Content-Security-Policy of `default-src 'none';
script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors
'none'` (plus `base-uri`/`form-action 'none'`), with no `unsafe-inline` and no
`unsafe-eval`; the dashboard loads no third-party script; payloads and error
messages are rendered as React text, never as HTML; and a Go test asserts the
built `index.html` contains no inline script, so the policy and the build
cannot quietly disagree.

### Read-only, by two independent reasons

No write from the browser: no cancel, retry, replay, or bulk action. First,
stacking a browser-initiated destructive action on a brand-new browser
credential story would change two trust properties in one milestone. Second,
every write that matters here (`POST .../cancel`, `.../retry`,
`/v1/dlq/{job_id}/replay`) requires a caller-chosen `Idempotency-Key`, and how
a browser should mint and reuse one across a double click or an ambiguous
timeout is a real design question a read-only dashboard does not have to
answer yet. The DLQ view names the CLI command that performs a replay.

### Types: hand-written, with a drift test against `api/openapi.yaml`

`dashboard/src/api/types.ts` is written by hand so a reviewer can diff it
against the OpenAPI document — the same "no hidden shape" reasoning
[`AGENTS.md`](../../AGENTS.md) §5 gives for explicit SQL. Each interface is
paired with a `FieldSpec` constant the TypeScript compiler forces to name
exactly that interface's fields with their presence and nullability; a test
compares those constants, every enum, and every route and query parameter the
client sends against the real `api/openapi.yaml`.

### Lint and format: Biome

`biome ci` (format check and lint) and `tsc --noEmit` are the gates. One tool
and one config replace ESLint and Prettier — the same one-tool argument
ADR-0016 makes for `ruff` replacing black, isort, and flake8.

### CI: a fifth job that runs the Make targets

A `dashboard` job, parallel to `checks`, `integration`, `sdk`, and `race`, for
the reason `sdk` has its own: a different toolchain, and a Go failure and a
frontend failure sharing one status line reads badly. **It uses no
`actions/setup-node`.** It runs `make dash-lint`, `make dash-test`, and
`make dash-build`, so the Dockerfile's pinned image stays the single
declaration of the Node version — the rule the workflow header already applies
to Go — and CI proves exactly what a contributor gets. It then asserts the
build left every tracked file untouched, and embeds it with
`TASKFORGE_REQUIRE_BUILT_DASHBOARD=1`, which turns the built-output test's skip
into a failure.

`make fmt`, `make lint`, and `make test` stay Go-only, matching the `sdk-*`
precedent, so a Go contributor and the fast `checks` job never touch the
frontend toolchain.

## Alternatives considered

**A separate Compose service on its own port.** It would force CORS onto
`taskforge-api` — an allowed-origin policy, preflight handling, and a
credentialed cross-origin request — for no benefit this milestone needs, and it
would add a service the running stack depends on. Rejected.

**Node installed on the host.** The conventional setup, and the fastest local
loop. It adds Node to §5's prerequisite list, which is an amendment to the
project specification needing explicit sign-off, not an implementation call.
Rejected; the container gives the same build with no amendment.

**Server-rendered Go templates, no Node at all.** The strongest fit with this
repository's minimalism, and a genuinely good option. Rejected because the
acceptance criterion is four distinct, independently tested states per view
across six views, with client-side pagination and filters; a component model
with a DOM test harness makes each state a small, directly testable unit, where
templates would push that testing into string-matching rendered HTML.

**Mounting at `/` with an exclusion list.** Rejected above: the exclusion list
is a second copy of the route table that would drift.

**A server-side proxy holding the credential.** The browser would never hold a
key, which is a stronger posture. It is also a new credential-storage and
session surface on the server — cookies, CSRF, expiry, revocation — a milestone
before this project has a secrets story for one. Deferred, not rejected: it is
the natural next step if the dashboard ever leaves loopback.

**Generating types with `openapi-typescript` or similar.** Rejected for the
reason given above: a generated client hides the shape behind a tool, and the
drift test gives the same safety with code a reviewer can read.

**`actions/setup-node` in CI.** It would restate the Node version in a second
place and test a path — Node on the runner — that no contributor uses.
Rejected.

**A `dash-dev` target running the Vite dev server in a container.** It would
need the container to reach `taskforge-api` on host loopback, which works on
Docker Desktop but not on a Linux host without host networking — a target that
works on one OS is a target that fails loudly on another.
[`AGENTS.md`](../../AGENTS.md) §4 adds targets only when the behavior behind
them works, so there is none; the loop is `make dash-build` and a restart.

**A client-side routing library.** Six views, one parameterized, need a router
of about a hundred lines with its tests beside it. Rejected as a dependency
without a job.

## Consequences

- Building, testing, linting, or formatting the dashboard needs Docker and
  nothing else. Running TaskForge needs no Node either: the dashboard is part
  of `taskforge-api`'s binary.
- A binary built before `make dash-build` serves a placeholder page, and logs
  `dashboard_built=false`. `make build` does not build the frontend; a
  developer who wants the dashboard runs `make dash-build` first.
- There is no hot-reload development loop. Iterating on the frontend means
  `make dash-test` for behavior and `make dash-build` plus a restart to see it.
- The operator's API key is readable by script in the dashboard's origin for
  the life of the tab. The CSP, the absence of third-party script, and
  text-only rendering are the mitigation; the server-side proxy above is the
  remedy if that stops being enough.
- The dashboard sees only its key's scope, exactly like the CLI and the SDK.
  There is no cross-scope view.
- `types.ts` and `api/openapi.yaml` are two hand-maintained copies of one
  contract. The drift test pins them together; the Go contract tests already
  pin the OpenAPI document to the handlers.
- Any future frontend in this repository inherits these decisions, or
  supersedes this record.
