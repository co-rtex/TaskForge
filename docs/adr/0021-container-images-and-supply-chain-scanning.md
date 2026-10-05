# ADR-0021: Container images and supply-chain scanning: one Dockerfile, pinned bases, blocking scanners, one exceptions file

- **Status:** Accepted
- **Date:** 2026-10-05

## Context

Until M8B nothing in the repository built a container image, and nothing scanned a
dependency or the git history. [ROADMAP.md](../ROADMAP.md) listed both under M8
("Docker builds, and secret and dependency scanning"); the owner narrowed M8B to
exactly that and moved the two M7A gates and the carried M8A review items to M8D.

Three things about the repository make the obvious versions of this work wrong:

- **The binaries embed what they serve.** `migrations/embed.go` compiles the SQL
  in, and `internal/dashboard/dashboard.go` compiles `internal/dashboard/dist` in.
  `dist` is gitignored apart from `.gitkeep`, so a clean clone builds an api that
  serves a placeholder page. An `api` image built from a clean clone would pass
  every check except being the product.
- **The toolchain has several declarations that must agree.** `go.mod` declares the
  language version, CI reads it (`go-version-file`), and an image needs a Go of its
  own. A Dockerfile that restates the version is a second source of truth that
  drifts silently: CI would test with one compiler and the image would ship binaries
  from another.
- **The scanners do not share a policy.** govulncheck has no way to accept a finding
  until a date; gitleaks, pip-audit and npm audit each have an ignore mechanism of
  their own, in four formats, with different expiry semantics (mostly none). A
  blocking scan with four bespoke ignore files is a scan people learn to bypass.

The owner decided the shape (recorded in the M8B prompt and repeated here as
decisions, not options): scanners **block** CI; exceptions live in **one committed
file** and carry `id, tool, reason, accepted_by, expires`; a malformed or expired
entry fails CI; and there is **one root Dockerfile** with a builder stage and one
final target per service.

## Decision

### Images

One root `Dockerfile`: a `builder` stage, a `runtime` stage, and six final targets
(`api`, `outbox`, `scheduler`, `reconciler`, `worker`, `migrate`), each
`FROM runtime` and each copying exactly one binary. There is no CLI image (the owner's
decision): the CLI is not a service.

- **Builder:** `golang:<version>@sha256:<digest>`, where `<version>` is exactly
  `go.mod`'s `go` directive. `tests/verification/images_test.go` fails if the two
  differ. `go.mod` therefore names a patch release (`go 1.25.14`), not a minimum.
  The one build command names the six `./cmd/taskforge-*` packages (never
  `./cmd/...`, which would add the CLI) with `CGO_ENABLED=0`, `-trimpath`,
  `-buildvcs=false` and `-ldflags="-s -w"`. `-buildvcs=false` because the build
  context has no `.git`; the commit is recorded by a label instead.
- **Runtime:** `gcr.io/distroless/static-debian12:nonroot@sha256:<digest>`. Static
  binaries need no libc, shell or package manager, and `nonroot` is uid 65532. Each
  image carries `org.opencontainers.image.source` and
  `org.opencontainers.image.revision` (from the `REVISION` build argument, which
  `make images` fills from `git rev-parse HEAD`).
- **Entrypoint:** exec form, the service's own binary. There is no shell in the
  image to put in the way.
- **Context:** `.dockerignore` is an allowlist: `go.mod`, `go.sum`, `cmd`,
  `internal` and `migrations`, minus `*_test.go`. `internal` is what carries the
  built dashboard into the build, because it is gitignored and only Docker's ignore
  file decides whether it enters.
- **The api image is built after the dashboard.** `make images` depends on
  `make dash-build`. Node stays pinned in `dashboard/Dockerfile` and nowhere else;
  the root Dockerfile never names a Node image.
- **No registry.** Nothing is pushed, there is no registry login, and CI builds
  `linux/amd64` only. An image's identity here is "built from this commit and
  checked", which is what a pull request needs. Publication is a deployment concern
  and belongs to M8C.

### The image smoke

`scripts/imagesmoke` (run by `make images-smoke` and by CI) inspects and runs the
built images. It does not trust the Dockerfile to say what an image is.

Per image: the image exists; its user is non-root; its entrypoint is the service's
binary in exec form; both labels are present and the revision is this commit (an
image built from an older commit is refused, not passed); its filesystem holds its
own binary and no shell; and, with `--network none` and one invalid variable, it
exits non-zero **with the specific message** naming that variable. The invalid
variable is chosen per service from its real validation rules, because running an
image with no environment does not reject: five of the six pass validation and fail
later, connecting to the database.

`migrate` runs against the job's PostgreSQL and the smoke asserts, directly in
PostgreSQL, that `schema_migrations` holds the embedded migrations (count and
highest version), then runs it again and requires "already up to date".

`api` runs detached, and the smoke fetches `/dashboard/` and requires a marker only
the built dashboard contains, fetches the hashed script the page references, and
requires it to be **byte-identical to the file in `internal/dashboard/dist`** that
the image was built from, and requires the service's log to say the dashboard is
built. **What this proves:** the image's binary serves exactly the build that was in
the context. **What it does not prove:** that `dist` was fresh relative to the
dashboard's source (that is what `make dash-build` before `make images` is for, and
CI's `dashboard` job proves the build itself), or that the dashboard works.

### The scan driver

`scripts/scan` (`make scan`, and CI) is a Go program that runs four tools, parses
their machine-readable output, applies `security/scan-exceptions.yaml`, prints one
line per finding (tool, id, location, excepted or not) and exits non-zero on any
finding that is not excepted.

| Tool | Version pin | Scope |
| --- | --- | --- |
| govulncheck | `go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 -json ./...`, under `GOTOOLCHAIN=go<go.mod version>` | Reachable vulnerabilities only: a finding counts when its trace begins with a called function. Imported-but-never-called is not a finding. |
| gitleaks | `ghcr.io/gitleaks/gitleaks:v8.30.1@sha256:c00b6bd0…` | The full git history of every ref, not the checked-out tree. |
| pip-audit | `pip-audit==2.10.1` | The Python SDK's runtime dependency tree, resolved in a clean virtual environment. |
| npm audit | in the Node image `dashboard/Dockerfile` pins, `--omit=dev`, high and above | The dashboard's production dependencies only. |

The driver exists because govulncheck has **no suppression flag**, and because four
exit-code conventions need one policy applied once. A tool that cannot run, or whose
output is not that tool's report (empty, truncated, a different format), **fails the
scan**: a scan that could not run is not a scan that found nothing. Findings are
matched to exceptions by the tool's own identifier or any of its aliases, within the
same tool only.

govulncheck runs under the toolchain `go.mod` declares because it reports
standard-library vulnerabilities against whichever Go runs it; on a workstation with
a newer Go the scan would pass for a compiler the images are not built with.
v1.7.0 is the newest govulncheck that builds with Go 1.25.x (v1.8.0 requires 1.26),
so the pin rises with `go.mod`'s Go, not before it.

### The exceptions file

One file, `security/scan-exceptions.yaml`. Each entry has all five fields: `id`,
`tool`, `reason`, `accepted_by`, `expires` (an ISO date). The driver fails the run
on: a missing or empty field, an unknown key (so a misspelled `expires` cannot make
an exception permanent), an unknown tool, a date that is not an ISO date, an expired
entry, or a duplicate. An entry holds through the **end of its `expires` day** (UTC)
and fails from the next. An entry that matches nothing in a run is **noted, not
failed**: a finding that has been fixed should prompt removal, not break the build.
An expired entry excuses nothing, even if it names a live finding.

The file is checked on every run, whatever the scanners find, so a lapsed exception
cannot sit unnoticed until the finding returns. `security/scan-exceptions.yaml` is
empty at M8B because nothing is excepted.

### Secrets

gitleaks keeps its default rules on. `.gitleaks.toml` allowlists only known fake
fixtures, each entry scoped to **a rule, a path anchored to one file, and the value**
(`condition = "AND"`), with a comment naming the fixture. `tests/verification`
fails an entry that lacks any of these. **A real secret is never allowlisted.** If
the scan finds one, the response is to revoke it and remove it from use, and to leave
the configuration alone; an allowlist entry wider than its fixture hides the next
real secret that lands in the same file.

gitleaks runs from its container image, pinned by version and by digest, and not as
`gitleaks/gitleaks-action`, which requires a licence for organisations.

### CI

Two new jobs rather than steps in existing ones.

- **`images`** needs Docker, the dashboard build and PostgreSQL, and its failures
  (a root image, a wrong schema) read differently from a Go test failure. It owns
  its Compose lifecycle, like `race`, because the smoke applies migrations from the
  migrate image to a database that has none; reusing the integration job's migrated
  database would make "applies the migrations" untestable.
- **`scan`** needs the full history, which no other job fetches, and its failures are
  of a different kind: an advisory published tomorrow fails it with no code change,
  and that must read as its own status line and not turn `checks` red. It runs
  `make scan`, so the exceptions apply identically in both places.

Every action is pinned by commit SHA, every image by digest, every Go tool at an
exact tag, pip-audit with `==`. `tests/verification/ci_supply_chain_test.go` holds
those rules and the shape of the two jobs.

## Alternatives considered

- **An image vulnerability scanner (Trivy, Grype).** Out of scope by the owner's
  decision, and largely redundant here: the images contain one static Go binary on a
  distroless base, so govulncheck covers the binary and the base is pinned by digest.
  The cost is that a vulnerability in the distroless base itself is not scanned; it
  is addressed by bumping the pinned digest.
- **Dependabot or Renovate.** Out of scope. Pins are bumped by hand, each in its own
  commit, with the suites green.
- **`golang/govulncheck-action`, `gitleaks/gitleaks-action`, and similar actions.**
  The first cannot express an exception with an expiry; the second needs a licence
  for organisations; both add an unpinned-by-us layer between the repository and the
  tool. A script run by `make scan` is the same command locally and in CI.
- **Each tool's native ignore mechanism** (a `.gitleaksignore` file, pip-audit's
  `--ignore-vuln`, an `npm audit` allow-list). Different formats, no common expiry,
  and govulncheck has none. One file, one schema, one expiry rule was the owner's decision.
- **Exceptions without expiry.** A permanent exception is a decision nobody owns
  any more. The date forces it back in front of a person.
- **Failing on imported-but-uncalled vulnerabilities.** Noisy to the point of being
  ignored. The owner chose reachable-only for govulncheck.
- **A second Dockerfile per service, or a base image per service.** Six Dockerfiles
  would have six places to bump a pin. One Dockerfile with one `runtime` stage pins
  each base exactly once, which a test enforces.
- **Alpine or Debian slim as the base.** A shell and a package manager in every
  image, for binaries that need neither.
- **Building the binaries on the host and copying them in.** The image would then
  hold whatever the developer's compiler produced. Building in the pinned builder
  makes the image a function of the commit and the digest.
- **Multi-arch images, SBOMs and signatures.** Not part of M8B's scope; they belong
  with publication, if ever, and with M8C.
- **A scheduled scan of `main`.** Would find a newly published advisory before the
  next pull request does. It adds a trigger and a notification path nobody has
  chosen; left for a later decision.

## Consequences

- **A pull request can fail for a reason that is not in it.** An advisory published
  after the last green run blocks the next scan. That is the cost of "scanners
  block", accepted by the owner. The response is an upgrade in its own commit with
  the suites green, or a dated exception with a reason.
- **`go.mod` names a patch release.** Raising the Go version is one edit to
  `go.mod` and one to the Dockerfile's builder (version and digest), kept in step by
  a test. This was forced at M8B: the first full scan found 35 reachable
  standard-library vulnerabilities against Go 1.25.0 and the fix was Go 1.25.14.
- **govulncheck is pinned below its newest release** until `go.mod` moves to Go 1.26.
- **The pip-audit scope has no lockfile.** The SDK pins `httpx>=0.27,<1.0` and
  nothing else, so what is audited is the resolution on the day of the scan. A
  vulnerability in a version `pip` would not choose today is not visible; a finding in
  one it would is.
- **npm audit and pip-audit depend on their registries being reachable.** An outage
  fails the scan (a tool that could not run is a failure), and a rerun clears it.
- **CI images are `linux/amd64`;** the images a developer builds on Apple silicon are
  `arm64`. Sizes and IDs differ; the checks do not. On a pull request the revision
  label is the merge commit the runner checked out, not the branch head.
- **Nothing here proves the images are deployable.** The smoke shows each image
  starts, rejects bad configuration, and (for `migrate` and `api`) does its job
  against PostgreSQL. That the services work together in containers, on a network,
  with a real bind address, is M8C, which needs its own decision about the non-loopback
  bind (ADR-0018).
- **The leak scan checks history, and history is permanent.** A fixture allowlisted
  here stays allowlisted for the commit that introduced it; a real secret found in
  history is a revocation, not a rewrite.
