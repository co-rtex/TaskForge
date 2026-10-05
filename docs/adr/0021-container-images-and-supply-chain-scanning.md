# ADR-0021: Container images and supply-chain scanning: one Dockerfile, pinned bases, blocking scanners, two acceptance mechanisms

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
  their own, in different formats, with different expiry semantics (mostly none). A
  blocking scan with four bespoke ignore files is a scan people learn to bypass.

The owner decided the shape (recorded in the M8B prompt and repeated here as
decisions, not options): scanners **block** CI; exceptions are committed, and carry
`id, tool, reason, accepted_by, expires`; a malformed or expired entry fails CI; and
there is **one root Dockerfile** with a builder stage and one final target per
service. The first version of the exceptions decision was *one file for all four
tools*. In the review of PR #22 the owner refined it into **two acceptance mechanisms
with different meanings** (below), because a gitleaks finding is not the kind of thing
a date can accept.

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

`migrate` runs against a database **the smoke creates for it**: `CREATE DATABASE
taskforge_imagesmoke_<pid>` on the server `TASKFORGE_DATABASE_URL` names, over a
separate read-write connection (`scripts/readdb` is read-only on purpose and cannot
create a database), dropped with `DROP DATABASE … WITH (FORCE)` on success, on
failure and on interrupt. The migrate image is pointed at it by rewriting only the
database name in the URL. The first run must **apply** every embedded migration, with
the count exact: "schema already up to date" on the first run is a failure, because
it is what a database something else already migrated says, and it would let an image
that applies nothing pass. The second run must report "already up to date". The smoke
then reads `schema_migrations` in that database, directly, and requires the embedded
count and highest version. The freshness of the database is therefore guaranteed by
the smoke itself and not by the lifecycle of the job that runs it. (The first version
of this check ran against the shared database and accepted "already up to date";
a host `make migrate` made it pass without the image applying anything. That was
found in review of PR #22 and fixed.)

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
their machine-readable output, applies `security/scan-exceptions.yaml` to the findings
of govulncheck, pip-audit and npm audit, prints one line per finding (tool, id,
location, excepted or not) and exits non-zero on any finding that is not excepted. A
gitleaks finding is accepted by gitleaks's own configuration, `.gitleaks.toml`, which
the driver hands to the container; a finding gitleaks still reports is never excepted.

| Tool | Version pin | Scope |
| --- | --- | --- |
| govulncheck | `go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 -json ./...`, under `GOTOOLCHAIN=go<go.mod version>` | Reachable vulnerabilities only: a finding counts when its trace begins with a called function. Imported-but-never-called is not a finding. |
| gitleaks | `ghcr.io/gitleaks/gitleaks:v8.30.1@sha256:c00b6bd0…` | The full git history of every ref, not the checked-out tree. |
| pip-audit | `pip-audit==2.10.1`, installed with its whole dependency tree from the hash-locked `security/pip-audit.requirements.txt` | The Python SDK's runtime dependency tree, resolved in a clean virtual environment (this tree is deliberately not locked: it is what is examined). |
| npm audit | in the Node image `dashboard/Dockerfile` pins, `--omit=dev`, high and above | The dashboard's production dependencies only. |

The driver exists because govulncheck has **no suppression flag**, and because four
exit-code conventions need one policy applied once. A tool that cannot run, or whose
output is not that tool's report (empty, truncated, a different format), **fails the
scan**: a scan that could not run is not a scan that found nothing. Findings are
matched to exceptions by the tool's own identifier or any of its aliases, within the
same tool only, and a gitleaks finding is never matched to an entry in the exceptions
file at all.

govulncheck runs under the toolchain `go.mod` declares because it reports
standard-library vulnerabilities against whichever Go runs it; on a workstation with
a newer Go the scan would pass for a compiler the images are not built with.
v1.7.0 is the newest govulncheck that builds with Go 1.25.x (v1.8.0 requires 1.26),
so the pin rises with `go.mod`'s Go, not before it.

### Two acceptance mechanisms, with distinct meanings

Accepting a finding means different things for different tools, so there are two
mechanisms and each is the only place for what it accepts. This was the owner's
decision in the PR #22 review.

**`security/scan-exceptions.yaml` is a dated risk acceptance, for govulncheck,
pip-audit and npm audit only.** Those findings are vulnerabilities in code the project
depends on, and "we accept this until the fix lands or the date arrives" is a
meaningful statement about them. Each entry has all five fields: `id`, `tool`,
`reason`, `accepted_by`, `expires` (an ISO date). The driver fails the run on: a
missing or empty field, an unknown key (so a misspelled `expires` cannot make an
exception permanent), a tool other than those three, a date that is not an ISO date,
an expired entry, or a duplicate. **An entry for `gitleaks` is an error** that names
`.gitleaks.toml`, and a gitleaks finding never matches an exception, even one handed to
the matcher directly. An entry holds through the **end of its `expires` day** (UTC) and
fails from the next. An entry that matches nothing in a run is **noted, not failed**:
a finding that has been fixed should prompt removal, not break the build. An expired
entry excuses nothing, even if it names a live finding. The file is checked on every
run, whatever the scanners find, so a lapsed exception cannot sit unnoticed until the
finding returns. It is empty at M8B because nothing is excepted.

**`.gitleaks.toml` is the only place a gitleaks finding is accepted.** gitleaks keeps
its default rules on. The file holds exactly two kinds of `[[allowlists]]` entry,
**both permanent by design, because history is permanent**:

1. **FIXTURE: a fake value.** `targetRules` (one rule), `paths` (one entry, anchored
   `^...$` and a literal file path), `regexes` (one entry, a **literal** value: no
   regex metacharacter other than an escaped one) and `condition = "AND"`, so rule,
   file and value must all match. A comment above the entry names the fixture. The
   value must appear verbatim in the named file at `HEAD`, so an entry cannot outlive
   its fixture, and a typo cannot silently allowlist nothing.
2. **REVOKED: a real secret that has already been revoked.** `targetRules` (one
   rule), `paths` (one anchored literal file) and `commits` (exactly one full 40-hex
   SHA), with `condition = "AND"` and **no `regexes`**: the entry never contains the
   secret's value. A comment above it states `Revoked YYYY-MM-DD by <name>` and what
   the credential was for. It excuses that rule, in that file, at that one commit, and
   nothing else: the same value committed again, in another commit, is a new finding.

`tests/verification` parses the file and fails an entry that is neither kind, or both
(a `commits` and a `regexes` together, or neither), or that carries any other key,
including a `description` on a REVOKED entry, which would be a place to put the value.
It also refuses any table other than `[extend]` and `[[allowlists]]`, so a global
`[allowlist]` or a custom rule cannot appear unreviewed. No REVOKED entry exists today.

**A live secret is never allowlisted. The procedure is: revoke it, then pin it.**
If the scan finds a real secret, stop. Revoke it at its source and confirm that it no
longer works; remove it from the current tree; and only then add a REVOKED entry for
the finding's rule, file and commit (the finding's id is `commit:file:rule:line`), with
the comment above it. Editing `.gitleaks.toml` first, to make the scan pass, is the
failure this procedure exists to prevent.

gitleaks runs from its container image, pinned by version and by digest, and not as
`gitleaks/gitleaks-action`, which requires a licence for organisations.

### CI

Two new jobs rather than steps in existing ones.

- **`images`** needs Docker, the dashboard build and PostgreSQL, and its failures
  (a root image, a wrong schema) read differently from a Go test failure. It owns
  its Compose lifecycle, like `race`, so that it never depends on another job's
  database. Whether the migrate image applies migrations does not rest on that: the
  smoke creates its own empty database.
- **`scan`** needs the full history, which no other job fetches, and its failures are
  of a different kind: an advisory published tomorrow fails it with no code change,
  and that must read as its own status line and not turn `checks` red. It runs
  `make scan`, so the acceptances apply identically in both places. It checks out the
  full history (`fetch-depth: 0`), and the driver refuses a shallow repository
  ("shallow clone: gitleaks would scan partial history; fetch full history"), so a
  checkout that fetched less cannot pass for a scan of the history.

Every action is pinned by commit SHA, every image by digest, every Go tool at an
exact tag, and **pip-audit's whole dependency tree is hash-locked**: the scanner is
installed from `security/pip-audit.requirements.txt`, in which pip-audit and every
package it needs is pinned with `==` and every distribution file carries a
`--hash=sha256:` digest, with `pip install --require-hashes --no-deps -r`. Pinning
only `pip-audit==X` would leave its dependencies (`requests`, `urllib3`, `rich`, …) to
resolve afresh on every run, so the check meant to find a compromised dependency would
itself run whatever PyPI served that day. The lock is generated once by a named,
versioned tool recorded in the file's header (`uv 0.12.23`, with `--universal` so that
the hashes cover linux/amd64 on Python 3.13 and a developer's Mac alike) and that tool
is not part of the scan. The version of pip-audit lives in that file and nowhere else.
`tests/verification/ci_supply_chain_test.go` and `pip_audit_lock_test.go` hold these
rules and the shape of the two jobs.


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
- **Each tool's native ignore mechanism** for the dated findings (pip-audit's
  `--ignore-vuln`, an `npm audit` allow-list). Different formats, no common expiry,
  and govulncheck has none. One file with one schema and one expiry rule for those three
  tools was the owner's decision.
- **Dated exceptions without expiry.** A permanent acceptance of a vulnerability is a
  decision nobody owns any more. The date forces it back in front of a person. (This
  is the argument for expiry on the three dependency scanners, and the reason it does
  not carry over to gitleaks, below.)
- **Accepting gitleaks findings through the dated exceptions file.** Rejected, in the
  PR #22 review. *History is permanent*: a fixture committed in 2026 is in the history
  in 2036, and a revoked credential stays revoked, so an expiry date on either would
  only create renewal churn: a date to bump that carries no information. Naming the
  finding by value would be worse. A revoked credential's value written into an
  exceptions entry, or into a `regexes` allowlist, puts that credential back into the
  tree, and excuses the same value wherever it is committed next. So REVOKED entries
  are pinned to a commit instead and never carry the value, and the dated file does not
  take gitleaks at all.
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
- **The SDK has no lockfile, so what pip-audit audits is the resolution on the day of
  the scan.** The SDK pins `httpx>=0.27,<1.0` and nothing else. A vulnerability in a
  version `pip` would not choose today is not visible; a finding in one it would is.
  (The scanner's own tree is locked; the tree it examines is not. They are different
  trees.)
- **The pip-audit lock must be regenerated by hand to move.** Bumping pip-audit, or
  picking up a fixed dependency of it, is an edit to the command in the lock's header,
  a rerun, and a reviewed diff. It is not picked up on its own. The `pip` that creates
  the scanner's virtual environment is the one bundled with the interpreter and is not
  itself locked; the lock covers everything pip installs into the environment (which
  includes a newer `pip`, a dependency of `pip-api`).
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
- **The leak scan checks history, and history is permanent.** A fixture entry stays
  until the fixture is removed from its file (a test then fails it); a REVOKED entry
  stays as long as the commit does. A real secret found in history is a revocation
  followed by a pin, not a rewrite of history and not a dated exception.
- **A REVOKED entry is as narrow as one commit.** A revoked value that was copied
  into several commits needs one entry per commit, and a fixture value reused in a
  second file needs its own entry. That is the cost of not allowlisting by value.
- **The shape check is a scoped text parser, not a TOML library,** because adding a
  dependency is the owner's decision. It reads only the subset `.gitleaks.toml`
  uses and refuses what it does not understand.
