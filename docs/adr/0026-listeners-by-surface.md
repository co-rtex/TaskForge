# ADR-0026: Listeners by surface

- **Status:** Accepted
- **Date:** 2026-10-14
- **Amends:** [ADR-0018](0018-browser-origin-guard-on-the-internal-surface.md): its rule 3
  (the `Host` rule) and its Q3 ("the Host allowlist is fixed to loopback ... nothing is
  configurable") become per listener. The admin listener keeps rule 3 exactly as ADR-0018
  wrote it; the control listener checks an exact-match allowlist instead. Rules 1 and 2,
  their order, the guard's outermost position, the `403 origin_refused` contract and every
  other section of ADR-0018 stand.

## Context

M8C's deliverable is validated Terraform that runs the binaries as separate ECS services.
At `f8f3346` the binaries refuse that topology in three places:

- every listener must bind loopback (`internal/config/config.go:173–183`);
- the worker's `TASKFORGE_WORKER_API_URL` must name a loopback host (`:404–408`);
- ADR-0018's rule 3 refuses every non-loopback `Host` on `/internal`
  (`internal/api/origin_guard.go:148`), so a worker in another container, which dials the
  API by a name such as `api`, is refused at registration.

ADR-0018 named this: "M8's ECS workers will reach the API across a network under a
non-loopback name, so M8 has to revisit the bind and the Host rule together." The owner
split M8C into M8C1 (this: make the binaries deployable) and M8C2 (the Terraform), and
decided the questions below on 2026-10-14.

What made a single loopback listener safe is that the routes it serves differ in what
protects them. Credential administration (`/internal/v1/api-keys`, `/internal/v1/worker-keys`),
the dashboard's static files and `/metrics` take no credential at all, by design
([ADR-0013](0013-database-backed-api-key-authentication.md),
[ADR-0017](0017-dashboard-toolchain-and-serving.md)): loopback is their protection. The
`/v1` surface authenticates every request with an API key. The worker-control routes
authenticate registration with a worker key and every later call with the session that
registration created, plus a revocation check
([ADR-0014](0014-worker-control-authentication.md)). Only the last two can leave loopback.

## Decision

### Q2. Three listeners, and each route on the ones its protection allows

`taskforge-api` has up to three listeners, each serving a fixed share of the one route
table ([ADR-0022](0022-the-route-table-is-the-single-source-of-routes.md)):

| Listener | Variable | Address | Serves |
| --- | --- | --- | --- |
| admin | `TASKFORGE_API_ADDR` | required, loopback only (unchanged) | everything it served before M8C1 |
| public | `TASKFORGE_API_PUBLIC_ADDR` | optional, any address | the eleven `/v1` routes, `/healthz`, `/readyz` |
| control | `TASKFORGE_API_CONTROL_ADDR` | optional, any address | the eight worker-control routes, `/healthz`, `/readyz` |

Key administration, worker-key administration, `/dashboard/` (with the bare-root
redirect) and `/metrics` are admin-only. With both optional addresses unset the process
opens exactly one listener and behaves byte-identically to before:
`internal/api/testdata/route_behavior.golden` is unchanged and was not regenerated.

The listener is a **column of the table**: every entry names its listeners
(`adminOnly`, `adminAndPublic`, `adminAndControl`, `everyListener`). One mux per
configured listener is built from the entries that name it, and each listener's `405`
fallbacks are derived from its own routes, so a path a listener does not serve answers
the catch-all's `404` there, as any unrouted path does. A column is a set rather than a
single value because the admin listener keeps serving every route; this is what "each
route group assigned to exactly one listener" means here: each group has exactly one
network listener (or none), and admin besides.

Three rules hold the column:

- **A startup check** (`checkListenerPlacement`) refuses to build any listener, the
  admin one included, from a table that puts a `groupKeys`, `groupWorkerKeys`,
  `groupDashboard` or `groupMetrics` route on a network listener, or any route whose
  *path* is the dashboard's, `/metrics`, or a key-administration path whatever group it
  declares; any `/internal/` path on public; or any `/v1/` path on control. Like
  ADR-0022's boundary check it reads paths, not only declarations, and it checks the whole
  table whichever listener is being built, so a process with no network listener still
  refuses a wrong table.
- **A table-consistency test** requires every entry to set its column explicitly and the
  column to follow the group: `/v1` on public, the control group on control, the probes on
  all three, everything else on admin alone. (An entry with no column is treated as
  admin-only, the fail-closed direction, so ADR-0022's synthetic-table tests keep passing
  unmodified; the consistency test holds that the real table never relies on it.)
- **Behavior tests** send every route to every listener and compare each listener's route
  set with a literal list.

Metrics labels are unchanged: the `route` label is the matched pattern whichever listener
a request arrived on. **No `listener` label was added.** Each pattern is served by a fixed
set of listeners, so the label would mostly restate the route, and for the probes (on all
three) it would multiply series for no question an operator has asked. A deployment that
needs per-listener traffic has it from the load balancer in front of each.

### Q3. Protection of the control listener

- **Rules 1 and 2 are unchanged and run first, in the same order:** any `Sec-Fetch-Site`,
  then any `Origin`, is refused, on every listener.
- **Rule 3 checks `Host` against `TASKFORGE_API_CONTROL_HOSTS`** on the control listener,
  and against loopback on the admin listener as before. The list is comma-separated and
  must not be empty when a control address is set; a list without a control address is
  refused too. Every entry must be a hostname (RFC 1123 labels) or an IP address, with no
  port, scheme or wildcard. Matching is **exact**: case-insensitive, one trailing dot
  normalized, the request's port optional, IPv6 brackets handled; never a suffix,
  prefix or substring match, so `api` admits `api`, `API`, `api.` and `api:8091` and
  refuses `api.evil`, `xapi` and `api.example`. Loopback is not implied: `127.0.0.1` and
  `localhost` are refused on control unless listed. A `Host` that does not parse is
  refused, as before. `internal/hostallow` is the one parser both the configuration check
  and the guard use, the way `internal/loopback` is shared by the bind rule and ADR-0018's
  rule 3.
- **ADR-0014's two tiers are unchanged:** a worker key at registration, then the session
  identity plus the revocation check on every later call. An integration test shows a
  revoked worker key cutting off its session on the control listener exactly as on admin.
- **Plain HTTP inside the network.** See the limitations below.

The public listener has no `/internal` route, so its guard has nothing to guard; its
rule 3 refuses every `Host`, so that if anything ever consulted it, it would fail closed.

The refusal is the same response on every listener, including its fixed message, which
still says "addressed to a loopback host". `api/openapi.yaml` documents that one message,
and the owner asked for no contract change; on the control listener the wording is
inaccurate, and is recorded as a limitation rather than changed here.

### Q4. No gate on the demonstration handlers

[ADR-0019](0019-demo-handlers-are-trusted-built-ins.md) stands: the deployed worker
declares `demo.echo`, `demo.sleep` and `demo.fail`, with no environment gate. ADR-0019
left the gate open for M8's deployment, and this is that decision. Everything ADR-0019
weighed is unchanged by a network: the handlers are strictly decoded data with fixed
failure codes; a key holder can only make its own scope's jobs sleep or fail, which it
can already do with any handler that takes time; the one shared resource is the
queue-wide concurrency limit, a property of the limit and not of these handlers; and a
gate's failure mode is still the worst available, a job that sits `QUEUED` with nothing to
say why. What a network adds is reachability of `/v1`, and that is an API key's question,
not a handler's. The deployed binary is the one the demonstrations run.

### Q5. The worker's opt-in to a remote API

The worker accepts a non-loopback `TASKFORGE_WORKER_API_URL` only with
`TASKFORGE_WORKER_ALLOW_REMOTE_API=true`. Without it the rule and its message
(`TASKFORGE_WORKER_API_URL must use a loopback host`) are unchanged. A value that is not
a boolean is refused rather than read as "no". The opt-in widens nothing else: the
worker's own health address stays loopback.

### Q6. A `healthcheck` subcommand

Each long-running binary (`taskforge-api`, `-outbox`, `-scheduler`, `-reconciler`,
`-worker`) takes `healthcheck` as its only argument: it loads its configuration as the
process would, GETs its own loopback `/healthz`, and exits `0` for a `200` and `1` for
anything else, including a configuration it cannot load. The images are distroless
([ADR-0021](0021-container-images-and-supply-chain-scanning.md)): there is no shell, curl
or wget for an orchestrator's health check to run, so the binary checks itself. All four
background binaries already served `/healthz` on their loopback health address
(`TASKFORGE_OUTBOX_ADDR`, `_RECONCILER_ADDR`, `_SCHEDULER_ADDR`, `_WORKER_ADDR`), so
nothing new is served; the api's targets the admin listener, which every configuration
opens. It is **liveness, not readiness**, on purpose: an orchestrator replaces an
unhealthy container, and a process whose database is briefly unreachable is not one to
replace.

## Failure and upgrade cases

- **A listener fails to bind.** `taskforge-api` binds every configured address before it
  serves on any. If one cannot be bound, the ones already bound are closed, the error names
  the listener and the address, and the process exits `1`: it is never half up. Once
  serving, a listener that stops for any reason other than shutdown takes the others down
  with it, for the same reason: a process whose control listener died while its public one
  answered would look healthy to a load balancer while every worker was cut off. SIGTERM
  drains every listener concurrently within the one existing grace period (15 s).
- **Mixed versions: none.** This is configuration only, with no schema change, no
  migration and no change to any request or response. An older worker cannot use a remote
  control URL, because it has no opt-in and keeps refusing it; that is by design. An older
  api ignores the new variables and opens its single loopback listener.
- **`Host` in deployment.** A load balancer or service discovery preserves the client's
  `Host`, so `TASKFORGE_API_CONTROL_HOSTS` must hold the exact name workers dial (the name
  in their `TASKFORGE_WORKER_API_URL`). A mismatch is refused `403 origin_refused` at
  registration, and the worker exits `1` on its first attempt rather than retrying
  (ADR-0018, "A worker that receives `403` on registration exits"), which makes the
  misconfiguration loud.
- **Bootstrap.** Credentials are minted only on the admin listener, from inside the API's
  host or container, with the CLI against loopback. `make topology-smoke` does exactly
  that. The api image holds no CLI and no shell (ADR-0021), so the smoke copies a linux
  build of `taskforge-cli` into the running container and runs it there; M8C2's runbook,
  which will use ECS Exec, has to decide how the CLI reaches the task, since ECS Exec into
  this image finds nothing to run.

## Alternatives considered

- **One network listener for both authenticated surfaces.** Simpler configuration, and
  the guard would then need the allowlist on a listener that also serves `/v1`, whose
  clients dial a different name; and a public load balancer would expose the
  worker-control routes to the internet. Rejected: the two surfaces have different
  clients, different names and different exposure.
- **Moving the admin-only routes to their own process.** It would remove the mixed
  listener but adds a binary and a second dashboard-serving story, and ADR-0017 chose
  same-origin serving deliberately. Not needed: the admin listener already is that
  boundary.
- **Widening the loopback predicate with a configurable list on the one listener.** It
  would leave key administration reachable on whatever address the listener binds.
  Rejected; that is exactly what the split prevents.
- **A suffix or wildcard allowlist** (`*.internal`). Convenient for service discovery,
  and the one form a rebinding attacker can sometimes satisfy by registering a name under
  a shared suffix. Rejected: an exact list is short in any real deployment.
- **TLS or mTLS on the control listener now, or a per-call worker credential.** The
  right next steps (below), but each is a credential-distribution design of its own, and
  the owner scoped them out of M8C1.
- **A `listener` metric label.** See Q2; not added.

## Consequences and stated limitations

- `taskforge-api` can serve `/v1` and the worker-control routes on network addresses
  while credential administration, the dashboard and metrics stay on loopback, and a worker
  can register and run jobs against an API in another container. `make topology-smoke`
  shows it with the real images.
- **The session id is a bearer token in plaintext inside the VPC.** Every worker-control
  call after registration is authorized by the session id and worker id it carries
  (ADR-0014), over plain HTTP. Anything that can read traffic on that network can replay a
  session until it ends or its worker key is revoked. The worker key itself crosses the
  network once per registration, also in plaintext. **TLS on the control listener** and a
  **per-call worker credential** (so a captured request authorizes nothing further) are
  the named follow-ups.
- `/v1` on the public listener is as protected as before (an API key per request), and is
  plain HTTP on the listener; M8C2's load balancer is where TLS for clients terminates.
- The probes are unauthenticated on every listener, as they were on the one listener
  before. They reveal nothing tenant-specific.
- The refusal message names loopback on the control listener too (Q3).
- The outbox, reconciler and scheduler still bind loopback "until authentication is
  implemented"; nothing about them changed.
- ADR-0018 is now partially superseded by both
  [ADR-0022](0022-the-route-table-is-the-single-source-of-routes.md) (its registration
  clause) and this record (its rule 3 and Q3).
