# ADR-0018: Browser-origin guard on the internal surface

- **Status:** Accepted, partially superseded by
  [ADR-0022](0022-the-route-table-is-the-single-source-of-routes.md). Two sentences
  of its "Registration: per route, inside the mux" section are replaced: the ones
  saying fail-closed coverage comes from two tests, one of which reads `server.go`
  for `/internal` patterns registered around `handleInternal`. That test no longer
  exists; every route is registered from one table, and an AST check over the
  package's non-test files holds that nothing registers around it. The guard, its
  rules, its order and its outermost position are unchanged, and every other
  section stands.
- **Date:** 2026-10-01

## Context

The `/internal/v1` routes are operator and worker plumbing, and all but one take
no credential. Key administration (`/internal/v1/api-keys`,
`/internal/v1/worker-keys`, and their `.../{key_id}/revoke` routes) is how a
credential comes into existence, so requiring one would make the system
unbootstrappable ([ADR-0013](0013-database-backed-api-key-authentication.md)).
Every worker-control route after registration trusts a session identity
registration already authenticated
([ADR-0014](0014-worker-control-authentication.md)). What keeps all of that
safe is that `taskforge-api` binds to loopback, so only a process on the same
machine can reach it.

A loopback bind keeps other machines out. It does not keep out a web page the
operator is looking at, because a browser on the same machine can be made to
send requests to `127.0.0.1`. Two exposures follow, and they were recorded
rather than fixed when they were found.

**CSRF and DNS rebinding, since M5A.** These routes check neither `Host` nor
the request's origin, and the key routes accept any `Content-Type`. Any website
the operator visits can send a cross-site "simple" request that mints or revokes
a key; the response is unreadable cross-origin, but the write happens. A
DNS-rebinding page makes the browser treat `127.0.0.1` as the attacker's own
origin, so it can read the response too.

**The dashboard's origin, since M6D.** [ADR-0017](0017-dashboard-toolchain-and-serving.md)
serves the dashboard same-origin with these routes. Before it, nothing on that
origin ran script. Now a page does, so a script injected into it — which the
CSP exists to prevent — could list every key and mint or revoke credentials for
any scope. ADR-0017 accepted that for a loopback-only listener, and named a
guard as the follow-up that would close it, with the decision whether to build
one left to the owner.

This ADR is that follow-up.

## Decision

### A stateless guard, outermost on every `/internal` route

A request to a registered `/internal/v1` route is refused with **`403` and
`Error.code` `origin_refused`** when any of the following holds, checked in this
order. The first rule that fires names the refusal in the log.

1. **`sec_fetch_site`** — the request carries a `Sec-Fetch-Site` header, with
   any value, including `none` and an empty value.
2. **`origin`** — it carries an `Origin` header, with any value, including
   `null` and the API's own origin.
3. **`host`** — its `Host` is empty, cannot be parsed, or is not loopback.

The refusal happens **before authentication, before the `405` answer for an
unsupported method, and before any handler reads the body.** The response body
is a fixed message that never echoes a header value. The log line carries the
request id and the rule name and never a header value. The guard holds no state,
needs no migration, and has no mixed-version behavior.

**It is not authentication.** It adds no credential and no principal, and the
OpenAPI document does not declare a security scheme for it
(`TestOpenAPI_TheInternalSurfaceIsDocumentedAsUnauthenticated` still passes
unmodified). It is a property of the request, not of who sent it: a local
process that omits the headers is exactly as able to call these routes as it was
before.

### Why each rule exists

No client of this surface sends `Sec-Fetch-Site` or `Origin`: the Go CLI, the
Python SDK, and the worker are not browsers. A browser sends them, and script
running in a page cannot remove or forge either — both are forbidden request
header names. So their presence, with any value, means a browser is acting, and
the value is irrelevant. That is why the rules test presence rather than
interpreting values: a rule that interpreted them would be one an attacker could
study for the value it lets through, and the API's own origin and `none` (a user
typing the URL into the address bar) are refused with the rest. Allowing the
API's own origin would reopen exactly the dashboard-origin exposure this closes.

The two header rules catch the two attacks that announce themselves. They do not
catch DNS rebinding, because a rebinding page is same-origin with itself from
the browser's point of view. What a page cannot change is the `Host` header: it
stays the attacker's hostname. So rule 3 refuses any `Host` that is not
loopback, which is the property that defeats rebinding, and it needs no header
from the browser at all.

**Rule 3 shares one predicate with the bind rule.** `isLoopbackHost` moved from
`internal/config` to a new package, `internal/loopback`, as `IsLoopbackHost`,
with identical behavior: `localhost` in any case, with or without a trailing dot,
or any IP for which `IsLoopback()` is true. The bind check, the worker's
`TASKFORGE_WORKER_API_URL` check, and the Host rule all call it, so a name one
accepts the other accepts.

Host parsing fails closed. A `Host` is `host`, `host:port`, or a bracketed IPv6
literal with or without a port. Exactly two forms have no port: a value with no
`:` at all, and one `[...]` with nothing after the `]`. Any other split error is
a refusal, and so is a successful split that yields an empty or non-numeric
port (`localhost:` splits without error, so the port is checked explicitly).

### Registration: per route, inside the mux

Every `/internal` pattern, including the method-less `405` fallbacks, is
registered through one helper, `handleInternal`, with the guard as the
outermost layer: outside `requireWorkerKey` and outside `methodNotAllowed`. A
guard that sat after authentication would let an unauthenticated browser learn
whether a worker key is valid; one that sat after the `405` would tell it which
methods a path allows.

It is per route, like `requireAPIKey`, rather than a middleware around the mux,
for two reasons. A guard outside the mux runs before the route pattern exists,
so every refusal would be labelled `unmatched` in metrics and spans and lose the
route it was aimed at. And the repository's convention is that protections are
visible in the route table: omitting the helper is a visible omission there.

Fail-closed coverage comes from tests rather than from the helper alone. One is
driven by `api/openapi.yaml`: for every `/internal` operation the document
declares, every refusal case is sent and a recording fake proves nothing behind
the guard was called. Another reads `server.go` and fails on any `/internal`
pattern registered around the helper. A registered but undocumented route is
caught by the second, a documented but unguarded one by the first.

The guard wraps registered patterns only. An unregistered path such as
`/internal/v1/nonexistent` stays the existing structured `404` even when
browser headers are present.

### Decisions

- **Q1. Scope: registered `/internal/*` routes only.** `/v1`, `/dashboard/`,
  `/metrics`, `/healthz` and `/readyz` are unchanged, and a test pins that so
  the scope cannot widen silently. That closes the exposure that is actually
  documented. `/v1` still requires a key the attacker does not have.
- **Q2. Any `Sec-Fetch-Site` is refused,** whatever its value. There is no
  supported reason for a browser to open this surface, and an operator who types
  `GET /internal/v1/api-keys` into the address bar is refused by design.
- **Q3. The Host allowlist is fixed to loopback,** the same predicate as the
  bind check. Nothing is configurable.
- **Q4. The contract is `403` with `origin_refused`** and a fixed message. The
  CLI maps it to `ExitRequestRejected`, because sending the identical request
  again will not help, and the Python SDK to `RequestRejectedError`, the class
  its contract test pins to that exit code. It is not `401`: no credential fixes
  it, and a caller that sees `unauthorized` goes looking for one.

## Alternatives considered

**A separate listener for the dashboard.** It would remove the shared origin
without inspecting a header. It is a new service the stack depends on and forces
CORS onto `taskforge-api` for the dashboard's `/v1` reads, which is what
ADR-0017 chose not to do. It also does nothing about the CSRF and rebinding
exposure from other sites, which has nothing to do with the dashboard. Rejected;
it remains an option if the dashboard ever leaves loopback.

**A CSRF token.** A token needs somewhere to live and a way for a legitimate
caller to obtain it, and the legitimate callers here are non-browser clients
with no session. It would be a new credential-shaped surface on routes whose
whole point is that they take no credential. Rejected.

**Prefix middleware around the mux.** Simpler to write, and fail-closed by
construction for any new `/internal` route. Rejected for the reasons above: it
runs before the route pattern is set, so a refusal loses its span name and
metric label, and it hides the protection from the route table. The cost of
choosing per route is that fail-closed coverage depends on the two tests rather
than on structure.

**A configurable Host allowlist.** Speculative configuration for a case nothing
needs today. The ECS workers M8 plans will have to revisit the Host rule anyway,
together with the loopback bind itself, and should do it deliberately. Rejected.

**Guarding the whole listener rather than `/internal`.** It would also close the
`/metrics` read described below, but it touches every non-loopback path any
future component adds and about sixty existing test requests. Left for M8's
deployment work.

## Consequences

- A script in the dashboard's origin can no longer reach key administration or
  the worker-control routes, and a website the operator visits can no longer mint
  or revoke a key. That closes both exposures ADR-0017 recorded; ADR-0017 gains a
  note saying so and stays Accepted.
- **DNS rebinding can still read `/metrics`, `/healthz` and `/readyz`,** and load
  the dashboard's static HTML, because they are outside the guard (Q1). `/metrics`
  carries no tenant data and no credential. The dashboard HTML is inert, since
  the operator's key lives in the real origin's `sessionStorage`, which a
  rebinding page does not share. `/v1` still requires a key the attacker does not
  have. Guarding the whole listener would close the `/metrics` read; see above.
- **A hostname alias for loopback now gets `403` on `/internal`.** A client that
  reaches the API through a name that resolves to loopback — an `/etc/hosts` entry
  such as `tf.local`, say — sent a non-loopback `Host` that was accepted before
  and is refused now. Address it as `127.0.0.1`, `[::1]` or `localhost`. This is
  the one visible behavior change, and it is an upgrade note rather than a
  migration.
- **Browsers that send no Fetch Metadata are covered by the `Origin` and `Host`
  rules only.** Those engines omit `Sec-Fetch-Site`, and they also omit `Origin`
  on a same-origin `GET`. A same-origin script in such a browser can therefore
  still list keys: the request carries none of the three markers. Cross-origin
  writes and every state-changing request carry `Origin` and are refused, and a
  rebinding page is refused by `Host`, in every engine. Current releases of
  Chromium, Firefox and Safari all send Fetch Metadata, so this gap is a
  property of old browsers.
- **It is not authentication,** and any non-browser local process still reaches
  these routes freely. Authentication on the key-management surface remains
  deliberately absent; see ADR-0013 and ADR-0014.
- **A worker that receives `403` on registration exits.** The worker's own
  configuration check already requires its API URL to use a loopback host, by the
  same predicate, so a hostname alias cannot be configured. If a `403` did arrive,
  `RemoteError.Retryable` is false for it (`internal/worker/client.go:612`),
  `Runner.retry` returns on the first attempt (`internal/worker/runner.go:839`),
  `Run` returns `register worker session: control plane returned HTTP 403
  (origin_refused)` (`runner.go:174`), and `taskforge-worker` logs it and exits `1`
  (`cmd/taskforge-worker/main.go:181`). It does not back off and does not loop.
  This was observed, not only read, and is not changed here.
- **The Host rule is coupled to the loopback bind.** Both rest on the claim that
  every legitimate caller addresses the API by a loopback name. M8's ECS workers
  will reach the API across a network under a non-loopback name, so M8 has to
  revisit the bind and the Host rule together, and decide at that point whether
  the allowlist becomes configuration and whether the guard widens to the whole
  listener.
