# ADR-0022: The route table is the single source of routes

- **Status:** Accepted
- **Date:** 2026-10-06

## Context

`taskforge-api` registers its routes on a `net/http` `ServeMux` in `Handler()`.
Until M8D1 that was imperative code: 57 registration calls (26 of them through
`handleInternal`), in an order a reader followed by eye, with the wrapper each route needs (`requireAPIKey`
on `/v1`; the browser-origin guard, outermost, on `/internal`; `requireWorkerKey`
on registration alone) applied by hand at each call, and a second block further
down of method-less `405` fallbacks, one per path, that had to agree with the
first.

Three things were kept in agreement with that code by hand, and each was
recorded as a gap rather than closed:

- `publicRoutes` in `internal/api/auth_test.go`, the list of public routes every
  one of them had to refuse an unauthenticated request on;
- `publicOperations` in `internal/api/auth_contract_test.go`, the same eleven
  routes as the spec's side of that comparison, whose own comment said that a
  real completeness gate needs a route registry inside `Handler()`;
- a test that read `server.go` as text and required exactly 26 lines of the form
  `s.handleInternal(mux,` ([ADR-0018](0018-browser-origin-guard-on-the-internal-surface.md)'s
  second fail-closed test).

Nothing walked the mux, because `ServeMux` offers no way to list what it holds.
So a route added without being added to those lists was uncovered rather than
failing, and a table-driven `Handler()` could not be written at all while the
26-count test stood, since it breaks by construction. M7A recorded the
completeness gate as deferred ([CURRENT_STATE.md](../CURRENT_STATE.md)), and the
owner split M8D so that this slice, M8D1, delivers it alone.

The owner also decided, for routes `api/openapi.yaml` does not document, that
they stay **out** of the spec. Three exist today, and a gate that demanded every
registered route be in the document would force one of them in.

## Decision

### One table, and the only registration path

`internal/api/routes.go` declares every route as one entry of `routeTable`:

| Field | Meaning |
| --- | --- |
| `method`, `path` | The pattern ServeMux sees is `method + " " + path`. They are separate fields because the `405` fallback is derived from the methods held for a path. |
| `surface` | `public`, `internal`, `probe` or `unlisted`. |
| `chain` | The wrappers the handler gets, outermost first: `none`, `api-key`, `guard`, or `guard+worker-key`. |
| `group` | The feature that must be wired for the route to exist: `always`, `metrics`, `dashboard`, `keys`, `workerKeys` or `control`. |
| `handler` | The handler. |
| `noFallback` | The path has no derived `405` fallback (below). |
| `reason` | Why an unlisted route is not in the spec. Non-empty on every unlisted entry and on no other. |

`Handler()` registers every enabled entry from the table, then the fallbacks
derived from it, then the single `/` catch-all, and nothing else. The zero value
of `chain` is not a valid chain, so an entry that forgot its chain panics at
registration rather than being served unwrapped. An unset `surface`, or a `group`
that does not fit its surface, is caught by `TestRoutes_TableIsInternallyConsistent`
(a zero group is never enabled, so without that check such an entry would simply not
be registered).

That nothing registers around the table is **held by a test, not by convention**.
`TestRoutes_NothingRegistersAroundTheTable` parses the non-test files of
`internal/api` and reports, with file and line, any call to a method or function
named `Handle` or `HandleFunc` that is not in `registerRoutes`, `register` or
`handleInternal`; any call there whose receiver is not that function's own
`*http.ServeMux` parameter; any registration in `registerRoutes` but the `/`
catch-all; any in `register` or `handleInternal` whose pattern is not their own
`pattern` parameter; a `register` call outside the loop over the table; and a
`handleInternal` call from anywhere but `register`. It also fails if
`registerRoutes` cannot be found, so renaming the registration code cannot make
the check pass by finding nothing. Calls are identified by syntax and by the
declared type of the receiver, not by matching text, so a comment, a call split
across lines, and `http.HandleFunc` on the default mux are all handled. The
checker is itself tested against synthetic sources, one per way of breaking the
rule. This replaces the 26-count test, which could say nothing about a
registration that was not on such a line.

### Wrappers, and why the guard stays outermost

Wrappers follow from `chain`, and `chain` follows from `surface`: a public route
is `requireAPIKey(handler)`; an internal route goes through `handleInternal`,
which puts `refuseBrowserOrigin` outside whatever else the route carries; probes
and unlisted routes are unwrapped. Registration alone is
`refuseBrowserOrigin(requireWorkerKey(handler))`. Every other worker-control
route resolves its scope from the session identity the request already carries
([ADR-0014](0014-worker-control-authentication.md)); that asymmetry is unchanged
and is now visible as the one `guard+worker-key` entry in the table.

The guard stays **outermost** for the reasons [ADR-0018](0018-browser-origin-guard-on-the-internal-surface.md)
gives, and they apply to the fallbacks as much as to the routes: a guard behind
authentication would let an unauthenticated browser learn whether a worker key is
valid, and one behind the `405` would tell it which methods a path allows. It
sits inside the mux, not around it, so a refused request still carries its route
pattern into its span name and its metric label.

### The invariants are keyed by path, not by what an entry declares

The guard and the API key are invariants of **where a client can reach a route**.
Every route whose path starts with `/internal/` must go through the browser-origin
guard, and so must its derived `405` fallback; every route whose path starts with
`/v1/` must be wrapped in `requireAPIKey`, and its fallback must be unwrapped. That
holds whatever `surface`, `chain` or `group` the entry declares, because a surface
is a label an entry gives itself and a label can be wrong. It is enforced three
ways, each of which holds without the others:

1. **The table-consistency rule.** `TestRoutes_TableIsInternallyConsistent` ties
   surface to path in both directions: a path under `/internal/` is
   `surfaceInternal`, a path under `/v1/` is `surfacePublic`, and a probe or an
   unlisted route is under neither. A violation names the entry and the direction.
2. **Behavior tests that select by path.** `TestRoutes_InternalRoutesAreGuardedOutermost`
   and `TestRoutes_PublicRoutesRequireAnAPIKey` take every enabled route under the
   prefix, and every derived fallback under `/internal/`, through one helper that
   never reads the declared surface or the chain a fallback was given.
3. **A startup check.** Before it registers anything, `registerRoutes` runs
   `checkRouteBoundaries`, a pure function, over every enabled route and every
   derived fallback and panics on a breach, naming the pattern and the wrapper it
   needs. A table that would register an unguarded `/internal` route, or an
   unauthenticated `/v1` one, is a server that never starts. The check changes no
   response, and the golden is unchanged by it.

The first version of this decision keyed the tests, and the fallback derivation, on
the declared surface. An entry declared unlisted at `/internal/v1/debug` then
registered an unguarded route and an unguarded `405` fallback beside it with every
test passing, and the same shape under `/v1/` skipped `requireAPIKey`. The test this
decision removed had selected by the `/internal/` path text, so that was a
regression of [ADR-0018](0018-browser-origin-guard-on-the-internal-surface.md)'s
fail-closed property. Review of the pull request found it; the three checks above
close it.

### Derived `405` fallbacks, and the two exceptions

For every path the table holds, the method-less pattern that answers a method the
table does not hold with the structured `405` is **derived**: its `Allow` header
is the methods the table holds for that path, sorted; it is registered only if
the group of the routes at that path is enabled, so a disabled group's path falls
to the `/` catch-all; and an internal path's fallback goes through the guard like
the routes beside it.

Two paths have **no** fallback and are marked `noFallback`: `GET /metrics` and
`GET /{$}`. Today a wrong method on either reaches the catch-all and answers its
structured `404`. Deriving a `405` for them would change an observable response
for no reason this milestone has, so it is preserved, not fixed. The set is pinned
by `TestRoutes_FallbacksAreDerivedFromTheTable`: a third path without a fallback
is a decision someone has to make in a test, not a default.

### Unlisted routes

A registered route the spec does not document is declared `surfaceUnlisted` with
a reason. Three exist; each is registered only when its feature is on:

| Route | Group | Reason |
| --- | --- | --- |
| `GET /metrics` | `metrics` | Prometheus text exposition for an operator's scraper on the loopback listener; not part of the client API. |
| `GET /dashboard/` | `dashboard` | Static files of the embedded operator dashboard, served as a subtree under `/dashboard/`; not API resources. |
| `GET /{$}` | `dashboard` | Sends the bare root to the embedded dashboard's mount point; part of serving its static files, not an API resource. |

`api/openapi.yaml` is not edited. `TestRoutes_UnlistedEntriesAreDeliberate`
requires every unlisted entry to give a reason, to be really registered, and to
appear in the spec neither as that operation nor as that path.

### How equivalence with the old registration was proved

The refactor was required to change no observable behavior. The proof is a golden
file, `internal/api/testdata/route_behavior.golden`, **generated from the
hand-registered `server.go` in the commit before the refactor**, and a refactor
commit that leaves it byte-for-byte unchanged (`git diff` of the file across the
two commits is empty).

It has 1,200 rows: 30 paths (every spec path with its parameters filled, the
three unlisted paths, and three that nothing registers), five methods, four
request variants, and two server configurations (every feature group on, and
every one off). A row records the status, the `Allow` header, the structured
error code, and the route pattern the mux matched; nothing that varies between
runs is recorded. The variants are a request with no credential, one carrying a
browser `Origin`, one addressed to a non-loopback `Host`, and one carrying a
valid credential. The fourth is an addition to the three first planned: every
public handler also refuses an unauthenticated caller itself, so a public route
registered without `requireAPIKey` answers the same `401` to a request with no
credential, and only a presented credential tells the two apart.

The matched pattern is read three ways that must agree on every request or in
aggregate: the `http.route` attribute of the span `withSpanRoute` names, a
`routeHolder` placed on the context under `routeCtxKey` when metrics are off, and
the `route` label of the HTTP request counter summed over the run when they are
on. They are one value by construction (`withSpanRoute` writes `r.Pattern` to the
holder and the span in one place), and the test checks that rather than assuming
it.

### What holds the table to what

| Property | Held by |
| --- | --- |
| The table and the spec are the same set of operations, in both directions | `TestRoutes_TableMatchesTheSpec` |
| Unlisted entries are deliberate | `TestRoutes_UnlistedEntriesAreDeliberate` |
| Nothing registers around the table | `TestRoutes_NothingRegistersAroundTheTable` |
| Every route under `/v1/` needs a key and consults the credential store, selected by path | `TestRoutes_PublicRoutesRequireAnAPIKey`, and the `TestAuth_*` tests, which now walk the table |
| The guard is outermost on every route and every derived fallback under `/internal/`, selected by path | `TestRoutes_InternalRoutesAreGuardedOutermost` |
| An entry's surface agrees with its path, both ways | `TestRoutes_TableIsInternallyConsistent` |
| A table that breaks the path rule is refused at startup | `TestRouteBoundaries_RefuseEachBreach`, `TestRouteBoundaries_TheRealTableIsAccepted`, `TestRoutes_HandlerRefusesATableThatBreaksABoundary` |
| The golden's unlisted paths are the table's | `TestRoutes_GoldenMatrixCoversEveryUnlistedRoute` |
| Registration alone requires a worker key | `TestRoutes_RegistrationAloneRequiresAWorkerKey` |
| Probes and unlisted routes are unwrapped | `TestRoutes_ProbesAndUnlistedRoutesNeedNoCredentialAndNoGuard` |
| Fallbacks say what the table and the spec say | `TestRoutes_FallbacksAreDerivedFromTheTable` |
| A feature group gates its routes and nothing else | `TestRoutes_FeatureGroupsGateTheirRoutesAndNothingElse` |
| Observable behavior did not change | `TestRouteBehavior_MatchesTheGolden` |

## Alternatives considered

**Keep the hand-maintained lists.** This was the status quo. It covered whatever
someone remembered to list, in one direction, from two different sources, and its
completeness gate was a count of lines of source text. A new route that nobody
added to a list failed nothing. Rejected: it is the gap this decision closes.

**Generate the spec from the code.** The document is the public contract. Its
value is its prose, its examples and its error enumeration, which a route table
does not carry; generating it would either lose them or move them into Go
strings. It would also make the spec describe what the code does rather than what
the code is held to, which is the wrong direction for a contract. Rejected.

**Generate the routes from the spec.** A generator is a build step and, in the
usual form, a dependency, and adding either is the owner's decision. The spec
also does not say what the table must: which wrapper a route carries, which
feature enables it, which routes are not API routes at all. Rejected; the table
and the spec are held to each other by a test instead.

**Wrap the mux in a type that records what is registered.** It produces a list as
a side effect of imperative registration, so "nothing registers around it" is
still unprovable without a syntax check, and the list cannot be read before the
mux is built. A table that is data can be read, compared with the spec and
filtered by feature before anything is registered.

## Consequences

- **Adding a route now requires two things.** An entry in `routeTable`, and either
  an operation in `api/openapi.yaml` or a reason it is not there. Until one is
  true `TestRoutes_TableMatchesTheSpec` or `TestRoutes_UnlistedEntriesAreDeliberate`
  fails, and a route registered any other way fails
  `TestRoutes_NothingRegistersAroundTheTable`.
- **ADR-0018's second fail-closed test is replaced.** That record said coverage of
  the guard came from two tests, one of which read `server.go` for `/internal`
  patterns registered around the helper. The guard is still outermost on every
  internal route and fallback, still through `handleInternal`. It is now a
  property of the **path**, not of what an entry declares: anything under
  `/internal/` must carry it, held by the three checks above, while the AST check
  holds that nothing registers around the table and covers every package file
  rather than the lines of one. ADR-0018's status records the clause it loses.
- **The golden pins behavior, so a deliberate routing change regenerates it.**
  `go test ./internal/api -run TestRouteBehavior_MatchesTheGolden -update-route-golden`,
  and every changed row is reviewed as a behavior change.
- **The golden covers the paths in its matrix.** A new unlisted route enters it
  only when its path is added to `goldenUnlisted` in the test; a new spec path
  enters it automatically. A route outside both is held by the other tests, not
  the golden.
- **The golden's `credentialed` rows run real handler code against a nil job
  store.** A public route with a valid key panics into the recovery middleware's
  `500`, as the existing tests rely on. A change in that behavior shows as changed
  rows.
- **The syntax check is scoped and syntactic.** It reads `internal/api` only: the
  four background services' own health muxes in `cmd/*/health.go` register their
  probes directly and are outside it. It identifies a `ServeMux` by the declared
  type of the receiver parameter, not by type-checking, so it fails closed on any
  call named `Handle` or `HandleFunc` outside the three registration functions,
  and renaming those functions means updating the check, which says so. Two
  things are outside it: a registration through a method value
  (`h := mux.HandleFunc`, then `h(...)`), which is neither a call to `Handle` nor
  one to `HandleFunc`, and a middleware that matches `r.URL.Path` and answers a
  request before the mux sees it, which registers nothing.
- **The path rule is a literal prefix.** A pattern whose first segment is a
  wildcard, such as `GET /{a}/v1/{b}`, matches a request under `/internal/` without
  carrying the prefix: `net/http`'s mux hands `GET /internal/v1/nonexistent` to it
  ahead of the `/` catch-all. Neither the table-consistency rule nor
  `checkRouteBoundaries` would see such an entry. No entry has one, and the check
  does not reject one.
- **The table is compared with the spec by method and path only.** Parameters,
  schemas and responses are not compared by this decision; the contract tests
  that read the spec's text still do that.
- **Built per server.** Each handler is a method value bound to the server, and
  `/metrics`' handler is resolved once when the table is built. Tests read the
  table through unexported accessors, and one test changes it through
  `Server.routeTableHook`, an unexported seam that nothing in production sets;
  nothing exposes the table at runtime, and no endpoint lists it.
- **Behavior is unchanged, including where it is arguably wrong.** A wrong method
  on `/metrics` or `/` is still a `404`, not a `405`.
