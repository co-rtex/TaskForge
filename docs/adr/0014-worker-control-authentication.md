# ADR-0014: Worker-control authentication with a separate, session-trusting credential

- **Status:** Accepted
- **Date:** 2026-09-14

## Context

[ADR-0013](0013-database-backed-api-key-authentication.md) authenticated the
public surface and deliberately deferred the internal worker-control surface,
recording the consequence rather than hiding it: claims filter on
`TASKFORGE_DEV_SCOPE`, so a job submitted under an API key for any other scope
was durable and readable but never claimed. It stayed `QUEUED` forever. That
was M5A's stated, accepted limitation, not a bug — but it made multi-tenant key
issuance useful only for isolating reads, cancellation, and the DLQ, never for
execution.

This milestone, M5B, closes that limitation by authenticating registration —
`PUT /internal/v1/worker-sessions/{worker_session_id}` — and removing
`TASKFORGE_DEV_SCOPE` entirely. Every worker-control scope now comes from a
real, scoped, revocable credential, exactly as a public scope already does.

[`docs/PROJECT_SPEC.md`](../PROJECT_SPEC.md) §6 fixed the boundary this record
has to honor: *"Worker/control scopes are separable from user scopes."* A
worker key and an API key must never be interchangeable, must be verified
against different tables, and a caller holding one must never authenticate
where the other is checked.

## Decision

### A second table, a second package, the same primitives

`worker_keys` (migration 0015) mirrors `api_keys` column for column: the same
`tfk_<lookup>.<secret>` format, the same prefix-plus-SHA256-hash storage, the
same revocation-by-timestamp shape. `internal/workerauth` mirrors
`internal/auth`'s shape — `Store`, `Key`, `Created`, `RevokeResult`,
`Principal`, `Create`/`Revoke`/`Get`/`List`/`Authenticate` — but is a
genuinely separate package with its own sentinels
(`workerauth.ErrUnauthorized` is not, and must never be made,
`errors.Is`-equal to `auth.ErrUnauthorized`) and its own `classifyDatabaseError`.

What *is* shared, deliberately, is `internal/auth`'s pure key-material
functions — `GenerateMaterial`, `ParseKey`, `HashSecret`, `VerifySecret`, and
the format constants. They know nothing about `api_keys` or `worker_keys`;
reusing them costs nothing and avoids two independently-maintained
implementations of the one thing that must not drift: what makes a credential
well-formed and how its secret is compared.

The result is two credential types that share an encoding and a comparison
algorithm, and share nothing else. A worker key presented where an API key is
checked, or the reverse, is rejected by `ParseKey` succeeding but the lookup
finding no row in the wrong table — the identical `ErrUnauthorized` every other
failure produces, not a special case.

### Two tiers: a credential once, a session forever after

A worker key authenticates exactly one call: registration. Every other
worker-control call — heartbeat, claim, lease renewal, and the fenced
start/succeed/fail/cancel transitions — trusts the session identity
registration already established, the same way it always has since M2 (a
worker id and a session id neither party can forge), and additionally checks
that the worker key which registered that session has not since been revoked.

This is the central design choice this record makes, and it rests on two
observations:

1. **A session's identity is already a bearer capability.** `SessionScope`'s
   read (an unlocked `SELECT` by primary key) costs less than a full
   credential comparison, and re-verifying a secret on every heartbeat would
   buy nothing a revocation check does not already buy more cheaply — every
   fenced transition already refuses a session that is not current and
   healthy.
2. **Re-presenting a secret on every call multiplies the surface that can leak
   it.** A worker key that is presented once, at process boot, and never again
   is a worker key that appears in exactly one request's logs, one process's
   memory, for one moment — not in every claim and every heartbeat for the
   life of the session.

Concretely: `migrations/0015_worker_keys.sql` adds a nullable
`worker_sessions.worker_key_id`, populated at registration.
`internal/workers.Store.SessionScope(ctx, sessionID)` — additive, changing
none of the store's eight existing fenced methods — reads a session's `scope`
and `worker_key_id` in one unlocked query. The HTTP layer
(`internal/api.Server.resolveWorkerControlScope`) composes that with
`workerauth.Store.IsRevoked(ctx, keyID)` — itself a plain, secret-free lookup —
before dispatching into the seven unmodified fenced handlers. Neither
`internal/workers` nor `internal/workerauth` knows the other exists; the HTTP
layer is the only place their answers meet.

A `nil` `worker_key_id` — a session registered before this migration, or
through any future path that never presents a worker key — is never treated
as revoked. That is not a gap tolerated for convenience; it is the literal
security posture every session had before this credential existed, and
inventing a value for it would be a fabricated fact a truthful migration must
not write (see migration 0015's own comment).

### Revocation refuses the next call; it does not reach into a held lease

Revoking a worker key does not force-expire a session it registered, and does
not touch a lease that session currently holds, and does not touch
reconciliation. It refuses the **next** control-plane call that session makes,
discovered by the identical `resolveWorkerControlScope` check every other call
already goes through. A request already in flight when revocation happens is
not interrupted — the same in-flight boundary ADR-0013 already documents for
API keys, now stated for worker keys too.

A session cut off this way is, from the reconciler's perspective,
indistinguishable from one whose process simply stopped heartbeating: its
session goes `UNHEALTHY` on PostgreSQL receipt time, its lease expires on
PostgreSQL's clock, and the reconciler abandons the attempt and requeues the
job through the exact, unmodified path a crashed worker's lease already
travels. Revocation is authorization deciding to stop trusting a session, not
a second, parallel mechanism for ending one — reusing crash recovery's own
correctness is the point, not a shortcut around building something dedicated.

The alternative — force-expiring the lease and fencing the session the moment
a key is revoked — was rejected because it would need its own correctness
proof (an immediate fence interacting with a claim or a heartbeat already in
flight) duplicating guarantees the reconciler already has, to buy a recovery
window measured in a session's staleness threshold rather than in a
credential's usable lifetime after revocation. That trade was not asked for.

### `requireWorkerKey` gates registration alone

`PUT /internal/v1/worker-sessions/{worker_session_id}` is the only
worker-control route wrapped in `requireWorkerKey`. Every other route's entire
dependency on authentication is `resolveWorkerControlScope`, called after
ordinary field validation and before the fenced store call. Both fail closed
identically to ADR-0013's stated posture: a `Server` built without
`WithWorkerAuth` refuses registration outright, and — because a session
registered with a real credential always carries a non-`nil` `worker_key_id`
— also refuses every later call from any such session, since there is no
worker-key store to ask whether that credential is still live. There is no
test-only bypass for either wrapper, for the identical reason there is none
for `requireAPIKey`.

### A distinct `WorkerKeyAuth` security scheme

`api/openapi.yaml` declares `WorkerKeyAuth` as its own OpenAPI security
scheme, applied to exactly the one route that requires it, rather than reusing
`ApiKeyAuth` or leaving the requirement to prose. The two schemes share a
`type: http` / `scheme: bearer` shape because both credentials are presented
identically, but a client generated from the spec must see two distinct
credential requirements for two boundaries that are never interchangeable —
structurally, not just in a paragraph a generator does not read.

### `TASKFORGE_WORKER_API_KEY`, and `TASKFORGE_DEV_SCOPE` retired

`TASKFORGE_DEV_SCOPE` is removed from `internal/config`, from every binary,
and from `.env.example`. It has no remaining reader: the public surface has
taken its scope from a key since M5A, and the internal surface now takes its
scope from one too. `taskforge-worker` gains a new required setting,
`TASKFORGE_WORKER_API_KEY`, presented as the `Authorization: Bearer` credential
on registration — named to match the existing `TASKFORGE_WORKER_*` convention
in `.env.example`, with no deviation from it.

### `GET /internal/v1/worker-keys` ships with creation and revocation

An operator who can mint and revoke a worker key but not list what exists
would have to reconstruct that state from logs or from `worker_sessions`
rows. The identical justification already accepted for
`GET /internal/v1/api-keys` in ADR-0013 applies unchanged: auditing which
credentials exist, and which are revoked, is a real operational need this
milestone's own revocation semantics create, and the listing carries no
secret by construction.

## Alternatives considered

**Reuse `internal/auth` and `api_keys` directly, adding a `kind` column to
distinguish the two credential types.** Rejected because it directly
contradicts `PROJECT_SPEC.md` §6's separable-scopes requirement: a shared
table means a single query mistake, or a single future migration, could
authenticate a worker key against the public surface or vice versa. Two
tables verified by two packages make that class of mistake a compile error
(different Go types) and a schema-level impossibility (different tables), not
a runtime check that has to remember to filter on `kind`.

**Re-verify a full credential on every worker-control call, not just
registration.** This is the naive, symmetric design, and it was rejected for
the reasons the "session forever after" section states: it buys nothing a
revocation check does not already buy more cheaply, and it re-exposes the
secret on every request instead of once per process boot.

**Force-expire a session's lease immediately on key revocation.** Rejected in
the "Revocation refuses the next call" section above: it would duplicate the
reconciler's own correctness for a marginal reduction in the window between
revocation and effect, a trade this milestone was not asked to make.

**Reuse the `ApiKeyAuth` OpenAPI security scheme for registration.** Rejected
per the confirmed decision this record implements: the two credentials guard
different trust boundaries, and the spec should say so structurally.

## Consequences

- `PUT /internal/v1/worker-sessions/{worker_session_id}` now requires a
  worker key. This is a **breaking change** for any existing worker process,
  with no compatibility shim — the same posture ADR-0013 took for the public
  surface.
- `TASKFORGE_DEV_SCOPE` is gone. `TASKFORGE_WORKER_API_KEY` is a new required
  setting for `taskforge-worker`.
- A job submitted under any authenticated scope is now claimed and executed
  by a worker registered under a matching worker key. M5A's recorded
  limitation is closed; ADR-0013's "Worker-control authentication is
  deferred" section is superseded by this record.
- Revoking a worker key refuses the next call any session it registered
  makes, without interrupting a request already in flight and without
  force-expiring a held lease — an abandoned lease is reconciled through the
  existing, unmodified crash-recovery path.
- Authentication costs one indexed lookup on registration (a full credential
  verification) and one indexed lookup on every other worker-control call (a
  cheap revocation check) — never a second secret comparison after
  registration.
- `worker_keys` starts empty on every upgrade path, and every pre-existing
  `worker_sessions` row gets a `NULL worker_key_id` rather than a fabricated
  one, for the identical reason `api_keys` started empty under ADR-0013: no
  earlier milestone ever persisted this credential, so there is nothing to
  reconstruct.

## Superseded clauses

This record supersedes ADR-0013's "Worker-control authentication is deferred,
and it costs something" section in full: the deferral it recorded is now
implemented, and the consequence it warned about (a job outside the
worker-control scope never being claimed) no longer holds. ADR-0013's other
decisions — the credential format, the one-indistinguishable-failure model,
fail-closed construction, and loopback-only key issuance — are unchanged and
apply identically to worker keys.
