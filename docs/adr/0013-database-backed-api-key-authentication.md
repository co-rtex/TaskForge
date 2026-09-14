# ADR-0013: Database-backed API-key authentication for the public surface

- **Status:** Accepted, partially superseded by
  [ADR-0014](0014-worker-control-authentication.md). Its "Worker-control
  authentication is deferred, and it costs something" section is replaced in
  full: that deferral is implemented and the limitation it recorded no longer
  holds. Every other section of this record stands unchanged.
- **Date:** 2026-09-13

## Context

Milestones M1 through M4 attributed every request — public and internal alike —
to one configured development scope, `TASKFORGE_DEV_SCOPE`. That was never
authentication and was always documented as temporary. It was also load-bearing
in a useful way: it forced the ownership model to be scoped correctly from M1, so
every job read, cancellation, replay, and DLQ listing has been filtering on a
scope since before a credential existed to supply one.

[`docs/PROJECT_SPEC.md`](../PROJECT_SPEC.md) §6 already fixed the shape of the
credential: *"API keys are high-entropy, returned exactly once, stored as a
lookup prefix plus a cryptographic hash, revocable, and scoped. Worker/control
scopes are separable from user scopes."* This record is not re-deciding that. It
decides the questions that sentence leaves open, and the boundaries that follow
from answering them now rather than later.

[`docs/ROADMAP.md`](../ROADMAP.md) lists M5 as four separate systems: API keys,
result storage with an object store, a CLI, and a Python SDK. This milestone is
the first of them, M5A, and it ships alone.

## Decision

### A prefix plus a hash, not a token format

A key is `tfk_<lookup>.<secret>`: `tfk_` as a recognizable marker, a 22-character
lookup segment from 16 random bytes, and a 43-character secret segment from 32
random bytes, both unpadded base64url. Only the lookup segment and a SHA-256
digest of the secret are stored.

The split exists so authentication is one indexed equality probe on a `UNIQUE`
column rather than a scan comparing a hash against every row. That is what keeps
the added check inside the request's existing timeout budget.

The separator is `.` and not `_` because base64url's alphabet contains `-` and
`_`. Splitting on `_` cuts at whichever underscore the random lookup segment
happens to contain, so roughly a quarter of generated keys would fail to parse —
a defect that a single-sample round-trip test passes three times in four.

A plain SHA-256 rather than a password KDF is justified by the generator, not by
convenience. The secret is 256 bits from `crypto/rand` with no guessable
structure, so there is no dictionary for a work factor to slow down. If key
material ever became user-chosen, that reasoning would no longer hold and this
decision would need superseding.

### One indistinguishable failure

A missing header, a malformed credential, an unknown prefix, a wrong secret, and
a revoked key all answer the same `401` with code `unauthorized` and a message
that names no cause. Verification runs before the revocation check so the two
cannot be separated by ordering either, and the comparison is
`subtle.ConstantTimeCompare` so they cannot be separated by timing.

Three different sentences would make a lookup prefix an oracle: present a guessed
prefix with any secret, and a distinguishing response tells you whether that
prefix exists and whether the key behind it is still live. The second half
matters on its own — it tells whoever holds a stolen key that it has been
revoked, which is precisely when they would otherwise stop being noticed.

A deadline inside the lookup is the one exception. It answers `503`, because "I
could not verify this" and "you are not authorized" are different facts, and
reporting the second for the first sends an operator hunting for a revocation
that never happened.

### The public surface fails closed

A `Server` constructed without a credential store answers `401` on every public
route, and does not register the key-management routes at all. A server that can
authenticate nobody has no authenticated caller to serve.

This is why the implementation contains no test-only authentication bypass. The
alternative considered — an explicit "auth disabled" constructor unreachable from
`cmd/` — would have needed a test to prove it stayed unreachable, and would have
left a code path whose entire purpose was to serve unauthenticated public
traffic. Failing closed removes the thing that would have to be guarded: a binary
that forgets to wire a credential store produces a visible outage rather than a
silent breach.

### Key issuance is a loopback HTTP surface

`POST /internal/v1/api-keys`, `GET /internal/v1/api-keys`, and
`POST /internal/v1/api-keys/{key_id}/revoke` live beside the worker-control
routes, on the same loopback-only bind, and are themselves unauthenticated.

**This is a real trust boundary and it is stated rather than hidden: anyone who
can reach loopback can mint a credential for any scope.** That is the same
population that can already drive the worker-control surface — register sessions,
claim jobs, and commit outcomes — so it does not widen who is trusted. It does
change what they can do: a worker-control caller can manipulate execution, while
a key minter can read and cancel any scope's jobs through the public API.

Bootstrapping is what forces the question. Something has to create the first
credential, and requiring a credential to create one is circular. The
alternatives were:

- **Wait for the CLI.** This would make authentication depend on a milestone that
  has not started, and the CLI would need this same database write anyway, so
  nothing is learned by waiting.
- **A separate bootstrap binary writing through the DSN.** A second way to write
  `api_keys`, with its own validation, its own hashing, and its own opportunity
  to drift from the one the API uses. One writer with one validation path is
  worth more than the small boundary this would buy.
- **Authenticate key management with a bootstrap secret from configuration.** A
  long-lived shared secret in an environment variable is a worse credential than
  the ones it would be protecting, and `.env` is already the file this project
  keeps secrets out of.

Key creation is deliberately **not** idempotent and carries no `Idempotency-Key`,
against the grain of every other mutating route here. An idempotent create would
have to return an existing secret to a repeat, which is the one thing a
write-only credential must never do. The cost is that an ambiguous creation
cannot be resolved by retrying: the caller lists keys and revokes the one nobody
received. The contract says so, and the `503` for that route is the only one in
this API that tells a caller *not* to repeat the request.

### Worker-control authentication is deferred, and it costs something

> **Superseded by [ADR-0014](0014-worker-control-authentication.md).** This
> section is kept verbatim as the record of what M5A actually shipped and
> why; it no longer describes current behavior. Worker-control authentication
> is implemented, `TASKFORGE_DEV_SCOPE` is removed, and the stranded-job
> limitation below is closed.

The internal worker-control surface still runs under `TASKFORGE_DEV_SCOPE`,
unauthenticated and loopback-bound. `PROJECT_SPEC.md` §6 keeps worker and user
scopes separable, and a worker credential is a genuinely different problem: a
worker holds authority over execution rather than over reading, and its
credential lifecycle is tied to process sessions that already have their own
fencing and replacement semantics. Conflating the two here would move a trust
boundary this milestone is not authorized to move.

Deferring it has a consequence that is not cosmetic. Claims filter on the
worker-control scope, so **a job submitted with a key whose scope differs from
`TASKFORGE_DEV_SCOPE` is durable, readable and cancelable, and will never be
claimed.** It stays `QUEUED`.

The failure is silent by nature, so it is handled three ways rather than
described once: an end-to-end test pins both halves, with an in-scope job
succeeding in the same run so the stranded one's fate is provably a scope
boundary rather than a broken stack; the creation handler warns at mint time,
naming the consequence, because that is the last moment an operator can act on
it; and [`CURRENT_STATE.md`](../CURRENT_STATE.md) records it as a limitation.

Multi-tenant key issuance is therefore useful today for isolation of reads,
cancellation, and the DLQ, and not yet for execution.

## Alternatives considered

**JWTs or another signed bearer token.** Self-contained tokens need no database
read to verify, which is their whole appeal. They also cannot be revoked without
one — a revocation list is the database read, reintroduced, plus a second
mechanism that can disagree with the token. `PROJECT_SPEC.md` §6 requires
revocable keys, and TaskForge already has PostgreSQL as its authoritative store
([ADR-0001](0001-postgresql-as-authoritative-state.md)); a credential whose truth
lived somewhere else would be the only piece of state that did. Key rotation and
expiry, which is what JWTs genuinely buy, is not a V1 requirement.

**mTLS.** Strong, and the wrong shape for the stated V1 experience:
`PROJECT_SPEC.md` §4 asks a developer to clone the repository and obtain a local
API key. Certificate issuance and distribution is a larger operational surface
than the thing being protected, and it would not remove the need for a scoped,
revocable identity in the database.

**Session tokens.** Sessions are for interactive logins. These credentials are
held by programs, which is why they are long-lived, scoped, and revoked rather
than expired and refreshed.

**Deferring authentication entirely until the CLI exists.** This is what the
roadmap's undivided M5 implied. It keeps the public surface unauthenticated
across a milestone that also adds result storage, so the first thing to be served
over the network would be job output. Slicing authentication out and shipping it
first inverts that.

## Consequences

- Every `/v1` request now requires a credential. This is a **breaking change**
  for any existing caller, with no compatibility shim, which is what the
  documentation promised a milestone-scoped development scope would mean.
- `TASKFORGE_DEV_SCOPE` stays required and validated. Its meaning narrows to the
  internal worker-control surface, and it no longer reaches the public API.
- Nothing under `/internal/v1` may be exposed off loopback — now including key
  issuance, where the consequence of getting that wrong is worse than it was.
- Authentication costs one indexed lookup per public request. The index makes it
  a probe rather than a scan; if it ever shows up in latency, a cache would need
  its own decision record, because a cached credential is a credential whose
  revocation is delayed.
- A job submitted under a scope no worker serves will not run. See above.
- `api_keys` starts empty on every upgrade path. There is no credential history
  to reconstruct, which makes 0014 the simplest migration in the repository —
  deliberately, and in contrast to 0011 through 0013.

## Superseded clauses

None. This record adds a decision; it replaces no clause of an accepted one.
When worker/control scopes are implemented, the deferral recorded here should be
superseded by the record that implements them.
