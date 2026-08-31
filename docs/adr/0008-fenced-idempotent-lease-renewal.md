# ADR-0008: Fenced, generation-versioned lease renewal

- **Status:** Accepted
- **Date:** 2026-08-31

## Context

M2 issued one fixed lease window and rejected any transition at or after its
expiry. That is correct but limiting: work that takes longer than
`TASKFORGE_LEASE_DURATION` could never commit an outcome, because its authority
lapsed while it was still running. M3 needs cooperative work to survive across
several lease windows without weakening a single M2 fence.

The obvious implementation is a mistake:

```sql
UPDATE leases SET expires_at = clock_timestamp() + duration WHERE id = $1 AND ...
```

Renewal is a network call from a worker to the control plane, and every network
call has an ambiguous outcome. A worker whose renewal request commits but whose
response is lost must retry. Under the statement above, that retry extends
authority a second time. The worker believes it holds one window; PostgreSQL has
granted two. The same statement also lets two concurrent renewals for the same
window both succeed, and lets a delayed request from an earlier window extend a
lease it no longer has any claim on.

That is not a small imprecision. Lease expiry is the mechanism that lets
reconciliation decide a worker is gone. Anything that can silently extend a lease
can silently delay recovery.

The existing five-part fence — job, attempt, lease, worker, session — answers
*who* may renew. It does not answer *which renewal this is*, and that second
question is the one an ambiguous retry raises.

## Decision

A lease carries a **monotonic renewal generation** and the **identity of the
request that produced it**:

- `leases.renewal_version` starts at `0`, the window issued by the claim itself.
- `leases.last_renewal_request_id` records the client-generated id of the renewal
  that produced the current generation. A `CHECK` constraint makes the two
  inseparable: generation 0 has no identity, and every later generation has one.
  A partial unique index ensures no two leases hold the same identity at the same
  time; the scope note below says exactly what that does and does not promise.

A renewal request carries the complete five-part fence plus a
`renewal_request_id` and the `expected_renewal_version` the caller believes is
current. Under the established `queue → worker session → job → attempt → lease`
lock order, with PostgreSQL time sampled after every lock:

- **Exact replay** — the stored identity equals the request's and the generation
  is exactly one past the expected one: return the committed window unchanged.
  Nothing moves. This is what makes an ambiguous response safe to retry.
- **Reuse of a live identity against another lease** — the id is currently
  recorded on a different lease: a deterministic domain conflict, looked up under
  the same locks so callers never see a raw uniqueness error. Only identities
  that are currently in force are constrained; see the scope note below.
- **Stale, competing, or future generation**: no mutation, stable conflict.
- **Lease not `ACTIVE`, or PostgreSQL time already at or past expiry**: rejected.
  Renewal never resurrects an expired, completed, released, or reconciled lease.
- **Job or attempt not in an executing state, or the session no longer current**:
  rejected by the existing fence.

Otherwise the lease is renewed once: `renewed_at` and `expires_at` from the
post-lock server sample, generation incremented, identity recorded. The response
carries the new generation, the new expiry, and the **PostgreSQL-measured**
remaining duration.

Renewal extends **lease authority only**. It does not touch the job's
`timeout_seconds` budget, which the worker measures once from execution start.

A worker converts the server-measured remaining duration into a conservative
monotonic local deadline. It never compares its own wall clock with `expires_at`.

The renewal cadence must fit several attempts inside one lease window;
`internal/config` rejects a `TASKFORGE_LEASE_RENEW_INTERVAL` greater than one
third of `TASKFORGE_LEASE_DURATION`.

## Alternatives considered

**Unversioned renewal fenced only by the existing five identifiers.** Rejected:
it is exactly the broken statement above. The fence proves who is asking, not
which request this is, so an ambiguous retry doubles the granted window.

**A dedicated `lease_renewals` history table, one row per renewal.** Genuinely
appealing — it makes replay detection trivial and gives an audit trail for free.
Rejected for M3 as more schema than the behavior requires: it needs its own
retention story, and durable attempt and lease history already answers the
operator questions M3 raises. Reasonable later, alongside real audit storage.

**Server-generated renewal tokens returned to the worker.** Equivalent in power,
but it makes the first request of a generation unrecoverable: a worker that never
received the token has nothing to replay. A client-generated id is available
before the request is sent, which is the whole point.

**Idempotency keyed on a time bucket.** Rejected as a correctness argument that
depends on clock alignment between worker and database, which
[ADR-0001](0001-postgresql-as-authoritative-state.md) forbids.

## Consequences

**Positive.** Long cooperative work survives across lease windows. An ambiguous
renewal is safe to retry under its original identity and generation. Duplicate
renewal cannot extend authority twice, competing renewals resolve to exactly one
winner, and a delayed old request cannot extend anything. Renewal, success, and
reconciliation serialize into exactly one valid outcome, and every rejection is a
stable domain error rather than a leaked driver error. The database, not
application code, enforces identity uniqueness and identity/generation
consistency.

**Negative.** A renewing worker must track a generation counter, so renewal is
stateful in a way heartbeat is not. Retry logic has to hold its request id and
expected version across attempts — generating a fresh id on retry is precisely
the bug this prevents, so the OpenAPI 503 contract says so explicitly. One extra
column pair and one extra index on `leases`.

**Known and accepted.** Only the *last* renewal identity is retained. A replay of
an identity two generations old is a stable conflict rather than a recognized
replay. That is the correct answer — the caller's view of the world is stale
either way — but it means the replay window is one generation, not unbounded.

## Scope note: what identity uniqueness does and does not promise

Added after review, because the original wording of this record promised more
than migration 0006 enforces.

`leases_last_renewal_request_id_idx` is a partial unique index over each lease's
**current** `last_renewal_request_id`. It therefore guarantees:

> No two leases hold the same renewal identity at the same time.

It does **not** guarantee that a renewal identity is unique for all time. Once a
lease renews again, its previous identity is no longer stored, leaves the index,
and a different lease could reuse it and be renewed normally.

That narrower guarantee is sufficient, and the difference is not a gap in
fencing:

- A renewal identity authorizes nothing by itself. Whether a lease is extended is
  decided by the per-lease `expected_renewal_version` check, under the full
  five-part fence. A caller that reuses a superseded id on another lease still
  has to satisfy that lease's own fence and generation, so it can extend only the
  lease it legitimately holds, once.
- Replay detection is per-lease and compares against that lease's current
  identity, so reuse elsewhere cannot make a replay look like a first attempt, or
  a first attempt look like a replay.
- Replaying a superseded identity against its original lease is still refused
  deterministically: the generation no longer matches.
- No raw uniqueness error can escape in any of these cases, which was the failure
  mode this decision set out to prevent.

Enforcing lifetime uniqueness would require retaining every renewal in a
`lease_renewals` table — the alternative rejected above — with its own unbounded
growth and retention policy, in exchange for rejecting a request that is already
harmless. That trade is not worth making, so the guarantee is stated narrowly
here, in migration 0006, in the OpenAPI description, and in `CURRENT_STATE.md`,
and the boundary is pinned by
`TestRenewal_ASupersededIdentityIsNotRetainedAndIsHarmless`.
