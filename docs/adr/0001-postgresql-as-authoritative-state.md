# ADR-0001: PostgreSQL as authoritative state

- **Status:** Accepted
- **Date:** 2026-08-29

## Context

A job system needs one place that decides what is true: whether a job exists, what
state it is in, which attempt is valid, who holds the lease, and whether a
completion is allowed. If that authority is split — or lives somewhere without
transactions — every interesting race becomes unresolvable.

Candidate homes for that authority: a relational database, Redis, the message
broker itself, or in-process memory.

The races TaskForge must resolve are not incidental. Two workers claiming one job,
a completion arriving as a lease expires, a cancel racing a success, a duplicate
submission arriving twice concurrently — all of these need a serialization point
with real transactional semantics and constraint enforcement.

## Decision

**PostgreSQL is the single authoritative store** for job, attempt, lease,
worker-session, idempotency, outbox, and logical DLQ state.

Every state transition that must be correct happens inside a PostgreSQL
transaction, and every invariant the database can express is expressed as a
constraint — primary and foreign keys, `CHECK`, unique and partial-unique indexes —
rather than being left to application code.

Access is explicit SQL through `pgx/v5`. No ORM sits between a reviewer and the
query that enforces correctness.

Corollaries:

- The broker holds notifications, never authoritative state.
- Redis is never authoritative job state.
- No correctness property may depend solely on in-memory state.
- PostgreSQL server time (`now()`) is authoritative for eligibility, expiry, and
  staleness. Client- and worker-supplied wall-clock time is never trusted.

## Alternatives considered

**Redis as primary state.** Fast, and the obvious choice if throughput were the only
goal. Rejected: no multi-statement transactional guarantees of the kind needed here
without Lua gymnastics, weak constraint enforcement, and durability tradeoffs that
are unacceptable for a system whose headline promise is that accepted jobs are not
lost.

**Broker as source of truth (SQS-only design).** Rejected: SQS has no queryable
state, no ordering guarantee, no way to express "at most one active lease per job",
and no way to atomically select the highest-priority eligible job. It also cannot
answer operator questions like "what is job X doing right now".

**In-memory state with periodic persistence.** Rejected outright: it fails the first
crash, which is the exact scenario TaskForge exists to handle correctly.

**A distributed consensus store (etcd, Consul).** Rejected as overkill for V1: it
adds an operational component without giving anything PostgreSQL cannot already do
at this scale, and it makes the project harder to read and learn from.

## Consequences

**Positive.** Races resolve to exactly one winner via row locks and constraints.
Invariants are enforced by the database, so application bugs cannot silently violate
them. Operators can query real state. The design is readable and teachable — the
correctness argument is visible in SQL.

**Negative.** PostgreSQL becomes the throughput ceiling and a single point of
failure for V1. Claim contention concentrates on the `jobs` table and must be
managed with `SKIP LOCKED` and careful indexing. Scaling past one primary would
require partitioning or sharding, which is explicitly out of V1 scope.

**Accepted risk.** V1 targets a scale where one PostgreSQL primary is comfortable.
That assumption is untested until the M8 benchmarks run, and it is recorded as an
unmeasured target, not a claim.
