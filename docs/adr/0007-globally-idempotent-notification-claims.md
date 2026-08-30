# ADR-0007: Globally idempotent notification claims

- **Status:** Accepted
- **Date:** 2026-08-29

## Context

The transactional outbox deliberately permits duplicate publication, and an
SQS-compatible broker may deliver the same event to different worker sessions. The
notification is advisory, so its job-id hint cannot be used as an authoritative
assignment. If claim idempotency were scoped only to one session, however, two copies
of one event could make two sessions reserve two different queued jobs. That would
violate the accepted rule that duplicate delivery of one event produces at most one
active lease.

Ambiguous HTTP responses create the related problem: after a claim commits, a worker
must be able to recover the same assignment on broker redelivery rather than issue a
new logical claim.

## Decision

This decision supersedes ADR-0003's M2 claim-identity and acknowledgement details
while leaving its V1 scheduler/re-notification target intact.

The outbox `event_id` is the claim request id. PostgreSQL enforces global uniqueness
of `leases.claim_request_id`, not uniqueness per worker session.

An exact replay by the owning session returns the original assignment. The same event
presented by another current session returns `DUPLICATE_NOTIFICATION`, with no
assignment and an explicit safe-to-acknowledge decision. Reusing an event id for a
different scope or queue is a conflict. The worker also collapses overlapping copies
inside one process, but that in-memory optimization is not the correctness boundary;
the global database constraint is. A transaction-scoped advisory lock derived from
the event id serializes same-id requests before queue-specific row locks, so concurrent
cross-queue reuse also returns the defined conflict instead of leaking a uniqueness
error. Hash collisions can only over-serialize unrelated claims.

## Alternatives considered

**Generate a fresh random claim id for every broker receive.** Rejected because a
committed-but-lost response cannot be recovered after redelivery.

**Make claim ids unique only within a process session.** Rejected because duplicate
delivery across sessions can reserve two jobs.

**Execute the notification's job-id hint directly.** Rejected because it bypasses
priority, capability, capacity, and current-state decisions and turns the broker into
authoritative state.

**Return the first session's assignment to another session.** Rejected because it
would transfer a lease across process identities and defeat fencing.

## Consequences

**Positive.** Broker and publisher duplicates are harmless across worker processes;
ambiguous claim responses are recoverable; only the owning session can execute the
assignment; acknowledgement decisions remain explicit.

**Negative.** Claim request ids are no longer arbitrary per-call UUIDs: trusted
workers must use the durable event id. One notification can wake at most one claim,
which is intentional; each submitted job has its own outbox event, and future
re-notification creates a new event id.
