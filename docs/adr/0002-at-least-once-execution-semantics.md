# ADR-0002: At-least-once execution semantics

- **Status:** Accepted
- **Date:** 2026-08-29

## Context

Job systems are routinely marketed as "exactly-once". For a system that invokes
arbitrary user code with external side effects, that claim is not achievable: the
handler could charge a credit card and then the process could die before the
outcome is recorded. On restart, the control plane cannot distinguish "the side
effect happened" from "it did not".

TaskForge must be precise about what it promises, because the whole point of the
project is to demonstrate understanding of these boundaries rather than paper over
them.

## Decision

TaskForge V1 provides:

> **Durable at-least-once job execution with idempotent control-plane transitions,
> fenced stale attempts, and application-level idempotency support for external
> handler side effects.**

Concretely, TaskForge guarantees:

- A job accepted by the API is durable before the caller gets a success response.
- A job never stops making progress because a broker message was lost, duplicated,
  or delayed.
- Duplicate or stale workers cannot corrupt TaskForge's own state: leases are fenced
  by worker session and attempt, at most one active lease exists per job, and a
  completion accepted once is never overwritten.
- Every race has exactly one winner.

TaskForge explicitly does **not** guarantee:

- That a handler body runs exactly once. A handler may run more than once.
- That an external side effect occurs exactly once.

To make duplicate execution survivable, TaskForge supplies stable job and attempt
identifiers to handlers so they can deduplicate their own side effects.

**The phrase "exactly-once" must not appear as a TaskForge guarantee anywhere — not
in the README, the API docs, the dashboard, a commit message, or a handoff.**

## Alternatives considered

**Claim exactly-once.** Rejected as false. It would also be self-defeating: the
project's value is demonstrating that the author knows where the line is.

**Effectively-once via transactional handler participation.** Real systems achieve
this by making the handler's side effect commit in the same transaction as the
completion record. Rejected for V1: it only works when the side effect is in the
same database, which forecloses the general handler model. It remains a legitimate
post-V1 extension for database-local handlers.

**At-most-once (never retry).** Rejected: it converts every transient failure into
permanent job loss, which is the opposite of the durability the project is about.

## Consequences

**Positive.** The promise is honest and defensible under scrutiny. It focuses design
effort where it actually pays off — fencing, idempotency, and reconciliation — rather
than on an impossible guarantee.

**Negative.** Handler authors carry real responsibility: any handler with external
side effects must be idempotent. This must be stated prominently in handler
documentation and examples, or users will be surprised by a duplicate charge.

**Follow-on requirement.** The publish-before-mark window in the outbox publisher
(see [ADR-0004](0004-transactional-outbox.md)) is a deliberate, documented instance
of this semantics — not a bug to be fixed.
