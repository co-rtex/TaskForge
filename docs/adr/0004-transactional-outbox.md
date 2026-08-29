# ADR-0004: Transactional outbox for broker delivery

- **Status:** Accepted
- **Date:** 2026-08-29

## Context

Submitting a job requires two effects: a durable row in PostgreSQL and a notification
on the broker. These are separate systems, so there is no shared transaction.

Doing them naively produces a dual-write bug in one of two directions:

- **Commit, then publish.** If the process dies between the two, the job is durable but
  no worker is ever notified. It sits queued forever.
- **Publish, then commit.** If the transaction rolls back, a notification exists for a
  job that does not. Workers chase a phantom.

Neither is acceptable for a system whose central promise is that accepted work is never
lost.

## Decision

Use the **transactional outbox** pattern.

Any committed state transition that requires a notification writes its outbox event
**in the same PostgreSQL transaction** as the state change. Job, idempotency record,
and outbox event commit atomically or not at all. There is no window in which a job is
durable but its notification was never recorded.

A separate publisher process drains the outbox:

1. **Claim** a batch of due pending events with `FOR UPDATE SKIP LOCKED`, increment the
   attempt counter, and push `available_at` forward by a backoff interval. **Commit.**
   The advanced `available_at` acts as a visibility timeout, so a publisher that dies
   mid-flight releases its events automatically rather than blocking them forever.
2. **Publish** to the broker **outside** any transaction, so no lock is held across
   network I/O.
3. **Mark published** in a second short transaction.

Backoff is bounded exponential with jitter, and the random source is injected so tests
are deterministic. Event schemas are versioned. Multiple publisher replicas are safe by
construction, because `SKIP LOCKED` guarantees no two claim the same row.

### The publish-before-mark window is deliberate

A crash between step 2 and step 3 causes the event to be published again once its
backoff expires. **This is an accepted at-least-once window, not a bug.** It is safe
because notifications are advisory: the claim query is what enforces single execution,
so a duplicate notification can never produce two active leases. See
[ADR-0002](0002-at-least-once-execution-semantics.md) and
[ADR-0003](0003-pull-based-claim-with-broker-notification.md).

## Alternatives considered

**Publish inside the database transaction.** Rejected: it holds row locks across a
network call to the broker, so broker latency becomes database lock contention and a
broker outage can stall the submission path entirely. It also does not remove the
window — a crash before commit still loses the mark.

**Two-phase commit across PostgreSQL and the broker.** Rejected: SQS does not
participate in XA, and even where 2PC is available it trades a small duplicate window
for a much worse in-doubt-transaction failure mode.

**Change data capture (Debezium / logical replication).** A genuinely strong
alternative that removes the polling loop. Rejected for V1: it adds Kafka Connect or an
equivalent runtime, and it hides the delivery mechanism behind infrastructure — the
opposite of what a project meant to teach this pattern should do. Reasonable post-V1.

**Listen/notify instead of an outbox table.** Rejected: `NOTIFY` is fire-and-forget and
is lost if no listener is connected, so it provides no durability and cannot be the
recovery path.

## Consequences

**Positive.** No job is ever durable-but-unnotified. Broker outages become pure latency:
the job stays durable, the event stays pending, and publication resumes on recovery with
no resubmission. Pending events are queryable, so stuck delivery is visible and
repairable. Publisher replicas scale horizontally.

**Negative.** An extra table and an extra process to operate. The outbox needs a
retention or archival policy so it does not grow without bound — deferred, and recorded
as debt. Publishing adds a polling interval of latency on top of the commit.

**Requires monitoring.** Pending-event count and publish-failure rate are first-class
metrics; a growing pending backlog is the primary signal that delivery is broken.
