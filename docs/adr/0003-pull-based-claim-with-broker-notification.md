# ADR-0003: Pull-based claim with broker notification

- **Status:** Accepted
- **Date:** 2026-08-29

## Context

TaskForge needs priority-aware dispatch, capability matching, and bounded worker and
queue concurrency. A naive design puts a message on an SQS queue and lets whichever
worker receives it run the job.

That design contains a contradiction. An SQS-style shared queue cannot reliably route
a message to a *specific* scheduler-selected worker: any consumer may receive any
message, delivery order is not guaranteed, and there is no way to express "only a
worker with capability `gpu` and a free slot may take this". Attempting to fix it with
per-worker queues creates a queue-per-worker explosion, breaks work stealing, strands
messages when a worker dies, and still cannot enforce a global queue concurrency limit.

Meanwhile, priority is meaningless in a FIFO-ish queue: a high-priority job submitted
after a backlog of low-priority ones sits behind them.

## Decision

TaskForge is **pull-based**. The broker is a **notification channel**, not a work
router.

1. The broker carries a small `work.available` notification: event id, schema version,
   event type, queue, a **non-authoritative** job-id hint, and trace metadata. It never
   carries the authoritative job payload.
2. A worker polls only while it holds a free local slot.
3. The worker presents its worker id, process-session id, queue, and capabilities to
   the control plane's claim operation.
4. **PostgreSQL decides**, in one transaction: enforce queue and worker capacity, match
   capabilities, select the highest-priority eligible job, create the attempt and lease,
   and move exactly one job `QUEUED → LEASED`.
5. The worker acknowledges the broker message only after the claim succeeds or the
   control plane confirms no eligible job remains.

Deterministic claim ordering:

```sql
ORDER BY priority DESC, available_at ASC, created_at ASC, id ASC
```

Because the notification is advisory, correctness cannot depend on it. A lost,
duplicated, or delayed notification must not strand work, so the scheduler re-notifies
stranded queued work under a bounded, rate-limited policy.

V1 implements **strict priority with deterministic tie-breaking**, not fairness.

## Alternatives considered

**Push-based dispatch (scheduler assigns to a chosen worker).** Rejected: it requires
either per-worker queues or a direct connection to every worker, and it makes the
scheduler responsible for liveness tracking before a lease even exists. It also
handles worker death badly — assigned-but-undelivered work needs its own recovery path.

**Per-worker SQS queues.** Rejected: unbounded queue proliferation, no work stealing,
stranded messages on worker death, and still no global concurrency enforcement.

**Database-only polling with no broker.** Genuinely viable and simpler, and it is what
the claim path effectively does. Rejected for V1 because a notification channel keeps
idle polling low and, more importantly, because building the transactional-outbox and
duplicate-delivery machinery is a core part of what this project exists to demonstrate.
The design deliberately keeps the system correct if the broker vanishes entirely.

**Broker-ordered priority (separate queue per priority band).** Rejected: coarse,
fails at tie-breaking, multiplies queues, and moves an authoritative decision out of
the authoritative store.

## Consequences

**Positive.** Priority, capabilities, and capacity are enforced in one auditable SQL
statement. Broker unreliability is reduced to a latency concern rather than a
correctness concern. Duplicate delivery is harmless because the claim query admits
only one winner. Work stealing is automatic — any eligible worker can take any
eligible job.

**Negative.** Every claim is a database transaction, so the claim path is the primary
contention point and must be carefully indexed and kept short. Idle workers still poll,
so poll interval and long-poll settings need tuning. Two moving parts (broker plus
database) must both be operated, even though only one is authoritative.

**Known and accepted.** Strict priority can starve low-priority work under sustained
high-priority load. This is a documented property with a test, not a defect. Weighted
fairness and aging are post-V1.
