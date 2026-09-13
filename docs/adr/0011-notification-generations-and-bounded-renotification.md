# ADR-0011: Notification generations and bounded re-notification

- **Status:** Accepted
- **Date:** 2026-09-01

## Context

[ADR-0003](0003-pull-based-claim-with-broker-notification.md) made the broker
advisory: correctness never depends on delivery. That is true, and M1 through M3
prove it — a lost, duplicated, delayed, or reordered notification cannot corrupt
durable state.

But correctness is not the only thing that matters. **Reachability** is separate
and is not free. A `QUEUED` job whose only notification was lost sits claimable
and unclaimed forever, because nothing wakes a worker to claim it. M3 documented
this as a known gap: reconciliation re-notifies the specific job it just
recovered, and nothing re-notifies a job whose *submission* notification
vanished.

M4 also introduces two new ways a job becomes eligible later rather than now — a
delayed submission and a retry-waiting job — and neither can be advertised at
the moment it is created, because a notification for work no worker may claim
yet is a wasted round trip that a worker must then decline to acknowledge.

The naive repair is a scheduler pass that re-notifies any `QUEUED` job. It is
wrong in two directions at once. Without a rate limit it turns one slow
publisher into an unbounded pile of duplicates. With only a "does this job have
a pending event" check it is worse than wrong — it is silently wrong in exactly
the case it exists for.

That last failure is worth stating precisely, because it is the whole reason
this record exists. [ADR-0004](0004-transactional-outbox.md)'s
publish-before-mark window can leave a `PENDING` event behind from an attempt
that is already over: the publisher published it, died before marking it, and
the job has since been claimed, abandoned, and requeued. A check that asks only
"is there a pending event for this job" sees that stale row and concludes the
job is already advertised. It is not. The event belongs to an eligibility
transition that ended, and the job's *current* transition has no notification at
all. The job would be freshly queued and permanently unadvertised, and the
mechanism built to prevent stranded work would be the thing stranding it.

## Decision

### A job carries a monotonic notification generation

`jobs.notification_generation` identifies **one eligibility transition**. It is
incremented every time the job newly becomes `QUEUED`: at immediate submission,
at scheduler promotion of a delayed or retry-waiting job, and at crash-recovery
requeue. `jobs.last_notification_at` records when the most recent event for the
current generation was created, from PostgreSQL time sampled after the authority
locks.

A delayed job starts at generation 0 with no `last_notification_at`, and a
`CHECK` constraint keeps the two inseparable. Generation 0 means exactly what it
says: no `work.available` event has ever been created for this job.

`outbox_events` gains a real `job_id` column and the `notification_generation`
the event advertises. Both are control-plane metadata: neither is serialized to
the broker, so the published wire contract is unchanged and no schema version is
bumped. The job-id hint already inside the envelope stays exactly as advisory as
it was — a hint buried in JSON is not something a correctness decision may rest
on.

### Eligibility and its notification commit together

Any transition into `QUEUED` increments the generation, stamps
`last_notification_at`, and writes the `work.available` event in the **same
transaction**. A job can never become claimable without the notification that
wakes a worker for it, and a crash before commit leaves neither.

The converse holds too: a transition into `PENDING` or `RETRY_WAIT` deliberately
writes **no** event. The job is durable and scheduled; the scheduler creates
exactly one event when PostgreSQL says it has actually become eligible.

### Re-notification is bounded three ways

A replacement notification is created only when all three hold, revalidated
under `queue → job` locks against a fresh post-lock sample:

1. the job is still `QUEUED`;
2. the configured interval has elapsed since `last_notification_at`;
3. **no `PENDING` event exists for the job's CURRENT generation.**

The third is the one this record is about. A stale event from an earlier
generation correctly fails to satisfy it, so it cannot suppress the notification
a new transition requires.

The replacement carries a **new event id** but the **same generation**: it
advertises the same eligibility transition, not a new one. A new id is required
because the old one may already have been consumed as a claim identity
([ADR-0007](0007-globally-idempotent-notification-claims.md)).
`last_notification_at` advances in the same statement, so the job is rate-limited
again immediately and N replicas cannot multiply events for one stranded job.

### Promotion is a separate bounded service

`taskforge-scheduler` promotes due `PENDING` and `RETRY_WAIT` jobs and
re-notifies stranded `QUEUED` work. Both sources go through one mechanism: from
the scheduler's side they differ only in how `available_at` got its value, and
answering "is this job due" twice in two places is how the two would eventually
disagree.

It holds **no broker connection**. It writes the authoritative outbox event and
`taskforge-outbox` owns every byte that reaches the broker, so a scheduler
crash, a broker outage, and a publisher restart stay three independent failures
rather than one entangled one.

Safety with N replicas is structural, not incidental. The candidate scans carry
no authority; each promotion's `UPDATE` names both the expected status and the
expected generation, so a second replica arriving later would have to match a
generation the first has already moved past.

### Configuration relationships are validated at startup

`TASKFORGE_SCHEDULER_RENOTIFY_AFTER` must be at least three polling intervals,
so an ordinary delivery has had several chances before anything is called lost,
and at least `TASKFORGE_OUTBOX_CLAIM_TIMEOUT`, because a claimed-but-unpublished
event is invisible to the pending-event check for exactly that window — a
shorter interval would decide an event was lost while a publisher was still in
the middle of publishing it.

## Alternatives considered

**Re-notify on every scheduler pass.** Rejected: one slow publisher becomes an
unbounded pile of duplicates, and every worker pays to receive and decline them.

**Rate-limit on `last_notification_at` alone, with no pending-event check.**
Rejected: it re-notifies work whose first notification is simply still in the
queue, which is the common case rather than the exceptional one.

**Check only "does a pending event exist for this job".** Rejected — this is the
silently-wrong option described above. It is the reason generations exist.

**Give the scheduler a broker connection and let it publish directly.** Rejected:
it duplicates the publisher, reintroduces the dual-write
[ADR-0004](0004-transactional-outbox.md) exists to prevent, and makes a broker
outage able to stop promotion, which is pure PostgreSQL work.

**Reuse the original event id for a re-notification.** Rejected: the id is the
globally unique claim identity and may already have been consumed, so the
replacement claim would collide with an attempt that is already over.

**Detect stranded work from the worker side, by having idle workers poll
PostgreSQL directly.** Rejected: it makes workers database clients, which
[ADR-0006](0006-session-bound-worker-eligibility.md)'s DB-less worker design
deliberately avoids, and it scales the polling load with worker count rather
than with stranded-job count.

## Consequences

**Positive.** A lost notification is repaired within a bounded, configurable
interval, and the repair is provably rate-limited, batch-limited, and
generation-aware. A stale publish-before-mark event can no longer suppress a
fresh transition's notification. Delayed and retry-waiting jobs cost no broker
traffic while they wait. Promotion is exactly-once per eligibility transition
across N replicas, enforced by the database rather than by leader election.
Reliability invariant 13 — retry scheduling survives service restarts — becomes
testable, because the schedule lives in PostgreSQL and nothing is held in a
process.

**Negative.** Two more columns on `jobs`, two on `outbox_events`, two more
partial indexes, and one more service to run. Every writer that moves a job into
`QUEUED` must remember to open a generation and write the event; that is
enforced by keeping both inside one helper rather than by convention.

**Known and accepted.** Re-notification repairs reachability, not delivery. If
the broker itself is down, the replacement event sits pending exactly like any
other, and the outbox publisher's own backoff owns that case. The interval is
also a real floor on how long a stranded job can sit unclaimed: with the
documented defaults that is up to a minute. That is a deliberate trade against
duplicate traffic, and it is configuration rather than architecture.
