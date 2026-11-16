# Case study: notification reachability across tenants

A record of one correctness repair, written for an engineer reviewing the work. The
decision is [ADR-0029](../adr/0029-scope-addressed-notification-streams.md); the evidence,
commands and raw figures are in [CURRENT_STATE.md](../CURRENT_STATE.md), "Notification
reachability repair (ADR-0029)". This page tells the story and does not restate either.

## 1. The defect

TaskForge's broker is advisory ([ADR-0003](../adr/0003-pull-based-claim-with-broker-notification.md)):
a notification only wakes a worker, and one PostgreSQL transaction decides what it claims.
A correctness audit (PR #33) found two ways that wakeup failed, and pinned each with an
integration test that asserted the wrong behavior:

- **A worker of tenant B deleted tenant A's only notification.** A's job then sat `QUEUED`
  beside an idle, healthy A worker until bounded re-notification (60 s by default), whose
  replacement faced the same odds.
- **An unclaimable job accumulated notifications.** A `gpu` job beside cpu-only workers
  gained one more live broker message per re-notification interval, forever: `[5 10 15 20]`
  live messages in six seconds in the test, about 1,440 a day per stuck job on the defaults.

Neither could lose a job or corrupt state. Both cost reachability, and the second cost load.

## 2. Root cause

One gap behind both. A claim decided "safe to delete this notification" relative to the
**claiming worker**: `QUEUE_EMPTY` meant "nothing due in *my* scope". Every scope shared one
broker queue, so that answer deleted other scopes' wakeups. And re-notification assumed a
published notification is either consumed or lost, when a notification nobody can use is
neither: it is **held**, redelivered and declined forever, or **unread**, because no worker
that could use it is polling.

## 3. The invariant that was violated

Stated after the fact as ARCHITECTURE §12 invariants 19 and 20: a notification is
acknowledged only by a claim in its own scope and queue; and a queued job that no current
session could claim receives no replacement notification.

## 4. What made it subtle

Claims are **fungible within a scope and queue**. A notification wakes a claim that takes the
best eligible job, not the job the notification names. So "this notification belongs to job
J" is never true, and every per-job repair the audit sketched either deleted a wakeup some
other job depended on or let leftovers accumulate with throughput.

## 5. Alternatives considered

Envelope scope with workers skipping foreign messages (SQS cannot address a consumer: the
delay grows with other tenants' worker counts); forwarding foreign notifications through the
outbox (O(tenants) hops per job, infinite with no worker); tracking declines per
notification (blind to unread notifications, and fungible swaps defeat it); exponential
backoff or short broker retention (slows growth or deletes a stuck job's only wakeup). Each
is weighed in ADR-0029.

## 6. The design

1. **Address the stream, not the job.** The unit of fungibility, (scope, queue), gets a
   broker queue of its own, named by a digest of the address and created on demand from the
   base queue's attributes. The publisher routes each event by its job's address; a worker
   learns its scope at registration and reads only its address. "Safe for this worker" now
   coincides with "safe for this notification".
2. **Decide relative to the notification.** The claim reads the event it was woken by:
   another address's event claims nothing and is not acknowledged; a leftover whose
   advertisement has ended is released when nobody can use it; a current one is held.
3. **Re-notify only for someone.** A replacement is written only when some current session
   could claim the job now. That covers held and unread notifications alike, and a lost one
   is still repaired once a claimant exists.

## 7. Evidence

- Both pinned tests reproduced on the base commit, then were **inverted, not deleted**: the
  foreign worker can no longer touch the notification, and the unclaimable job holds
  `[1 1 1 1]` live messages with one event, then completes when a capable worker joins.
- Five mutations (routing removed, each guard disabled, the gate disabled, the deferral
  unstamped) each fail the new tests.
- A multi-tenant workload no benchmark had, run on the base commit and on the fix: at 8
  scopes × 10 jobs, the base left 23 of 80 jobs unclaimed for the whole 20 s window, with 51
  leases taken under another tenant's notification; the fix claimed all 80 in about a
  second, with none.
- The concurrent foreign-presentation test found a real wrinkle in the first version: a
  consumed foreign event fell into the older conflict error rather than the defined
  outcome.

- A recorded benchmark at the fix (shipped defaults, 10,000 jobs, 24 worker kills) met the
  same three targets as the committed headline record and missed the same two, for the same
  recorded reasons, with every job completed. It ran on different hardware, so it shows the
  repair did not break the measured behavior; it is not an improvement claim.

## 8. Costs and limitations

Per-address queues are created by the application, which needs `sqs:CreateQueue` on a name
prefix and leaves queue retirement to operators. Messages on the old shared queue are
repaired by re-notification after an upgrade, not read. Within one address, workers with
different capabilities still meet a held notification at random. The gate cannot see which
queue a session polls. ADR-0029 records each, and none weakens a correctness guarantee.
