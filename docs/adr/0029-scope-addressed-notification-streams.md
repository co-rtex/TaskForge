# ADR-0029: Scope-addressed notification streams and claimability-gated re-notification

- **Status:** Accepted
- **Date:** 2026-11-16
- **Supersedes in part:**
  - [ADR-0003](0003-pull-based-claim-with-broker-notification.md) rule 5: "the control plane
    confirms no eligible job remains" is now decided relative to the notification, not only
    to the claiming worker.
  - [ADR-0007](0007-globally-idempotent-notification-claims.md): reusing another scope's or
    queue's notification as a claim identity is answered `FOREIGN_NOTIFICATION`, not the
    conflict error, when the identity is a notification of another address.
  - [ADR-0011](0011-notification-generations-and-bounded-renotification.md): re-notification
    gains a fourth bound, claimability.

## Context

The correctness audit after M8C2 (PR #33) pinned two defects with integration tests that
asserted the wrong behavior ([CURRENT_STATE.md](../CURRENT_STATE.md), "Correctness audit
after M8C2"). Both were reproduced on `98dbe3f` before this change:
`TestOpenDefect_AnotherScopesWorkerDeletesTheOnlyNotification` passed, and
`TestOpenDefect_AnUnclaimableJobsNotificationsAccumulate` recorded live broker messages
`[5 10 15 20]` and 20 outbox events for one job in six seconds.

They share one root cause. A claim decides whether a notification is safe to delete
relative to the **claiming worker**, and bounded re-notification assumes a published
notification is either consumed or lost. Neither assumption holds:

1. **Cross-scope consumption.** Every scope's notifications went to one broker queue, and
   since [ADR-0014](0014-worker-control-authentication.md) each scope runs its own workers.
   `Claim` selects within the worker's scope, so a scope-B worker receiving scope A's
   notification got `QUEUE_EMPTY` (safe to delete) when B had nothing due, or claimed B's
   own job under A's event id when it had. Either way A's job was left with no wakeup until
   re-notification, whose replacement met the same odds. With N scopes sharing the queue a
   replacement reached A's workers with probability roughly 1/N per delivery.
2. **Accumulation.** A notification nobody can use is not lost: `NO_ELIGIBLE_JOB` and
   `CAPACITY_EXHAUSTED` leave it in the broker, redelivered every visibility timeout, and a
   notification for an address no worker is polling is simply unread. Re-notification saw
   the job still `QUEUED` with nothing `PENDING` and added another every interval, without
   bound. The same arithmetic applied to any job waiting longer than the interval behind a
   saturated fleet.

The constraint that shapes every option: **claims are fungible within one scope and
queue.** A notification wakes a claim that takes the highest-priority eligible job of the
worker's (scope, queue), not the job the notification names (ADR-0003, ADR-0007). So no
rule that looks at one job's notifications can tell a redundant notification from one
another job depends on, and nothing may assume a notification belongs to the job it names.

## Decision

### 1. One broker queue per (scope, queue) address

`queue.Address{Scope, Queue}` names a notification stream: exactly the set of jobs one
claim chooses among. Each address is delivered on its own broker queue, named
`<TASKFORGE_BROKER_QUEUE_NAME>-<16 hex>`, the first 64 bits of SHA-256 over
`scope NUL queue` (`queue.NotificationQueueName`).

- `taskforge-outbox` publishes each event to the queue of the address of the job it
  advertises, read from the authoritative `jobs` row in the claim-for-publication statement
  (`outbox.Store.ClaimDue`), never from the envelope. The wire contract is unchanged.
- `taskforge-worker` learns its scope from registration (`scope` is added to the
  `WorkerSession` response) and reads only the queue of `(its scope, TASKFORGE_WORKER_QUEUE)`.
  A route it cannot resolve is fatal before any receive; it never falls back.
- `sqsbroker.Router` resolves an address's queue with `GetQueueUrl` and otherwise creates
  it with `CreateQueue`, copying the base queue's visibility timeout, retention, delay,
  long-poll default, size limit and encryption attributes, so the operator configures the
  family once, on the base queue. Either side may be first; creation is idempotent and a
  lost creation race resolves the winner's queue. An address's queue that disappears after
  it was resolved (a broker restart that keeps queues in memory, as ElasticMQ does, brings
  back only the configured base; an operator may delete one) is resolved or created again
  by the next operation that reports it missing, which is then retried once. The base queue
  must still exist: it is the template and the readiness target.
- `TASKFORGE_BROKER_QUEUE_NAME` is validated as a standard-queue name of at most 63
  characters, so every derived name fits SQS's 80.

This makes "safe to acknowledge relative to the claiming worker" coincide with "safe to
acknowledge relative to the notification", because every consumer of an address's queue
claims from exactly that address. It also ends the analogous delay between TaskForge
queues that shared a broker queue, where a worker of one queue left another queue's
notification invisible for a visibility timeout.

### 2. The claim decides relative to the notification it was woken by

`Store.Claim` reads the event its claim identity names, if it is a `work.available` event
(`readPresentedNotification`): the job's scope and queue, and whether the event is
**current** (its job is `QUEUED` in the generation the event was written for). The read
needs no lock: scope and queue never change, and generations only grow, so an event that
is not current never becomes current again.

- **`FOREIGN_NOTIFICATION`** (not safe to acknowledge, nothing claimed): the event belongs
  to another address. Under §1 a worker never receives one; this is the guard for when
  routing is wrong (a digest collision, a misconfigured base name, a pre-ADR-0029 shared
  queue during an upgrade). It applies also when the event was already consumed by its own
  address, which ADR-0007 answered with a conflict error.
- **`STALE_NOTIFICATION`** (safe to acknowledge): nothing due in the address is eligible for
  this session, and the event is not current. Its job left `QUEUED` or was requeued under a
  newer generation, so the job it names no longer depends on it. This is what removes the
  leftovers a fungible claim produces: when a cpu worker claims a cpu job with a stuck gpu
  job's notification, the cpu job's own notification is left over, and before this change
  it was declined `NO_ELIGIBLE_JOB` for as long as the gpu job waited.
- A claim identity that is not an outbox event keeps exactly its previous answer.
- `CAPACITY_EXHAUSTED` is unchanged, current or stale: under load a leftover is the cover
  for a job whose own notification was spent on another, and releasing it would trade a
  30 s redelivery for a 60 s re-notification (ADR-0024's tail).

### 3. Re-notification is gated on claimability

A replacement is created only when, in addition to ADR-0011's three bounds, **some current
session could claim the job now** (`someSessionCouldClaim`): a `HEALTHY` session of the
job's scope with the queue's worker group, the job's capabilities and job type, its logical
worker below its concurrency limit, and the queue below `max_concurrency`. This is the
claim's own eligibility minus the choice among due jobs, read without locks under the
queue and job locks the re-notification already takes.

The reasoning: re-notification cannot tell a lost notification from a held or unread one,
and it does not need to. A replacement is worth creating only for someone, and a held
notification's job is exactly one nobody can claim now. A worker polls only while it holds
a free slot, so a saturated fleet is not reading either. When a session that could claim
the job appears, or a slot frees, the next scan re-notifies it, so a genuinely lost
notification is still repaired.

A deferred job is stamped `jobs.renotify_checked_at`, and the scan orders by
`GREATEST(last_notification_at, renotify_checked_at)` (migration 0019, index
`jobs_renotify_due_idx`, replacing `jobs_stranded_queued_idx`). Without it, deferred jobs,
whose `last_notification_at` does not move, would fill every oldest-first batch forever and
starve every claimable stranded job behind them. `worker_sessions_healthy_scope_idx` keeps
the gate from reading a scope's whole session history.

## Invariants

- **I1 (no misdirected deletion).** A notification is deleted only after a claim in its own
  address returned a safe disposition for it. Routing guarantees only its own address's
  workers receive it; the foreign guard holds it when routing does not.
- **I2 (no cross-scope claim).** No lease is created under the identity of another scope's or
  queue's notification.
- **I3 (no current notification released without effect).** `STALE_NOTIFICATION` never applies
  to a current event, so the only ways a `QUEUED` job's current notification leaves the
  broker are the ones it always had: a claim (of it or, fungibly, of another job), a
  confirmed empty address, or a duplicate already consumed.
- **I4 (eventual discovery).** A `QUEUED` job is re-advertised within one re-notification
  interval (plus at most one more after a deferral, plus one scheduler pass and one publish)
  of the moment a session that could claim it exists with a free slot, whatever happened to
  its earlier notifications. Assumptions: the broker eventually delivers, and a scheduler
  and publisher run.
- **I5 (bounded live set).** Re-notification never adds a message for a job nobody can claim,
  so while a job is unclaimable its live notifications cannot grow by re-notification; and a
  leftover is removed at its first `NO_ELIGIBLE_JOB` presentation. Live messages are created
  only by eligibility transitions and by re-notifications of claimable jobs.

PostgreSQL remains the only authority. Nothing above changes what a claim may take, lease,
fencing, outcome precedence, the outbox's at-least-once publication or idempotent
submission; the new decisions only choose whether a broker message is kept or released,
and whether one more is written.

## Alternatives considered

**Scope in the envelope and workers that skip other scopes' notifications.** Rejected. SQS
cannot deliver to a chosen consumer: a skipped message returns after a visibility timeout
to a random consumer, so the expected delay grows with the number of other scopes'
workers, and releasing it at once (visibility 0) turns it into a hot loop of claims.

**Hand a foreign notification back through the outbox.** Rejected. On a shared queue each
hop reaches the right scope with probability ~1/N, so a job costs O(N) claims and outbox
rows, and with no worker in the scope it ping-pongs forever.

**Per-worker queues.** Still rejected for ADR-0003's reasons. Per-address queues are
different in kind: they never cross an authorization boundary, keep work stealing within
an address, where it is allowed, and their number is bounded by scopes × queues, not by
processes.

**Record when a declined notification was last presented, and skip re-notifying a job whose
own notification is still being declined** (the audit's first-named option). Rejected as
the primary bound: it misses the cases where nobody presents anything (no worker for the
scope, a saturated fleet), and because claims are fungible a job's own notification is
regularly spent on another job, leaving the job re-notified while the leftover lives on, a
live set that grows with throughput. It also adds a write to the claim's decline path.

**Exponential backoff of re-notification per generation, or broker retention near a few
intervals.** Rejected as primary bounds. Backoff slows growth to logarithmic but does not
bound it and slows the repair of real loss by the same factor; a short retention deletes
held notifications that are a stuck job's only wakeup. Retention remains a reasonable
operator safeguard and is inherited from the base queue.

**Release every stale notification, including on `CAPACITY_EXHAUSTED`.** Rejected; see §2.

**Deterministic discovery at registration (a probe claim when a worker starts).** Not taken
here. It would make a newly joined worker's first claim independent of the broker, but it
is a new worker behavior with its own lost-response and attempt-budget questions, and I4
already bounds rediscovery by the re-notification interval.

## Consequences

**Positive.** The two pinned defects are inverted into positive tests: a scope-B worker
cannot reach scope A's notification, which survives until A's worker claims it, and one
unclaimable job holds exactly one live message (`[1 1 1 1]`, one event) and still completes
once a capable worker joins. A tenant's dispatch latency no longer depends on other
tenants' worker counts. Re-notification traffic is proportional to claimable stranded work.

**Negative and operational.**

- One broker queue per address, created by the application. On AWS the outbox needs
  `sqs:GetQueueUrl`, `sqs:CreateQueue` and `sqs:SendMessage` on
  `arn:aws:sqs:<region>:<account>:<base>-*`, and the workers `sqs:GetQueueUrl`,
  `sqs:CreateQueue`, `sqs:ReceiveMessage` and `sqs:DeleteMessage` there; both keep
  `sqs:GetQueueUrl` and `sqs:GetQueueAttributes` on the base queue (the template and
  readiness). PR #31's Terraform granted SQS actions on the base queue alone, which would
  refuse every address; PR #36, stacked on it, adds exactly these grants. A queue is never
  deleted by TaskForge or by `terraform destroy`: the address queues are outside Terraform
  state, and retiring them is an operator action. Their number is bounded by scopes that
  have jobs × configured queues (`jobs.queue` references `queues`), plus one stray, empty
  queue per misspelt `TASKFORGE_WORKER_QUEUE` a worker is started with.
- Only the attributes listed in §1 are copied: not a redrive policy (deliberately), not a
  queue access policy, not tags. An organization that requires tags on create would refuse
  `CreateQueue`. A base queue on SSE-KMS would make the creating roles need the key's
  `kms:GenerateDataKey` and `kms:Decrypt`. None of this was exercised on AWS SQS; every
  routing test ran against ElasticMQ.
- An upgrade is not seamless for messages already on the old shared queue: new workers do
  not read it. Each such job is repaired by re-notification within one interval, into its
  address's queue, as soon as a new worker that could claim it is registered. Old workers
  that keep reading the shared queue receive `FOREIGN_NOTIFICATION` for other scopes'
  events (they treat an unknown outcome as a claim error and do not acknowledge, which is
  safe), and old workers ignore the new `scope` field. Order: apply migration 0019 and roll
  the api first, since a new worker needs the `scope` registration now returns and exits
  when it is absent; then roll the outbox and the workers together. Rolling the code back
  leaves 0019 in place, which the old code ignores, apart from its stranded-job scan losing
  the index 0019 replaced; the old, ungated re-notification re-advertises each `QUEUED` job
  onto the base queue within one interval, and the address queues stay behind, unread.
- A deferral costs a job up to one more re-notification interval after a session that could
  claim it appears, when its earlier notification was genuinely lost. The gate's extra reads
  were not measured under a large stranded backlog; [CURRENT_STATE.md](../CURRENT_STATE.md)
  records that the scan uses its index at 20,000 queued jobs and that the recorded benchmark,
  which has no stranded backlog, met the same targets.

**Known and accepted.**

- Within one address, workers with different capabilities still share a queue, so a held
  notification reaches a minority-capable worker at random, about once per visibility
  timeout per (live message ÷ consumers). Before this change the unbounded accumulation
  incidentally shortened that wait; it is now bounded by I4 instead.
- The gate does not know which TaskForge queue a session polls: a session of the right
  group configured for another queue of the same group counts as able to claim, so a job
  in a queue that only such sessions could serve is still re-notified once per interval.
  Recording a session's queue at registration would close it and is a schema and API change
  left for an owner decision.
- A digest collision between two addresses (about 2⁻⁶⁴ per pair) puts them on one queue;
  the foreign guard keeps that from deleting anything, at the cost of redelivery delays.
