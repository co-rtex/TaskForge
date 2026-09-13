# ADR-0012: The logical DLQ, and replay as an idempotent new job

- **Status:** Accepted
- **Date:** 2026-09-01

## Context

[ADR-0009](0009-abandoned-attempts-consume-the-attempt-budget.md) made
`DEAD_LETTERED` reachable in M3 and recorded the consequence as a visible
product gap: an operator could see one dead-lettered job through
`GET /v1/jobs/{job_id}` but could not list them and could not act on any of
them. M4 owns closing that gap, and closing it raises three questions the
roadmap deliberately left open.

**What does "replay" do to the original job?** Reliability invariant 2 says a
terminal job never returns to a non-terminal state. A job's attempts, leases,
failure metadata, and the record of why it died are the durable answer to "what
happened here", and that answer is what an operator is looking at when they
decide to replay.

**How do "retry" and "replay" relate?** `docs/PROJECT_SPEC.md` §4 lists both
`POST /v1/jobs/{job_id}/retry` and `POST /v1/dlq/{job_id}/replay`.
[ARCHITECTURE.md](../ARCHITECTURE.md) §10 said the relationship would be defined
explicitly rather than implemented ambiguously, and pointed here.

**What stops one operator click from becoming three jobs?** Replay is a button
in a dashboard and a command in a CLI. Double-clicks, retries after a timeout,
and two on-call engineers reacting to the same alert are the normal case, not
the exceptional one.

## Decision

### One authoritative table, one insertion helper

`dlq_entries` holds one row per dead-lettered job, and `job_id` is `UNIQUE`.
Every path that sets a job to `DEAD_LETTERED` — permanent failure, exhausted
retryable failure, exhausted timeout, and ADR-0009's exhausted abandonment —
inserts through the same transactional helper, in the same transaction as the
transition. Four separate `INSERT`s would be four chances for the reason
vocabulary, the scope binding, or the timestamp source to drift.

An entry records scope, queue, job id, the terminal attempt, the reason, and the
instant the job dead-lettered. There is exactly one timestamp: it is both the
operator-visible "when" and the pagination key, so there is no second one to
disagree with it.

Existing M3 dead-lettered jobs are backfilled as `ATTEMPTS_EXHAUSTED`, linked to
their final attempt. They are real jobs an operator must be able to act on, and
leaving them invisible after the upgrade would be a worse gap than the one
ADR-0009 accepted.

Listing joins the immutable job and its terminal attempt for bounded metadata —
queue, type, priority, budget, attempt number and status, failure class, code,
and safe message — and deliberately **no payload**. A list endpoint that
returned payloads would let one request pull an arbitrary amount of user data;
a single job is still readable through `GET /v1/jobs/{job_id}`.

Pagination is keyset on `(created_at DESC, id DESC)`, not `OFFSET`. Two jobs can
dead-letter in the same instant — two reconciler replicas finishing two
exhausted attempts, for example — so ordering by timestamp alone is not a total
order, and `OFFSET` over a table that is still growing skips and repeats rows.
The cursor is opaque: it is a position, not an authorization, and encoding it
keeps the ordering columns out of the public contract.

### Replay creates a new job and never resurrects the old one

The original job, its attempts, its leases, its failure metadata, and its
dead-letter entry are left exactly as they are. A distinct new job is created,
linked back through `jobs.replayed_from_job_id`.

The replacement copies the original's queue, job type, canonical payload,
priority, attempt budget, timeout, and capabilities — and nothing else. It is
immediately eligible: `scheduled_at` is null and `available_at` is server time,
because a replay is an operator saying "run this now". Carrying the original's
schedule forward would delay it by an instant that has already passed, and
carrying its retry state forward would make the operator wait out a backoff for
an attempt that never happens.

It gets a fresh attempt budget and a fresh notification generation, and its
`work.available` event is written in the same transaction as the job and the
replay identity.

### Operator retry IS DLQ replay

`POST /v1/jobs/{job_id}/retry` and `POST /v1/dlq/{job_id}/replay` are two routes
onto one service with one idempotency namespace. Both require an
`Idempotency-Key`; both accept only a `DEAD_LETTERED` job that has a logical DLQ
entry. The same identity presented through either route returns the same
replacement job.

Two routes exist because operators reach for both names, not because there are
two operations. Implementing them separately is how they would eventually answer
differently for the same request.

### The database enforces idempotency, not application code

`dlq_replays` is keyed by `(scope, original_job_id, idempotency_key)` — a
composite primary key, in exactly the shape `idempotency_records` already uses
for submission. Concurrent identical requests both insert a job; one wins the
identity insert, and the loser rolls back, discarding its own job and leaving no
orphan, then reads the winner's replacement. First success returns `201`; an
exact replay returns the same replacement with `200`.

There is no fingerprint to compare, unlike submission: everything about the
replacement is derived from the original job, so one `(scope, original, key)`
can only ever have meant one request. A retry after an ambiguous response
therefore always returns the committed replacement rather than a conflict.

**Different keys deliberately create different replacement jobs.** That is the
feature, not a leak: an operator replaying the same failure twice on purpose —
after fixing a dependency, then after fixing a second one — gets two jobs, and
the DLQ entry's replay count says so.

## Alternatives considered

**Reset the original job to `QUEUED` and clear its attempts.** The obvious
implementation, and the reason this record exists. Rejected: it violates
reliability invariant 2, destroys the failure history an operator is looking at
when they decide to replay, and makes "how many times has this failed" a
question nothing can answer.

**Keep the original job and append a new attempt to it.** Better, but still
rejected. `max_attempts` counts total attempts including the first
([PROJECT_SPEC.md](../PROJECT_SPEC.md) §4), so a replay would either exhaust
immediately or require a second, hidden budget. It also makes attempt history
span two operator decisions with nothing distinguishing them.

**Make retry and replay separate operations with separate semantics.** Rejected:
nobody could state the difference without inventing one, and two implementations
of one intent drift.

**Key replay idempotency on the original job id alone.** Rejected: it makes a
job replayable exactly once, ever, which is wrong for the ordinary operational
case of retrying after each of two fixes.

**Store the DLQ as a view over `jobs WHERE status = 'DEAD_LETTERED'`.** Genuinely
appealing — no table, no backfill, no chance of divergence. Rejected because the
reason and the terminal attempt are not derivable: `ATTEMPTS_EXHAUSTED` and
`PERMANENT_FAILURE` are different operator situations, and a view would have to
re-derive which by inspecting attempt history on every read. A real row also
gives the unique constraint that makes a duplicate entry impossible rather than
merely unlikely.

**Put the payload in the DLQ listing.** Rejected: unbounded user data in a list
endpoint.

## Consequences

**Positive.** The gap ADR-0009 accepted is closed, including for jobs that
dead-lettered before this milestone. Terminal history is genuinely immutable, so
"what happened to this job" has one answer forever. One operator intent produces
one replacement job however many times the request is sent, enforced by
PostgreSQL. Replay linkage is queryable in both directions: forward through
`replayed_from_job_id`, backward through the entry's replay count.

**Negative.** Two more tables. A replayed job's lineage is a chain rather than a
single record, so "show me everything about this work" means following
`replayed_from_job_id` — a real cost paid to keep each link immutable. Every
future path that reaches `DEAD_LETTERED` must call the shared helper; the unique
constraint turns forgetting it into a loud failure rather than a silent gap, but
it is still a rule to remember.

**Known and accepted.** The DLQ listing has no filtering beyond scope and no
sorting beyond newest-first. Operator-facing search, filtering by reason or job
type, and bulk replay are dashboard concerns and belong with the dashboard
(M6), not here. Nothing in this record forecloses them.
