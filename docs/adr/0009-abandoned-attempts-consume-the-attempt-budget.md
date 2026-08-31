# ADR-0009: An abandoned attempt consumes the attempt budget

- **Status:** Accepted
- **Date:** 2026-08-31

## Context

M3 reconciliation abandons the attempt behind an expired lease and must then
decide what happens to its job. Three canonical statements constrain that
decision and, read carelessly, appear to conflict:

- [PROJECT_SPEC.md](../PROJECT_SPEC.md) §4: `max_attempts` counts **total**
  attempts, including the first.
- [ARCHITECTURE.md](../ARCHITECTURE.md) §11: M3 reconciliation "transitions the
  job according to retry budget".
- [ROADMAP.md](../ROADMAP.md): M4 owns retry policy, failure classification,
  backoff, `RETRY_WAIT`, the logical DLQ, and replay.

Two readings fail outright. If an abandoned attempt did not count, a job could be
abandoned without limit and `max_attempts` would mean nothing for the failure
mode the milestone exists to handle. If it counted but reconciliation always
requeued, a job whose budget was exhausted would return to `QUEUED` — and the
claim predicate already refuses a job whose attempt count has reached
`max_attempts`:

```sql
AND (SELECT count(*) FROM job_attempts a WHERE a.job_id = j.id) < j.max_attempts
```

That job would sit `QUEUED` forever, claimable by nobody, with a `work.available`
notification advertising work that cannot be taken. Leaving it `RUNNING` with no
lease is no better: it is stranded in a state no live process will ever change.

So M3 cannot avoid the question. It can only answer it narrowly or badly.

## Decision

**An `ABANDONED` attempt counts toward `max_attempts`,** exactly like any other
attempt. Reconciliation then does the smallest thing that keeps the job's state
coherent:

- **Budget remaining** → the job returns to `QUEUED`, `available_at` and
  `updated_at` are set from the same server sample, and a **new**
  `work.available` outbox event is written in the same transaction. This is crash
  recovery, not retry: there is no backoff, no jitter, no `RETRY_WAIT`, and no
  failure classification. The job was interrupted, not judged.
- **Budget exhausted** → the job becomes `DEAD_LETTERED`. That is the minimal
  terminal consequence needed to avoid a permanently unclaimable nonterminal job,
  and nothing more: no `dlq_entries` table, no failure classes, no retry delay,
  no DLQ listing, no replay, no operator retry.

The recovery event gets a fresh id. The original outbox event id is the globally
unique claim identity and has already been consumed
([ADR-0007](0007-globally-idempotent-notification-claims.md)); reusing it would
make the replacement claim collide with the attempt just abandoned.

**Everything else about failure remains M4.** A handler that returns an error
still reports no outcome; its lease simply expires into this same path. Timeout
outcomes, `TIMED_OUT`, `RETRY_WAIT`, cancellation, the DLQ API, and replay are
untouched by this record.

## Alternatives considered

**Do not count abandoned attempts.** Rejected: it contradicts "total attempts,
including the first" and makes a crash-looping worker able to consume a job
forever without ever exhausting it.

**Requeue regardless of budget and let M4 sort it out.** Rejected: it knowingly
creates a `QUEUED` job that the existing claim predicate can never claim, plus a
notification for work nobody can take. Shipping a known-stranded state and
calling it someone else's milestone is not deferral, it is a defect.

**Leave the job `RUNNING` with no active lease when the budget is exhausted.**
Rejected for the same reason: a nonterminal state that no live process will ever
change again is a leak, and it would make queue and worker capacity accounting
harder to reason about rather than easier.

**Build the full M4 DLQ in M3 so the terminal case is complete.** Rejected: it
expands the milestone well past its boundary. `DEAD_LETTERED` is already in the
job status `CHECK` constraint and already documented as the terminal state for an
exhausted budget, so using it requires no new schema and no new API.

## Consequences

**Positive.** A crashed worker's job is recovered immediately, without waiting for
retry machinery that does not exist yet. Every job reachable by reconciliation
ends in a state that is either claimable or terminal — never stranded. The
attempt budget means the same thing for a crash as for any other outcome. No
speculative M4 table, endpoint, or policy is created.

**Negative.** `DEAD_LETTERED` becomes reachable in M3, one milestone before the
DLQ that reads it. An operator can observe a dead-lettered job through
`GET /v1/jobs/{job_id}` but cannot list or replay it until M5 and M4 respectively.
This is a visible product gap and is recorded as such in
[CURRENT_STATE.md](../CURRENT_STATE.md).

**Known and accepted.** A job dead-lettered this way carries no failure
classification and no stored error, because M3 implements neither. Its attempt
history shows `ABANDONED`, which is an accurate account of what happened: the
work was interrupted and its budget ran out. M4 adds richer classification for
attempts that fail rather than vanish.
