# ADR-0010: Durable outcome identity and terminal-outcome precedence

- **Status:** Accepted
- **Date:** 2026-09-01

## Context

M4 gives an attempt four ways to end that M3 did not have: a worker reports a
failure, the attempt outlives its execution budget, cancellation wins, or — as
before — the worker vanishes. Each of them can be true at the same instant as
another, and each of them is reported or detected through a channel that can
fail ambiguously.

Two distinct problems fall out of that.

**Ambiguity.** Failure reporting is a network call, so a worker whose report
commits but whose response is lost must retry. Under a naive implementation that
retry is a second failure: it consumes another place in the attempt budget,
draws fresh jitter and therefore lands on a different retry instant, and — at
the end of the budget — creates a second dead-letter entry for one job. That is
the same class of defect [ADR-0008](0008-fenced-idempotent-lease-renewal.md)
found in renewal, in a place where the consequences are worse: renewal's damage
was a doubled window, this one's is a job that silently loses an attempt it
never had.

The existing five-part fence does not help. It answers *who* is reporting, not
*which report this is*.

**Precedence.** More than one of these conditions is routinely true at once. A
handler that stalls past its deadline and then stops renewing has both a due
deadline and a lapsed lease. A job canceled while its handler is running long
has both a cancellation and a due deadline. A worker that returns an error a
moment after its budget expired has both a handler failure and a timeout.
Picking whichever condition the code happens to check first produces a state
machine nobody can reason about, and in one case produces a real defect:
recording a genuine timeout as `ABANDONED` requeues it immediately with no
backoff and no failure detail, so a job whose handler reliably takes too long
loops through its entire attempt budget at full speed and its history says it
was interrupted rather than that it ran out of time.

## Decision

### Every terminal outcome a client requests carries a retained identity

`job_attempts.outcome_request_id` holds the client-generated identity of the
request that produced the attempt's recorded outcome — a failure report or a
cooperative cancellation acknowledgment. A partial unique index makes it unique
for the **lifetime of attempt history**, not merely while it is in force.

That is deliberately stronger than ADR-0008's renewal identity, and the
difference is not an inconsistency. A renewal identity is superseded by the next
generation and stops being stored; an outcome identity is the permanent record
of one terminal decision, so nothing ever releases it.

The decision is computed exactly once and persisted **on the attempt**:
classification, safe code and message, the chosen delay, and the resulting
`retry_at`. An exact replay returns those stored values unchanged. Reusing the
identity for a different attempt, or replaying it against its own attempt with a
different classification, code, or message, is `outcome_conflict` — a stable
domain error, never a leaked uniqueness violation.

The values returned to the caller are read back from the `UPDATE`'s `RETURNING`
clause rather than from what Go computed, and the retry instant is derived from
the millisecond-truncated delay. Both exist so a first response and its own
replay cannot disagree by rounding.

Server-detected outcomes — timeout and abandonment — carry no identity, because
nobody requested them. Their idempotency comes from the attempt no longer being
`LEASED` or `RUNNING` once the transition commits.

### Precedence is fixed and stated

Under the authority locks, against one post-lock `clock_timestamp()` sample:

1. **Cancellation**, if the job is `CANCEL_REQUESTED`. It already won durably;
   the only thing left is to finalize it. It never retries, never dead-letters,
   and never consumes attempt budget.
2. **Timeout**, if the attempt's persisted `timeout_at` has passed. This holds
   whether the timeout was found by the dedicated due-timeout scan or by the
   expired-lease scan, so a lease that lapsed around a deadline that had already
   passed is a timeout, not an abandonment.
3. **Abandonment**, otherwise — M3's path, unchanged
   ([ADR-0009](0009-abandoned-attempts-consume-the-attempt-budget.md)).

A worker-reported failure observed at or after the deadline is refused with
`attempt_timed_out` and records nothing, so a handler's own classification can
never overwrite a server-authoritative one.

`Succeed` and `Fail` check the deadline **before** lease usability. When a
timeout wins it also releases the lease, so both conditions are true afterwards;
reporting "lease expired" would tell the worker it lost authority when what
actually happened is that its budget ran out.

### `EXHAUSTED` is a job-level reason, not an attempt status

The final attempt keeps its truthful status — `FAILED`, `TIMED_OUT`, or
`ABANDONED`. Why the *job* ended is recorded once, on its dead-letter entry, as
`PERMANENT_FAILURE` or `ATTEMPTS_EXHAUSTED`.

### A worker may declare only two classifications

`RETRYABLE` and `PERMANENT`, through a typed handler error carrying a stable
lowercase code and an optional message the handler asserts is safe. `TIMED_OUT`,
`CANCELED`, and `ABANDONED` are server-authoritative and are rejected with `422`
if a worker presents one.

A plain Go error, a wrapped dependency error, and a recovered panic all become a
generic retryable failure with a generic message. Their raw text is not stored,
not returned, and not logged — that text is the one place payload fragments,
credentials, driver output, and stack traces reliably appear.

Bounds are enforced in Go and again by `CHECK` constraints: the code is a
lowercase token of at most 64 bytes, the message at most 512 bytes with no
control characters and no line breaks.

## Alternatives considered

**Fence the failure report with the existing five identifiers and nothing
else.** Rejected: it is exactly the renewal bug in a worse place. The fence
proves who is asking, so an ambiguous retry becomes a second failure.

**Make the failure report idempotent by comparing the stored classification
instead of an identity.** Tempting, and it needs no new column. Rejected
because two genuinely different failures of the same attempt are
indistinguishable under it, and because the retry instant is drawn from a random
source — a second report with the same classification would still have to either
recompute the delay (a different answer) or return a stored one it cannot prove
it produced.

**Let a worker report `TIMED_OUT` itself.** Rejected. A worker that has stalled,
been paused, or lost its clock is exactly the worker least able to judge whether
it timed out, and an uncooperative handler that keeps running must be fenced out
of committing anything at all rather than trusted to describe itself.

**Store the raw handler error and sanitize it on the way out.** Rejected: it
puts the unbounded, unsafe value in the database, where the next reader — a
dashboard, a log shipper, an export — is one code path away from leaking it.
Sanitizing at the boundary the value crosses is the only place it works.

**Check lease usability before the deadline.** Rejected after the fact: it
compiles, it is not wrong, and it produces a misleading answer in the one case
that matters most. See the precedence section.

## Consequences

**Positive.** An ambiguous outcome report is safe to retry, and retrying it is
provably free. Two workers, a worker and a reconciler, or two reconcilers
contending on one attempt produce exactly one terminal outcome. Attempt history
records what actually happened rather than which code path noticed first.
Failure detail is bounded and safe by construction, in the database as well as
in Go. An operator reading a dead-lettered job sees both the truthful attempt
status and the job-level reason.

**Negative.** One more column pair and one more lifetime-unique index on
`job_attempts`. A worker must hold an outcome identity across retries, so
outcome reporting is stateful in the way renewal already is — and generating a
fresh identity on retry is precisely the bug this prevents, so the OpenAPI 503
contract for both operations says so explicitly rather than merely recommending
it.

**Known and accepted.** The identity is retained forever, so `job_attempts`
carries one more UUID per terminal outcome for as long as attempt history is
kept. That is the cost of making the replay window unbounded rather than
one-generation-wide as renewal's is, and it is the right trade for a decision
that is permanent.

Nothing here changes ADR-0009. An abandoned attempt still consumes the attempt
budget and still requeues immediately with no backoff; only the budget
arithmetic is shared with retry, through a policy that returns a zero delay for
that class precisely so the two cannot drift.
