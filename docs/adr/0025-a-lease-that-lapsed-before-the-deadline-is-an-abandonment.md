# ADR-0025: A lease that lapsed before the deadline is an abandonment

- **Status:** Accepted
- **Date:** 2026-10-14
- **Amends:** [ADR-0010](0010-durable-outcome-identity-and-terminal-precedence.md),
  "Precedence is fixed and stated", item 2, on the expired-lease path only.

## Context

ADR-0010 fixes the precedence among a killed or stalled attempt's possible outcomes:
cancellation, then a timeout if the attempt's persisted `timeout_at` is due, then
ADR-0009's abandonment. Item 2 applies on both reconciler paths, the due-timeout
sweep and the expired-lease scan. On the expired-lease path that makes the outcome
depend on **when the reconciler arrives**. A lease that lapsed before the deadline is
`ABANDONED` if a pass reaches it before `timeout_at` and `TIMED_OUT` if a pass reaches it
at or after.

M8D4 measured what that does to a killed worker
([CURRENT_STATE.md](../CURRENT_STATE.md), "M8D4"). A `RUNNING` attempt whose worker
died before its first renewal has `expires_at = acquired_at + lease` and
`timeout_at = started_at + timeout_seconds`. With `timeout_seconds` equal to the lease,
`timeout_at − expires_at` is the attempt's claim→start latency. That window measured
27–92 ms on an idle fleet (condition D) and 218–225 ms under load (condition A), against
a 2 s reconciler poll:

| Series (M8D4, 2026-10-14) | `RUNNING` attempts at the kill | `ABANDONED` | `TIMED_OUT` |
| --- | --- | --- | --- |
| D, `timeout_seconds` 30 (= lease), never renewed | 40 | 0 | **40**, all on the expired-lease path, resolved 1.92–2.01 s after `expires_at` |
| D, `timeout_seconds` 300 | 40 | **40** | 0 |
| A (shipped load), `timeout_seconds` 30 | 2 | 0 | 2, on the expired-lease path |
| E, renewed, `timeout_seconds` 25 | 40 | 0 | 40, by the timeout sweep with the lease live |

D's jobs slept 5 s and could not have run out of time; they were killed. The same kill
reads as a crash at `timeout_seconds` 300 and as a timeout at 30, decided by the
reconciler's phase against a window of tens or hundreds of milliseconds and not by
anything about the attempt. `readdb.Recoveries`, which ADR-0020's recovery figure is
read from, counts `ABANDONED` attempts only, so such kills also drop out of the
benchmark's recovery figure.

M8D4's decision memo set out three options: (a) keep deadline-first; (b) a lease that
expired before `timeout_at` is `ABANDONED`; (c) a session that went stale before
`timeout_at` is `ABANDONED`. **The owner chose (b) on 2026-10-14**, with four decisions
this record follows:

- **Q1**, the tie: an attempt is `ABANDONED` only when `expires_at` is *strictly* before
  `timeout_at`.
- **Q2**, the report paths: unchanged.
- **Q3**: no recorded run; the committed records stay byte-identical.
- **Q4**: the milestone is M8D5, between M8D4 and M8C.

## Decision

### The rule

On the expired-lease path, under the authority locks and against the one post-lock
`clock_timestamp()` sample, after the existing revalidation that the lease is `ACTIVE`
and has lapsed (sample ≥ `expires_at`) and that the attempt is still `LEASED` or
`RUNNING`:

1. **Cancellation**, if the job is `CANCEL_REQUESTED`. Unchanged.
2. **Timeout**, only if `timeout_at` is set, `expires_at ≥ timeout_at`, and `timeout_at`
   is due (at or before the sample).
3. **Abandonment** otherwise, through ADR-0009 unchanged. This now includes a lease
   that expired strictly before its deadline even when that deadline is due by the time
   the pass reaches it.

**The tie.** `expires_at == timeout_at` is `TIMED_OUT`. The lease did not lapse
*before* the deadline: it was usable up to the instant the deadline arrived. This keeps
the convention ADR-0010 already uses for a deadline equal to the sample, that the
deadline's own instant belongs to the timeout.

The implementation is one predicate, `fenceState.timedOutBeforeLapse`
(`internal/workers/reconcile.go:339`), used only by the expired-lease switch
(`reconcile.go:266`) in place of `timedOut`. Nothing else changes:

| Caller of `timedOut()` | What it decides | Changed? |
| --- | --- | --- |
| `internal/workers/timeout.go:97` | the due-timeout sweep; takes only an attempt with a usable lease | no |
| `internal/workers/heartbeat.go:222` | renewal refuses a due deadline | no |
| `internal/workers/store.go:653` | `Succeed` answers `attempt_timed_out` | no |
| `internal/workers/outcome.go:103` | `Fail` answers `attempt_timed_out` | no |

A `LEASED` attempt that never started has no `timeout_at` and is abandoned, as before.

### Why the rule depends only on stored instants

Once a lease has lapsed, neither instant the rule compares can change:

- `expires_at` is written at claim (`internal/workers/store.go:422`) and by renewal
  (`internal/workers/heartbeat.go:281`). Renewal refuses a lease that is no longer usable
  (`heartbeat.go:209`) and an attempt whose deadline is due (`heartbeat.go:222`).
- `timeout_at` is written only by Start (`internal/workers/store.go:577`), which requires
  a usable lease (`store.go:526`). So a deadline exists only if Start committed before
  the lease lapsed.

On this path the sample is at or after `expires_at` (`reconcile.go:215`). So
`expires_at ≥ timeout_at` already makes the deadline due, and the `timedOut` conjunct
adds nothing; it stays in the predicate for clarity. The verdict for a given attempt
is therefore the same from every reconciler replica, on every pass, however late. Under
ADR-0010's item 2 it depended on the sample.

The timeout sweep is consistent with the rule without any change. It takes an attempt
only while its lease is usable (sample < `expires_at`) and its deadline is due
(`timeout_at` ≤ sample), so every attempt it records `TIMED_OUT` has
`expires_at > timeout_at`.

### Report paths are unchanged (Q2)

`Succeed` and `Fail` still check the deadline before lease usability. Once `timeout_at`
is due they answer `attempt_timed_out` and record nothing (`store.go:653`,
`outcome.go:103`). That holds even for an attempt whose lease lapsed first, which the
reconciler will now record `ABANDONED`. There is no API, OpenAPI or SDK change.

The answer stays true as a statement about the report: the attempt's execution budget
had run out, so nothing it reports can commit. It is not a statement of the attempt's
recorded outcome, which is the reconciler's alone. A worker can receive
`attempt_timed_out` for an attempt whose history then says `ABANDONED`. That pairing
is accepted rather than closed, because closing it would change the report contract.
The report records nothing, so attempt history still holds exactly one outcome.

### ADR-0010's wording

ADR-0010 said the timeout applies if `timeout_at` "has passed". The code has always
treated a deadline equal to the post-lock sample as due (`!serverNow.Before(timeoutAt)`,
pinned by M8D4's case (e)). As part of this amendment ADR-0010 now says "is due (at or
before the post-lock sample)", and its status names the clause this record replaces.

## The worker paths, and the risk this accepts

M8D4's worker-path analysis ([CURRENT_STATE.md](../CURRENT_STATE.md), "Worker-path
analysis (for option (b))") traced every way a live worker's lease can stop being
renewed. With the control plane reachable and the session current, a live worker renews
until its handler returns. The rule leaves those attempts unchanged:

- **A cooperative handler** is cancelled at most 1 s plus a round trip before
  `timeout_at`. The last renewal was at most one interval earlier, so on the shipped
  values the lease expires about 19 s after the deadline. The configuration requires
  `3 × renew ≤ lease` (`internal/config/config.go:238`), so `lease − renew ≥ ⅔ lease` in
  any valid configuration. The deadline comes first, and the timeout sweep takes the
  attempt with its lease live. If the pass is late and finds the lease lapsed,
  `expires_at > timeout_at` still makes it `TIMED_OUT`. A test pins this: case (h),
  `expires_at = timeout_at + 19 s`, both past.
- **An uncooperative handler** keeps renewal running until the server refuses it at
  the deadline. The lease is live at `timeout_at`: a timeout, as before.

A live worker's lease lapses before `timeout_at` in these cases:

- the renewal path itself fails: the control plane is unreachable for at least
  `lease − renew`, a renewal is definitively rejected, or a confirmation comes too late;
- the worker shuts down;
- the session is lost;
- the handler has already returned and its report failed.

In each the work was interrupted or is over, and `ABANDONED` (ADR-0009: interrupted, not
judged) is the truthful outcome. It is now recorded however late the reconciler
arrives.

**The accepted risk: self-starvation.** A handler that starves its own process or
host can stop renewals from being sent or answered for `lease − renew` (20 s on the
shipped values) while its deadline is still ahead. CPU or memory exhaustion, or a
paused or swapping VM, would do it. Each such attempt is now `ABANDONED` and requeued
at once, with no backoff and no failure detail. Before, it was `TIMED_OUT` whenever the
pass came after the deadline. This is accepted because it is bounded twice:

- **By `max_attempts`.** An abandoned attempt consumes budget (ADR-0009). The job ends
  `DEAD_LETTERED` with reason `ATTEMPTS_EXHAUSTED` and a final `ABANDONED` attempt after
  exactly `max_attempts` attempts. It cannot loop forever.
- **By time.** Each round costs at least `lease − renew` of lost renewals before the
  lease can lapse.

`TestPrecedence_AHealthyWorkerThatKeepsLosingItsLeaseExhaustsItsBudget` pins the
bound with a healthy session through `max_attempts` rounds. How often the path
happens is not measured. An uncooperative handler also keeps running after its
authority is gone, so its replacement can overlap the fenced execution sooner than
before. That already happens whenever the deadline is further away than the lease.

## Upgrade and failure cases

**Mixed versions during a rollover.** Reconcilers on the old and new rule can run at
once. No migration is needed: the rule reads columns every version already writes.
Both versions decide under the same row locks, in the same lock order, and both
revalidate that the attempt is still executing. Whichever transaction commits first
records the attempt's one outcome under its own rule; the other then finds the attempt
no longer executing and skips it. `TestReconcile_ConcurrentReconcilersProduceOneRepair`
and `TestTimeout_ConcurrentReconcilersProduceOneOutcome` pin the one-outcome guarantee.
The two rules disagree only about an attempt whose lease lapsed strictly before a
deadline that is due by the time the pass arrives. During a rollover such an attempt
gets one outcome, `TIMED_OUT` or `ABANDONED`, according to which version won the lock.

**A reconciler outage.** Under ADR-0010's item 2, every executing attempt reached
after a long outage was `TIMED_OUT` once its deadline had passed. Under this rule the
outage no longer decides:

- **A dead worker's attempts**, whose leases lapsed before their deadlines, end
  `ABANDONED` and are requeued at once.
- **A lease a live worker kept renewing past its deadline** ends `TIMED_OUT`, with
  backoff.

A long outage can therefore release a burst of immediate requeues. Each is an
interruption, and each consumes budget.

**Rollback.** A rollback restores the old rule for decisions made afterwards. Outcomes
already recorded are immutable history and are not rewritten in either direction.

## Consequences for the recovery figure and the records (Q3)

- **The recovery figure.** ADR-0020's recovery figure is the replacement's claim
  minus the kill, over `ABANDONED` attempts (`readdb.Recoveries`). A kill of a
  never-renewed `RUNNING` attempt whose `timeout_seconds` is close to the lease was
  `TIMED_OUT` and dropped from the figure. It is now `ABANDONED` and counted. **Future
  recorded runs will count these kills as recoveries**; the recorded runs do not. Their
  replacement no longer waits for a backoff and a promotion: M8D4 measured about 2 s
  between the two (D median 33.98 s against D-300's 31.96 s).
- **The committed records.** The records in `docs/benchmarks/` predate this rule. They
  stay byte-identical, and M8D5 makes no recorded run. The `TIMED_OUT` kills that M8A,
  M8D3 and M8D4 observed describe the rule before this one.
- **The recovery probe.** The probe's per-attempt precedence identity encoded
  ADR-0010's rule (`TIMED_OUT` ⇔ `released_at ≥ timeout_at`). It now checks this one:
  `TIMED_OUT` ⇔ `released_at ≥ timeout_at` and `expires_at ≥ timeout_at`. An outcome the
  old rule recorded for a lease that lapsed first now fails it.

## Alternatives considered

**(a) Keep deadline-first.** Rejected by the owner. It is safe, since a timing-out job
can never skip backoff. But M8D4 showed that it attributes kills to timeouts according
to the reconciler's phase, and that the recovery figure silently drops them.

**(c) A session that went stale before `timeout_at` is `ABANDONED`.** Rejected. It would
decide an attempt's class from another table, `worker_sessions`. The answer would also
depend on where in its run the job was killed, quantized by the heartbeat interval,
the stale threshold and the reconciler poll. It would not move condition E, and it
carries the same partition risk as (b), decided by heartbeats rather than by the
attempt's own lease.

**Compare a derived expiry: `renewed_at + lease`, or `acquired_at`.** Rejected.
`expires_at` is the instant authority actually ended, written by the transaction that
granted it. `renewed_at + lease` agrees with it only while the configured lease
duration has not changed since the renewal, and `acquired_at + lease` only for a lease
never renewed. Mutation M3 in M8D5's evidence shows the tests telling them apart.

**The tie as `ABANDONED`.** Rejected (Q1). At the tie the lease was usable right up to
the deadline. Calling that an abandonment would also break the convention that the
deadline's own instant is due.

**Move the new rule ahead of cancellation.** Rejected. Cancellation already won durably,
and ADR-0010's reasons for putting it first are unchanged.

## Consequences

**Positive.** A lapsed lease's outcome is a function of two stored instants, not of when
a reconciler happened to run. A killed worker's attempt reads as what happened to it,
an interruption, and is counted in the recovery figure from now on. Every timeout
found while a lease is live, and every lease renewed past its deadline, is exactly as
before. There is no schema, API, OpenAPI, worker or migration change.

**Negative.** A handler that starves its own renewals while its deadline is ahead is
requeued with no backoff each round, bounded by `max_attempts`. A worker can be told
`attempt_timed_out` for an attempt recorded `ABANDONED` (Q2). The benchmark's recovery
figure will not be directly comparable between records made before and after this rule.

**Known and accepted.** The self-starvation path is named, not measured. The
committed records and M8D4's figures describe the earlier rule.
