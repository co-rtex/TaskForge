# ADR-0024: The 50 s recovery is a notification held at the tail, by intended behavior

- **Status:** Accepted
- **Date:** 2026-10-10

## Context

The headline benchmark record ([2026-10-05-d796722](../benchmarks/2026-10-05-d796722.md))
measured sixteen worker-failure recoveries. Fourteen took 31.96 to 33.98 s, the 30 s
lease plus a reconciler pass plus an outbox pass. The last kill's two attempts took
50.04 s each, and nothing in the record could say why. M8D3 was the investigation
([ROADMAP.md](../ROADMAP.md)): reproduce it, then name it as explained, a defect, or
not reproduced.

[ADR-0020](0020-benchmark-methodology.md) defines recovery as the replacement's claim
minus the kill, and says among its limitations that recovery with the shipped lease
is "bounded below by the lease". It says nothing about what can add to it. This record
replaces that limitation with a fuller one, because the investigation found that a
recovery at the end of a run can include up to one broker visibility timeout. It does
not change what recovery measures.

## Decision

### What the 50.04 s was

M8D3's probe (`go run ./scripts/bench recovery-probe`) split each recovery into five
segments PostgreSQL records: S1 the lease running out, S2 the reconciler, S3 inside the
reconciler's transaction, S4 the outbox publisher, and S5 broker delivery and the
claim. Under steady load and on an idle system no recovery exceeded the lease plus
5 s. With the kill placed as kill 23 was, so that lease expiry falls after the last
submission, most trials did, and every excess was in S5. The figures, the trial counts
and the controlled variations are in [CURRENT_STATE.md](../CURRENT_STATE.md)'s M8D3
section. The mechanism is three documented decisions acting together:

1. **A restarted worker is charged for its dead boot's leases.** A killed worker is
   restarted under the same name, and its dead boot's active leases keep counting
   against the logical worker's capacity until they expire and are reconciled
   ([ADR-0006](0006-session-bound-worker-eligibility.md)). The new boot polls with all
   of its local slots, so it can receive a notification while it has no logical
   capacity, and the claim is `CAPACITY_EXHAUSTED`.
2. **That notification is not acknowledged.** A worker acknowledges a message only when
   the claim succeeded or no eligible job remains ([ADR-0003](0003-pull-based-claim-with-broker-notification.md)
   step 5), so the broker keeps it invisible for its visibility timeout, 30 s.
3. **A claim takes the oldest eligible job, not the job its notification names**
   (ADR-0003's ordering). While load continues, the held message's job is claimed by a
   later notification, and the shortfall moves to the newest job. When submission
   ends, one job is left with no live notification. When the killed worker's lease is
   reconciled, the recovered job is requeued with a fresh notification, and that
   notification's claim takes the older stranded job instead. The recovered job is
   claimed only when the held message returns.

So the recovered job waits until the held message's receive plus the visibility
timeout. With the receive about 20 s after the kill, near the end of submission, that
is about 50 s after the kill, which is what the record shows.

### It is explained, not a defect

Each of the three is a recorded decision with its reason, and the outcome they produce
is bounded: the held message comes back after one visibility timeout, and
[ADR-0011](0011-notification-generations-and-bounded-renotification.md)'s
re-notification would repair a job left with no message at all. In every trial the
recovered job was claimed, by the held message, when it returned. M8D3 changes no
production code. Whether a recovery that can wait
one extra visibility timeout at the tail is acceptable, and whether to change any of
the three decisions, is the owner's decision, not this record's.

### What it means for the benchmark

- The definition of recovery is unchanged, and so is the recorded verdict (MISSED; it
  would be missed at 32 s too).
- ADR-0020's limitation "Recovery with the shipped lease is bounded below by the lease"
  is replaced by: **recovery with the shipped lease is bounded below by the lease, and
  a kill whose lease expires after the last submission can add up to one broker
  visibility timeout (30 s by default) to it, by the mechanism above.** A kill made
  while load continues does not, because later notifications claim the stranded job.
- The mechanism is pinned in `tests/integration/recovery_outlier_test.go`, through the
  real store and with no clock, so a change to any of the three decisions that removes
  it will be seen.

## Alternatives considered

**Record it only in CURRENT_STATE.** Rejected: the benchmark's methodology says what a
recovery figure can contain, and a reader of ADR-0020 would otherwise still read the
lease as the only thing that shapes it.

**Call it a defect and propose a fix.** Not this record's to make. A worker could poll
only with its logical capacity, acknowledge a `CAPACITY_EXHAUSTED` message, or claim
the job its notification names; each changes the control plane's claim and
acknowledgement contract, and each is the owner's decision.

## Consequences

- [ADR-0020](0020-benchmark-methodology.md)'s status names this record and the one
  limitation it replaces. Every other section of ADR-0020 stands, and ADR-0023 is
  unaffected.
- The committed benchmark records are not edited.
- The finding is one machine's, against ElasticMQ rather than SQS; the probe is a
  reproduction and not the original run, whose database is gone.
