# ADR-0023: The throughput tolerance, derived from the headline record

- **Status:** Accepted
- **Date:** 2026-10-06

## Context

[ADR-0020](0020-benchmark-methodology.md) fixed, before the first benchmark run, the
one tolerance in the rules: the sustained-throughput target is Met if the run's
completions **and** its offered rate are each at least 99% of 1,000 jobs a minute,
990, and no submission failed. It justified the 1% in a sentence: a paced run
"completes it to within the jobs in flight at each end of the window, so 999.8
against 1,000 says nothing about the system". It did not say how many jobs that is.
The review of M8A asked for the figure, and for [PROJECT_SPEC.md](../PROJECT_SPEC.md)
§7 to say what the verdict does and does not claim.

ADR-0020 is an accepted record, so it is not edited
([the ADR rules](README.md)). This record is the later one that replaces that
sentence: it states what the window-boundary effect is, derives its size from the
headline record's own figures, and says what the recorded verdict then means.

The definitions come from ADR-0020 and `scripts/bench`. The steady window is the
half-open span `[window_start, window_end)` of PostgreSQL time. The **offered** rate
is the jobs *created* in the window per minute. The **throughput** is the jobs that
*finished* (reached `SUCCEEDED`) in the window per minute. They are two counts over
the same window of two different events of each job, so they differ only by what is
in flight at the window's edges: a job created just before `window_end` is offered
but has not finished, and one created just before `window_start` finishes inside
the window without having been offered in it.

## Decision

### The window-boundary bound

The most the edge effect can cost is the jobs in flight at one edge, taking the
other edge as costing nothing (the start edge only ever adds). A job is in flight
from its creation to its finish: its dispatch latency (creation to the claim, which
the record measures) plus the 50 ms it sleeps. The number of jobs in flight is the
offered rate times that time, taken at the **99th percentile**, which is
conservative: the number that is in flight on average is the offered rate times the
*mean* time, which is lower.

From the headline run, [docs/benchmarks/2026-10-05-d796722.json](../benchmarks/2026-10-05-d796722.json):

| Figure | Value | Source |
| --- | --- | --- |
| Window | 299.98913 s = 4.999819 min | `steady_window_seconds` |
| Jobs created in the window | 5,001 | `jobs_created_in_window` |
| Jobs finished in the window | 4,999 | `jobs_succeeded_in_window` |
| Offered rate λ | 5,001 / 299.98913 s = **16.6706 jobs/s** (1000.236 a minute) | |
| Dispatch latency, p99 | 1,009.416 ms | `dispatch_latency_ms.p99` (n = 5,001) |
| Job duration | 50 ms | the fixed workload |

```
time in flight    W = 1.009416 s + 0.050 s                  = 1.059416 s
jobs in flight    N = λ x W = 16.6706 jobs/s x 1.059416 s   = 17.66 jobs
over the window     = 17.66 jobs / 4.999819 min             = 3.53 jobs/min
                                                            = 0.353% of 1,000
```

Taking the maximum latency (1,046.601 ms) instead of the p99 gives 18.28 jobs,
3.66 jobs a minute, 0.366%. So the window-boundary effect is **at most about 18
jobs, 3.5 jobs a minute, 0.35%** for this run.

### What the record shows against it

The run created 5,001 jobs in the window and finished 4,999: **2 jobs short, 0.400
jobs a minute (1000.236 offered, 999.836 measured), 0.040%.** That is 8.8 times
smaller than the bound (17.66 jobs against 2). The shortfall is within what the
window edges alone explain, so it says nothing about the system, and **the recorded
verdict stands: Met, in the sense that the system kept pace with the load it was
offered.** The offered rate was itself 0.024% above the target, so the run offered
what it was meant to.

### What the verdict does not claim

It does not claim headroom. The offered load was fixed at 1,000 a minute and no
saturation point was searched for, so "kept pace" is a statement about that load and
not about how much more the system could have taken. [PROJECT_SPEC.md](../PROJECT_SPEC.md)
§7's cell now says that: *kept pace with the offered load; headroom not measured*.

### The coded tolerance is looser than the bound, and is left alone

`JudgeThroughput` floors both measured and offered at 1,000 − 1% = **990 jobs a
minute**. That is 10 jobs a minute below the target, 50 jobs over the window, 2.83
times the 17.66-job bound. A run that measured 990 would be Met under the code and
would be a real shortfall, about 32 jobs more than the window edges can explain.

It is **not tightened here.** `JudgeThroughput` and the coded 1% are unchanged, and
so is the recorded verdict, because the committed record and the code that judged it
must stay consistent: a rule tightened now would be a rule the committed record was
not judged by. **Tightening it waits for the next recorded run,** which would be
judged by the tightened rule from the start. This record does not say when that is.

### A discrepancy this record found and does not fix

ADR-0020 says the shortfall is "judged against the rate that was actually offered".
The code does not do that: it compares measured and offered, each, with the
absolute floor of 990, and never measured with offered. For the headline run the two
agree (offered 1000.24 and measured 999.84 both clear 990, and the measured is 0.4
below the offered), so no verdict differs, but a run that offered 1,000 and measured 991 would
be Met by the code and a 9-job shortfall by the prose. The derivation above is of
the shortfall the prose describes. Which of the two a tightened rule should
implement is part of the decision that waits for the next recorded run.

## Alternatives considered

**Tighten `JudgeThroughput` now, to the derived bound.** It would make the code say
what the derivation says. It would also mean the committed record's verdict was
produced by a rule that no longer exists, and the first run judged by the new rule
would be one whose result nobody had seen. The owner decided this is documentation
only. Rejected for this change; it is the next recorded run's to do.

**Drop the tolerance and require at least 1,000.** The edge effect can leave a few
jobs on either side of the window, so a system that kept up exactly can measure
below 1,000 and would be recorded MISSED for it. The headline run itself measured
999.84. Rejected.

**Count the cohort: the jobs created in the window that eventually finish.** It
removes the edge effect entirely, since every offered job is then counted
once. It also changes the metric ADR-0020 defined and both records report, so the
two committed records would no longer be comparable with it. Not now.

## Consequences

- **PROJECT_SPEC.md §7's throughput cell** reads: Met: kept pace with the offered load
  (999.83 of 1000.23/min offered, within the window-boundary bound derived in this
  record); headroom not measured. Its other cells are unchanged.
- **ADR-0020's status** records the sentence it loses: its justification of the 1% and
  its statement that the shortfall is judged against the offered rate. Its decision,
  the 1%, and every other section stand.
- **The bound is one run on one machine.** It uses that run's p99 latency, and says
  nothing about another machine, another load or a run with a different latency. A
  slower system has more jobs in flight at an edge and a larger bound.
- **The bound is conservative, not exact.** The p99 stands in for a mean, and the
  start edge's gain is taken as zero. The true edge effect is smaller, which is why
  a 2-job shortfall sits so far inside it.
- **Nothing in code or in either committed record changes.** The two records are not
  edited, `scripts/bench/targets.go` is unchanged, and the next recorded run is where a
  tightened rule, and the choice between the two readings above, would be made.
