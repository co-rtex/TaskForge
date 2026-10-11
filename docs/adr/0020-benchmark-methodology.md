# ADR-0020: Benchmark methodology: PostgreSQL's clock, the shipped defaults, and a CI that only smokes

- **Status:** Accepted, partially superseded by
  [ADR-0023](0023-the-throughput-tolerance-derived-from-the-headline-record.md). Two
  sentences of its "What Met and MISSED mean" section are replaced: the one
  justifying the 1% throughput tolerance ("completes it to within the jobs in flight
  at each end of the window"), which ADR-0023 derives from the headline record's own
  figures, and the one saying the shortfall "is judged against the rate that was
  actually offered", which the code does not do (it floors measured and offered at
  990 each). The 1%, the Met and MISSED rules as coded, every recorded verdict and
  every other section stand. Also partially superseded by
  [ADR-0024](0024-the-50s-recovery-is-a-held-notification-at-the-tail.md), which
  replaces one limitation, "Recovery with the shipped lease is bounded below by the
  lease", with a fuller one: a kill whose lease expires after the last submission was
  observed to add up to about one broker visibility timeout (30 s by default) in every
  reproduction; a second hold is not excluded, and re-notification after
  `TASKFORGE_SCHEDULER_RENOTIFY_AFTER` (60 s shipped) is the backstop. The definition of
  recovery is unchanged.
- **Date:** 2026-10-03

## Context

[PROJECT_SPEC.md](../PROJECT_SPEC.md) §7 lists five performance targets and says
that none may appear as an achieved result until "a reproducible run records them
under [docs/](..) with SHA, environment, command, and limitations".
[AGENTS.md](../../AGENTS.md) §9 repeats it: a target is a target until a
reproducible run measures it. M8A is that run.

A benchmark is a set of definitions before it is a set of numbers, and each
definition has a cheap version that is wrong in a way that flatters the system.
"Latency" measured with the load generator's clock mixes two machines' clocks
and counts the generator's own scheduling. "Throughput" measured as jobs
submitted per minute measures the generator. A kill that lands on an idle worker
measures nothing, and a recovery average hides the slowest recovery. A benchmark
re-run until it passes is a lottery. This record fixes the definitions, and the
rules for what a number may be filed under, before any run, so that none of them
can be chosen after seeing a result.

The owner fixed the workload and the scope of M8A: `demo.sleep` for 50 ms,
offered at 1,000 jobs a minute to 12 workers of 4 slots each, no search for a
saturation point; the recorded run on the owner's machine, with CI running a
smoke only and never recording numbers; the headline run on the shipped default
timings, with at most one second, labelled run on a tuned profile.

## Decision

### One clock

**Every instant a measurement or a window boundary uses is PostgreSQL's:** a
column written from `now()` or `clock_timestamp()`, or a `SELECT
clock_timestamp()` the harness reads at the moment it crosses a boundary. The
harness's own clock paces the offered load and bounds waits. It stamps nothing.
Submission and claim are both PostgreSQL times, so a latency is a difference of
two readings of one clock. Nothing is converted, and clock skew between the
harness and the database cannot enter a figure.

The harness checks that rule is not being undermined from outside. A Docker
Desktop VM's clock is stepped when the host sleeps, and every lease and heartbeat
is judged on that clock. A watchdog compares the elapsed time on PostgreSQL's
clock with the elapsed time on the harness's monotonic clock every five seconds
and aborts the run, naming the cause, if they differ by more than two seconds. It
also aborts if a worker the injector did not kill exits. The steady window is
checked the same way at the end. A run the watchdog aborted is not recorded.

### The workload

`POST /v1/jobs` over `net/http`, never one CLI process per job, with
`job_type=demo.sleep`, `payload={"duration_ms":50}`, `max_attempts=3`,
`timeout_seconds=30`, and an `Idempotency-Key` per job. A transport error, a 5xx
or a 429 is retried with the same key, which makes the retry safe; the retries are
counted and recorded. The generator is **open-loop**: submission *i* is due at
`start + i x interval`, whatever time earlier ones fired, and a late slot fires at
once. A slow system cannot lower the load it is offered. The pacer's worst
lateness is recorded.

### Throughput

A run has a warm-up (at least 30 s), a steady window (at least 5 min), and a
drain. The window is **half-open**, `[Start, End)`, in PostgreSQL time: a job
finishing exactly at `Start` is in the window, one finishing exactly at `End` is
in the drain, and adjacent phases count every instant once. All three durations
are recorded.

- **Throughput** is the number of this run's jobs that reached `SUCCEEDED` with a
  finish time inside the window, per minute of PostgreSQL time. A job's finish
  time is the `finished_at` of its `SUCCEEDED` attempt, the instant the
  transaction wrote when it set the job `SUCCEEDED`, so a job retried after an
  abandonment counts once, at its success.
- **Offered rate** is the number of jobs whose `jobs.created_at` is inside the
  window, per minute. It is recorded beside throughput, because a system cannot
  be shown to sustain a load it was not offered.

### Dispatch latency

For every immediate job (`scheduled_at IS NULL`) created in the window,
**attempt 1's `job_attempts.created_at` minus the job's `jobs.created_at`.**
Delayed jobs are excluded: their wait is the schedule the caller asked for. The
report is p50, p95, p99, max and n.

- `job_attempts.started_at` is the worker's start report and is not dispatch. A
  query that read it would add the worker's startup and the report's round trip
  to a latency that has already ended; a test seeds a started time 50 ms later
  than the claim and asserts the claim is what is returned.
- A job in the window that has no attempt 1 yet is **counted as unclaimed and not
  dropped**. Averaging only the claimed jobs would remove the slowest observations
  of all. Any unclaimed job makes the target MISSED.
- **A caveat the definition carries, stated here and in every record.**
  `job_attempts.created_at` is `now()`, the *start* of the claim transaction. A
  lock wait inside that transaction is not in it, so the figure can understate
  dispatch. `jobs.created_at` is the start of the submit transaction, which
  overstates by that transaction's duration. The two do not cancel. A
  **supplementary** figure is therefore reported beside it, from
  `leases.acquired_at`, which the claim samples with `clock_timestamp()` after all
  its locks. It is never the target's figure and never replaces it.

### Percentiles

One method, **nearest-rank**: the value at 1-based rank `ceil(p/100 x n)` of the
sorted sample, computed in integers. It returns a value that was observed. The
p95 of 20 values is the 19th. The median of an even count is the lower middle
value, not the average of the two. A floating-point `ceil(0.95 x n)` is wrong by
one rank for some `n` (60 gives 58 against a true 57), so the rank is not computed
that way, and a test pins the cases.

### Fault injection and recovery

10,000 jobs at the same offered rate, so the submission phase is ten minutes. On
a **seeded** schedule of at least 20 kills (24 in a recorded run) laid across that
phase, the harness SIGKILLs a worker's whole process group and starts it again
under **the same worker name**: the same logical worker, a new process boot, which
is what a supervisor produces. Registration replaces the prior boot's session and
leaves its leases to expire. The seed, the kill count and each kill's PostgreSQL
time are recorded. The same seed gives the same schedule: times and victim draws.

- **Which worker.** With 50 ms jobs at 1,000 a minute a worker is holding an
  attempt for a few percent of the time, so a kill on a random live worker would
  almost always land on an idle one and measure nothing. The victim is drawn from
  the workers that hold an attempt when the kill is due, waiting up to two seconds
  for one, and from all live workers only if none ever does. Each kill records
  which it was. The candidates are sorted, so the same state gives the same
  victim. The consequence is stated in every record: recovery is measured for the
  attempts a kill hit, and kills prefer an occupied worker.
- **Recovery** is, for each attempt bound to the killed session that ends
  `ABANDONED`, the **replacement attempt's `job_attempts.created_at` minus the
  PostgreSQL time read immediately before the SIGKILL** (the session is read, then
  the clock, then the signal). It is the replacement's claim, not its start report.
  An abandoned attempt with no replacement is counted as never replaced and not
  left out. A replacement claimed before the kill is a problem, not a negative
  number.
- **Completion** is the share of the jobs in the run's scope that are `SUCCEEDED`
  at a fixed deadline: five minutes after the last submission, or sooner if every
  job is terminal. Every other final status and every dead-letter reason is
  reported with its count.

### What Met and MISSED mean

Fixed here, before any run. Each verdict is printed beside the unrounded figure
and the settings that govern it.

| Target | Met when |
| --- | --- |
| Sustained throughput, 1,000 jobs/min | Throughput **and** offered rate are each at least 990 jobs/min, and no submission failed. |
| Dispatch latency, p95 < 500 ms | p95 is strictly under 500 ms **and** no job in the window was left unclaimed. |
| Fault-injection volume, 10,000 jobs | At least 10,000 jobs exist in the run's scope. |
| Completion, at least 99.7% | `SUCCEEDED x 1000 >= jobs x 997`, in integers. |
| Worker-failure recovery, < 30 s | The **maximum** recovery is strictly under 30 s, every abandoned attempt has a replacement, and at least one attempt was abandoned. |

The throughput row carries the only tolerance, 1%, and it is one-sided. A paced
run offers the target to within one submission and completes it to within the jobs
in flight at each end of the window, so 999.8 against 1,000 says nothing about the
system; a rate above target is never excused, and the shortfall is judged against
the rate that was actually offered. Recovery reads the maximum because "recovery <
30 s" is not met by a median that is, while one attempt took longer. A target with
nothing to compare (no kill abandoned an attempt) is `NOT MEASURED`, which is
neither.

### The headline run uses the shipped defaults

`stack.ShippedTimings` spells out every value `internal/config` defaults to, so a
record can list them, and a test loads the services' configuration with an empty
environment and fails if any differs. The headline run uses it unchanged. **If a
target is missed it is recorded as MISSED**, next to the settings that decide it
(the lease for recovery, the outbox poll for dispatch), and is not re-run to
improve it. At most one second run is made, on `stack.TunedTimings`, a shorter
lease, tighter liveness and faster scans, whose nine changed values a test pins.
It is labelled TUNED in its title, its filename and its first paragraph, and it
never replaces the headline run. A run repeated because of an environment fault is
reported with the fault.

### What a recorded run refuses to do

A recorded run (`--record`, which requires `throughput faults` and every fixed
definition) refuses to start unless:

- `git status --porcelain` is empty (untracked files count);
- every binary in `bin/` carries `vcs.revision` equal to `HEAD` and
  `vcs.modified=false` (`go build` stamps both), so the SHA in the record is a
  fact about the binaries that ran and a stale `bin/` cannot be measured under a
  new name;
- the workers, slots, rate, warm-up, window, job count, kill count and deadline
  are the fixed values or longer.

**Every mode, recorded or not, also refuses while another TaskForge service binary
is running on the machine.** The outbox publisher, the scheduler and the reconciler
claim rows from tables every scope shares, so a stray one takes part in the work of
the stack being measured: it publishes another run's notifications to a queue
nobody reads, and the scheduler then re-notifies the jobs it stranded. This was
learned from a harness crash that left a whole stack behind: five later smoke runs
and eight integration tests failed in ways that looked like a slow broker, and a
wrong diagnosis cost a commit that was then reverted. The check matches the
executable and not the text of a command line, and it names each pid. It sees this
machine only; a stack on another host pointed at the same database is invisible to
it.

It refuses to write a record whose run is invalid, and it never overwrites a
record (`O_EXCL`). A record contains the commit, the clean tree, the command, the
environment (CPU, cores, memory, OS, **power source**, Go, Docker and the Docker
VM's resources, the PostgreSQL version, the compose images), every `TASKFORGE_*`
timing in effect, the seed, the results with a verdict for each target, and a
Limitations section. The `.json` holds aggregates and the fault timeline, never
per-job rows.

### CI smokes and never records

`make bench-smoke` runs a small throughput run and a small fault run on the
short-lease profile, with a kill that must hit an attempt. It asserts that the
harness produced valid measurements, with each assertion tested by feeding it a
broken result, and it records nothing. CI runs it in the integration job and does
not run `make bench`.

## Alternatives considered

- **Timestamps from the harness's clock.** Simpler, and wrong: it mixes the
  generator's clock with the database's, counts the generator's scheduling, and
  makes the figure depend on skew between two machines' clocks.
- **Latency to `started_at`.** It is what a trace shows, and it is not dispatch.
- **Latency from `leases.acquired_at` as the target's figure.** Stricter, and
  arguably the better definition. The owner fixed the claim's `created_at`;
  the lease figure is reported beside it so the difference is visible instead of
  silent.
- **Interpolated percentiles.** They report values nobody observed, and the
  method used must be one a reader can recompute by counting.
- **Scraping `/metrics` histograms.** They measure inside one process and cannot
  express "submission to claim" across three.
- **Recording from CI.** A shared runner's CPU is not the project's, varies run to
  run, and would put a number in the repository that nothing about the project
  explains.
- **A tuned profile as the headline.** A benchmark of a configuration nobody
  ships is a statement about the configuration. The shipped one is what a reader
  gets.
- **Searching for the saturation point.** A different experiment, with a
  different question; the owner scoped M8A to the target load.
- **Killing a uniformly random worker.** More literal, and measures almost
  nothing at this workload. The occupied-worker preference is recorded in every
  result it affects.
- **A 99% completion margin, or a median recovery.** Rejected as roundings in the
  system's favor.
- **One long run instead of two.** Fault injection disturbs the throughput being
  measured, so the two are separate runs, each with a stack and a scope of its
  own.

## Consequences

- A result is a statement about **one machine, once**: no variance, no
  confidence interval. It is reproducible (the commit, the command, the seed and
  the settings are recorded) and not generalizable. A reader should expect the
  same shape on a different machine and not the same numbers.
- The load generator, five services, up to 12 workers and the Docker VM share one
  host, and PostgreSQL's data is on tmpfs in that VM, so the results exclude the
  network and disk a deployment has. The broker is ElasticMQ, not SQS.
- The benchmark's workload is the `demo.sleep` handler that
  [ADR-0019](0019-demo-handlers-are-trusted-built-ins.md) registers in every
  worker. M8C decides whether a deployment gates the demo handlers; if it does,
  the benchmark must be run against a worker that registers them.
- Recovery with the shipped lease is bounded below by the lease: an attempt is
  abandoned when its lease expires, so a kill soon after a claim cannot be
  recovered in less than most of the lease. This is a property of the
  configuration the record prints next to the verdict, not of the harness.
- The harness aborts runs it can show are untrustworthy. It cannot detect
  everything: CPU throttling on battery power, thermal limits and contention from
  other processes are not detected, and the record states the power source and
  Low Power Mode so a reader can judge. A laptop with its lid closed sleeps whatever
  `caffeinate` holds, and each wake steps the Docker VM's clock; the watchdog turns
  that into an aborted run and not a wrong number.
- `scripts/readdb` is outside `scripts/internal` so `tests/integration` can import
  the queries it tests. The harness and the test share one copy of each query.
