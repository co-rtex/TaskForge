# ADR-0019: The demonstration handlers are trusted built-ins, always registered

- **Status:** Accepted
- **Date:** 2026-10-02

## Context

[PROJECT_SPEC.md](../PROJECT_SPEC.md) §5 makes two commands part of V1's success
criteria: `make demo`, which runs real jobs that succeed, retry, and dead-letter,
and `make demo-failure`, which shows a worker crash, lease expiry, attempt
abandonment, reassignment, and eventual success. §6 and
[AGENTS.md](../../AGENTS.md) §10 draw the boundary that governs how: TaskForge
executes only trusted handlers registered in its own binary, and never an
uploaded script, a shell command, or a dynamic plugin.

Until M7B the only registered handler was `demo.echo`, which returns at once and
never fails. A demonstration of failure needs a job that fails on request and a
job that is still running when its worker is killed or frozen. The integration
suite has those, injected through the registry seam, but a handler injected by a
test cannot be shown running in the binary that ships. A job whose type no worker
declared is never claimed at all
([ADR-0006](0006-session-bound-worker-eligibility.md)): it stays `QUEUED`,
silently, which is what the M5E live-stack run observed when it tried.

So the question M7A left open is whether handlers that fail and stall become
production handlers in `taskforge-worker`, and what bounds them if they do. It is
a decision about the trust boundary, not about a Make target.

## Decision

`demo.sleep` and `demo.fail` are registered in the production `taskforge-worker`,
unconditionally, in the same list as `demo.echo`
(`cmd/taskforge-worker/handlers.go`, `registerTrustedHandlers`). There is no
environment gate and no separate build. A unit test pins the exact set
`[demo.echo demo.fail demo.sleep]`, so a handler cannot be added to or dropped
from the worker without a reviewer seeing a one-line change to a list of three.

### The bounds

Both handlers decode their payload strictly: an unknown field, a missing field, a
value of the wrong type, a non-integer where an integer is required, and anything
after the object are all refused. A refused payload is a `Permanent`
`invalid_payload` failure, so it dead-letters at once instead of spending an
attempt budget on a payload that will be refused identically every time.

- **`demo.sleep`** takes exactly `{"duration_ms": n}` with
  `1 <= n <= jobs.MaxTimeoutSeconds * 1000`. The ceiling is derived from the
  constant that bounds a job's own `timeout_seconds`, not copied from it: a sleep
  longer than any attempt budget could never finish. It waits on a timer inside a
  `select` with the context, so cancellation, the attempt deadline, and loss of
  lease authority each end it at once. It returns `{"slept_ms": n}`.
- **`demo.fail`** takes exactly `{"class": "retryable"}` or
  `{"class": "permanent"}`. Its error code is the fixed `demo_failure`, and its
  message is a fixed string per class. The caller chooses a class and nothing
  else: no part of a payload is ever echoed into a failure's code or message, and
  both codes match the `error_code` pattern in `api/openapi.yaml`, which a test
  reads from the document.

### Abuse analysis

What a key holder can do with these handlers, and what it cannot.

- **No new code can run.** The handlers are compiled in. This ADR changes which
  trusted code is compiled into the binary, not the rule that only such code
  runs.
- **A payload is data.** Each handler decodes it into a struct of one field and
  acts on nothing else. Neither reaches a shell, the filesystem, or the network,
  and neither calls anything beyond the standard library's JSON decoder and a
  timer.
- **A key holder can make its own scope's jobs sleep or fail, and that is all.**
  A job belongs to the scope of the key that submitted it, and is claimed only by
  a worker registered under a worker key for that same scope (`Claim` selects on
  `j.scope`, `internal/workers/store.go`). What a handler writes is its own
  attempt's outcome: a result or a bounded failure.
- **Capacity is held for at most the job's `timeout_seconds`.** A sleeping
  attempt occupies one lease, and so one of its worker's `concurrency_limit`
  slots and one of its queue's `max_concurrency`, until it returns, is canceled,
  or reaches its deadline. At the deadline the worker cancels the handler
  (`internal/worker/runner.go:495`) and reports nothing, and the reconciler
  records `TIMED_OUT` and closes the lease, `RELEASED` or `EXPIRED`
  (`internal/workers/timeout.go:121`). A long-running handler holds capacity
  identically.
- **One resource is shared across scopes, and it should be said plainly.** A
  queue's `max_concurrency` counts `ACTIVE` leases across every scope
  (`internal/workers/store.go`, the `activeForQueue` count has no scope filter).
  A scope whose own workers run long `demo.sleep` jobs can therefore occupy
  slots of that shared limit for up to `timeout_seconds` each. This is not new
  machinery: any long job from any handler does the same. What is new is that
  before M7B no production handler could hold a slot for long. It needs a
  worker registered for the caller's own scope, which is not something a key
  holder obtains by submitting a job. V1 has no multi-tenancy
  ([PROJECT_SPEC.md](../PROJECT_SPEC.md) §9), so this is recorded, not mitigated.
- **A key holder can fill its own scope's dead-letter queue with deliberate
  failures.** That is its own scope's data, listed only under its own key.

### What the runner does with each way a sleep ends

`demo.sleep` returns `ctx.Err()` when its context ends, and the runner does not
classify that return value. It classifies the cause it recorded on the context,
and a recorded cause outranks whatever the handler returned
(`internal/worker/runner.go:569-604`). Cancellation by an operator reports a
fenced cancellation acknowledgment (`:570`). The attempt deadline reports
nothing, and only reconciliation records `TIMED_OUT` (`:576`). Loss of lease
authority reports nothing and lets crash recovery happen (`:586`). Worker
shutdown reports nothing (`:594`). Only a handler error with none of those causes
is reported as a failure (`:601`). None of this was changed.

## Alternatives considered

- **An environment gate** (a variable that has to be set for the worker to
  declare the demonstration handlers). Rejected. It adds a configuration surface
  and a second behavior of the shipped binary, and the demonstrations would then
  prove "the binary with the flag" rather than the binary that ships. Its failure
  mode is also the worst available: a worker without the flag simply never claims
  a `demo.sleep` job, which sits `QUEUED` with nothing to say why. It buys no
  safety either way: default-on is the decision above, and default-off only moves
  the decision into every deployment's configuration.
- **A separate build** (a build tag, or a second worker binary that carries the
  handlers). Rejected. The demonstrations would run a binary that is not the one
  `make build` ships, the two would have to be kept in step, and the sentence
  "TaskForge executes only handlers compiled into its binary" would become "into
  one of its binaries". The demonstrations exist to show the real system.
- **Test-only handlers, as in the integration suite.** Sufficient to prove the
  behavior, which `tests/integration` already does, and unable to demonstrate it:
  a demonstration of the binaries that ship cannot depend on a handler only a
  test process has.
- **A scripted worker that speaks the control protocol.** Rejected as a
  fabrication. It would show the control plane reacting to calls the demonstration
  itself made, not a worker doing anything, and TaskForge's rule is that nothing
  mocked is presented as real.
- **A handler that fails and then succeeds**, to show a retry recovering. Not
  built. It needs the attempt number in `Execution`, which is a change to the
  handler contract, and the retry is instead shown by a retryable failure spending
  its whole budget, which exercises the same scheduling and ends in the DLQ.

## Consequences

- The production worker declares three job types, so `Registry.Types()` holds
  three and the worker-emitted `job_type` metric label's ceiling is four, the
  three types plus the single `other` bucket that bounds everything else.
- **M8's deployment ships these handlers.** Anything deployed from this binary
  can run a `demo.sleep` or `demo.fail` job for a key holder whose scope has
  workers. If that is not wanted there, the gate this ADR declined can be added
  with M8's deployment configuration, at which point the cost above (a job type
  that silently stays `QUEUED`) is a choice made with a real deployment in view.
- The queue-wide capacity sharing above is a property to revisit if V1's
  single-operator posture changes. It is a property of the queue limit rather than
  of these handlers.
- Any handler added after this one is a visible change to a pinned list and, by
  this record's standard, needs the same analysis of what its payload can reach.
