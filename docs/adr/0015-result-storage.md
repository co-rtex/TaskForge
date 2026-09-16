# ADR-0015: Result storage, inline and object-backed, with attempt-scoped object keys

- **Status:** Accepted
- **Date:** 2026-09-16

## Context

`docs/PROJECT_SPEC.md` item 13 is the last piece of M5's original scope this
repository had not yet built: "Retrieve small and large results." Every
trusted handler already produces a `json.RawMessage` result
(`internal/worker/handler.go`'s `Handler.Execute` signature, unchanged since
M2), and `internal/worker/runner.go` has discarded it on every successful
attempt since then. This milestone stops discarding it, stores it durably,
and serves it back.

Two things this milestone depends on are already in place. M5A and M5B mean
the public surface and the worker-control surface each authenticate with
their own scoped, revocable credential, so a result-retrieval endpoint has
somewhere real to check a caller's scope against, rather than retrofitting
authorization onto a route built before it existed. And ADR-0005's reasoning
for ElasticMQ — real broker semantics locally, no AWS account, no cost —
applies identically to object storage: a large result needs a real
S3-compatible endpoint locally, not a stub.

A first question this record answers explicitly, because a plan that only
implied the answer would be a plan the acceptance criterion could not
actually be checked against: **small and large result storage are one
milestone, not two.** ROADMAP.md's M5C acceptance criterion is "small and
large results round-trip; the threshold is explicit and tested on both sides
of the boundary" — a single, joint property. A milestone split the way
M5A/M5B split authentication (each half independently, completely testable
on its own) is not available here: "the threshold is tested on both sides"
cannot be honestly claimed by either half alone. So M5C ships both together.

## Decision

### One table, keyed by job, not by attempt

`migrations/0016_results.sql` adds `results`: `job_id UUID PRIMARY KEY
REFERENCES jobs (id)`, a `NOT NULL` `attempt_id` for operator traceability,
a denormalized `scope` (matching `job_attempts.scope` and `leases.scope`'s
existing precedent, with a composite foreign key to `jobs (id, scope)`
reusing the unique constraint migration 0011 already added), a `location`
column constrained to `inline` or `object`, and a `results_location_shape`
CHECK tying `location` to exactly which of `inline_body` or
`object_bucket`/`object_key`/`checksum_sha256` is populated. Zero new
columns on `job_attempts`.

Keying by `job_id` rather than `attempt_id` is deliberate and is what makes
`Succeed`'s replay branch safe by construction — see below. It is also
correct: `internal/workers.Store.Succeed` is the only writer, and it only
ever reaches the insert from its non-replay commit path, which requires the
job to still be `RUNNING`. At most one attempt can ever move a job out of
`RUNNING` into `SUCCEEDED`, so at most one row is ever written per job.

### `internal/objectstore` and `internal/results`: a strict package boundary

`internal/objectstore` is a thin, domain-free `aws-sdk-go-v2/service/s3`
client: `Put`, `Get`, `EnsureBucket`, nothing that knows about jobs,
attempts, or scopes. `internal/results` owns the `results` table:
`Classify(body, threshold) Location`, `InsertResultTx` (mirroring
`outbox.InsertWorkAvailableTx` and `lifecycle.InsertDLQEntryTx`'s shape —
`pgx.Tx` in, no pool-based insert to accidentally call instead), and
`Store.Get`.

**`internal/workers` depends on `internal/results` for the insert helper and
never depends on `internal/objectstore` directly.** `Store.Succeed` calls
`results.InsertResultTx` inside its own transaction; it never uploads
anything anywhere. The upload already happened, worker-side, before
`Succeed` was ever called — see "Upload before report" below. This mirrors
the M5B precedent of `internal/workers` and `internal/workerauth` never
depending on each other, with `internal/api` as the one place two
independent answers meet: here, `internal/api.Server` is where a
`results.Store` metadata row and an `internal/objectstore.Client`'s bytes
meet, for the one handler that needs both.

### `Succeed` gains one parameter; its replay branch is untouched code, not a stronger constraint

`internal/workers.Store.Succeed(ctx, scope, fence, result *ResultRef) error`
is the one fenced-method signature change this milestone makes. `ResultRef`
(`internal/workers/types.go`) carries exactly what `InsertResultTx` needs:
`Location`, `Inline`, `Bucket`, `Key`, `SizeBytes`, `Checksum`. A `nil`
`ResultRef` means the job succeeded with nothing to record.

The replay branch —
`if state.jobStatus == "SUCCEEDED" && state.attemptStatus == AttemptSucceeded && state.leaseStatus == LeaseCompleted { return tx.Commit(ctx) }`
— is unchanged, and unchanged is the whole point: it returns before `result`
is ever looked at, so a replayed `Succeed` call cannot attempt a second
`results` insert. That is not merely what the table's `PRIMARY KEY (job_id)`
would reject if reached; the code path does not reach it.
`TestSucceedReplay_DoesNotInsertASecondResult` proves both: a mutation that
duplicated the insert into the replay branch made it fail with a real
`duplicate key value violates unique constraint "results_pkey"`, confirming
the schema backstop, before being reverted.

### Upload before report

`internal/worker/runner.go` classifies a handler's output and, for a result
at or above the configured threshold, uploads it to the object store
*before* `Succeed` is ever called — never inside `Succeed`'s own PostgreSQL
transaction. Two reasons, not one:

- **Locking.** `Succeed`'s transaction takes the established
  `queue → worker session → job → attempt → lease` authority lock order
  (`lockAuthorityRows`). Holding those locks across a slow or unreachable
  network call to the object store would block every other fenced operation
  on the same queue for as long as that call took.
- **Failure mode reuse.** If the upload fails even after the bounded retry
  helper every other worker-control call already uses (`Runner.retry`), the
  worker reports nothing at all and abandons the attempt — indistinguishable
  from any other reason a worker fails to report in time.
  `docs/PROJECT_SPEC.md` §2 already accepts "a handler may run more than
  once" as a guarantee, and ADR-0009's crash-recovery path already handles
  an unreported attempt completely: lease lapse, reconciliation, requeue
  while budget remains. A failed upload needed zero new control-plane
  failure-mode logic to handle correctly, because it is not a new class of
  failure.

The object key is deterministic: `results/<scope>/<job_id>/<attempt_id>`.
See "Known limitation" below for the one consequence that key shape accepts.

### The retrieval endpoint is core public surface, proxied, not redirected

`GET /v1/jobs/{job_id}/result` is authenticated by the same `ApiKeyAuth`
scheme and the same `scopeOrUnauthorized` path every other public route
uses — no new credential type. It is registered unconditionally, exactly
like `POST /v1/jobs` and `GET /v1/jobs/{job_id}`, not conditionally like the
worker-control surface: `PROJECT_SPEC.md` item 13 lists result retrieval
alongside submission and inspection as core V1 surface, not opt-in admin
plumbing, so an unauthenticated caller gets the same `401` every other
public route gives rather than a `404` that would make a real route look
like it does not exist. A server built without `WithResults` answers a
sanitized `500` for an authenticated caller instead — a real operator
misconfiguration, not a route that does not exist.

It serves the result as a direct proxy of the bytes — reading the metadata
row, and for an object-located result, fetching the bytes from
`internal/objectstore` itself and writing them to the response — rather than
redirecting to a presigned URL. A presigned URL would need its own
expiration and scope-check story independent of this API's; a proxy means
100% of retrievals pass through the identical authentication and scope
check every other endpoint here does, at the cost of this process handling
the bytes itself. For the sizes this milestone targets (results, not bulk
data transfer), that cost is the right side of the trade.

### The threshold is shared configuration, validated against the request-size limit

`TASKFORGE_RESULT_INLINE_THRESHOLD_BYTES` (default `65536`) lives on the
shared `internal/config.Config` struct, not a worker-only config surface,
because classification is a worker-only *decision* but the number itself is
a fact about the whole system's contract — the same reasoning that puts
`Broker*` on the shared struct even though only the worker and outbox use
it.

It is validated to be positive and strictly less than
`TASKFORGE_MAX_REQUEST_BYTES`. This is load-bearing, not defensive
decoration: an inline result travels inside the fenced `/succeed` request
body, which is itself subject to the same body-size limit every route is.
A threshold at or above that limit would let a worker classify a result as
"small enough to inline" whose own reporting request could never actually
be delivered — the job would have genuinely succeeded, yet the worker could
never tell the control plane so, and the attempt would be silently
abandoned to crash recovery for a reason nothing in its own configuration
explained.

### Object-store credentials follow the Broker\* precedent, not WorkerAPIKey's

`TASKFORGE_RESULTS_ACCESS_KEY_ID` / `TASKFORGE_RESULTS_SECRET_ACCESS_KEY`
default to `"local"` / `"local"`, exactly like `TASKFORGE_BROKER_*` — a
safe, required-with-a-default value, because this is process-to-
infrastructure trust (this deployment's own object store), not the
caller-to-system trust `TASKFORGE_WORKER_API_KEY` represents (blank by
default, operator-provisioned, a real bearer credential). The two are
different trust tiers with different cost-of-a-wrong-default profiles, and
the existing config surface already drew that line once; this milestone
follows it rather than inventing a third convention.

### `taskforge-api` never depends on the object store being reachable at boot; `taskforge-worker` does

`internal/objectstore.New` performs no network call — it only builds the S3
client. `taskforge-worker` calls the separate `EnsureBucket` once at
startup, so a misconfigured or unreachable bucket fails the worker's boot
rather than its first upload, the same reasoning `sqsbroker.New` resolving
its queue URL at construction already established. `taskforge-api` never
calls `EnsureBucket` at all: unlike the worker, it only ever *reads*
object-located results on demand, and unlike its relationship to the
database, it has never depended on the broker being reachable to start
either (it does not touch the broker at all; only `taskforge-outbox` and
`taskforge-worker` do). Coupling `taskforge-api`'s entire boot to the object
store being reachable would have been a stronger dependency than the
process actually has: only a request that happens to need an object-located
result ever touches it, and that path already answers a clean `500` if the
object store is unreachable, exactly as it would for any other backend
failure.

### MinIO, then LocalStack

This milestone's local/CI object store was planned as MinIO, matching
`docs/ARCHITECTURE.md`'s existing technology-choices table and
`docs/ROADMAP.md`'s M5C entry. During implementation, `docker pull
minio/minio` and `docker pull minio/mc` both answered "repository does not
exist" — confirmed against the real Docker Hub registry API, independent of
any local network policy, and consistent with the minio/minio project
having stopped publishing new community-edition container images. No
substitute MinIO distribution was found to be reliably available.
`compose.yaml` uses LocalStack's S3 provider (`SERVICES=s3`) instead.
`internal/objectstore` needed no code change: it already speaks the real
AWS S3 API against whatever endpoint it is configured with, exactly as
`internal/queue/sqsbroker` already speaks the real SQS API against
ElasticMQ or AWS interchangeably. `docs/ARCHITECTURE.md`'s technology table
and `docs/ROADMAP.md`'s M5C entry are updated to say so.

### An unbounded default HTTP client, not the checksum default, was the actual cause of a CI hang

Hosted CI's large-result end-to-end test hung for the full test timeout with
no error at all, against real LocalStack, on the first two pushes that
introduced this milestone. The first diagnosis was aws-sdk-go-v2's
known default (since SDK v1.30) of attaching a trailing checksum to
`PutObject` via `aws-chunked` transfer encoding — framing some
S3-compatible servers cannot parse — and the fix applied was
`RequestChecksumCalculation`/`ResponseChecksumValidation` set to
`WhenRequired` on the local-endpoint client, matching a widely reported
fix for the same symptom against MinIO.

That fix did not resolve the hang; the identical failure recurred on the
next push. Reading the vendored SDK source
(`aws-sdk-go-v2/service/internal/checksum`) directly rather than guessing
again showed why: the trailing-checksum code path is gated on
`req.IsHTTPS()` before it ever engages, and this project's LocalStack
endpoint is plain HTTP. The checksum default was never the cause here —
the fix is harmless and is kept as defense in depth for a local endpoint
later fronted by TLS, but it explains nothing about an HTTP endpoint.

The actual cause, confirmed by reading
`aws-sdk-go-v2/aws/transport/http.BuildableClient`'s source directly:
its overall request `Timeout` defaults to the zero value — unbounded —
unless a caller sets one. Only the Expect-100-Continue wait (1s) and TLS
handshake (10s) are bounded by default; a connection that is accepted but
never answered hangs the calling goroutine forever, which is exactly what
held the worker's concurrency slot for the full 15-second test poll (and
would have held it indefinitely outside a test). `internal/objectstore.New`
now sets an explicit 10-second `Timeout` via
`awshttp.NewBuildableClient().WithTimeout(...)`, matching the timeout
`internal/worker`'s control-plane HTTP client already uses for its own
calls, and preserving every other `BuildableClient` default (proxy from
environment, TLS 1.2 minimum, connection pooling) rather than
hand-rolling a replacement transport.

This record keeps the disproved checksum diagnosis rather than deleting
it, because the wrong turn and how it was ruled out are exactly what a
future reader hitting the same symptom needs — including the reminder
that a plausible, well-documented public fix for a same-looking symptom
against a different tool is not evidence it applies to this one.

## Known limitation: an abandoned attempt's uploaded object is orphaned, permanently

The object key `results/<scope>/<job_id>/<attempt_id>` is attempt-scoped,
not job-scoped, by deliberate choice — see "Alternatives considered" below
for the job-scoped alternative and why it was rejected. The consequence:

An attempt whose object upload succeeds, but whose worker process then dies
or loses its session before it ever calls `Succeed`, leaves that uploaded
object in the object store with nothing in PostgreSQL ever pointing at it.
`docs/PROJECT_SPEC.md` §2 already accepts this exact crash window as a
source of re-execution — "a handler may run more than once" — and
reconciliation already recovers the job through the ordinary abandoned-
attempt path (ADR-0009): a replacement attempt runs, and if it succeeds, it
uploads under its *own*, different `attempt_id` and its `Succeed` call
writes the `results` row for the replacement. The first attempt's object is
never referenced by that row, or by anything else, ever again.

This is a storage cost, not a correctness or invariant violation. Nothing
is ever observably `SUCCEEDED` with a broken or missing result reference:
the `results` row that exists always points at real, retrievable bytes,
because it is written in the same transaction as the `SUCCEEDED` transition
that makes the job's outcome observable at all. The orphan is simply never
pointed at by anything — an unreachable, unbilled-for-in-this-milestone
waste of object-store space, not a correctness gap a caller could ever
observe.

Garbage-collecting orphaned attempt-scoped objects is out of scope for this
milestone and is left for later. No specific future milestone is named for
it here; that is a future planning decision, not this record's.

## Alternatives considered

**A job-scoped, overwrite object key
(`results/<scope>/<job_id>`).** Considered and rejected. It closes the
orphaning gap above by construction — a replacement attempt's upload would
simply overwrite the first attempt's object at the same key, so nothing
would ever be orphaned. It was rejected anyway: it would make a retried
upload (an ambiguous response from the object store, retried by the same
attempt) and a genuinely different attempt's upload indistinguishable at
the storage layer, trading a cost-only, silently-accepted limitation for a
correctness-adjacent ambiguity about which attempt's bytes a concurrent
reader might observe mid-overwrite. Attempt-scoped keys keep every write
immutable once made, which is the simpler invariant to reason about, and
the cost the simpler invariant accepts is bounded and stated above rather
than hidden.

**Splitting M5C into a small-result milestone and a large-result
milestone**, mirroring how M5 split into M5A–M5D. Rejected: unlike
authentication, where the public and worker-control surfaces each had an
independently complete, independently testable acceptance criterion, "the
threshold is explicit and tested on both sides of the boundary" is one
property about one boundary. A milestone that shipped only the inline half
could not honestly claim that acceptance criterion, and a milestone that
shipped only the object half would have nothing to be a boundary between.

**A presigned-URL redirect instead of a server-side proxy** for retrieval.
Rejected in "The retrieval endpoint" above: a redirect needs its own
expiration and scope-enforcement story, independent of and potentially
inconsistent with this API's; a proxy keeps 100% of retrievals on this
API's own, already-audited authentication path.

**Bucket provisioning via an init container or a one-off migration-style
script**, instead of lazy creation inside `internal/objectstore.New`
(API) / `EnsureBucket` (worker). Rejected for the same reason
`taskforge-migrate` is a real binary rather than a shell script run by
convention: idempotent creation inside the client that needs the bucket to
exist is one fewer moving part in `compose.yaml` and one fewer thing a
fresh clone can forget to run, and `CreateBucket` tolerating
`BucketAlreadyOwnedByYou`/`BucketAlreadyExists` already makes concurrent
creation from both `taskforge-api` and `taskforge-worker` safe.

## Consequences

- `results` (migration 0016) is a new table; zero existing columns or
  tables change. This is not a breaking change: `POST
  .../attempts/{attempt_id}/succeed`'s request body gains `result` as a
  genuinely optional field, so an existing worker that never sends it keeps
  working exactly as before.
- `internal/workers.Store.Succeed` and the `WorkerControl.Succeed` /
  `ControlPlane.Succeed` interfaces it flows through all gained one
  parameter. Every caller in this repository was updated in the same
  change; an out-of-tree caller would need the same update.
- A large result adds one object-store round trip to the worker's
  success path, before `Succeed` is called, bounded by the same retry
  policy every other worker-control call already uses.
- `taskforge-worker`'s boot now depends on the configured object store and
  bucket being reachable, exactly as it already depends on the broker;
  `taskforge-api`'s boot does not.
- Local and CI infrastructure gained a third service. `compose.yaml`,
  `scripts/wait-for-infra.sh`, and the two CI jobs that start local
  infrastructure were all updated together, so none of them can drift from
  the others about what is actually required to run this milestone.
- An abandoned attempt's uploaded object is orphaned permanently — see
  "Known limitation" above. Nothing currently reclaims that space.
