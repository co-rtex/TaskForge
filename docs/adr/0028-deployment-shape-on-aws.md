# ADR-0028: The deployment shape on AWS

- **Status:** Accepted
- **Date:** 2026-10-17
- **Builds on:** [ADR-0026](0026-listeners-by-surface.md) (the listeners and the control
  listener's Host allowlist) and [ADR-0027](0027-aws-native-configuration-and-the-cli-sidecar.md)
  (AWS-native configuration, the cli sidecar, and the decisions Q4–Q8 this milestone
  implements: Cloud Map, the HTTPS-only ALB, one NAT gateway, `sslmode=require`, the
  validate-only CI job). Neither is repeated here.

## Context

M8C1 and M8C2 made the binaries deployable as separate services on AWS. M8C3, the last M8
milestone, describes that deployment in Terraform: `deploy/terraform/`, which passes
`terraform fmt -check`, `terraform init -backend=false` and `terraform validate`, locally
(`make tf-validate`) and in CI, and is **never planned or applied**. The owner decided its
own shape on 2026-10-15 (Q1–Q6 below); everything else follows from the two ADRs above.

## Decision

### Q1. Read-only root filesystems, except the cli sidecar's

Every container has `readonlyRootFilesystem = true` except `cli`, whose root must be
writable for ECS Exec (ADR-0027). The api container is **unverified**: ECS Exec is enabled
on the api task, AWS documents a read-only root as unsupported for ECS Exec, and whether
that reaches a container in the task you do not exec into is not documented. The setting is
`var.api_readonly_root_filesystem` (default `true`); RUNBOOK.md's fallback sets it `false`,
which changes the api container only.

### Q2. Each service's environment is a JSON file

`deploy/terraform/env/<service>.json` holds each container's environment, read by
`ecs.tf` with `jsondecode(file(...))`. Two placeholders, `${aws_region}` and
`${results_bucket}`, are substituted there and nowhere else, because the files cannot know
Terraform's inputs. `tests/verification/terraform_env_test.go` holds the files to the
code, with no new Go dependency:

- the variables each binary can read are **derived** from `internal/config/config.go` with
  `go/ast`: every `"TASKFORGE_..."` literal passed to `env`, `envInt`, `envInt64`,
  `envFloat`, `envDuration`, `envSet`, `envList` or `os.LookupEnv`; those inside
  `LoadWorker` are the worker's, the rest the shared set. Every name in every file must be
  in its binary's set;
- every ECS service has `TASKFORGE_AWS_DEFAULT_CHAIN=true` and both regions; the binaries
  that build an AWS client are derived from their imports (api, outbox, worker);
- none of the six endpoint or key variables appears, and neither secret
  (`TASKFORGE_DATABASE_URL`, `TASKFORGE_WORKER_API_KEY`) is plain environment;
- the api's listeners and Host allowlist, and the worker's control URL and opt-in, are
  pinned;
- migrate carries only what it reads, derived from the `cfg.<Field>` selectors in its main
  mapped through `Load`'s literal (`LogLevel`; `DatabaseURL` is the secret);
- only the binaries whose main imports `internal/database` get the database URL secret
  (every one but the worker);
- `ecs.tf` substitutes exactly the two placeholders and wires both secrets by `valueFrom`;
- `allowed_ingress_cidrs` defaults to `[]`, read from `variables.tf` (see Alternatives).

### Q3. Every service starts at zero tasks

Each ECS service's `desired_count` is a variable defaulting to 0. RUNBOOK.md's order: push
the images, run the migrate task (`aws ecs run-task`), the api to 1, mint the keys through
the cli sidecar, put the worker key secret's value, the outbox, reconciler and scheduler to
1, then the worker (one task; see "One worker identity"). **The scheduler is safe with more than one replica**: promotion is
tested with four concurrent replicas on separate connections
(`TestScheduler_ConcurrentReplicasPromoteEachTransitionOnce`,
`tests/integration/scheduler_test.go:246`), and re-notification locks the queue and the job
row and revalidates `last_notification_at` before stamping it
(`internal/jobs/schedule.go:227–290`), so a second replica skips. Re-notification has no
concurrent-replica test, so the runbook starts with 1. The outbox is covered by
[ADR-0004](0004-transactional-outbox.md), the reconciler by ARCHITECTURE §11.

### Q4. ECR, the image tag and the architecture

Terraform creates one ECR repository per image (seven), with immutable tags and scan on
push. `image_tag` has no default. `cpu_architecture` (default `X86_64`, or `ARM64`) sets
every task's `runtime_platform` and must match the pushed images.

### Q5. One root module

`deploy/terraform/` is one root module, one file per concern, no child modules.
`versions.tf` pins exact versions, each checked against its release page and chosen at
least three weeks old:

| Pin | Released | Source |
| --- | --- | --- |
| Terraform `1.16.4` | 2026-09-23 | [hashicorp/terraform v1.16.4](https://github.com/hashicorp/terraform/releases/tag/v1.16.4) |
| `hashicorp/aws` `6.66.0` | 2026-09-21 | [terraform-provider-aws v6.66.0](https://github.com/hashicorp/terraform-provider-aws/releases/tag/v6.66.0) |
| `hashicorp/random` `3.9.1` | 2026-09-11 | [terraform-provider-random v3.9.1](https://github.com/hashicorp/terraform-provider-random/releases/tag/v3.9.1) |
| `hashicorp/setup-terraform` `v4.0.1`, commit `dfe3c3f87815947d99a8997f908cb6525fc44e9e` (CI) | 2026-05-12 | [setup-terraform v4.0.1](https://github.com/hashicorp/setup-terraform/releases/tag/v4.0.1) |
| `hashicorp/terraform:1.16.4@sha256:985cdc6c1d9b0a65b83377f666efd2f740b47f02ac55be1ced3d18f7d3b0e829` (`make tf-validate` with no local terraform) | 2026-09-23 | [Docker Hub hashicorp/terraform](https://hub.docker.com/r/hashicorp/terraform) |

`.terraform.lock.hcl` is committed with hashes for linux and darwin, amd64 and arm64, and CI
and `make tf-validate` initialize with `-lockfile=readonly`. The backend is `backend "s3"
{}`, an empty partial configuration given at init with S3-native locking
(`use_lockfile`); `terraform.tfvars.example` holds placeholders only.

The HTTPS listener's `ssl_policy` defaults to `ELBSecurityPolicy-TLS13-1-2-Res-PQ-2025-09`,
which [AWS recommends](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/describe-ssl-policies.html)
("AWS recommends implementing the new post-quantum TLS (PQ-TLS) based security policy"):
TLS 1.3 and 1.2, with hybrid post-quantum key exchange where the client offers it.

### Q6. Defaults for the stores

RDS: `db.t4g.micro`, PostgreSQL 16 (compose runs `postgres:16-alpine`), single-AZ, storage
encrypted, not publicly accessible, deletion protection on, a final snapshot required, 7-day
backups, each a variable. SQS uses SSE-SQS and S3 uses SSE-S3, so no role needs a KMS
permission. The results bucket blocks all public access and enforces bucket-owner
ownership.

### One worker identity: at most one worker task, stopped before its replacement starts

*Implemented in the fix round for PR #31 (review finding B1), Terraform only, as the owner
decided.*

**The mechanism.** Every worker task runs the one task definition, so every one registers
as the logical worker `TASKFORGE_WORKER_NAME` names (`ecs-worker`, `env/worker.json`). The
`workers` table is `UNIQUE (scope, name)`
(`migrations/0002_workers_sessions_and_claims.sql:55`), and `Store.Register` makes the new
boot's session current and marks the previous current session `OFFLINE` in the same
transaction (`internal/workers/store.go:53–106`). The fenced runner then gets
`ErrSessionLost` (`internal/worker/runner.go:373`) and the process exits. Two tasks would
therefore fence each other forever: each restart registers and fences the other. With one
task and ECS's default rolling deploy (minimum 100 % healthy, maximum 200 %), the new task
starts while the old one runs and fences its in-flight attempts, which then wait for lease
expiry instead of draining.

**The cap.** `worker_desired_count` has a validation admitting exactly 0 and 1
(`contains([0, 1], var.worker_desired_count)`). One task runs up to
`TASKFORGE_WORKER_CONCURRENCY` jobs at once (4 by default).

**Stop-before-start.** The worker is its own `aws_ecs_service`, apart from the outbox,
reconciler and scheduler, with `deployment_minimum_healthy_percent = 0` and
`deployment_maximum_percent = 100`: ECS stops the old task (SIGTERM, then the worker's
drain) before it starts the new one.

**The stop timeout.** The worker drains for up to `TASKFORGE_WORKER_SHUTDOWN_TIMEOUT`, 15 s
by default (`internal/config/config.go:593`; `internal/worker/runner.go:127–128`), then
shuts its health server down within a fresh 15 s (`cmd/taskforge-worker/main.go:177–180`).
ECS sends the stop signal (SIGTERM by default) and then `SIGKILL` after the container's
`stopTimeout`, which defaults to 30 s on Fargate (platform 1.3.0 or later) and may be at
most 120 s ([task definition parameters, container
timeouts](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html#container_definition_timeout);
[StopTask](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_StopTask.html)).
30 s would leave no margin over the worst case, so the worker container sets `stopTimeout =
45`.

**Enforced by tests** (`tests/verification/terraform_env_test.go`):
`TestTerraformVars_WorkerCountIsAtMostOne`, `TestTerraformECS_WorkerStopsBeforeItStarts`
(the two deployment percentages, the 45 s stop timeout, and the worker not also in the
background `for_each`), and `TestTerraformECS_WorkerHasNoPathToTheDatabase`.

### The region is us-east-1

*Implemented in the same fix round (review finding N1).* `aws_region` has a validation
requiring `us-east-1`. The worker calls `CreateBucket` on the existing results bucket at
startup with no `LocationConstraint` (`internal/objectstore/objectstore.go:148`) and
tolerates only `BucketAlreadyOwnedByYou` and `BucketAlreadyExists`. In `us-east-1`, S3
documents that re-creating a bucket you own "returns 200 OK and resets the bucket access
control lists" ([CreateBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html));
ACLs are disabled here (`BucketOwnerEnforced`). Outside `us-east-1`, whether S3 answers that
call with `BucketAlreadyOwnedByYou` or `IllegalLocationConstraintException` is
**unverified**. The code fix that sends a `LocationConstraint` lifts the validation.
`TestTerraformVars_RegionIsUSEast1` pins it.

### IAM, from the call sites

The execution role (ECS itself) pulls from the seven repositories (`ecr:GetAuthorizationToken`
has no resource-level permission and is `*`), writes to the seven log groups, and reads
exactly the two secrets. Each task role grants what its binary's code calls, and nothing
else; SQS and S3 grants name the one queue and the one bucket.

| Role | Action | Resource | Call site (the binary reaches it through) |
| --- | --- | --- | --- |
| api | `s3:GetObject` | bucket `/*` | `internal/objectstore/objectstore.go:200`, via `s.objects.Get` at `internal/api/server.go:498` |
| api | `ssmmessages:{Create,Open}{Control,Data}Channel` | `*` | not the binary: the SSM agent ECS Exec injects into the task ([ECS Exec permissions](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html#ecs-exec-required-iam-permissions)) |
| outbox | `sqs:GetQueueUrl` | the queue | `internal/queue/sqsbroker/sqsbroker.go:65`, via `sqsbroker.New` at `cmd/taskforge-outbox/main.go:91` |
| outbox | `sqs:SendMessage` | the queue | `sqsbroker.go:103`, via `p.broker.Publish` at `internal/outbox/publisher.go:111` |
| outbox | `sqs:GetQueueAttributes` | the queue | `sqsbroker.go:155` (`Ping`), via the readiness check at `cmd/taskforge-outbox/main.go:117` |
| worker | `sqs:GetQueueUrl` | the queue | `sqsbroker.go:65`, via `sqsbroker.New` at `cmd/taskforge-worker/main.go:95` |
| worker | `sqs:ReceiveMessage` | the queue | `sqsbroker.go:121`, via `r.broker.Receive` at `internal/worker/runner.go:271` |
| worker | `sqs:DeleteMessage` | the queue | `sqsbroker.go:143`, via `r.broker.Delete` at `internal/worker/runner.go:798` |
| worker | `sqs:GetQueueAttributes` | the queue | `sqsbroker.go:155` (`Ping`), via the readiness check at `cmd/taskforge-worker/main.go:158` |
| worker | `s3:CreateBucket` | the bucket | `internal/objectstore/objectstore.go:148` (`EnsureBucket`), at `cmd/taskforge-worker/main.go:110` |
| worker | `s3:ListBucket` (authorizes `HeadBucket`) | the bucket | `objectstore.go:171` (`Ping`), via the readiness check at `cmd/taskforge-worker/main.go:160` |
| worker | `s3:PutObject` | bucket `/*` | `objectstore.go:183`, via `r.objects.Put` at `internal/worker/runner.go:667` |
| reconciler, scheduler | none | | their mains import neither `internal/queue/sqsbroker` nor `internal/objectstore` |
| migrate | no task role | | it calls no AWS API |

Those are every call into the two AWS clients in `internal/` and `cmd/` (no other method of
`*sqs.Client` or `*s3.Client` is called), and every one has an action above. An operator
policy (`operator_exec_policy_arn`, output, attached to no one) allows `ecs:ExecuteCommand`
only where `ecs:container-name` is `cli`, plus `ecs:ListTasks` and `ecs:DescribeTasks` in
the cluster.

### Network

Two AZs, public and private subnets, one NAT gateway (ADR-0027 Q6). Security groups:
the ALB takes 443 and 80 from `allowed_ingress_cidrs` (none by default) and sends only to
the api on 8090; the api takes 8090 from the ALB only and 8091 from the workers only; the
api, outbox, reconciler, scheduler and migrate tasks send 443 out and 5432 to the database;
the worker is in the worker group alone, which sends 443 out and 8091 to the api, **and has
no path to the database**, because it never opens it
(`TestTerraformECS_WorkerHasNoPathToTheDatabase` pins both); the database takes 5432 from the first group only.

## Failure and upgrade cases

- **A worker started before its secret has a value fails to start, by design**: the secret
  is created with no version, ECS cannot read it, and the task stops before the binary runs.
- **The api's readiness passes before the migrations run.** The handoff assumed it fails;
  the repository disagrees: `/readyz` is `database.Ping`, a connection ping
  (`internal/database/database.go:42–47`), so it does not see a missing schema, and the
  requests that need it fail with a sanitized 500 instead. The runbook runs the migrations
  first for that reason.
- **If ECS Exec refuses the task because the api's root is read-only**, apply the Q1
  fallback.
- **Rolling deploys:** the migrations run before the services update. **Compatibility across
  one version is a precondition each release must meet**, not a property of this
  configuration: while the services roll, an old api may serve a new worker and the
  reverse. Configuration-only changes (the listeners, AWS mode; ADR-0026, ADR-0027) meet it.
- **The worker's deployment gap.** Because the worker deploys stop-then-start ("One worker identity"), a
  release has a gap, up to one task start long, in which no worker claims. Jobs wait
  `QUEUED`; none is lost.
- **The worker's stop timeout** is 45 s, above its worst-case shutdown of 15 s of drain plus
  15 s of health-server shutdown, where Fargate's default of 30 s would leave no margin
  ("One worker identity").
- **Teardown** needs deletion protection turned off first; the final snapshot is taken and
  kept, and fails if one of that name exists. The bucket and the repositories must be
  emptied first. RUNBOOK.md §6.

## Alternatives considered

- **Child modules per concern.** Reuse nothing here would use, and an indirection a reviewer
  has to follow for every resource. Rejected (Q5).
- **Environment inline in `ecs.tf`.** Terraform alone would read it, so no Go test could hold
  it to `internal/config` without parsing HCL. Rejected (Q2).
- **Reading the `allowed_ingress_cidrs` default with an HCL parser.** A new Go dependency
  for one value. `terraform fmt -check` (in CI and `make tf-validate`) makes `variables.tf`
  canonical, so the test finds the block by its header and its closing brace at column 0
  and requires its single `default` line to be `[]`. A `validation` block was rejected as
  the proof: it constrains values passed in, not the value an operator gets by passing
  nothing.
- **Per-index worker services** (worker-0, worker-1, ..., each with its own task definition
  and `TASKFORGE_WORKER_NAME`). It would allow more than one worker today, but it multiplies
  services, task definitions and env files by the count, and makes scaling an edit to the
  configuration. Rejected for this milestone's scope.
- **Per-task worker identity in code**, for example deriving the worker's name from the ECS
  task ID when `TASKFORGE_WORKER_NAME` is unset. It would lift the cap with one service.
  It is the named follow-up; this milestone changes no code.
- **VPC endpoints instead of the NAT gateway**, and **multi-AZ RDS.** Both cost more from
  the first hour; both are one variable or resource away.

## Consequences and limitations

- **Never planned or applied.** `validate` proves the configuration is well-formed against
  the provider schemas; it proves nothing an apply would find (quotas, names, an IAM
  action AWS names differently than the provider accepts).
- **Never verified:** Fargate's ECS Exec agent (ADR-0027 verified the EC2 agent's source);
  the api's read-only root beside ECS Exec on its task (Q1); a task role actually being
  resolved by the SDK's default chain in a running task; and what S3 answers the worker's
  `CreateBucket` outside `us-east-1` ("The region is us-east-1", which is why it is enforced).
- **One worker at a time** ("One worker identity"): one task, up to `TASKFORGE_WORKER_CONCURRENCY` jobs, and a
  claim gap on every worker deployment. Per-task worker identity is the code follow-up that
  lifts it.
- **`us-east-1` only** ("The region is us-east-1"), until the worker's `CreateBucket` sends a `LocationConstraint`.
- **Cost-shaped choices:** one NAT gateway (a single AZ of egress, and its hourly and
  per-GB charge), single-AZ RDS (no standby).
- **The control listener is plain HTTP inside the VPC** (ADR-0026's accepted limitation):
  the session id and, at registration, the worker key cross it in cleartext. TLS and a
  per-call credential remain the named follow-ups.
- **`sslmode=require` without server verification**, and **the database password in
  Terraform state**, as ADR-0027 Q7 records; the runbook requires the encrypted backend.
- **The database URL uses the master user** for every binary that opens the database;
  per-service database roles are a follow-up.
