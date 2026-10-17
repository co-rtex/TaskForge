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
1, then the workers. **The scheduler is safe with more than one replica**: promotion is
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
the worker sends 443 out and 8091 to the api, **and has no path to the database**, because
it never opens it; the database takes 5432 from the first group only.

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
- **Rolling deploys:** the migrations run before the services update. The api and the
  workers are compatible across one version (configuration only, ADR-0026).
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
- **VPC endpoints instead of the NAT gateway**, and **multi-AZ RDS.** Both cost more from
  the first hour; both are one variable or resource away.

## Consequences and limitations

- **Never planned or applied.** `validate` proves the configuration is well-formed against
  the provider schemas; it proves nothing an apply would find (quotas, names, an IAM
  action AWS names differently than the provider accepts).
- **Never verified:** Fargate's ECS Exec agent (ADR-0027 verified the EC2 agent's source);
  the api's read-only root beside ECS Exec on its task (Q1); a task role actually being
  resolved by the SDK's default chain in a running task; and whether the worker's
  `CreateBucket` with no `LocationConstraint` against its existing bucket outside
  `us-east-1` answers "already owned" (tolerated) or `IllegalLocationConstraintException`
  (the worker refuses to start). The last is a code question, outside this milestone.
- **Cost-shaped choices:** one NAT gateway (a single AZ of egress, and its hourly and
  per-GB charge), single-AZ RDS (no standby).
- **The control listener is plain HTTP inside the VPC** (ADR-0026's accepted limitation):
  the session id and, at registration, the worker key cross it in cleartext. TLS and a
  per-call credential remain the named follow-ups.
- **`sslmode=require` without server verification**, and **the database password in
  Terraform state**, as ADR-0027 Q7 records; the runbook requires the encrypted backend.
- **The database URL uses the master user** for every binary that opens the database;
  per-service database roles are a follow-up.
