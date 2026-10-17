# TaskForge on AWS: runbook

**This configuration has never been planned or applied.** CI and `make tf-validate` only
check it (`terraform fmt -check`, `terraform init -backend=false`, `terraform validate`).
Applying it creates billable resources (a NAT gateway, an ALB, RDS, Fargate tasks, and
more) from the first minute, whether or not any task runs. The decisions behind it are in
[ADR-0028](../../docs/adr/0028-deployment-shape-on-aws.md), building on
[ADR-0026](../../docs/adr/0026-listeners-by-surface.md) and
[ADR-0027](../../docs/adr/0027-aws-native-configuration-and-the-cli-sidecar.md).

Values in angle brackets are yours to fill in. Every command is run from
`deploy/terraform/` unless it says otherwise.

## 1. Prerequisites

1. **An encrypted remote state bucket, with locking.** The state holds the database
   password (it is in the `database-url` secret Terraform writes), so a local or
   unencrypted state file leaks it. Create, outside this configuration, an S3 bucket with
   default encryption, versioning, and all public access blocked, then write a
   `backend.hcl` (gitignored) such as:

   ```hcl
   bucket       = "<state-bucket>"
   key          = "taskforge/terraform.tfstate"
   region       = "<region>"
   encrypt      = true
   use_lockfile = true # S3-native locking; no DynamoDB table
   ```

   and initialize with it:

   ```bash
   terraform init -backend-config=backend.hcl
   ```

2. **An ACM certificate** in the same region for the name clients will use, issued and
   validated. Its ARN is `acm_certificate_arn`.
3. **The ingress CIDRs**: who may reach the load balancer. `allowed_ingress_cidrs`
   defaults to `[]`, which leaves 443 and 80 closed to everyone.
4. **The images, built for `cpu_architecture`.** `cpu_architecture` (default `X86_64`)
   sets every task's `runtime_platform`, and it must match the images. `make images` builds
   for the machine it runs on (an Apple-silicon Mac builds `arm64`); for `X86_64` build
   with `--platform linux/amd64`. Check with
   `docker image inspect --format '{{.Architecture}}' taskforge-api:dev`.
5. `cp terraform.tfvars.example terraform.tfvars` (gitignored) and fill it in. Leave every
   `*_desired_count` at 0 for the first apply.

## 2. First deployment, in order

### 2.1 Create everything, with no task running

```bash
terraform apply
```

Every ECS service starts at `desired_count = 0`, so nothing pulls an image yet. The
`worker-key` secret is created with **no value**.

### 2.2 Push the seven images

```bash
aws ecr get-login-password --region <region> | \
  docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
for svc in api outbox scheduler reconciler worker migrate cli; do
  docker build --platform linux/amd64 --target "$svc" --build-arg REVISION="$(git rev-parse HEAD)" \
    --tag "<account>.dkr.ecr.<region>.amazonaws.com/taskforge/$svc:<image_tag>" ../..
  docker push "<account>.dkr.ecr.<region>.amazonaws.com/taskforge/$svc:<image_tag>"
done
```

Run `make dash-build` at the repository root first, so the api embeds the real dashboard.
Tags are immutable: a new build needs a new `image_tag`. `terraform output
ecr_repository_urls` prints the seven repository URLs.

### 2.3 Run the migrations

```bash
aws ecs run-task --cluster "$(terraform output -raw cluster_name)" \
  --task-definition "$(terraform output -raw migrate_task_definition)" \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[<private-subnet-1>,<private-subnet-2>],securityGroups=[<task-sg>],assignPublicIp=DISABLED}"
```

`terraform output private_subnet_ids` and `terraform output task_security_group_id` give
the values. Wait for it, and require exit code 0:

```bash
aws ecs wait tasks-stopped --cluster <cluster> --tasks <task-arn>
aws ecs describe-tasks --cluster <cluster> --tasks <task-arn> \
  --query 'tasks[0].containers[0].exitCode'
```

Its log (`/taskforge/migrate`) ends `migrations complete` or `schema already up to date`.

### 2.4 Start the api

```bash
terraform apply -var api_desired_count=1
```

Wait for the target group to report the task healthy. Its health check is the public
`/healthz`.

### 2.5 Mint the first keys through the cli sidecar

Attach `terraform output -raw operator_exec_policy_arn` to whoever runs this step. It
allows `ecs:ExecuteCommand` into the `cli` container only. Then:

```bash
TASK=$(aws ecs list-tasks --cluster <cluster> --service-name <name>-api --query 'taskArns[0]' --output text)

aws ecs execute-command --cluster <cluster> --task "$TASK" --container cli --interactive \
  --command "/taskforge-cli --api-url http://127.0.0.1:8080 worker-keys create --scope <scope> --name ecs-workers"

aws ecs execute-command --cluster <cluster> --task "$TASK" --container cli --interactive \
  --command "/taskforge-cli --api-url http://127.0.0.1:8080 api-keys create --scope <scope> --name <client>"
```

The image has no shell. The command is split into words and the program runs directly,
so it is **one program and its arguments**: no pipes, no redirection, no `$VAR`, no
quoting tricks. Each prints one JSON object; its `key` is shown once and never again. The
command runs as root in the sidecar (ECS Exec always does). ECS Exec logging is off
(`NONE`) because the image has no `script`, `cat` or `sh`. CloudTrail's `ExecuteCommand`
event records who ran it.

### 2.6 Put the worker key

Write the worker key's value to a file outside the repository, with mode 600, then:

```bash
aws secretsmanager put-secret-value --secret-id "$(terraform output -raw worker_key_secret_arn)" \
  --secret-string "file://<path-to-file>"
```

Then delete the file. Reading from a file keeps the key out of shell history and the
process list.

### 2.7 Start the background services

```bash
terraform apply -var api_desired_count=1 -var outbox_desired_count=1 \
  -var reconciler_desired_count=1 -var scheduler_desired_count=1
```

Run one of each. More than one is safe for each:

- **outbox:** claiming is serialized in PostgreSQL ([ADR-0004](../../docs/adr/0004-transactional-outbox.md)).
- **reconciler:** every pass is revalidated under row locks and is idempotent ([ARCHITECTURE](../../docs/ARCHITECTURE.md) §11).
- **scheduler:** promotion is tested with four concurrent replicas
  (`TestScheduler_ConcurrentReplicasPromoteEachTransitionOnce`,
  `tests/integration/scheduler_test.go:246`). Re-notification locks the queue and the job
  row and revalidates `last_notification_at` under the lock before stamping it
  (`internal/jobs/schedule.go:227–290`), so a second replica finds the stamp and skips. No
  test runs re-notification on concurrent replicas, though, so start with 1.

### 2.8 Start the workers

```bash
terraform apply -var api_desired_count=1 -var outbox_desired_count=1 \
  -var reconciler_desired_count=1 -var scheduler_desired_count=1 -var worker_desired_count=<n>
```

Keep these values in `terraform.tfvars` from now on, so a later plain `terraform apply`
does not scale anything back to 0.

## 3. If the api task refuses to start (the Q1 fallback)

The api container runs with a read-only root filesystem, while ECS Exec is enabled on its
task. AWS documents a read-only root as unsupported for ECS Exec, and whether that
refusal reaches the api container (as opposed to only the `cli` container you exec into)
is **unverified** (ADR-0028 Q1). If the api task fails to start, or `aws ecs
describe-tasks` shows its `ExecuteCommandAgent` not `RUNNING`, make **only the api
container** writable:

```bash
terraform apply -var api_readonly_root_filesystem=false   # plus your desired counts
```

Every other container keeps a read-only root. Record the outcome in ADR-0028.

## 4. Releases (rolling deploys)

1. Build and push all seven images under a new `image_tag`.
2. Run the migrations first (2.3), with the new migrate image: set `image_tag` and apply
   with the services' current counts, then run the migrate task. The services then roll
   onto the same `image_tag`.
3. The api and the workers are compatible across one version during the roll: listeners and
   AWS mode are configuration only (ADR-0026, ADR-0027), and there is no wire change between
   them in this configuration.

## 5. Known failure modes

- **A worker started before its secret has a value fails to start, by design.** ECS cannot
  read a secret with no version, and the task stops with a `ResourceInitializationError`.
  Do 2.6 before 2.8.
- **Before the migrations run, readiness still passes.** `/readyz` only pings the database
  (`internal/database/database.go:42–47`), so it does not detect a missing schema. Requests
  that need the schema fail with a sanitized 500. That is why 2.3 comes before 2.4.
- **A worker outside `us-east-1` creating its bucket: unverified.** At startup the worker
  calls `CreateBucket` on the existing results bucket with no `LocationConstraint`
  (`internal/objectstore/objectstore.go:148`), and tolerates only "already owned by you" or
  "already exists". In `us-east-1` that call succeeds against your own bucket. Elsewhere,
  whether S3 answers "already owned" or `IllegalLocationConstraintException` (which stops
  the worker from starting) has not been checked. If the workers fail with that error, the
  fix is in code, not here; `us-east-1` avoids the question.
- **ECS Exec on Fargate is verified from the open-source EC2 agent's behavior, not
  Fargate's** (ADR-0027). If 2.5 fails with an error about a shell, record it in ADR-0028.

## 6. Teardown

Deletion protection and the required final snapshot are deliberate. To destroy:

1. Scale every service to 0: `terraform apply` with every `*_desired_count = 0`.
2. Turn deletion protection off: `terraform apply -var db_deletion_protection=false`.
3. Empty the results bucket (`aws s3 rm s3://<results-bucket> --recursive`) and delete the
   images in the seven ECR repositories. Neither is force-deleted by this configuration.
4. `terraform destroy`. RDS takes the final snapshot `<name>-final`, which fails if a
   snapshot of that name already exists; delete or rename the old one first. The snapshot
   is kept, and keeps costing storage, until you delete it.
5. The two secrets enter their recovery window (`secret_recovery_window_days`, default
   7). The state bucket is yours to delete last.
