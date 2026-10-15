# ADR-0027: AWS-native configuration and the CLI sidecar

- **Status:** Accepted
- **Date:** 2026-10-15
- **Amends:** [ADR-0021](0021-container-images-and-supply-chain-scanning.md) in one
  respect only: there are now seven images, not six; the seventh is `taskforge-cli`. Its
  rules (one binary per image, a distroless non-root base pinned once, exec-form
  entrypoints, labels) apply to it unchanged, and the api image is unchanged.

## Context

M8C1 ([ADR-0026](0026-listeners-by-surface.md)) made the binaries deployable as separate
services. Two things still stood between them and a credible AWS deployment, and the owner
split the Terraform out to M8C3 so that this milestone removes both first (decided
2026-10-15).

1. **The binaries could not use the AWS SDK's defaults.** `internal/config`'s `env()`
   treats an empty or blank value as unset and returns the default, so the broker and
   results endpoints (`http://127.0.0.1:9324`, `http://127.0.0.1:4566`) and the static keys
   (`local`/`local`) could never be cleared. `sqsbroker.New` and `objectstore.New` already
   use the SDK's default endpoint and credential chain when given `""`; nothing could give
   them `""`.
2. **There was no way to mint the first credentials in ECS.** Key administration is on the
   admin listener only, on loopback (ADR-0026), and the api image holds one binary and no
   shell (ADR-0021).

## Decision

### Q2. One explicit switch: `TASKFORGE_AWS_DEFAULT_CHAIN`

Parsed strictly with `strconv.ParseBool`; any other value is refused. An empty value is
unset, as `env()` reads every variable. When it is **true**:

- the broker and results endpoints, access key ids and secret keys resolve to `""`, so the
  SQS and S3 clients set no `BaseEndpoint` (and S3 no path-style addressing) and no static
  credentials provider: the SDK resolves the endpoint from the region and the credentials
  from its default chain (an ECS task role);
- startup **refuses** if any of `TASKFORGE_BROKER_ENDPOINT`, `TASKFORGE_RESULTS_ENDPOINT`,
  `TASKFORGE_BROKER_ACCESS_KEY_ID`, `TASKFORGE_RESULTS_ACCESS_KEY_ID`,
  `TASKFORGE_BROKER_SECRET_ACCESS_KEY` or `TASKFORGE_RESULTS_SECRET_ACCESS_KEY` is
  **present** in the environment, even with an empty value. Presence is read with
  `os.LookupEnv`, not `env()`, because `env()` hides an empty value. Each refusal names the
  variable and never echoes its value;
- `TASKFORGE_BROKER_REGION` and `TASKFORGE_RESULTS_REGION` are required, present and
  non-empty: the region decides the endpoint, so a defaulted `us-east-1` would silently pick
  one.

When it is false or unset, everything is byte-identical to before, and `env()`'s semantics
are unchanged. The three binaries that build these clients (taskforge-api: S3;
taskforge-outbox: SQS; taskforge-worker: both) now build their options through one
`OptionsFromConfig` per client package, so none of them can map the configuration
differently. `New` in both packages is unchanged in behavior; `sqsbroker`'s client
construction was split from its `GetQueueUrl` call so a test can read the resolved client
options with no network.

### Q3. A seventh image, `taskforge-cli`, run as a sidecar in the api task

The `cli` Dockerfile target holds `taskforge-cli` alone on the same pinned distroless
`nonroot` base, with the same labels, an exec-form `ENTRYPOINT ["/taskforge-cli"]` and
`CMD ["idle"]`. `taskforge-cli idle` blocks until SIGTERM or SIGINT and exits 0; it
resolves no address and makes no network call. In the api's ECS task (`awsvpc`), every
container shares one network namespace, so the sidecar reaches the admin listener on
`127.0.0.1`. An operator mints the first keys with ECS Exec into the sidecar:

```
aws ecs execute-command --cluster <cluster> --task <task> --container cli --interactive \
  --command "/taskforge-cli --api-url http://127.0.0.1:8080 api-keys create --scope <s> --name <n>"
```

ADR-0021's one-binary-per-image rule and the api image are unchanged.
`make topology-smoke` mints its keys exactly this way: the cli image idling with
`--network container:<api>` and `docker exec` of the CLI in it, with no shell.

**Verified from AWS documentation and source, before the sidecar was built:**

- **(a) ECS Exec can run a command in a container with no shell: yes.**
  [Running commands using ECS Exec](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-exec-run.html)
  says `--interactive` is required and `--command` names what runs (its example is
  `/bin/sh`, but that is only the example). How the command is started is in the agents'
  source. The ECS container agent writes the in-container SSM agent's configuration with
  `"ContainerMode": true`
  ([amazon-ecs-agent `manager_init_task.go` L44–L60](https://github.com/aws/amazon-ecs-agent/blob/fef29b5c38aefb4768cfc6c665b108338c5beb0d/agent/engine/execcmd/manager_init_task.go#L44-L60)).
  In container mode the SSM agent splits the command with shlex and `exec`s it directly,
  `exec.Command(commands[0], commands[1:]...)`, with no `sh`
  ([amazon-ssm-agent `shell_unix.go` L76–L98](https://github.com/aws/amazon-ssm-agent/blob/e5da10f27a438dc1737865589f9ee1e92587145d/agent/session/shell/shell_unix.go#L76-L98)).
  The agent binaries themselves are bind-mounted in
  ([ECS Exec, Architecture](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-exec.html#ecs-exec-architecture)),
  so the image needs none of them. Consequences: a command is one program and its
  arguments, with no pipes, redirection or variable expansion; an empty command falls back
  to `sh`, which this image does not have; and session **logging** needs `script` and `cat`
  in the image ([ECS Exec, Logging](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-exec.html#ecs-exec-logging)),
  which it does not have, and the agent's session logger runs `sh`. M8C3 must set the
  cluster's `executeCommandConfiguration` logging to `NONE` (or verify on a real task what
  `DEFAULT` does), and rely on CloudTrail's `ExecuteCommand` event for the audit of who
  ran a command.
- **(b) ECS Exec does not work with `readonlyRootFilesystem=true`.** "The SSM agent
  requires that the container file system can be written to in order to create the
  required directories and files. Therefore, making the root file system read-only using
  the `readonlyRootFilesystem` task definition parameter, or any other method, isn't
  supported." ([ECS Exec, Considerations](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-exec.html#ecs-exec-considerations)).
  **The sidecar's root filesystem must be writable.** Whether the api container in the
  same task can keep a read-only root while ECS Exec is enabled for the task is not
  verified: Exec is enabled per task and the agent is started in each container, and the
  documentation states only the unsupported case. M8C3 decides it on a real task.
- Also from the Considerations: commands run as `root` whatever the container's user
  ("The SSM agent and its child processes run as root even when you specify a user ID for
  the container"), and the task needs a task role with the SSM Session Manager
  permissions.

**Not verified:** the Fargate agent is not open source, so container mode on Fargate rests
on AWS's statement that ECS Exec supports Fargate with the same SSM mechanism, not on read
code. M8C3, or its first apply, must confirm a shell-less exec on the launch type it uses.

### Decided now, implemented by M8C3

- **Q4. Service discovery and ports.** Cloud Map private DNS: workers dial
  `api.taskforge.internal:8091`, and `TASKFORGE_API_CONTROL_HOSTS=api.taskforge.internal`
  (the exact name, ADR-0026). The control port accepts traffic only from the worker
  security group; the public port only from the ALB's security group.
- **Q5. Ingress.** An internet-facing ALB, HTTPS only. `acm_certificate_arn` is a required
  variable; HTTP redirects to HTTPS. `allowed_ingress_cidrs` defaults to `[]` (closed).
  The target health check is the public `/healthz`.
- **Q6. Egress.** One NAT gateway. Its cost and its single point of failure (one AZ) are
  recorded; VPC endpoints (SQS, S3, Secrets Manager, ECR, CloudWatch Logs, SSM messages)
  are the alternative.
- **Q7. The database.** `sslmode=require` in the database DSN, which lives in Secrets
  Manager. Not `verify-full`, because the images carry no RDS CA bundle. Terraform
  generates the password, so its state holds it, and the runbook requires an encrypted
  remote backend before any apply.
- **Q8. CI.** One job: `terraform fmt -check`, `terraform init -backend=false`,
  `terraform validate`, with the Terraform version pinned and the provider lock file
  committed. No plan.
- The timings stay at the shipped defaults; the open question in the ROADMAP is unchanged.

**Facts M8C3 must carry over:**

- **IAM for S3.** The worker calls `EnsureBucket` → `CreateBucket` at startup and tolerates
  `BucketAlreadyOwnedByYou` and `BucketAlreadyExists` (`internal/objectstore`,
  `cmd/taskforge-worker`), and readiness calls `HeadBucket`. The worker's task role
  therefore needs `s3:CreateBucket` and `s3:ListBucket` (which authorizes `HeadBucket`) on
  the bucket's ARN, besides object `s3:PutObject`/`s3:GetObject` on `<bucket ARN>/*`; the
  api needs `s3:GetObject`.
- **The SQS queue.** One standard queue, `taskforge-work-available`, with a 30 s visibility
  timeout, 0 s delivery delay and 0 s receive wait, as `deploy/local/elasticmq.conf`
  configures it locally. ADR-0024's held-notification analysis depends on the 30 s.

## Alternatives considered

- **Change `env()` so an empty value means empty.** It would clear the endpoints, but it
  changes the meaning of every variable at once: an operator who blanked a timeout or a
  batch size to "take the default" would get a zero instead. Rejected; `env()` is
  unchanged.
- **Per-setting sentinels** (`TASKFORGE_BROKER_ENDPOINT=default`, say). Six magic values to
  get right, any one of which left unset still falls back to `127.0.0.1` or `local` in a
  deployment, silently, and a mix of real and local settings remains expressible. Rejected
  for one switch that refuses a mix.
- **The CLI in the api image.** Two binaries in one image breaks ADR-0021's rule and puts
  credential administration into the internet-facing service's image, and it would make
  the api container the one an operator execs into. Rejected.
- **A database-direct bootstrap task** (a one-off task that inserts the first key rows with
  the migrate image's database access). It bypasses the API's own key creation and its
  hashing in a second implementation, needs the database credential in a second task
  definition, and a bug in it would be a credential-store bug. Rejected: the sidecar goes
  through the same routes and code as every other key.

## Consequences and limitations

- With the switch on, every binary talks to real SQS and S3 through the region's default
  endpoints and the SDK's default credential chain; with it off, nothing changed.
- `make images` builds seven images; `make images-smoke` checks the seventh like the others
  and requires it to idle and to exit 0 on SIGTERM.
- **The sidecar's root filesystem must be writable** (b), and ECS Exec commands in it run
  as root. It holds no secret and serves nothing; what it can do is what anything on the
  api's loopback could already do, which is why who may Exec into it is the control (IAM
  `ecs:ExecuteCommand`, conditioned on the container name).
- **ECS Exec session logging is unavailable in this image** (no `script`, `cat` or `sh`);
  the audit is CloudTrail's record of who ran `ExecuteCommand` against which container.
- **`sslmode=require` encrypts but does not verify the server** (Q7): a network attacker
  able to impersonate the database endpoint inside the VPC is not detected. Shipping the RDS
  CA bundle and `verify-full` is the follow-up.
- **Terraform state holds the database password** (Q7); an unencrypted or local state file
  would leak it.
