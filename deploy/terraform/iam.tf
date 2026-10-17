# IAM, least privilege. docs/adr/0028-deployment-shape-on-aws.md maps every task-role action
# below to the file:line call site in the binary that needs it, and every such call site to
# an action here. SQS and S3 grants name the one queue and the one bucket; nothing on them
# is "*".

data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# --- The execution role: what ECS itself needs to start a container. ---------------

resource "aws_iam_role" "execution" {
  name               = "${var.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "execution" {
  # GetAuthorizationToken has no resource-level permissions; it must be "*".
  statement {
    sid       = "EcrToken"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
  statement {
    sid       = "EcrPull"
    actions   = ["ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage"]
    resources = [for repo in aws_ecr_repository.this : repo.arn]
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [for group in aws_cloudwatch_log_group.this : "${group.arn}:*"]
  }
  statement {
    sid       = "Secrets"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.database_url.arn, aws_secretsmanager_secret.worker_key.arn]
  }
}

resource "aws_iam_role_policy" "execution" {
  name   = "execution"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution.json
}

# --- Task roles: what each binary itself calls. ---------------------------------------
#
# reconciler and scheduler build no AWS client (cmd/taskforge-reconciler and
# cmd/taskforge-scheduler import neither internal/queue/sqsbroker nor
# internal/objectstore), so their roles carry no policy at all.

locals {
  task_roles = toset(["api", "outbox", "reconciler", "scheduler", "worker"])
}

resource "aws_iam_role" "task" {
  for_each           = local.task_roles
  name               = "${var.name}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "api" {
  statement {
    sid       = "ReadResults"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.results.arn}/*"]
  }
  # ECS Exec: the SSM agent ECS injects into the task opens its session channels with the
  # task role. These are the agent's calls, not the binary's, and have no resource-level
  # permissions.
  statement {
    sid = "EcsExec"
    actions = [
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
    ]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "outbox" {
  statement {
    sid       = "PublishNotifications"
    actions   = ["sqs:GetQueueUrl", "sqs:SendMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.work_available.arn]
  }
}

data "aws_iam_policy_document" "worker" {
  statement {
    sid       = "ConsumeNotifications"
    actions   = ["sqs:GetQueueUrl", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.work_available.arn]
  }
  # CreateBucket (EnsureBucket at startup) and ListBucket (which authorizes HeadBucket,
  # the readiness check) act on the bucket itself.
  statement {
    sid       = "ResultsBucket"
    actions   = ["s3:CreateBucket", "s3:ListBucket"]
    resources = [aws_s3_bucket.results.arn]
  }
  statement {
    sid       = "WriteResults"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.results.arn}/*"]
  }
}

resource "aws_iam_role_policy" "api" {
  name   = "api"
  role   = aws_iam_role.task["api"].id
  policy = data.aws_iam_policy_document.api.json
}

resource "aws_iam_role_policy" "outbox" {
  name   = "outbox"
  role   = aws_iam_role.task["outbox"].id
  policy = data.aws_iam_policy_document.outbox.json
}

resource "aws_iam_role_policy" "worker" {
  name   = "worker"
  role   = aws_iam_role.task["worker"].id
  policy = data.aws_iam_policy_document.worker.json
}

# --- The operator policy: ECS Exec into the cli sidecar and nothing else. ---------------
#
# Created, output, and attached to no one: the operator attaches it to whoever runs the
# runbook's bootstrap step.

data "aws_iam_policy_document" "operator_exec" {
  statement {
    sid     = "ExecIntoTheCliSidecarOnly"
    actions = ["ecs:ExecuteCommand"]
    resources = [
      aws_ecs_cluster.this.arn,
      "${replace(aws_ecs_cluster.this.arn, ":cluster/", ":task/")}/*",
    ]
    condition {
      test     = "StringEquals"
      variable = "ecs:container-name"
      values   = ["cli"]
    }
  }
  statement {
    sid       = "FindTheTask"
    actions   = ["ecs:ListTasks", "ecs:DescribeTasks"]
    resources = ["*"]
    condition {
      test     = "ArnEquals"
      variable = "ecs:cluster"
      values   = [aws_ecs_cluster.this.arn]
    }
  }
}

resource "aws_iam_policy" "operator_exec" {
  name        = "${var.name}-operator-exec"
  description = "ECS Exec into the cli container of TaskForge tasks only, to mint the first keys (RUNBOOK.md)."
  policy      = data.aws_iam_policy_document.operator_exec.json
}
