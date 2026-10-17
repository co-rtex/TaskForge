# The ECS cluster, one task definition per binary, and one service per long-running binary.
#
# Every container's environment is env/<service>.json, read here with jsondecode(file()).
# tests/verification/terraform_env_test.go holds those files to what internal/config reads.
# Two tokens in their values are substituted here, because the files cannot know them:
# "${aws_region}" and "${results_bucket}". Nothing else is substituted.
#
# Secrets reach the containers with valueFrom, never as plain environment: the database
# URL for every binary that opens the database (every one but the worker, which reaches
# PostgreSQL only through the api's control listener), and the worker key for the worker.
# tests/verification/terraform_env_test.go derives which binaries open the database from
# their imports and holds this map to it.

resource "aws_ecs_cluster" "this" {
  name = var.name

  # ECS Exec session logging needs `script`, `cat` and `sh` in the image, and the
  # distroless images have none of them (ADR-0027), so it is off. CloudTrail's
  # ExecuteCommand event is the audit of who ran a command.
  configuration {
    execute_command_configuration {
      logging = "NONE"
    }
  }
}

locals {
  services = toset(["api", "outbox", "reconciler", "scheduler", "worker", "migrate"])

  environment = {
    for svc in local.services : svc => [
      for name, value in jsondecode(file("${path.module}/env/${svc}.json")) : {
        name  = name
        value = replace(replace(value, "$${aws_region}", var.aws_region), "$${results_bucket}", aws_s3_bucket.results.bucket)
      }
    ]
  }

  database_secret = [{ name = "TASKFORGE_DATABASE_URL", valueFrom = aws_secretsmanager_secret.database_url.arn }]

  secrets = {
    api        = local.database_secret
    outbox     = local.database_secret
    reconciler = local.database_secret
    scheduler  = local.database_secret
    worker     = [{ name = "TASKFORGE_WORKER_API_KEY", valueFrom = aws_secretsmanager_secret.worker_key.arn }]
    migrate    = local.database_secret
  }

  image = { for name in local.images : name => "${aws_ecr_repository.this[name].repository_url}:${var.image_tag}" }

  log_configuration = {
    for name in local.log_groups : name => {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.this[name].name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = name
      }
    }
  }

  # Each binary's own `healthcheck` subcommand, exec form: the images have no shell.
  health_check = {
    for svc in ["api", "outbox", "reconciler", "scheduler", "worker"] : svc => {
      command     = ["CMD", "/taskforge-${svc}", "healthcheck"]
      interval    = 30
      timeout     = 5
      retries     = 3
      startPeriod = 30
    }
  }

  # One container per task, except the api's (below).
  single_container = {
    for svc in ["outbox", "reconciler", "scheduler", "worker", "migrate"] : svc => [merge(
      {
        name                   = svc
        image                  = local.image[svc]
        essential              = true
        readonlyRootFilesystem = true
        environment            = local.environment[svc]
        secrets                = local.secrets[svc]
        logConfiguration       = local.log_configuration[svc]
      },
      svc == "migrate" ? {} : { healthCheck = local.health_check[svc] },
    )]
  }

  # The api task: the api, and the cli sidecar that shares its network namespace so an
  # operator can mint the first keys on its loopback admin listener through ECS Exec
  # (ADR-0027). The api's read-only root beside ECS Exec on the task is UNVERIFIED
  # (ADR-0028 Q1); RUNBOOK.md's fallback is var.api_readonly_root_filesystem = false, which
  # changes the api container only. The cli's root is writable because ECS Exec requires
  # it.
  api_containers = [
    {
      name                   = "api"
      image                  = local.image["api"]
      essential              = true
      readonlyRootFilesystem = var.api_readonly_root_filesystem
      environment            = local.environment["api"]
      secrets                = local.secrets["api"]
      logConfiguration       = local.log_configuration["api"]
      healthCheck            = local.health_check["api"]
      portMappings = [
        { containerPort = 8090, protocol = "tcp" },
        { containerPort = 8091, protocol = "tcp" },
      ]
    },
    {
      name                   = "cli"
      image                  = local.image["cli"]
      essential              = false
      command                = ["idle"]
      readonlyRootFilesystem = false
      logConfiguration       = local.log_configuration["cli"]
    },
  ]

  containers = merge({ api = local.api_containers }, local.single_container)

  task_role_arn = {
    api        = aws_iam_role.task["api"].arn
    outbox     = aws_iam_role.task["outbox"].arn
    reconciler = aws_iam_role.task["reconciler"].arn
    scheduler  = aws_iam_role.task["scheduler"].arn
    worker     = aws_iam_role.task["worker"].arn
    migrate    = null # migrate calls no AWS API
  }
}

resource "aws_ecs_task_definition" "this" {
  for_each                 = local.services
  family                   = "${var.name}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu[each.key]
  memory                   = var.task_memory[each.key]
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = local.task_role_arn[each.key]
  container_definitions    = jsonencode(local.containers[each.key])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }
}

# --- Services. Every desired count defaults to 0 (RUNBOOK.md raises them in order). ---

locals {
  desired_count = {
    api        = var.api_desired_count
    outbox     = var.outbox_desired_count
    reconciler = var.reconciler_desired_count
    scheduler  = var.scheduler_desired_count
    worker     = var.worker_desired_count
  }

  service_security_groups = {
    api        = [aws_security_group.tasks.id, aws_security_group.api.id]
    outbox     = [aws_security_group.tasks.id]
    reconciler = [aws_security_group.tasks.id]
    scheduler  = [aws_security_group.tasks.id]
    worker     = [aws_security_group.worker.id]
  }
}

resource "aws_ecs_service" "api" {
  name                              = "${var.name}-api"
  cluster                           = aws_ecs_cluster.this.id
  task_definition                   = aws_ecs_task_definition.this["api"].arn
  desired_count                     = local.desired_count["api"]
  launch_type                       = "FARGATE"
  enable_execute_command            = true
  health_check_grace_period_seconds = 60

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = local.service_security_groups["api"]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = "api"
    container_port   = 8090
  }

  service_registries {
    registry_arn = aws_service_discovery_service.api.arn
  }

  depends_on = [aws_lb_listener.https]
}

resource "aws_ecs_service" "background" {
  for_each               = toset(["outbox", "reconciler", "scheduler", "worker"])
  name                   = "${var.name}-${each.key}"
  cluster                = aws_ecs_cluster.this.id
  task_definition        = aws_ecs_task_definition.this[each.key].arn
  desired_count          = local.desired_count[each.key]
  launch_type            = "FARGATE"
  enable_execute_command = false

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = local.service_security_groups[each.key]
    assign_public_ip = false
  }
}
