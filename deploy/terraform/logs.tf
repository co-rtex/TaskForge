# One log group per container name. The cli sidecar's idle prints nothing, but a
# container with a log configuration needs a group to exist.

locals {
  log_groups = toset(["api", "cli", "outbox", "reconciler", "scheduler", "worker", "migrate"])
}

resource "aws_cloudwatch_log_group" "this" {
  for_each          = local.log_groups
  name              = "/${var.name}/${each.key}"
  retention_in_days = var.log_retention_days
}
