# One ECR repository per image (ADR-0028 Q4), with immutable tags and scan on push. The
# images are built by `make images` and pushed by the operator (RUNBOOK.md).

locals {
  images = toset(["api", "outbox", "scheduler", "reconciler", "worker", "migrate", "cli"])
}

resource "aws_ecr_repository" "this" {
  for_each             = local.images
  name                 = "${var.name}/${each.key}"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}
