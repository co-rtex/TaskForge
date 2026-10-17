# Security groups. Each rule is one resource, so the set is readable as a list:
#
#   ALB      443 and 80 in from var.allowed_ingress_cidrs (none by default); out to api:8090
#   api      8090 in from the ALB only; 8091 in from the workers only
#   tasks    out 443 (AWS APIs and ECR, through the NAT) and 5432 to the database only
#   worker   out 8091 to the api only (on top of tasks)
#   rds      5432 in from the tasks only
#
# DNS to the VPC's resolver is not filtered by security groups, so no rule names it.

resource "aws_security_group" "alb" {
  name        = "${var.name}-alb"
  description = "TaskForge load balancer"
  vpc_id      = aws_vpc.this.id
}

resource "aws_security_group" "api" {
  name        = "${var.name}-api"
  description = "TaskForge api tasks: the public and control listeners"
  vpc_id      = aws_vpc.this.id
}

resource "aws_security_group" "tasks" {
  name        = "${var.name}-tasks"
  description = "Every TaskForge task: egress to AWS APIs and the database"
  vpc_id      = aws_vpc.this.id
}

resource "aws_security_group" "worker" {
  name        = "${var.name}-worker"
  description = "TaskForge worker tasks: egress to the api's control listener"
  vpc_id      = aws_vpc.this.id
}

resource "aws_security_group" "rds" {
  name        = "${var.name}-rds"
  description = "TaskForge database"
  vpc_id      = aws_vpc.this.id
}

locals {
  alb_ingress = {
    for pair in setproduct(var.allowed_ingress_cidrs, [80, 443]) :
    "${pair[0]}:${pair[1]}" => { cidr = pair[0], port = pair[1] }
  }
}

resource "aws_vpc_security_group_ingress_rule" "alb" {
  for_each          = local.alb_ingress
  security_group_id = aws_security_group.alb.id
  description       = "HTTPS (and the HTTP redirect) from an allowed CIDR"
  cidr_ipv4         = each.value.cidr
  ip_protocol       = "tcp"
  from_port         = each.value.port
  to_port           = each.value.port
}

resource "aws_vpc_security_group_egress_rule" "alb_to_api" {
  security_group_id            = aws_security_group.alb.id
  description                  = "To the api's public listener"
  referenced_security_group_id = aws_security_group.api.id
  ip_protocol                  = "tcp"
  from_port                    = 8090
  to_port                      = 8090
}

resource "aws_vpc_security_group_ingress_rule" "api_public_from_alb" {
  security_group_id            = aws_security_group.api.id
  description                  = "Public listener, from the load balancer only"
  referenced_security_group_id = aws_security_group.alb.id
  ip_protocol                  = "tcp"
  from_port                    = 8090
  to_port                      = 8090
}

resource "aws_vpc_security_group_ingress_rule" "api_control_from_workers" {
  security_group_id            = aws_security_group.api.id
  description                  = "Control listener, from the workers only"
  referenced_security_group_id = aws_security_group.worker.id
  ip_protocol                  = "tcp"
  from_port                    = 8091
  to_port                      = 8091
}

resource "aws_vpc_security_group_egress_rule" "tasks_https" {
  security_group_id = aws_security_group.tasks.id
  description       = "AWS APIs (SQS, S3, Secrets Manager, ECR, CloudWatch Logs, SSM messages) through the NAT gateway"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

resource "aws_vpc_security_group_egress_rule" "tasks_postgres" {
  security_group_id            = aws_security_group.tasks.id
  description                  = "PostgreSQL"
  referenced_security_group_id = aws_security_group.rds.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}

resource "aws_vpc_security_group_egress_rule" "worker_to_control" {
  security_group_id            = aws_security_group.worker.id
  description                  = "The api's control listener"
  referenced_security_group_id = aws_security_group.api.id
  ip_protocol                  = "tcp"
  from_port                    = 8091
  to_port                      = 8091
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_tasks" {
  security_group_id            = aws_security_group.rds.id
  description                  = "PostgreSQL from TaskForge tasks only"
  referenced_security_group_id = aws_security_group.tasks.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}
