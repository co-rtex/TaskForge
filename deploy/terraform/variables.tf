# Every input. Nothing here has a default that opens the system: ingress is closed until
# allowed_ingress_cidrs names who may reach the load balancer, and every service starts at
# zero tasks (RUNBOOK.md brings them up in order).

variable "aws_region" {
  description = "The AWS region to deploy into. Also the region the services' SQS and S3 clients resolve their endpoints from (TASKFORGE_BROKER_REGION, TASKFORGE_RESULTS_REGION)."
  type        = string
}

variable "name" {
  description = "Name prefix for every resource."
  type        = string
  default     = "taskforge"
}

variable "vpc_cidr" {
  description = "The VPC's CIDR block. Two public and two private /20 subnets are carved from it."
  type        = string
  default     = "10.40.0.0/16"
}

variable "acm_certificate_arn" {
  description = "ARN of an ACM certificate, in aws_region, for the load balancer's HTTPS listener. Required: there is no HTTP listener that serves traffic."
  type        = string
}

variable "allowed_ingress_cidrs" {
  description = "CIDR blocks allowed to reach the load balancer on 443 (and 80, which only redirects). Empty, the default, means closed."
  type        = list(string)
  default     = []
}

variable "ssl_policy" {
  description = "The HTTPS listener's security policy. The default is the one AWS recommends: TLS 1.3 and 1.2, with hybrid post-quantum key exchange."
  type        = string
  default     = "ELBSecurityPolicy-TLS13-1-2-Res-PQ-2025-09"
}

variable "image_tag" {
  description = "The tag of all seven images in their ECR repositories, as pushed. The repositories' tags are immutable."
  type        = string
}

variable "cpu_architecture" {
  description = "The CPU architecture every task runs on. Must match the architecture the images were built for."
  type        = string
  default     = "X86_64"

  validation {
    condition     = contains(["X86_64", "ARM64"], var.cpu_architecture)
    error_message = "cpu_architecture must be X86_64 or ARM64."
  }
}

variable "results_bucket_name" {
  description = "Name of the S3 bucket for large results (globally unique)."
  type        = string
}

variable "log_retention_days" {
  description = "Retention of every service's CloudWatch log group."
  type        = number
  default     = 30
}

variable "secret_recovery_window_days" {
  description = "How long a deleted secret can be recovered."
  type        = number
  default     = 7
}

# --- Desired counts: all zero until the runbook raises them in order. ----------

variable "api_desired_count" {
  description = "Number of api tasks."
  type        = number
  default     = 0
}

variable "outbox_desired_count" {
  description = "Number of outbox tasks."
  type        = number
  default     = 0
}

variable "reconciler_desired_count" {
  description = "Number of reconciler tasks."
  type        = number
  default     = 0
}

variable "scheduler_desired_count" {
  description = "Number of scheduler tasks."
  type        = number
  default     = 0
}

variable "worker_desired_count" {
  description = "Number of worker tasks."
  type        = number
  default     = 0
}

# --- Task sizes. -------------------------------------------------------------------

variable "task_cpu" {
  description = "Fargate CPU units per task, by service. The api task also holds the cli sidecar."
  type        = map(number)
  default = {
    api        = 512
    outbox     = 256
    reconciler = 256
    scheduler  = 256
    worker     = 512
    migrate    = 256
  }
}

variable "task_memory" {
  description = "Fargate memory (MiB) per task, by service."
  type        = map(number)
  default = {
    api        = 1024
    outbox     = 512
    reconciler = 512
    scheduler  = 512
    worker     = 1024
    migrate    = 512
  }
}

# --- The database. -------------------------------------------------------------------

variable "db_instance_class" {
  description = "RDS instance class."
  type        = string
  default     = "db.t4g.micro"
}

variable "db_engine_version" {
  description = "PostgreSQL major version; compose runs postgres:16-alpine."
  type        = string
  default     = "16"
}

variable "db_allocated_storage" {
  description = "RDS storage in GiB."
  type        = number
  default     = 20
}

variable "db_multi_az" {
  description = "Whether RDS runs a standby in a second AZ. Off by default, for cost (ADR-0028)."
  type        = bool
  default     = false
}

variable "db_backup_retention_days" {
  description = "Automated backup retention."
  type        = number
  default     = 7
}

variable "db_deletion_protection" {
  description = "RDS deletion protection. RUNBOOK.md's teardown turns it off deliberately."
  type        = bool
  default     = true
}
