output "alb_dns_name" {
  description = "The load balancer's DNS name; point the certificate's domain at it."
  value       = aws_lb.this.dns_name
}

output "cluster_name" {
  description = "The ECS cluster."
  value       = aws_ecs_cluster.this.name
}

output "service_names" {
  description = "The ECS services, by binary."
  value       = merge({ api = aws_ecs_service.api.name }, { for k, s in aws_ecs_service.background : k => s.name })
}

output "task_definition_families" {
  description = "The task definition families, by binary."
  value       = { for k, td in aws_ecs_task_definition.this : k => td.family }
}

output "migrate_task_definition" {
  description = "The migrate task definition, run once per release with aws ecs run-task (RUNBOOK.md)."
  value       = aws_ecs_task_definition.this["migrate"].arn
}

output "private_subnet_ids" {
  description = "Where run-task places the migrate task."
  value       = aws_subnet.private[*].id
}

output "task_security_group_id" {
  description = "The security group run-task gives the migrate task."
  value       = aws_security_group.tasks.id
}

output "ecr_repository_urls" {
  description = "Where to push each image, by image name."
  value       = { for k, repo in aws_ecr_repository.this : k => repo.repository_url }
}

output "worker_key_secret_arn" {
  description = "The worker key secret, created empty; RUNBOOK.md puts its value."
  value       = aws_secretsmanager_secret.worker_key.arn
}

output "operator_exec_policy_arn" {
  description = "Attach to whoever runs the bootstrap: ECS Exec into the cli container only."
  value       = aws_iam_policy.operator_exec.arn
}
