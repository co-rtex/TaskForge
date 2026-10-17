# Two secrets, injected into containers with valueFrom (never in plain environment):
#
#   the database URL, built here with sslmode=require (ADR-0027 Q7: encrypted, not
#   verified, because the images carry no RDS CA bundle);
#   the worker key, created with NO value: the runbook mints it through the cli sidecar
#   and puts it with `aws secretsmanager put-secret-value`. A worker started before then
#   fails to start, by design.
#
# The database password is in Terraform's state; RUNBOOK.md requires an encrypted remote
# backend before any apply.

resource "aws_secretsmanager_secret" "database_url" {
  name                    = "${var.name}/database-url"
  recovery_window_in_days = var.secret_recovery_window_days
}

resource "aws_secretsmanager_secret_version" "database_url" {
  secret_id     = aws_secretsmanager_secret.database_url.id
  secret_string = "postgres://${aws_db_instance.this.username}:${random_password.db.result}@${aws_db_instance.this.address}:${aws_db_instance.this.port}/${aws_db_instance.this.db_name}?sslmode=require"
}

resource "aws_secretsmanager_secret" "worker_key" {
  name                    = "${var.name}/worker-key"
  recovery_window_in_days = var.secret_recovery_window_days
}
