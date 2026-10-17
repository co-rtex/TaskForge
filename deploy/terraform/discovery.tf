# Cloud Map private DNS (ADR-0027 Q4): the api ECS service registers its tasks as
# api.taskforge.internal, the exact name workers dial and TASKFORGE_API_CONTROL_HOSTS
# allows (ADR-0026).

resource "aws_service_discovery_private_dns_namespace" "this" {
  name = "taskforge.internal"
  vpc  = aws_vpc.this.id
}

resource "aws_service_discovery_service" "api" {
  name = "api"

  dns_config {
    namespace_id   = aws_service_discovery_private_dns_namespace.this.id
    routing_policy = "MULTIVALUE"

    dns_records {
      type = "A"
      ttl  = 10
    }
  }
}
