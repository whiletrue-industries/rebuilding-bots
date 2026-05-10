output "service_connect_dns" {
  value       = "phoenix:6006"
  description = "Service Connect endpoint inside the cluster — clients (botnim-api, LibreChat) connect to this for OTLP ingest and GraphQL queries"
}

output "task_definition_arn" {
  value       = aws_ecs_task_definition.phoenix.arn
  description = "ARN of the Phoenix ECS task definition"
}

output "security_group_id" {
  value       = aws_security_group.phoenix.id
  description = "Security group ID for the Phoenix task (egress-only)"
}

output "service_arn" {
  value       = aws_ecs_service.phoenix.id
  description = "ARN/ID of the Phoenix ECS service"
}

output "log_group_name" {
  value       = aws_cloudwatch_log_group.phoenix.name
  description = "CloudWatch log group where Phoenix container logs are written"
}
