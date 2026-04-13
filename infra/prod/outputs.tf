output "app_url" {
  description = "Public HTTPS URL for the botnim API"
  value       = module.botnim_api.app_url
}

output "ecr_repository_url" {
  description = "ECR repository URL to push images to"
  value       = module.botnim_api.ecr_repository_url
}

output "task_role_arn" {
  description = "IAM task role ARN (for debugging or adding extra permissions)"
  value       = module.botnim_api.task_role_arn
}

output "es_file_system_id" {
  description = "EFS filesystem ID backing the Elasticsearch data directory"
  value       = module.es_efs.file_system_id
}

output "es_backups_bucket" {
  description = "S3 bucket for Elasticsearch snapshots"
  value       = aws_s3_bucket.es_backups.id
}
