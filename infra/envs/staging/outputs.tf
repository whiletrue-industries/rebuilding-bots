output "app_url" {
  description = "Public HTTPS URL for the botnim API"
  value       = module.botnim_api.app_url
}

output "ecr_repository_url" {
  description = "ECR repository URL to push images to"
  value       = module.botnim_api.ecr_repository_url
}

output "task_role_arn" {
  description = "IAM task role ARN"
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

output "word_docs_bucket" {
  description = "S3 bucket for short-lived Word doc downloads (/tools/generate_word_doc uploads)"
  value       = aws_s3_bucket.word_docs.id
}

output "extraction_artifacts_bucket" {
  description = "S3 bucket backing the ArtifactStore (extraction seed/ + cache/ artifacts)"
  value       = aws_s3_bucket.extraction_artifacts.id
}
