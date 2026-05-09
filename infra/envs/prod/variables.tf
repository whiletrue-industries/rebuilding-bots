variable "region" {
  description = "AWS region for the deployment (injected by terragrunt)."
  type        = string
  default     = "il-central-1"
}

variable "environment" {
  description = "Deployment environment name (injected by terragrunt)."
  type        = string
  default     = "prod"
}

variable "image_tag" {
  description = "Docker image tag to deploy from the module-managed ECR repository. Use 'bootstrap' for the first apply, then a real SHA."
  type        = string
  default     = "bootstrap"
}

variable "desired_count" {
  description = <<-EOT
    Desired ECS task count. Set to 0 for first bootstrap apply, then 2 (or
    higher) for real operation. The pre-2026-05-09 single-task constraint
    (sqlite-over-NFS cache at /srv/cache) is gone — the cache moved to Aurora
    and the /srv/cache mount was removed. Daily refresh + sanity background
    jobs are now guarded by a postgres advisory lock, so multiple tasks
    coordinate cleanly.
  EOT
  type        = number
  default     = 0

  validation {
    condition     = var.desired_count >= 0 && var.desired_count <= 4
    error_message = "desired_count must be between 0 and 4. The shared ALB target group has plenty of headroom; raise the cap here only if needed."
  }
}

variable "listener_priority" {
  description = "Unique ALB listener rule priority for /botnim/* path routing on the shared botnim.build-up.team host."
  type        = number
  default     = 100
}

variable "elasticsearch_image" {
  description = "Elasticsearch Docker image tag. Must match the version botnim code expects."
  type        = string
  default     = "docker.elastic.co/elasticsearch/elasticsearch:8.11.0"
}
