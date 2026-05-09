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
    Desired ECS task count. See infra/envs/prod/variables.tf for the rationale
    on why >1 is now safe (post-2026-05-09 cache + lock changes).
  EOT
  type        = number
  default     = 0

  validation {
    condition     = var.desired_count >= 0 && var.desired_count <= 4
    error_message = "desired_count must be between 0 and 4."
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
