variable "image_tag" {
  description = "Docker image tag to deploy from the module-managed ECR repository. Use 'bootstrap' for the first apply, then a real SHA."
  type        = string
  default     = "bootstrap"
}

variable "desired_count" {
  description = "Desired ECS task count. Set to 0 for first bootstrap apply (before an image has been pushed), then 1 for real operation. Must stay at 1 — the task has a stateful Elasticsearch sidecar with an EFS volume that cannot be safely shared across tasks."
  type        = number
  default     = 0

  validation {
    condition     = var.desired_count >= 0 && var.desired_count <= 1
    error_message = "desired_count must be 0 or 1. Horizontal scaling is not supported for this task due to the Elasticsearch sidecar."
  }
}

variable "listener_priority" {
  description = "Unique ALB listener rule priority for /botnim/* path routing on the shared botnim.build-up.team host."
  type        = number
  default     = 100
}

variable "elasticsearch_image" {
  description = "Elasticsearch Docker image tag. Must match the version the botnim code expects."
  type        = string
  default     = "docker.elastic.co/elasticsearch/elasticsearch:8.11.0"
}
