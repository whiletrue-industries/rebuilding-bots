variable "env" {
  type        = string
  description = "Deployment env: staging | prod"
  validation {
    condition     = contains(["staging", "prod"], var.env)
    error_message = "env must be staging or prod"
  }
}

variable "cluster_name" {
  type        = string
  description = "Name of the ECS cluster (typically buildup-shared) the service registers in"
}

variable "vpc_id" {
  type        = string
  description = "VPC the service runs in"
}

variable "subnet_ids" {
  type        = list(string)
  description = "Private subnets for the Fargate task ENI"
}

variable "service_connect_namespace_arn" {
  type        = string
  description = "ARN of the Service Connect namespace (typically the buildup-shared namespace)"
}

variable "phoenix_db_secret_arn" {
  type        = string
  description = "Secrets Manager secret containing the full PHOENIX_SQL_DATABASE_URL (postgresql://phoenix_app:<pw>@<aurora-writer>:5432/phoenix)"
}

variable "task_cpu" {
  type        = number
  default     = 512
  description = "Fargate task CPU units"
}

variable "task_memory" {
  type        = number
  default     = 1024
  description = "Fargate task memory MiB"
}

variable "phoenix_image_tag" {
  type        = string
  default     = "version-7.0.0"
  # Renamed from `image_tag` so it cannot be shadowed by deploy.sh's
  # repo-wide `export TF_VAR_image_tag=$IMAGE_TAG` (the git short-SHA
  # used for botnim-api / librechat ECR images). When the names collided,
  # terragrunt silently rendered `arizephoenix/phoenix:<git-sha>` and the
  # Phoenix task failed with CannotPullContainerError every deploy.
  description = "arizephoenix/phoenix Docker tag — pin to a known-good version"
}

variable "secrets_kms_key_arn" {
  type        = string
  default     = ""
  description = "Optional CMK ARN that encrypts phoenix_db_secret. When non-empty, the task exec role gets kms:Decrypt scoped to this key with a kms:ViaService condition for Secrets Manager. REQUIRED whenever the secret is CMK-encrypted (the default for new env-scoped secrets in buildup-shared)."
}

# HARD PIN — phoenix has no public surface. Flipping this is a code change reviewed by a human.
variable "expose_publicly" {
  type        = bool
  default     = false
  description = "Hard-pinned to false. Phoenix is internal-only — Service Connect ingress only, no ALB."
  validation {
    condition     = var.expose_publicly == false
    error_message = "expose_publicly is hard-pinned to false. Phoenix must never be on the public internet. Flipping requires removing this validation block in code review."
  }
}
