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
  description = "Phoenix Docker tag — pin to a known-good version"
}

variable "phoenix_image_repository" {
  type        = string
  default     = "arizephoenix/phoenix"
  # On 2026-05-24 prod started failing CannotPullContainerError with 429
  # toomanyrequests against registry-1.docker.io because Fargate's outbound
  # NAT shares an IP pool with everyone else in the region, and DockerHub's
  # unauthenticated rate limit (100 pulls / 6h / source IP) was being eaten
  # by neighbours. Overriding this to an in-account ECR mirror
  # (e.g. 086879295714.dkr.ecr.il-central-1.amazonaws.com/mirror/phoenix)
  # eliminates the DockerHub dependency entirely. See parlibot/CLAUDE.md
  # for the mirror-image recipe.
  description = "Container image repository (without tag). Defaults to the public DockerHub repo; override to an ECR mirror when DockerHub rate limits are biting."
}

variable "aurora_security_group_id" {
  type        = string
  description = "SG ID of the shared Aurora cluster (read from /buildup/shared/<env>/contract → aurora.security_group_id, owned by org-infra). The phoenix module appends an aws_security_group_rule on this SG to permit phoenix's task SG on TCP 5432, mirroring what org-infra's modules/app does automatically when enable_aurora_access=true."
}

variable "internal_service_clients_sg_id" {
  type        = string
  description = "SG ID of the cluster-wide internal-service-clients SG that botnim-api / librechat tasks attach to. Consumed by an aws_security_group_rule that allows TCP 6006 ingress on phoenix from that SG so Service Connect calls succeed (the SC sidecar forwards directly to the upstream ENI, which AWS still enforces SG ingress on). Sourced from /buildup/shared/<env>/contract → internal_services.client_security_group_id; defined in ../buildup-org-infra."
}

variable "extra_client_security_group_ids" {
  type        = list(string)
  default     = []
  description = "Additional task SGs allowed to reach phoenix:6006. Workaround for app modules that don't attach the cluster-wide internal-service-clients SG to their ENIs (e.g., the botnim-api task currently uses an org-infra modules/app ref that predates that auto-attach). For each SG in this list the phoenix module appends a parallel aws_security_group_rule alongside the canonical internal_service_clients_sg_id ingress. Remove the entry once the upstream module bumps its modules/app ref to one that includes the cluster-wide client SG."
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
