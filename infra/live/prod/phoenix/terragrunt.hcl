################################################################################
# Phoenix LLM-tracing collector — prod stack
#
# Instantiates infra/modules/phoenix for the prod environment.
#
# Convention note: see infra/live/staging/phoenix/terragrunt.hcl for the
# rationale on why this stack is self-contained (does not include root.hcl).
# Short version: the root.hcl environment auto-detection uses
# basename(path_relative_to_include()) which yields "phoenix" for this path,
# breaking the state_buckets map lookup. We define state + provider inline.
#
# Org-infra contract values (vpc_id, private_subnet_ids, kms_key_arn) are read
# from /buildup/shared/prod/contract via run_cmd — same parameter that
# infra/envs/prod/data.tf reads as a Terraform data source.
#
# Prerequisites (operator must satisfy before `terragrunt plan`):
#   1. aws sso login --profile anubanu-prod
#   2. botnim/prod/phoenix-db-url secret created in Secrets Manager:
#        postgresql://phoenix_app:<pw>@<aurora-prod-writer>:5432/phoenix
#      (Task A2 operator gate — the phoenix DB + role must exist on prod Aurora
#      first; follow the prod manual seed recipe in CLAUDE.md if needed)
#   3. The /buildup/shared/prod/contract SSM parameter exists and is readable.
#   4. Confirm the service_connect_namespace_arn local below matches the
#      real namespace for the prod buildup-shared cluster:
#        aws --profile anubanu-prod servicediscovery list-namespaces \
#          --query "Namespaces[?Name=='buildup-shared'].Arn" --output text
#   5. The prod ECS execute-command guard in CLAUDE.md applies: the KMS-
#      encrypted CloudWatch log group issue means operator probing after apply
#      uses aws ssm start-session port-forward (not execute-command).
################################################################################

locals {
  env    = "prod"
  region = "il-central-1"

  # State bucket for prod — mirrors the state_buckets map in root.hcl.
  state_bucket = "buildup-org-tfstate-prod"

  # AWS account ID for prod. Used to construct the Secrets Manager ARN.
  # Verify with: aws --profile anubanu-prod sts get-caller-identity
  aws_account_id = "REPLACE_WITH_PROD_ACCOUNT_ID"

  # ECS cluster + Service Connect namespace are read from the prod platform
  # contract (see local.contract below). Discovery on staging on 2026-05-10
  # confirmed the cluster name is per-env (buildup-staging on staging,
  # presumably buildup-prod on prod) — older docs that said "buildup-shared"
  # are stale. Reading from contract.ecs.cluster_name and
  # contract.internal_services.cloud_map_namespace_arn makes this stack
  # automatically follow whatever org-infra publishes.

  # Secrets Manager ARN for the Phoenix DB connection string.
  # Secret must be created out-of-band before apply.
  # Secret name: botnim/prod/phoenix-db-url
  # Secret value format: postgresql://phoenix_app:<pw>@<aurora-writer>:5432/phoenix
  phoenix_db_secret_arn = "arn:aws:secretsmanager:${local.region}:${local.aws_account_id}:secret:botnim/prod/phoenix-db-url"

  # Read the platform contract from SSM at terragrunt parse time.
  _contract_json = run_cmd(
    "--terragrunt-quiet",
    "aws", "--profile", "anubanu-prod",
    "ssm", "get-parameter",
    "--name", "/buildup/shared/prod/contract",
    "--with-decryption",
    "--query", "Parameter.Value",
    "--output", "text",
  )
  contract = jsondecode(local._contract_json)
}

# State backend — separate state file from the botnim-api prod stack.
remote_state {
  backend = "s3"

  generate = {
    path      = "backend_generated.tf"
    if_exists = "overwrite_terragrunt"
  }

  config = {
    bucket                 = local.state_bucket
    key                    = "projects/botnim-api/prod/phoenix/terraform.tfstate"
    region                 = local.region
    encrypt                = true
    skip_region_validation = true
    use_lockfile           = true
  }
}

# Provider — generates provider "aws" only (no terraform {} block).
# The phoenix module's main.tf already declares required_providers; generating
# another terraform { required_providers } block here would cause a "duplicate
# required providers configuration" error at terraform init. Terragrunt's own
# version constraint is satisfied by the module; we only need the provider
# configuration (region + default_tags) injected at the live-stack layer.
generate "provider" {
  path      = "provider_generated.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<-EOF
    provider "aws" {
      region = "${local.region}"

      default_tags {
        tags = {
          Project     = "botnim-api"
          Environment = "${local.env}"
          ManagedBy   = "terragrunt"
          Component   = "phoenix-tracing"
        }
      }
    }
  EOF
}

# Module source — phoenix module in this repo.
terraform {
  source = "${get_repo_root()}//infra/modules/phoenix"
}

inputs = {
  env = local.env

  # ECS cluster + Service Connect namespace — read from prod platform contract.
  cluster_name                  = local.contract.ecs.cluster_name
  service_connect_namespace_arn = local.contract.internal_services.cloud_map_namespace_arn

  # Network — from /buildup/shared/prod/contract SSM parameter.
  vpc_id     = local.contract.network.vpc_id
  subnet_ids = local.contract.network.private_subnet_ids

  # CMK used for Secrets Manager secrets in prod.
  secrets_kms_key_arn = local.contract.ecs.kms_key_arn

  # SG that botnim-api + librechat tasks attach to as Service Connect clients.
  # Phoenix opens TCP 6006 ingress from this SG so SC sidecar calls actually
  # reach phoenix's task ENI (without it: SC reports "no healthy upstream"
  # → trace-fetch route returns 502). Owned by org-infra (see
  # ../buildup-org-infra), exposed via /buildup/shared/prod/contract.
  internal_service_clients_sg_id = local.contract.internal_services.client_security_group_id

  # Phoenix DB secret — set by Task A2 operator gate.
  phoenix_db_secret_arn = local.phoenix_db_secret_arn

  # Resource sizing — prod same as staging for v1; scale after soak.
  task_cpu    = 512
  task_memory = 1024

  # Explicit pin — prevents ad-hoc `terragrunt apply` calls from silently
  # tracking future module-default bumps. Update during intentional upgrades.
  # NOTE: variable is `phoenix_image_tag`, not the more natural `image_tag`.
  # deploy.sh exports TF_VAR_image_tag (the git short-SHA used for botnim-api
  # / librechat ECR images) repo-wide; the bare name collided and Terragrunt
  # silently rendered `arizephoenix/phoenix:<git-sha>` → CannotPullContainerError.
  phoenix_image_tag = "version-7.0.0"

  # Defense-in-depth: Phoenix must never be on the public internet.
  # The module already defaults this to false and enforces it via a validation
  # block; we repeat it here so this stack's intent is visible without reading
  # the module source.
  expose_publicly = false
}
