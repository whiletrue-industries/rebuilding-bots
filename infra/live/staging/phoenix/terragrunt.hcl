################################################################################
# Phoenix LLM-tracing collector — staging stack
#
# Instantiates infra/modules/phoenix for the staging environment.
#
# Convention note: this stack lives at infra/live/staging/phoenix/ — one level
# deeper than the flat live/<env>/terragrunt.hcl pattern used by the botnim-api
# stacks. The root root.hcl auto-detection derives environment from
# basename(path_relative_to_include()), which would yield "phoenix" here and
# break the state_buckets map. To avoid that, this stack is intentionally
# self-contained: it defines its own remote_state and generate "provider"
# blocks instead of including root.hcl. This is safe — the botnim-api and
# phoenix stacks are entirely independent Terraform state files.
#
# Org-infra contract values (vpc_id, private_subnet_ids, kms_key_arn) are read
# from the /buildup/shared/staging/contract SSM parameter via run_cmd at
# terragrunt parse time — the same parameter that infra/envs/staging/data.tf
# reads via a Terraform data source. Using run_cmd here avoids creating a
# separate infra/envs/ wrapper directory just for SSM reads.
#
# Prerequisites (operator must satisfy before `terragrunt plan`):
#   1. aws sso login --profile anubanu-staging
#   2. botnim/staging/phoenix-db-url secret created in Secrets Manager:
#        postgresql://phoenix_app:<pw>@<aurora-writer>:5432/phoenix
#      (Task A2 operator gate — the phoenix DB + role must exist first)
#   3. The /buildup/shared/staging/contract SSM parameter exists and contains
#      a JSON object with network.vpc_id, network.private_subnet_ids, and
#      ecs.kms_key_arn fields (written by buildup-org-infra — should already
#      exist in any running staging env).
#   4. Confirm the service_connect_namespace_arn local below matches the
#      real namespace for buildup-shared:
#        aws --profile anubanu-staging servicediscovery list-namespaces \
#          --query "Namespaces[?Name=='buildup-shared'].Arn" --output text
################################################################################

locals {
  env    = "staging"
  region = "il-central-1"

  # State bucket for staging — mirrors the state_buckets map in root.hcl.
  state_bucket = "buildup-org-tfstate-staging"

  # AWS account ID for staging. Used to construct the Secrets Manager ARN
  # without an AWS data source call (avoids a provider-init dependency at
  # `terragrunt validate-inputs` time).
  aws_account_id = "377114444836"

  # ECS cluster + Service Connect namespace are read from the platform
  # contract (see local.contract below). Discovery on 2026-05-10 confirmed
  # the actual cluster name is "buildup-staging" (NOT "buildup-shared" as
  # CLAUDE.md and the original plan suggested — those references are stale)
  # and the namespace is "buildup-staging.local" (DNS_PRIVATE).
  # Reading both from the contract makes this stack automatically follow
  # any future cluster/namespace renames done by org-infra.

  # Secrets Manager ARN for the Phoenix DB connection string.
  # Secret must be created out-of-band (Task A2 operator gate) before apply.
  # Secret name: botnim/staging/phoenix-db-url
  # Secret value format: postgresql://phoenix_app:<pw>@<aurora-writer>:5432/phoenix
  phoenix_db_secret_arn = "arn:aws:secretsmanager:${local.region}:${local.aws_account_id}:secret:botnim/staging/phoenix-db-url"

  # Read the platform contract from SSM at terragrunt parse time.
  # run_cmd is the standard terragrunt pattern for values not exposed as
  # sibling stack outputs. The contract contains vpc_id, private_subnet_ids,
  # and ecs.kms_key_arn — the same fields read by infra/envs/staging/data.tf.
  _contract_json = run_cmd(
    "--terragrunt-quiet",
    "aws", "--profile", "anubanu-staging",
    "ssm", "get-parameter",
    "--name", "/buildup/shared/staging/contract",
    "--with-decryption",
    "--query", "Parameter.Value",
    "--output", "text",
  )
  contract = jsondecode(local._contract_json)
}

# State backend — separate state file from the botnim-api staging stack.
remote_state {
  backend = "s3"

  generate = {
    path      = "backend_generated.tf"
    if_exists = "overwrite_terragrunt"
  }

  config = {
    bucket                 = local.state_bucket
    key                    = "projects/botnim-api/staging/phoenix/terraform.tfstate"
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

  # ECS cluster + Service Connect namespace — read from platform contract.
  cluster_name                  = local.contract.ecs.cluster_name
  service_connect_namespace_arn = local.contract.internal_services.cloud_map_namespace_arn

  # Network — from /buildup/shared/staging/contract SSM parameter.
  vpc_id     = local.contract.network.vpc_id
  subnet_ids = local.contract.network.private_subnet_ids

  # CMK used for Secrets Manager secrets in this env. The phoenix module's
  # task exec role gets kms:Decrypt scoped to this key via ViaService condition.
  secrets_kms_key_arn = local.contract.ecs.kms_key_arn

  # SG that botnim-api + librechat tasks attach to as Service Connect clients.
  # The phoenix module opens TCP 6006 ingress from this SG so SC sidecar calls
  # actually reach phoenix's task ENI (without it: SC reports "no healthy
  # upstream" → trace-fetch route returns 502). Owned by org-infra (see
  # ../buildup-org-infra), exposed via /buildup/shared/staging/contract.
  internal_service_clients_sg_id = local.contract.internal_services.client_security_group_id

  # Workaround: the botnim-api task ENI doesn't carry the cluster-wide
  # internal-service-clients SG (its app module ref
  # `feat/ecs-efs-and-sidecars-v2` predates the auto-attach that
  # LibreChat's `648976c` ref includes), so the canonical ingress rule
  # above silently denies bot→phoenix traffic. Every OTLP export from
  # botnim-api was failing without a visible error — Phoenix logs only
  # ever showed LibreChat's spans. Allowlist the bot's task SG directly
  # until the bot module ref is bumped. The SG id is hardcoded because
  # it's the long-lived task SG created once by org-infra modules/app;
  # if the bot ever rebuilds its SG (terraform replace), this needs an
  # update.
  extra_client_security_group_ids = ["sg-06e042ecc3b57aa9b"]

  # Shared Aurora cluster's SG. Phoenix module appends a rule on this SG
  # allowing phoenix's task SG on 5432 — Aurora's ingress allowlist is
  # explicit per task SG (NOT internal-service-clients-sg-based), so phoenix
  # needs its own entry. Org-infra owns the SG itself; we only add an
  # additive aws_security_group_rule.
  aurora_security_group_id = local.contract.aurora.security_group_id

  # Phoenix DB secret — set by Task A2 operator gate.
  phoenix_db_secret_arn = local.phoenix_db_secret_arn

  # Resource sizing — 512/1024 ran out of headroom for the GraphQL/UI side
  # after a few days of accumulated traces (OTLP POST /v1/traces continued
  # ingesting fine but all GET/POST to /graphql, /healthz, / returned 504
  # at the service-connect upstream-timeout boundary). Bumped to 1 vCPU /
  # 2 GiB on 2026-05-14 so the GraphQL handler has the headroom it needs.
  task_cpu    = 1024
  task_memory = 2048

  # Explicit pin — prevents ad-hoc `terragrunt apply` calls from silently
  # tracking future module-default bumps. Update during intentional upgrades.
  # NOTE: variable is `phoenix_image_tag`, not the more natural `image_tag`.
  # deploy.sh exports TF_VAR_image_tag (the git short-SHA used for botnim-api
  # / librechat ECR images) repo-wide; the bare name collided and Terragrunt
  # silently rendered `arizephoenix/phoenix:<git-sha>` → CannotPullContainerError.
  #
  # 2026-05-14: bumped from version-7.0.0 → version-15.9.0 to pick up the fix
  # for arize-ai/phoenix#4006 ("graphql queries become slow as data
  # increases", PR #4159 merged 2024-08-07). On v7.0.0 every GET/POST to
  # /graphql, /healthz, / returned 504 at the service-connect 15s upstream
  # timeout while POST /v1/traces continued ingesting fine — exact match
  # for the upstream bug.
  phoenix_image_tag = "version-15.9.0"

  # Defense-in-depth: Phoenix must never be on the public internet.
  # The module already defaults this to false and enforces it via a validation
  # block; we repeat it here so this stack's intent is visible without reading
  # the module source.
  expose_publicly = false
}
