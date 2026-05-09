################################################################################
# botnim-api ECS task
#
# One Fargate task — the primary api container only (FastAPI on :8000).
# Elasticsearch sidecar + EFS data/cache volumes removed; the api now connects
# to the shared Aurora PostgreSQL cluster via the DB_* env vars below.
#
# Uses modules/app directly (new preferred pattern — see docs/shared-ecs-app-techdebt.md
# in buildup-org-infra). The `public = {...}` block wires up the shared ALB.
################################################################################

data "aws_ssm_parameter" "database_credentials_secret_arn" {
  name = "/buildup/projects/botnim/staging/database_credentials_secret_arn"
}

module "botnim_api" {
  source = "git::https://github.com/Build-Up-IL/org-infra.git//modules/app?ref=feat/ecs-efs-and-sidecars-v2"

  app_name       = "botnim-api"
  container_port = 8000
  container_name = "api"

  environment = var.environment

  image_tag     = var.image_tag
  desired_count = var.desired_count

  enable_autoscaling = false
  max_capacity       = 1
  min_capacity       = 1

  # Resource allocation: api only now (ES sidecar removed).
  # Total task = 1 vCPU, 3 GB.
  cpu    = 1024
  memory = 3072

  public = {
    # Shared hostname: both botnim-api and librechat live on botnim.<zone>.
    # botnim-api owns the DNS record for this host and gets /botnim/* routing.
    # librechat co-habits at the same host with /* catch-all and does NOT
    # create its own DNS record.
    subdomain         = "botnim"
    health_check_path = "/health"
    listener_priority = var.listener_priority
    path_patterns     = ["/botnim/*"]
  }

  # Publish botnim-api as an internal Service Connect endpoint so librechat
  # (same VPC, different Fargate task) can call it without hairpinning
  # through the public ALB. The public /botnim/* route above still works for
  # external callers; this just adds an in-VPC path for siblings.
  #
  # Defaults: discovery_name = app_name = "botnim-api", client_alias_port =
  # container_port = 8000, app_protocol = "http". In-cluster URL:
  #   http://botnim-api:8000
  #
  # See infra/envs/prod/main.tf for the rationale on raising these from the
  # 15s/5min defaults to 60s/120s — staging mirrors prod so we catch any
  # regression in vector-search latency before promotion.
  internal_server = {
    per_request_timeout_seconds = 60
    idle_timeout_seconds        = 120
  }

  enable_aurora_access = true

  environment_variables = merge(
    {
      ENVIRONMENT = var.environment
      # S3 bucket for /tools/generate_word_doc uploads. Bucket lifecycle
      # auto-purges objects after 7 days; presigned URLs are shorter-lived.
      WORD_DOCS_BUCKET = aws_s3_bucket.word_docs.id
    },
  )

  secret_arns = concat(
    [data.aws_ssm_parameter.database_credentials_secret_arn.value],
    [aws_secretsmanager_secret.word_docs_signer.arn],
  )

  secret_environment_variables = merge(
    {
      "OPENAI_API_KEY_${upper(var.environment)}" = aws_secretsmanager_secret.openai_api_key.arn
      # Consumed by backend/api/refresh_auth.py to authenticate the Lambda's
      # calls to /admin/refresh. Value is set out-of-band via Secrets Manager.
      BOTNIM_ADMIN_API_KEY        = aws_secretsmanager_secret.refresh_admin_api_key.arn
      BOTNIM_SANITY_ADMIN_API_KEY = aws_secretsmanager_secret.sanity_admin_api_key.arn
    },
    {
      DB_HOST     = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:host::"
      DB_PORT     = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:port::"
      DB_NAME     = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:dbname::"
      DB_USER     = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:username::"
      DB_PASSWORD = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:password::"
    },
    {
      # Long-lived IAM user creds for signing presigned URLs. See word_docs.tf.
      WORD_DOCS_SIGNING_AWS_ACCESS_KEY_ID     = "${aws_secretsmanager_secret.word_docs_signer.arn}:aws_access_key_id::"
      WORD_DOCS_SIGNING_AWS_SECRET_ACCESS_KEY = "${aws_secretsmanager_secret.word_docs_signer.arn}:aws_secret_access_key::"
    },
  )

  efs_volumes = [
    # Daily refresh job writes fresh extraction CSVs to this AP via the
    # /admin/refresh endpoint's background thread. Concurrent writes are
    # guarded by a postgres advisory lock (`backend/api/server.py`
    # `_REFRESH_LOCK_KEY`) so this is safe across desired_count > 1.
    {
      name               = "specs-extraction"
      file_system_id     = module.es_efs.file_system_id
      access_point_id    = module.es_efs.access_point_ids["specs-extraction"]
      transit_encryption = "ENABLED"
      iam_authorization  = "DISABLED"
      root_directory     = "/"
    },
  ]

  # Mount EFS volumes in the primary container. /srv/cache (sqlite KV caches)
  # was REMOVED on 2026-05-09 — see infra/envs/prod/main.tf for context.
  primary_container_mount_points = [
    {
      container_path = "/srv/specs/unified/extraction"
      source_volume  = "specs-extraction"
      read_only      = false
    },
  ]

  efs_security_group_ids = [module.es_efs.mount_target_security_group_id]

  # Combined inline policy attached to the api task role. Currently grants:
  #  - S3 read/write on the ES snapshots bucket (TODO(post-soak): remove after
  #    Window C closes — see backups.tf)
  #  - s3:PutObject on the word-docs bucket (see word_docs.tf) so
  #    /tools/generate_word_doc can upload rendered .docx artifacts.
  task_role_policy_json = data.aws_iam_policy_document.task_role.json
}

# Compose the per-feature IAM docs into the single JSON the upstream module
# accepts via task_role_policy_json. Keeps each feature's statements colocated
# with the resource it protects.
data "aws_iam_policy_document" "task_role" {
  source_policy_documents = [
    data.aws_iam_policy_document.es_backups_write.json,
    data.aws_iam_policy_document.word_docs_write.json,
  ]
}

################################################################################
# Private-zone alias for botnim.staging.build-up.team -> public ALB.
#
# Background: Route53 split-horizon DNS for staging.build-up.team. The public
# zone has botnim.staging.build-up.team (created automatically by org-infra's
# modules/app from public.subdomain), but the private zone (associated with
# the staging VPC) does NOT — it only had auth-admin until now. VPC-internal
# clients (LibreChat ECS tasks calling the OpenAPI tool) get NXDOMAIN because
# the private zone shadows the public one for the staging.build-up.team
# suffix. Adding the same alias in the private zone makes VPC clients resolve
# to the public ALB address (which is reachable from the VPC).
#
# Hairpin / asymmetric-routing concern: VPC -> public-ALB-IP traffic
# generally works on AWS as long as the security group allows the source
# CIDR. The shared ALB already accepts 0.0.0.0/0:443 so this works.
################################################################################
resource "aws_route53_record" "botnim_private" {
  zone_id = local.contract.operator_ingress.private_zone_id
  name    = "botnim.${trimsuffix(local.contract.dns.zone_name, ".")}"
  type    = "A"

  alias {
    name                   = local.contract.alb.dns_name
    zone_id                = local.contract.alb.zone_id
    evaluate_target_health = false
  }
}
