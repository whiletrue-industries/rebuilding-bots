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
  internal_server = {}

  enable_aurora_access = true

  environment_variables = merge(
    {
      ENVIRONMENT = var.environment
    },
  )

  secret_arns = concat(
    [data.aws_ssm_parameter.database_credentials_secret_arn.value],
  )

  secret_environment_variables = merge(
    {
      "OPENAI_API_KEY_${upper(var.environment)}" = aws_secretsmanager_secret.openai_api_key.arn
      # Consumed by backend/api/refresh_auth.py to authenticate the Lambda's
      # calls to /admin/refresh. Value is set out-of-band via Secrets Manager.
      BOTNIM_ADMIN_API_KEY = aws_secretsmanager_secret.refresh_admin_api_key.arn
    },
    {
      DB_HOST     = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:host::"
      DB_PORT     = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:port::"
      DB_NAME     = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:dbname::"
      DB_USER     = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:username::"
      DB_PASSWORD = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:password::"
    },
  )

  efs_volumes = [
    # Persistent cache for botnim sync. Two sqlite KV stores live under
    # /srv/cache: `metadata` (dynamic_extraction schema results) and
    # `embedding` (OpenAI embedding vectors), both keyed on source content
    # hash. Warming this across task restarts turns a ~30-min cold sync into
    # a ~1-min warm sync — every doc that hasn't changed skips both OpenAI
    # round trips. api_server.sh creates the subdirs at runtime since the
    # mount shadows whatever the Dockerfile pre-created.
    #
    # SINGLE-WRITER CONSTRAINT: the cache is sqlite-over-NFS. That is safe
    # with exactly one writer and silently corrupts with concurrent writers
    # because NFS does not honor POSIX advisory locks the way a local FS
    # does. This is only OK while desired_count = 1 and no autoscaling.
    # If we ever need >1 task, this mount must be removed (or the caches
    # must move to a concurrency-safe store like DynamoDB/ElastiCache).
    {
      name               = "cache"
      file_system_id     = module.es_efs.file_system_id
      access_point_id    = module.es_efs.access_point_ids["cache"]
      transit_encryption = "ENABLED"
      iam_authorization  = "DISABLED"
      root_directory     = "/"
    },
    # Daily refresh job writes fresh extraction CSVs to this AP via the
    # /admin/refresh endpoint's background thread. Single-writer (api task
    # only) — same sqlite-over-NFS rationale as /srv/cache applies, though
    # this AP stores plain CSVs with no locking concerns.
    {
      name               = "specs-extraction"
      file_system_id     = module.es_efs.file_system_id
      access_point_id    = module.es_efs.access_point_ids["specs-extraction"]
      transit_encryption = "ENABLED"
      iam_authorization  = "DISABLED"
      root_directory     = "/"
    },
  ]

  # Mount EFS volumes in the primary container:
  #  - /srv/cache: the persistent sqlite KV caches described above.
  #  - /srv/specs/unified/extraction: the daily refresh job's output CSVs,
  #    persisted across task restarts so a new deploy doesn't lose fresh
  #    scrape results. Seeded from the image on first boot via
  #    seed_extraction_if_empty in api_server.sh.
  primary_container_mount_points = [
    {
      container_path = "/srv/cache"
      source_volume  = "cache"
      read_only      = false
    },
    {
      container_path = "/srv/specs/unified/extraction"
      source_volume  = "specs-extraction"
      read_only      = false
    },
  ]

  efs_security_group_ids = [module.es_efs.mount_target_security_group_id]

  # Grant S3 write access for on-demand Elasticsearch snapshots.
  # TODO(post-soak): remove after Window C closes (~T+30d) — ES backups bucket
  # and associated IAM policy can be decommissioned once the Aurora migration
  # soak period ends and we confirm no rollback to ES.
  task_role_policy_json = data.aws_iam_policy_document.es_backups_write.json
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
