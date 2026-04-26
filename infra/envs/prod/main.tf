################################################################################
# botnim-api ECS task
#
# One Fargate task — the primary api container only (FastAPI on :8000).
# Elasticsearch sidecar + EFS data volume removed; the api now connects
# to the shared Aurora PostgreSQL cluster via the DB_* env vars below.
#
# Uses modules/app directly (new preferred pattern — see docs/shared-ecs-app-techdebt.md
# in buildup-org-infra). The `public = {...}` block wires up the shared ALB.
################################################################################

data "aws_ssm_parameter" "database_credentials_secret_arn" {
  name = "/buildup/projects/botnim/prod/database_credentials_secret_arn"
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
    health_check_path = "/health"
    host_headers      = ["botnim.build-up.team"]
    listener_priority = var.listener_priority
    path_patterns     = ["/botnim/*"]
  }

  enable_aurora_access = true

  environment_variables = {
    ENVIRONMENT = "production"
  }

  secret_arns = concat(
    [data.aws_ssm_parameter.database_credentials_secret_arn.value],
  )

  secret_environment_variables = merge(
    {
      OPENAI_API_KEY_PRODUCTION = aws_secretsmanager_secret.openai_api_key.arn
    },
    {
      DB_HOST     = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:host::"
      DB_PORT     = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:port::"
      DB_NAME     = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:dbname::"
      DB_USER     = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:username::"
      DB_PASSWORD = "${data.aws_ssm_parameter.database_credentials_secret_arn.value}:password::"
    },
  )

  # Grant S3 write access for on-demand Elasticsearch snapshots.
  # TODO(post-soak): remove after Window C closes (~T+30d) — ES backups bucket
  # and associated IAM policy can be decommissioned once the Aurora migration
  # soak period ends and we confirm no rollback to ES.
  task_role_policy_json = data.aws_iam_policy_document.es_backups_write.json
}
