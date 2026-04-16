################################################################################
# botnim-api ECS task
#
# One Fargate task with two containers:
#  1. api (primary) — FastAPI on :8000, handles /botnim/retrieve/* via ALB
#  2. elasticsearch (sidecar) — single-node ES, reachable at localhost:9200,
#     data persisted on EFS so it survives task restarts
#
# Uses modules/app directly (new preferred pattern — see docs/shared-ecs-app-techdebt.md
# in buildup-org-infra). The `public = {...}` block wires up the shared ALB.
################################################################################

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

  # Resource allocation: api is light; elasticsearch needs real memory.
  # Total task = 1 vCPU, 3 GB.
  cpu    = 1024
  memory = 3072

  public = {
    health_check_path = "/health"
    host_headers      = ["botnim.build-up.team"]
    listener_priority = var.listener_priority
    path_patterns     = ["/botnim/*"]
  }

  environment_variables = {
    ENVIRONMENT            = "production"
    ES_HOST_PRODUCTION     = "http://localhost:9200"
    ES_USERNAME_PRODUCTION = "elastic"
  }

  secret_environment_variables = {
    OPENAI_API_KEY_PRODUCTION = aws_secretsmanager_secret.openai_api_key.arn
    ES_PASSWORD_PRODUCTION    = aws_secretsmanager_secret.elasticsearch_password.arn
  }

  sidecar_containers = [
    {
      name  = "elasticsearch"
      image = var.elasticsearch_image

      environment = {
        "node.name"                         = "es01"
        "cluster.name"                      = "botnim-cluster"
        "discovery.type"                    = "single-node"
        "bootstrap.memory_lock"             = "true"
        "xpack.security.enabled"            = "true"
        "xpack.security.http.ssl.enabled"   = "false"
        "xpack.license.self_generated.type" = "basic"
        "ES_JAVA_OPTS"                      = "-Xms1g -Xmx1g"
      }

      secret_environment_variables = {
        ELASTIC_PASSWORD = aws_secretsmanager_secret.elasticsearch_password.arn
      }

      port_mappings = [] # localhost-only, no ALB exposure

      mount_points = [
        {
          container_path = "/usr/share/elasticsearch/data"
          source_volume  = "es-data"
          read_only      = false
        },
      ]

      health_check = {
        command = [
          "CMD-SHELL",
          "curl -s -u elastic:$ELASTIC_PASSWORD http://localhost:9200 | grep -q 'You Know, for Search'",
        ]
        interval     = 30
        retries      = 5
        start_period = 60
        timeout      = 10
      }

      cpu    = 512
      memory = 2048
    },
  ]

  efs_volumes = [
    {
      name               = "es-data"
      file_system_id     = module.es_efs.file_system_id
      access_point_id    = module.es_efs.access_point_ids["es-data"]
      transit_encryption = "ENABLED"
      iam_authorization  = "DISABLED"
      root_directory     = "/"
    },
  ]

  efs_security_group_ids = [module.es_efs.mount_target_security_group_id]

  # Grant S3 write access for on-demand Elasticsearch snapshots.
  task_role_policy_json = data.aws_iam_policy_document.es_backups_write.json
}
