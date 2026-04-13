################################################################################
# botnim-api ECS task
#
# Runs two containers in the same task definition:
#  1. botnim-api (primary) — FastAPI, port 8000, handles /botnim/retrieve/*
#  2. elasticsearch (sidecar) — single-node ES, reachable via localhost:9200
#
# The sidecar's data dir is mounted from an EFS filesystem so it survives
# task restarts. Task count is fixed at 1 (no horizontal scaling).
################################################################################

module "botnim_api" {
  source = "git::https://github.com/Build-Up-IL/org-infra.git//modules/shared-ecs-app?ref=feat/ecs-efs-and-sidecars"

  app_name          = "botnim-api"
  container_port    = 8000
  container_name    = "api"
  health_check_path = "/health"

  # Route /botnim/* on the shared botnim.build-up.team host to this task.
  host_headers      = ["botnim.build-up.team"]
  path_patterns     = ["/botnim/*"]
  listener_priority = var.listener_priority

  image_tag     = var.image_tag
  desired_count = var.desired_count

  # No horizontal autoscaling — the task owns stateful ES data on EFS.
  enable_autoscaling = false
  max_capacity       = 1
  min_capacity       = 1

  # Resource allocation: botnim-api itself is light (256/512) but
  # elasticsearch needs real memory. Total task = 1 vCPU, 3 GB.
  cpu    = 1024
  memory = 3072

  environment_variables = {
    ENVIRONMENT            = "production"
    ES_HOST_PRODUCTION     = "http://localhost:9200"
    ES_USERNAME_PRODUCTION = "elastic"
  }

  secret_environment_variables = {
    OPENAI_API_KEY_PRODUCTION = aws_secretsmanager_secret.openai_api_key.arn
    ES_PASSWORD_PRODUCTION    = aws_secretsmanager_secret.elasticsearch_password.arn
  }

  # Elasticsearch sidecar with EFS-backed data directory.
  sidecar_containers = [
    {
      name  = "elasticsearch"
      image = var.elasticsearch_image

      # ES needs root user to write the pid file, then drops to elasticsearch user.
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

      port_mappings = [] # not exposed to the ALB — only reachable via localhost

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
          "curl -s -u elastic:$ELASTIC_PASSWORD http://localhost:9200 | grep -q 'missing authentication credentials\\|You Know, for Search'",
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

  # Grant S3 write access for on-demand / scheduled ES snapshots.
  task_role_policy_json = data.aws_iam_policy_document.es_backups_write.json
}
