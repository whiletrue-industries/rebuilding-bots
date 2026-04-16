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

  environment_variables = merge(
    {
      ENVIRONMENT      = var.environment
      # Populate Elasticsearch on container startup; handled by api_server.sh.
      # Idempotent (botnim sync --backend es recreates indices).
      SYNC_ON_STARTUP  = "1"
    },
    {
      "ES_HOST_${upper(var.environment)}"     = "http://localhost:9200"
      "ES_USERNAME_${upper(var.environment)}" = "elastic"
    },
  )

  secret_environment_variables = {
    "OPENAI_API_KEY_${upper(var.environment)}" = aws_secretsmanager_secret.openai_api_key.arn
    "ES_PASSWORD_${upper(var.environment)}"    = aws_secretsmanager_secret.elasticsearch_password.arn
  }

  sidecar_containers = [
    # init-clean-es: wipes the Elasticsearch data dir on every task start.
    # Why the big hammer: an ungraceful shutdown leaves multiple lock files
    # (/node.lock, /snapshot_cache/write.lock, etc.) that each block ES boot.
    # Since botnim sync re-creates all indices from source on startup, the
    # EFS is effectively cache-only — wiping it is safe and makes cold starts
    # deterministic. If we later add durable ES data we'd replace this with a
    # targeted lock cleanup plus a graceful shutdown hook.
    {
      name      = "init-clean-es"
      image     = "public.ecr.aws/docker/library/busybox:1.36"
      essential = false
      command = [
        "sh",
        "-c",
        "echo '[init-clean-es] removing stale ES data dir contents'; find /mnt/es-data -mindepth 1 -delete 2>/dev/null || true; ls -la /mnt/es-data; echo '[init-clean-es] done'",
      ]
      mount_points = [
        {
          container_path = "/mnt/es-data"
          source_volume  = "es-data"
          read_only      = false
        },
      ]
    },
    {
      name  = "elasticsearch"
      image = var.elasticsearch_image

      depends_on = [
        {
          container_name = "init-clean-es"
          condition      = "SUCCESS"
        },
      ]

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
  ]

  # Mount both EFS volumes in the primary container:
  #  - /mnt/es-data: lets api_server.sh touch the ES data dir (historical,
  #    no longer used now that init-clean-es handles lock cleanup, but kept
  #    until that split is cleaned up).
  #  - /srv/cache: the persistent sqlite KV caches described above.
  primary_container_mount_points = [
    {
      container_path = "/mnt/es-data"
      source_volume  = "es-data"
      read_only      = false
    },
    {
      container_path = "/srv/cache"
      source_volume  = "cache"
      read_only      = false
    },
  ]

  efs_security_group_ids = [module.es_efs.mount_target_security_group_id]

  # Grant S3 write access for on-demand Elasticsearch snapshots.
  task_role_policy_json = data.aws_iam_policy_document.es_backups_write.json
}
