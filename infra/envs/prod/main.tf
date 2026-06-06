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
    # Shared hostname: both botnim-api and librechat live on botnim.<zone>.
    # botnim-api owns the DNS record for this host and gets /botnim/* routing.
    # librechat co-habits at the same host with /* catch-all and does NOT
    # create its own DNS record. Using `subdomain` (vs `host_headers`) is what
    # triggers org-infra's auto-wiring of the service-connect ingress SG.
    subdomain = "botnim"
    # subdomain is kept (drives the build-up.team DNS record + the auto-wiring
    # noted above); host_headers is added so the listener rule also matches the
    # legacy botnim.co.il host (served via the shared ALB + an ACM SNI cert).
    # host_headers replaces the default [<fqdn>], so botnim.build-up.team MUST
    # stay in the list.
    host_headers      = ["botnim.build-up.team", "botnim.co.il", "www.botnim.co.il"]
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
  # The SC Envoy sidecar fronts ALL inbound traffic on port 8000 (ALB →
  # ENI:8000 is iptables-redirected to Envoy → localhost). Envoy's default
  # per-request timeout is 15s — too tight for vector ANN + LLM-tool calls
  # under shared-Aurora load (we hit this on 2026-05-09 producing 504s with
  # target_processing_time clustered exactly at 15s). Raise to 60s, well
  # below the ALB idle timeout of 120s, so saturated DB queries return a
  # real response instead of a synthetic gateway timeout.
  internal_server = {
    per_request_timeout_seconds = 60
    idle_timeout_seconds        = 120
  }

  enable_aurora_access = true

  environment_variables = {
    ENVIRONMENT = "production"
    # S3 bucket for /tools/generate_word_doc uploads. Bucket lifecycle
    # auto-purges objects after 7 days; presigned URLs are shorter-lived.
    WORD_DOCS_BUCKET = aws_s3_bucket.word_docs.id
    # S3 bucket backing the ArtifactStore (extraction seed/ + cache/). When
    # set, botnim.storage.get_artifact_store() selects S3Store(bucket);
    # unset it silently falls back to an ephemeral LocalFsStore, so this
    # var is what actually activates the EFS->S3 migration in this env.
    BOTNIM_ARTIFACT_BUCKET = aws_s3_bucket.extraction_artifacts.id
    # S3Store passes region_name straight to boto3 and does NOT default it
    # (unlike word_doc/storage.py, which hard-defaults il-central-1). Set it
    # explicitly so the client always uses the il-central-1 regional
    # endpoint required for SigV4 there, regardless of IMDS region lookup.
    AWS_REGION = var.region
    # 2026-05-26: disable the per-run extraction LLM-call ceiling. The
    # default 5000 (botnim/_concurrency.py:DEFAULT_LLM_CALL_CEILING) cut
    # the daily refresh short on backlog days when cache misses spike;
    # since fap-sync now uses its own OpenAI key
    # (OPENAI_API_KEY_PRODUCTION_FAP_SYNC) the cost-isolation arg for the
    # cap is gone. 0 disables the circuit breaker.
    EXTRACTION_MAX_LLM_CALLS_PER_RUN = "0"
  }

  secret_arns = concat(
    [data.aws_ssm_parameter.database_credentials_secret_arn.value],
    [aws_secretsmanager_secret.word_docs_signer.arn],
  )

  secret_environment_variables = merge(
    {
      OPENAI_API_KEY_PRODUCTION = aws_secretsmanager_secret.openai_api_key.arn
      # Dedicated key the daily refresh uses while inside
      # botnim.config.fap_sync_context. Falls back to OPENAI_API_KEY_PRODUCTION
      # transparently if the secret value is unset, so the refresh still runs
      # before the secret is populated.
      OPENAI_API_KEY_PRODUCTION_FAP_SYNC = aws_secretsmanager_secret.openai_api_key_fap_sync.arn
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
      # Long-lived IAM user creds used by botnim/word_doc/storage.py to sign
      # presigned URLs. Avoids the STS-token bloat that breaks LibreChat's
      # markdown renderer on URLs >2KB. See word_docs.tf rationale.
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

  # Mount EFS volumes in the primary container.
  #  - /srv/cache (sqlite KV caches): REMOVED on 2026-05-09. Used to back the
  #    legacy ES-backend embedding/metadata caches; under the Aurora backend
  #    those caches live in `documents.content_hash` (delta sync) and the
  #    `extraction_cache` table. Removing the mount lifts the single-writer
  #    constraint that previously pinned us to desired_count=1.
  #  - /srv/specs/unified/extraction: the daily refresh job's output CSVs,
  #    persisted across task restarts so a new deploy doesn't lose fresh
  #    scrape results. Seeded from the image on first boot via
  #    seed_extraction_if_empty in api_server.sh.
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
    data.aws_iam_policy_document.extraction_artifacts_rw.json,
    data.aws_iam_policy_document.extraction_artifacts_kms.json,
  ]
}
