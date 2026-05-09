################################################################################
# Shared EFS filesystem for botnim-api
#
# Currently one POSIX-isolated access point in active use:
#  - specs-extraction: /srv/specs/unified/extraction in the primary container.
#                     Daily refresh job writes fresh CSVs here. On first deploy
#                     (empty EFS) api_server.sh seeds from the image-baked copy
#                     at /srv/specs-seed. Backed up via the same aws_backup_plan
#                     as the other APs (whole-filesystem snapshot).
#
# The `cache` access point used to back /srv/cache (sqlite KV caches for the
# legacy ES backend). Unmounted on 2026-05-09 — under the Aurora backend the
# embedding/metadata caches live in the `documents` table (delta-sync
# content_hash) and the `extraction_cache` table. The AP is kept here as a
# no-op so the next terragrunt apply doesn't churn EFS access points; a
# follow-up PR can drop it once we're confident no stale code paths attempt
# to mount /cache.
#
# Note: the es-data access point has been removed as part of the Aurora migration
# (the ES sidecar no longer runs). The EFS filesystem itself is retained for the
# specs-extraction AP.
# TODO(post-soak): remove after Window C closes (~T+30d) — evaluate whether
# the EFS filesystem itself can be decommissioned once the soak period ends.
################################################################################

module "es_efs" {
  source = "git::https://github.com/Build-Up-IL/org-infra.git//modules/ecs-app-efs?ref=feat/ecs-efs-and-sidecars-v2"

  name               = "botnim-api-es"
  vpc_id             = local.contract.network.vpc_id
  private_subnet_ids = local.contract.network.private_subnet_ids

  access_points = [
    {
      name      = "cache"
      path      = "/cache"
      posix_uid = 1000
      posix_gid = 1000
    },
    {
      name      = "specs-extraction"
      path      = "/specs-extraction"
      posix_uid = 1000
      posix_gid = 1000
    },
  ]
}
