################################################################################
# Shared EFS filesystem for botnim-api
#
# Three POSIX-isolated access points on one filesystem:
#  - es-data:         /usr/share/elasticsearch/data for the ES sidecar
#                     (init-clean-es wipes this on every task start; see main.tf).
#  - cache:           /srv/cache in the primary api container — sqlite KV caches
#                     (metadata extraction + embeddings) warmed across task restarts
#                     so the first botnim sync after a deploy is the only expensive one.
#                     SAFE ONLY WHILE desired_count = 1; see main.tf for the reason.
#  - specs-extraction: /srv/specs/unified/extraction in the primary container.
#                     Daily refresh job writes fresh CSVs here. On first deploy
#                     (empty EFS) api_server.sh seeds from the image-baked copy
#                     at /srv/specs-seed. Backed up via the same aws_backup_plan
#                     as the other APs (whole-filesystem snapshot).
################################################################################

module "es_efs" {
  source = "git::https://github.com/Build-Up-IL/org-infra.git//modules/ecs-app-efs?ref=feat/ecs-efs-and-sidecars-v2"

  name               = "botnim-api-es"
  vpc_id             = local.contract.network.vpc_id
  private_subnet_ids = local.contract.network.private_subnet_ids

  access_points = [
    {
      name      = "es-data"
      path      = "/es"
      posix_uid = 1000
      posix_gid = 1000
    },
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
