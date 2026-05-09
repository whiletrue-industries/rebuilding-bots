################################################################################
# Shared EFS filesystem for botnim-api — see infra/envs/prod/efs.tf for details.
# /srv/cache (sqlite KV caches) was unmounted on 2026-05-09 along with the SC
# timeout + desired_count=2 changes; see prod/efs.tf for the rationale.
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
