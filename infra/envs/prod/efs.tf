################################################################################
# EFS filesystem for the Elasticsearch sidecar's data directory
#
# One access point (posix 1000:1000) owned by the elasticsearch container user.
# Survives task restarts; snapshots are written to the S3 bucket in backups.tf.
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
  ]
}
