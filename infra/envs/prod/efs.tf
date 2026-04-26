################################################################################
# EFS filesystem for botnim-api
#
# The es-data access point has been removed as part of the Aurora migration
# (the ES sidecar no longer runs in production). The EFS filesystem module is
# retained here because backups.tf still references module.es_efs.file_system_id
# for AWS Backup.
#
# TODO(post-soak): after Window C closes (~T+30d), evaluate decommissioning
# this EFS filesystem entirely if no access points remain and AWS Backup can
# be pointed at a different target (or removed).
################################################################################

module "es_efs" {
  source = "git::https://github.com/Build-Up-IL/org-infra.git//modules/ecs-app-efs?ref=feat/ecs-efs-and-sidecars-v2"

  name               = "botnim-api-es"
  vpc_id             = local.contract.network.vpc_id
  private_subnet_ids = local.contract.network.private_subnet_ids

  access_points = []
}
