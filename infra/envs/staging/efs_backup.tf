################################################################################
# AWS Backup for the botnim-api EFS filesystem
#
# Primary motivation: protect the /srv/cache access point (two sqlite KV stores
# for dynamic_extraction metadata + OpenAI embeddings). Losing /srv/cache forces
# another ~5.5 h cold sync; AWS Backup guarantees a point-in-time copy.
#
# The same plan also snapshots /es (the ES sidecar's Lucene data dir) but we
# do NOT rely on it for ES recovery. Lucene files are written continuously and
# a crash-consistent file-level snapshot can capture half-merged segments that
# refuse to boot. ES's native repository-s3 snapshot API is what we'll use for
# real ES recovery once that hook lands (see Monday task on post-sync snapshot).
# The EFS backup is belt-and-suspenders for the /cache side.
#
# Schedule: once daily at 03:00 UTC (quiet hour, staging). Retention: 30 days.
# No cold-storage transition — the filesystem is small enough that warm
# storage is cheaper than the pre-conversion floor cost.
################################################################################

resource "aws_backup_vault" "efs" {
  name = "botnim-api-${var.environment}"
  # No KMS key — AWS Backup auto-creates and manages a CMK per account per
  # region for the default vault. We don't need CMK-level access controls
  # here; the vault is scoped to this account and AWS Backup's service role.
}

resource "aws_backup_plan" "efs" {
  name = "botnim-api-${var.environment}-efs"

  rule {
    rule_name         = "daily"
    target_vault_name = aws_backup_vault.efs.name
    # 03:00 UTC every day. Far from any likely deploy window.
    schedule = "cron(0 3 * * ? *)"

    start_window      = 60  # minutes AWS Backup has to start the job
    completion_window = 180 # minutes to finish it

    lifecycle {
      delete_after = 30 # days; no cold transition
    }
  }
}

################################################################################
# Service role for AWS Backup to read/write EFS + tag resources
################################################################################

data "aws_iam_policy_document" "backup_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["backup.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "backup" {
  name               = "botnim-api-${var.environment}-efs-backup"
  assume_role_policy = data.aws_iam_policy_document.backup_assume.json
}

resource "aws_iam_role_policy_attachment" "backup" {
  role       = aws_iam_role.backup.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForBackup"
}

resource "aws_iam_role_policy_attachment" "backup_restores" {
  role       = aws_iam_role.backup.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForRestores"
}

################################################################################
# Which resources this plan protects
################################################################################

resource "aws_backup_selection" "efs" {
  name         = "botnim-api-${var.environment}-efs"
  plan_id      = aws_backup_plan.efs.id
  iam_role_arn = aws_iam_role.backup.arn

  resources = [
    # Single target: the shared EFS filesystem that holds both /es and /cache
    # access points. AWS Backup snapshots the whole filesystem, not individual
    # access points, so both access points end up in every recovery point.
    "arn:aws:elasticfilesystem:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:file-system/${module.es_efs.file_system_id}",
  ]
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}
