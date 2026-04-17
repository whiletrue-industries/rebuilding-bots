################################################################################
# S3 bucket for Elasticsearch snapshots
################################################################################

locals {
  es_backups_bucket_name = "botnim-api-es-backups-${var.environment}"
  es_backups_bucket_arn  = "arn:aws:s3:::${local.es_backups_bucket_name}"
}

resource "aws_s3_bucket" "es_backups" {
  bucket = local.es_backups_bucket_name
}

resource "aws_s3_bucket_public_access_block" "es_backups" {
  bucket                  = aws_s3_bucket.es_backups.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "es_backups" {
  bucket = aws_s3_bucket.es_backups.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "es_backups" {
  bucket = aws_s3_bucket.es_backups.id

  rule {
    id     = "glacier-then-expire"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = 30
      storage_class = "GLACIER"
    }

    expiration {
      days = 90
    }
  }
}

data "aws_iam_policy_document" "es_backups_write" {
  # Actions required by Elasticsearch's built-in repository-s3 module so
  # the botnim-api container can register an S3 repository and take
  # snapshots at cold-sync time and on its 6-h cron loop. The SDK inside
  # ES discovers creds from the ECS container credentials endpoint using
  # the task role to which this policy is attached; no static keys.
  statement {
    sid    = "ESSnapshotBucket"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [local.es_backups_bucket_arn]
  }

  statement {
    sid    = "ESSnapshotObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${local.es_backups_bucket_arn}/*"]
  }
}
