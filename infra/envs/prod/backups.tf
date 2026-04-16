################################################################################
# S3 bucket for Elasticsearch snapshots
################################################################################

resource "aws_s3_bucket" "es_backups" {
  bucket = "botnim-api-es-backups-${var.environment}"
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
  statement {
    sid    = "WriteBackups"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.es_backups.arn,
      "${aws_s3_bucket.es_backups.arn}/*",
    ]
  }
}
