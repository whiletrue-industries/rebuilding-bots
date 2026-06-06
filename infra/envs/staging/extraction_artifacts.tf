################################################################################
# S3 bucket for botnim extraction artifacts (the ArtifactStore S3 backend).
#
# Replaces the EFS specs-extraction access point as the durable home for
# extraction artifacts. botnim/storage/get_artifact_store() resolves to
# S3Store(bucket) in this env; keys mirror the old relative path under
# config_dir/extraction with two top-level prefixes:
#   seed/<bot>/<relpath>    -> immutable operator data (relies on versioning)
#   cache/<bot>/<relpath>   -> re-derivable artifacts (incl. wikitext cache)
#
# Versioning is ON so a corrupt/accidental overwrite of a seed/ object is
# recoverable. Public access is fully blocked; the only reader/writer is the
# botnim-api task role (RW policy below) — there are no presigned URLs here.
#
# SSE-KMS with the platform CMK (not AES256 like word_docs) because seed/
# holds operator-curated source data we want CMK-level audit + key control.
# The task exec/role needs kms:Decrypt scoped to ViaService=s3 (statement
# at the bottom of this file) to read SSE-KMS objects.
#
# Lifecycle: abort dangling multipart uploads after 1 day; expire NONCURRENT
# versions after 90 days (cap the cost of versioning); NO expiration on the
# current version — seed/ data must never auto-delete and cache/ is cheap to
# keep warm.
#
# Naming: env-scoped to keep staging artifacts out of prod and to comply with
# S3's global namespace. If `botnim-extraction-<env>` ever collides on first
# apply, prepend the account id (mirrors the word_docs.tf escape hatch).
################################################################################

resource "aws_s3_bucket" "extraction_artifacts" {
  bucket = "botnim-extraction-${var.environment}"

  tags = {
    Project = "botnim"
    Env     = var.environment
    Purpose = "extraction-artifacts"
  }
}

resource "aws_s3_bucket_public_access_block" "extraction_artifacts" {
  bucket                  = aws_s3_bucket.extraction_artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "extraction_artifacts" {
  bucket = aws_s3_bucket.extraction_artifacts.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "extraction_artifacts" {
  bucket = aws_s3_bucket.extraction_artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = local.contract.ecs.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "extraction_artifacts" {
  bucket = aws_s3_bucket.extraction_artifacts.id

  rule {
    id     = "abort-incomplete-mpu"
    status = "Enabled"

    filter {
      prefix = ""
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {
      prefix = ""
    }

    # Cap the cost of versioning: drop superseded versions after ~90 days.
    # The CURRENT version is never expired (no top-level `expiration {}`):
    # seed/ data must persist and cache/ is cheap to keep warm.
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

# IAM statements granting the botnim-api task role read/write on this bucket.
# Composed into the task role policy via aws_iam_policy_document.task_role
# (see main.tf) using source_policy_documents, so word_docs_write /
# es_backups_write keep their own scope.
data "aws_iam_policy_document" "extraction_artifacts_rw" {
  statement {
    sid    = "ExtractionArtifactsObjectRW"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:GetObjectVersion",
      # Object-level: needed by a future streaming/multipart upload path
      # (boto3 upload_fileobj falls back to MPU for large objects). Low
      # runtime risk today but listed by the spec (§9).
      "s3:AbortMultipartUpload",
    ]
    # Static ARN derived from bucket name (not resource attr) so the composed
    # task_role_policy_json is plan-time-knowable; otherwise the ecs-service
    # module's `count = task_role_policy_json != "" ? 1 : 0` fails with
    # "value depends on resource attributes" at first plan (same gotcha as
    # word_docs_write).
    resources = ["arn:aws:s3:::botnim-extraction-${var.environment}/*"]
  }

  statement {
    sid    = "ExtractionArtifactsBucketList"
    effect = "Allow"
    # ArtifactStore.list(prefix) (the wikitext-chunks glob) calls ListBucket.
    # GetBucketLocation lets boto3 resolve the bucket's region (spec §9).
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = ["arn:aws:s3:::botnim-extraction-${var.environment}"]
  }
}

# kms:Decrypt + kms:GenerateDataKey for the task role on the platform CMK,
# scoped to S3 only, so the task can read/write the SSE-KMS objects above.
# Mirrors the exec-role grant in refresh.tf but on the TASK role (the running
# container uses the task role for S3 I/O) and for the s3 service.
data "aws_iam_policy_document" "extraction_artifacts_kms" {
  statement {
    sid    = "ExtractionArtifactsKMSViaS3"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey",
    ]
    resources = [local.contract.ecs.kms_key_arn]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["s3.${data.aws_region.current.name}.amazonaws.com"]
    }
  }
}
