################################################################################
# S3 bucket for short-lived Word doc downloads.
#
# The /tools/generate_word_doc endpoint renders a .docx and uploads it here,
# then returns a presigned URL the LLM embeds in chat. Objects are auto-purged
# after 7 days by the lifecycle rule below; the presigned URL is shorter-lived
# (set on the application side). Public access is fully blocked — distribution
# is via presigned URLs only.
#
# Naming: env-scoped to keep prod artifacts out of staging and to comply with
# S3's global namespace. If `botnim-word-docs-<env>` ever collides on first
# apply, prepend the account id.
################################################################################

resource "aws_s3_bucket" "word_docs" {
  bucket = "botnim-word-docs-${var.environment}"

  tags = {
    Project = "botnim"
    Env     = var.environment
    Purpose = "word-doc-downloads"
  }
}

resource "aws_s3_bucket_public_access_block" "word_docs" {
  bucket                  = aws_s3_bucket.word_docs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "word_docs" {
  bucket = aws_s3_bucket.word_docs.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "word_docs" {
  bucket = aws_s3_bucket.word_docs.id

  rule {
    id     = "delete-after-7-days"
    status = "Enabled"

    filter {
      prefix = ""
    }

    expiration {
      days = 7
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# IAM statements granting the botnim-api task role write access to this bucket.
# Composed into the task role policy via aws_iam_policy_document.task_role
# (see main.tf) using source_policy_documents so es_backups_write keeps its
# own scope.
data "aws_iam_policy_document" "word_docs_write" {
  statement {
    sid    = "WordDocsPutAndGet"
    effect = "Allow"
    # GetObject is required because SigV4 presigned URLs are signed with
    # this task role's credentials; S3 evaluates GetObject against the
    # SIGNER's role, not the downloader. Without GetObject the download
    # link returns 403 AccessDenied even with a valid presigned URL.
    actions = ["s3:PutObject", "s3:GetObject"]
    # Static ARN derived from bucket name (not resource attr) so the
    # composed task_role_policy_json is plan-time-knowable; otherwise the
    # ecs-service module's `count = task_role_policy_json != "" ? 1 : 0`
    # fails with "value depends on resource attributes" at first plan.
    resources = ["arn:aws:s3:::botnim-word-docs-${var.environment}/*"]
  }
}

# ───────────────────────────────────────────────────────────────────────────
# Long-lived IAM user used to *sign* presigned download URLs.
#
# Why not just use the task role's STS creds? Two reasons:
# (1) STS session tokens push the URL past ~2KB — LibreChat's markdown
#     renderer drops the trailing &X-Amz-Signature query param at that
#     length, breaking every link. With long-lived IAM user creds the
#     URL has no X-Amz-Security-Token (~700 chars shorter) and renders
#     fully.
# (2) Presigned URLs signed by STS creds expire when the underlying
#     session expires (≤36h on Fargate task roles), regardless of the
#     ?X-Amz-Expires we ask for. Long-lived IAM creds let the 7-day
#     ?X-Amz-Expires we advertise actually mean 7 days.
#
# Scope is intentionally narrow: GetObject on this bucket only. No
# PutObject — uploads still go through the task role.
# ───────────────────────────────────────────────────────────────────────────
resource "aws_iam_user" "word_docs_signer" {
  name = "botnim-word-docs-signer-${var.environment}"
  path = "/botnim/"
  tags = {
    Project = "botnim"
    Env     = var.environment
    Purpose = "presigned-url-signing"
  }
}

resource "aws_iam_user_policy" "word_docs_signer" {
  name   = "GetObjectOnBucket"
  user   = aws_iam_user.word_docs_signer.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "GetWordDocs"
      Effect   = "Allow"
      Action   = ["s3:GetObject"]
      Resource = "arn:aws:s3:::botnim-word-docs-${var.environment}/*"
    }]
  })
}

resource "aws_iam_access_key" "word_docs_signer" {
  user = aws_iam_user.word_docs_signer.name
}

resource "aws_secretsmanager_secret" "word_docs_signer" {
  name        = "botnim-api/${var.environment}/word-docs-signer-creds"
  description = "Long-lived IAM user creds used to sign presigned word-doc download URLs"
}

resource "aws_secretsmanager_secret_version" "word_docs_signer" {
  secret_id = aws_secretsmanager_secret.word_docs_signer.id
  secret_string = jsonencode({
    aws_access_key_id     = aws_iam_access_key.word_docs_signer.id
    aws_secret_access_key = aws_iam_access_key.word_docs_signer.secret
  })
}
