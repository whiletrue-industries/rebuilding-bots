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
    sid       = "WordDocsPutObject"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.word_docs.arn}/*"]
  }
}
