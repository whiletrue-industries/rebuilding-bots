################################################################################
# Daily refresh automation for botnim-api knesset PDF feeds.
#
# Chain:
#   EventBridge Schedule (cron)
#     → Lambda "botnim-refresh-invoker" (VPC-attached)
#       → POST http://botnim-api:8000/botnim/admin/refresh via Service Connect
#         → background thread in botnim-api runs fetch-and-process + sync
#
# Alerting:
#   CloudWatch Logs metric filter on /ecs/botnim-api-<env>/api log group for
#   "REFRESH_FAILED" → CloudWatch Alarm → SNS topic → email.
#   Lambda's built-in Errors metric → same SNS topic (catches cases where the
#   API is fully unreachable and the in-API logging path can't fire).
#
# Why this lives inside the API task rather than as a standalone ECS task:
#   ES runs as a sidecar in botnim-api and is only reachable at localhost:9200
#   within that task's network namespace. A standalone task would need ES
#   exposed via Service Connect or split into its own service — see the
#   design doc "Tension 3" for context.
################################################################################

# ---------------------------------------------------------------------------
# SNS topic + email subscription
# ---------------------------------------------------------------------------

resource "aws_sns_topic" "refresh_failures" {
  name = "botnim-refresh-failures-${var.environment}"
}

variable "refresh_alert_email" {
  description = "Email address to subscribe to the refresh-failures SNS topic."
  type        = string
  default     = "amir.wilf@build-up.team"
}

resource "aws_sns_topic_subscription" "refresh_failures_email" {
  topic_arn = aws_sns_topic.refresh_failures.arn
  protocol  = "email"
  endpoint  = var.refresh_alert_email
}

# ---------------------------------------------------------------------------
# CloudWatch Logs metric filter + alarm on REFRESH_FAILED
#
# The botnim-api log group is created by Build-Up-IL/org-infra//modules/app
# (which delegates to modules/ecs-service). Its actual name follows the
# convention "/ecs/<environment>/<service_name>" and is exported as
# module.botnim_api.log_group_name. Using the module output (instead of
# reconstructing the name) gives us an implicit dependency on the log
# group's creation, so this metric filter cannot apply before the log
# group exists.
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_metric_filter" "refresh_failed" {
  name           = "botnim-refresh-failed-${var.environment}"
  log_group_name = module.botnim_api.log_group_name
  pattern        = "REFRESH_FAILED"

  metric_transformation {
    name          = "RefreshFailed"
    namespace     = "Botnim/${var.environment}"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "refresh_failed" {
  alarm_name          = "botnim-refresh-failed-${var.environment}"
  alarm_description   = "REFRESH_FAILED logged by botnim-api refresh thread"
  namespace           = "Botnim/${var.environment}"
  metric_name         = "RefreshFailed"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.refresh_failures.arn]
}

# ---------------------------------------------------------------------------
# Lambda: package + role + VPC + function
# ---------------------------------------------------------------------------

data "archive_file" "refresh_invoker" {
  type        = "zip"
  source_dir  = "${path.module}/lambda/refresh_invoker"
  output_path = "${path.module}/.terraform-artifacts/refresh_invoker.zip"
  excludes    = ["test_handler.py", "__pycache__"]
}

data "aws_iam_policy_document" "refresh_lambda_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "refresh_lambda" {
  name               = "botnim-refresh-invoker-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.refresh_lambda_assume.json
}

# AWS-managed policy that grants the Lambda everything it needs to run in a VPC:
# ENI management, CloudWatch Logs write, etc.
resource "aws_iam_role_policy_attachment" "refresh_lambda_vpc" {
  role       = aws_iam_role.refresh_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

data "aws_iam_policy_document" "refresh_lambda_secrets" {
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.refresh_admin_api_key.arn]
  }
  statement {
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = [local.contract.ecs.kms_key_arn]
  }
}

resource "aws_iam_role_policy" "refresh_lambda_secrets" {
  name   = "refresh-admin-secret-read"
  role   = aws_iam_role.refresh_lambda.id
  policy = data.aws_iam_policy_document.refresh_lambda_secrets.json
}

resource "aws_security_group" "refresh_lambda" {
  name        = "botnim-refresh-invoker-${var.environment}"
  description = "Egress for refresh-invoker Lambda to reach botnim-api and AWS APIs"
  vpc_id      = local.contract.network.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Allow this Lambda SG into the botnim-api service's SG on port 8000.
# Build-Up-IL/org-infra//modules/app exports this as `security_group_id`
# (the SG attached to the ECS service / task ENIs; see modules/app/outputs.tf).
resource "aws_security_group_rule" "api_ingress_from_refresh_lambda" {
  type                     = "ingress"
  from_port                = 8000
  to_port                  = 8000
  protocol                 = "tcp"
  security_group_id        = module.botnim_api.security_group_id
  source_security_group_id = aws_security_group.refresh_lambda.id
  description              = "refresh-invoker Lambda to botnim-api on 8000"
}

resource "aws_lambda_function" "refresh_invoker" {
  function_name = "botnim-refresh-invoker-${var.environment}"
  role          = aws_iam_role.refresh_lambda.arn
  runtime       = "python3.12"
  handler       = "handler.handler"
  timeout       = 30
  memory_size   = 128

  filename         = data.archive_file.refresh_invoker.output_path
  source_code_hash = data.archive_file.refresh_invoker.output_base64sha256

  vpc_config {
    subnet_ids         = local.contract.network.private_subnet_ids
    security_group_ids = [aws_security_group.refresh_lambda.id]
  }

  environment {
    variables = {
      ADMIN_API_KEY_SECRET_ARN = aws_secretsmanager_secret.refresh_admin_api_key.arn
      REFRESH_ENDPOINT_URL     = "http://botnim-api:8000/botnim/admin/refresh"
    }
  }
}

# Alarm on Lambda itself failing to invoke the endpoint (separate from the
# in-API REFRESH_FAILED path).
resource "aws_cloudwatch_metric_alarm" "refresh_lambda_errors" {
  alarm_name          = "botnim-refresh-invoker-errors-${var.environment}"
  alarm_description   = "Refresh invoker Lambda is failing to reach botnim-api"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.refresh_invoker.function_name
  }

  alarm_actions = [aws_sns_topic.refresh_failures.arn]
}

# ---------------------------------------------------------------------------
# EventBridge Scheduler: daily at 04:00 UTC (07:00 Asia/Jerusalem).
# Backups run at 03:00 UTC (see efs_backup.tf); refresh runs an hour later so
# the two jobs don't contend for the filesystem.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "refresh_scheduler_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "refresh_scheduler" {
  name               = "botnim-refresh-scheduler-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.refresh_scheduler_assume.json
}

data "aws_iam_policy_document" "refresh_scheduler_invoke" {
  statement {
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.refresh_invoker.arn]
  }
}

resource "aws_iam_role_policy" "refresh_scheduler_invoke" {
  name   = "invoke-refresh-lambda"
  role   = aws_iam_role.refresh_scheduler.id
  policy = data.aws_iam_policy_document.refresh_scheduler_invoke.json
}

resource "aws_scheduler_schedule" "refresh" {
  name                = "botnim-refresh-${var.environment}"
  group_name          = "default"
  schedule_expression = "cron(0 4 * * ? *)"
  state               = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.refresh_invoker.arn
    role_arn = aws_iam_role.refresh_scheduler.arn
    input    = jsonencode({})
  }
}

# ---------------------------------------------------------------------------
# kms:Decrypt for the ECS task execution role on the CMK that encrypts our
# Secrets Manager entries.
#
# Build-Up-IL/org-infra//modules/app grants the execution role
# secretsmanager:GetSecretValue on every ARN in secret_environment_variables,
# but does NOT grant kms:Decrypt on the CMK. Existing secrets (openai, es)
# work today because their stored ciphertext predates the move to a CMK
# (encrypted with the AWS-managed aws/secretsmanager key). New secrets
# (and any future put-secret-value on the existing ones) get encrypted with
# the CMK and fail at task-start with "AccessDeniedException: Access to KMS
# is not allowed". Granting decrypt on the CMK to the exec role closes that
# gap once and for all. Scoped narrowly: kms:Decrypt only, on this CMK only,
# only when invoked via the secretsmanager service.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "exec_role_kms_decrypt" {
  statement {
    sid       = "DecryptSMSecretsCMK"
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = [local.contract.ecs.kms_key_arn]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["secretsmanager.${data.aws_region.current.name}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "exec_role_kms_decrypt" {
  name   = "kms-decrypt-sm-cmk"
  role   = "botnim-api-${var.environment}-api-execution-role"
  policy = data.aws_iam_policy_document.exec_role_kms_decrypt.json
}
