################################################################################
# Twice-daily sanity DoD runner.
#
# Chain:
#   EventBridge Schedule (cron 03:00 + 12:00 UTC)
#     → Lambda "botnim-sanity-invoker" (NOT VPC-attached)
#       → POST https://botnim.build-up.team/botnim/admin/sanity
#         → daemon thread in botnim-api runs capture + judge + render + finalize
#
# Alerting:
#   * SANITY_FAILED log → CloudWatch metric filter → alarm → SNS
#   * SANITY_REGRESSION log → separate metric filter → alarm → SNS
#   * Lambda Errors metric → alarm → SNS (catches API-down case)
################################################################################

# Reuse the refresh-failures SNS topic. New topic only if alert volume forces a split.
locals {
  sanity_sns_topic_arn = aws_sns_topic.refresh_failures.arn
}

# ---------------------------------------------------------------------------
# Secret: admin API key
# ---------------------------------------------------------------------------

resource "random_password" "sanity_admin_api_key" {
  length  = 48
  special = false
}

resource "aws_secretsmanager_secret" "sanity_admin_api_key" {
  name = "botnim-api/${var.environment}/sanity-admin-api-key"
  # Encrypt with the cluster KMS key so the existing exec-role kms:Decrypt
  # policy in refresh.tf covers it (uses kms:ViaService scoping).
  kms_key_id = local.contract.ecs.kms_key_arn
}

resource "aws_secretsmanager_secret_version" "sanity_admin_api_key" {
  secret_id     = aws_secretsmanager_secret.sanity_admin_api_key.id
  secret_string = random_password.sanity_admin_api_key.result
}

# Mount the secret into botnim-api as BOTNIM_SANITY_ADMIN_API_KEY.
# (Build-Up-IL/org-infra//modules/app exposes secret_environment_variables;
# add this entry to the staging main.tf alongside the existing refresh key.)

# ---------------------------------------------------------------------------
# CloudWatch metric filters + alarms (twin rail)
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_metric_filter" "sanity_failed" {
  name           = "botnim-sanity-failed-${var.environment}"
  log_group_name = module.botnim_api.log_group_name
  pattern        = "SANITY_FAILED"
  metric_transformation {
    name          = "SanityFailed"
    namespace     = "Botnim/${var.environment}"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "sanity_failed" {
  alarm_name          = "botnim-sanity-failed-${var.environment}"
  alarm_description   = "SANITY_FAILED logged by botnim-api sanity thread"
  namespace           = "Botnim/${var.environment}"
  metric_name         = "SanityFailed"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [local.sanity_sns_topic_arn]
}

resource "aws_cloudwatch_log_metric_filter" "sanity_regression" {
  name           = "botnim-sanity-regression-${var.environment}"
  log_group_name = module.botnim_api.log_group_name
  pattern        = "SANITY_REGRESSION"
  metric_transformation {
    name          = "SanityRegression"
    namespace     = "Botnim/${var.environment}"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "sanity_regression" {
  alarm_name          = "botnim-sanity-regression-${var.environment}"
  alarm_description   = "SANITY_REGRESSION (red banner) on /d/sanity"
  namespace           = "Botnim/${var.environment}"
  metric_name         = "SanityRegression"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [local.sanity_sns_topic_arn]
}

# ---------------------------------------------------------------------------
# Lambda: package + role + function (NOT VPC-attached — mirrors refresh)
# ---------------------------------------------------------------------------

data "archive_file" "sanity_invoker" {
  type        = "zip"
  source_dir  = "${path.module}/lambda/sanity_invoker"
  output_path = "${path.module}/.terraform-artifacts/sanity_invoker.zip"
  excludes    = ["test_handler.py", "__pycache__"]
}

resource "aws_iam_role" "sanity_lambda" {
  name               = "botnim-sanity-invoker-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.refresh_lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "sanity_lambda_basic" {
  role       = aws_iam_role.sanity_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "sanity_lambda_secrets" {
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.sanity_admin_api_key.arn]
  }
  statement {
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = [local.contract.ecs.kms_key_arn]
  }
}

resource "aws_iam_role_policy" "sanity_lambda_secrets" {
  name   = "sanity-admin-secret-read"
  role   = aws_iam_role.sanity_lambda.id
  policy = data.aws_iam_policy_document.sanity_lambda_secrets.json
}

resource "aws_lambda_function" "sanity_invoker" {
  function_name = "botnim-sanity-invoker-${var.environment}"
  role          = aws_iam_role.sanity_lambda.arn
  runtime       = "python3.12"
  handler       = "handler.handler"
  timeout       = 30
  memory_size   = 128

  filename         = data.archive_file.sanity_invoker.output_path
  source_code_hash = data.archive_file.sanity_invoker.output_base64sha256

  environment {
    variables = {
      SANITY_ADMIN_API_KEY_SECRET_ARN = aws_secretsmanager_secret.sanity_admin_api_key.arn
      SANITY_ENDPOINT_URL             = "https://botnim.build-up.team/botnim/admin/sanity"
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "sanity_lambda_errors" {
  alarm_name          = "botnim-sanity-invoker-errors-${var.environment}"
  alarm_description   = "Sanity invoker Lambda is failing to reach botnim-api"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    FunctionName = aws_lambda_function.sanity_invoker.function_name
  }
  alarm_actions = [local.sanity_sns_topic_arn]
}

# ---------------------------------------------------------------------------
# EventBridge Scheduler: 03:00 + 12:00 UTC
# ---------------------------------------------------------------------------

resource "aws_iam_role" "sanity_scheduler" {
  name               = "botnim-sanity-scheduler-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.refresh_scheduler_assume.json
}

data "aws_iam_policy_document" "sanity_scheduler_invoke" {
  statement {
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.sanity_invoker.arn]
  }
}

resource "aws_iam_role_policy" "sanity_scheduler_invoke" {
  name   = "invoke-sanity-lambda"
  role   = aws_iam_role.sanity_scheduler.id
  policy = data.aws_iam_policy_document.sanity_scheduler_invoke.json
}

resource "aws_scheduler_schedule" "sanity" {
  name                = "botnim-sanity-${var.environment}"
  group_name          = "default"
  schedule_expression = "cron(0 3,12 * * ? *)"
  state               = "ENABLED"
  flexible_time_window {
    mode = "OFF"
  }
  target {
    arn      = aws_lambda_function.sanity_invoker.arn
    role_arn = aws_iam_role.sanity_scheduler.arn
    input    = jsonencode({})
  }
}
