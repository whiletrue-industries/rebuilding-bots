################################################################################
# Phoenix LLM-tracing collector — internal-only ECS Fargate service
#
# Security invariant: Phoenix MUST NOT be reachable from the public internet.
# There is no ALB, no DNS record, no public IP. The only ingress path is
# intra-cluster Service Connect on port 6006. The expose_publicly input
# variable is hard-pinned to false via a validation block; flipping it
# requires a code change reviewed by a human.
#
# Clients inside the same ECS cluster reach Phoenix via Service Connect:
#   OTLP traces:  http://phoenix:6006/v1/traces
#   GraphQL UI:   http://phoenix:6006/graphql
################################################################################

terraform {
  required_version = ">= 1.7.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.37"
    }
  }
}

data "aws_region" "current" {}

################################################################################
# Security group
#
# Egress only — Phoenix must reach Aurora (via the cluster's internal routing /
# Service Connect) and pull its Docker image from the internet.
#
# NO ingress rules. Service Connect handles intra-cluster reachability without
# any SG ingress rule — Fargate tasks in the same namespace communicate through
# the service proxy sidecar, not through SG-controlled ports.
#
# CODE-REVIEW RED FLAG: adding an ingress rule here defeats the no-public-surface
# invariant. Any PR that adds an ingress rule to this SG must explain why and
# receive explicit security sign-off.
################################################################################

resource "aws_security_group" "phoenix" {
  name        = "phoenix-${var.env}"
  # AWS EC2 SG description must be ASCII-only (no em dashes, no Unicode).
  description = "Phoenix LLM-tracing: egress-only. NO ingress -- Service Connect only. Adding ingress here is a code-review red flag."
  vpc_id      = var.vpc_id

  egress {
    description = "Allow all outbound (Aurora, ECR, CloudWatch, Secrets Manager)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "phoenix-${var.env}"
  }
}

################################################################################
# CloudWatch log group
################################################################################

resource "aws_cloudwatch_log_group" "phoenix" {
  name              = "/ecs/phoenix-${var.env}"
  retention_in_days = 14

  tags = {
    Name = "phoenix-${var.env}"
  }
}

################################################################################
# IAM — task execution role
#
# Grants the ECS agent permissions to:
#   - Pull the container image from ECR (public, so AmazonECSTaskExecutionRolePolicy
#     covers it via the ECR public endpoint; listed explicitly for clarity)
#   - Write logs to CloudWatch
#   - Fetch the phoenix DB secret from Secrets Manager at task start
################################################################################

data "aws_iam_policy_document" "task_exec_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "task_exec" {
  name               = "phoenix-${var.env}-exec-role"
  assume_role_policy = data.aws_iam_policy_document.task_exec_assume.json

  tags = {
    Name = "phoenix-${var.env}-exec-role"
  }
}

# AWS-managed policy: ECR pull + CloudWatch Logs write
resource "aws_iam_role_policy_attachment" "task_exec_managed" {
  role       = aws_iam_role.task_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "task_exec_secrets" {
  statement {
    sid       = "GetPhoenixDbSecret"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.phoenix_db_secret_arn]
  }
}

resource "aws_iam_role_policy" "task_exec_secrets" {
  name   = "phoenix-db-secret-read"
  role   = aws_iam_role.task_exec.id
  policy = data.aws_iam_policy_document.task_exec_secrets.json
}

# Optional: when the secret is CMK-encrypted (all new secrets in buildup-shared),
# the exec role also needs kms:Decrypt on the CMK, scoped via kms:ViaService so
# the grant only applies when KMS is invoked through the Secrets Manager path.
# Mirrors the pattern in infra/envs/staging/refresh.tf:259-277.
data "aws_iam_policy_document" "task_exec_kms_decrypt" {
  count = var.secrets_kms_key_arn == "" ? 0 : 1

  statement {
    sid       = "DecryptSMSecretsCMK"
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = [var.secrets_kms_key_arn]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["secretsmanager.${data.aws_region.current.name}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "task_exec_kms_decrypt" {
  count  = var.secrets_kms_key_arn == "" ? 0 : 1
  name   = "phoenix-${var.env}-task-exec-kms-decrypt"
  role   = aws_iam_role.task_exec.id
  policy = data.aws_iam_policy_document.task_exec_kms_decrypt[0].json
}

################################################################################
# IAM — task role
#
# Grants the running container permissions needed for operator tooling:
#   - ssmmessages: required for `aws ecs execute-command` / SSM Session Manager
#     port-forward so operators can reach the Phoenix UI without a public
#     load balancer.
################################################################################

data "aws_iam_policy_document" "task_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "task" {
  name               = "phoenix-${var.env}-task-role"
  assume_role_policy = data.aws_iam_policy_document.task_assume.json

  tags = {
    Name = "phoenix-${var.env}-task-role"
  }
}

data "aws_iam_policy_document" "task_ssm" {
  statement {
    sid    = "ECSExecSSMMessages"
    effect = "Allow"
    actions = [
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "task_ssm" {
  name   = "ecs-exec-ssmmessages"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_ssm.json
}

################################################################################
# ECS task definition
################################################################################

resource "aws_ecs_task_definition" "phoenix" {
  family                   = "phoenix-${var.env}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.task_cpu
  memory                   = var.task_memory

  execution_role_arn = aws_iam_role.task_exec.arn
  task_role_arn      = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name  = "phoenix"
      image = "arizephoenix/phoenix:${var.image_tag}"

      portMappings = [
        {
          containerPort = 6006
          name          = "phoenix-otlp"
          protocol      = "tcp"
          appProtocol   = "http"
        }
      ]

      environment = [
        { name = "PHOENIX_HOST", value = "0.0.0.0" },
        { name = "PHOENIX_PORT", value = "6006" },
        { name = "PHOENIX_DATA_RETENTION_DAYS", value = "7" },
      ]

      secrets = [
        {
          name      = "PHOENIX_SQL_DATABASE_URL"
          valueFrom = var.phoenix_db_secret_arn
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.phoenix.name
          "awslogs-region"        = data.aws_region.current.name
          "awslogs-stream-prefix" = "phoenix"
        }
      }

      essential = true
    }
  ])

  tags = {
    Name = "phoenix-${var.env}"
  }
}

################################################################################
# ECS service
#
# load_balancer block deliberately absent — Phoenix is Service Connect only.
# No ALB, no NLB, no target group. The service_connect_configuration below
# is the ONLY ingress path: other tasks in the same cluster namespace reach
# Phoenix at http://phoenix:6006.
################################################################################

resource "aws_ecs_service" "phoenix" {
  name            = "phoenix-${var.env}"
  cluster         = var.cluster_name
  task_definition = aws_ecs_task_definition.phoenix.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  # enable_execute_command = true allows operators to port-forward into the
  # running Phoenix task via `aws ecs execute-command` / SSM Session Manager.
  # This is the only way to reach the Phoenix UI without exposing it publicly.
  enable_execute_command = true

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = [aws_security_group.phoenix.id]
    assign_public_ip = false
  }

  service_connect_configuration {
    enabled   = true
    namespace = var.service_connect_namespace_arn

    service {
      port_name      = "phoenix-otlp"
      discovery_name = "phoenix"

      client_alias {
        port = 6006
      }
    }
  }

  # desired_count is managed manually via deploy.sh / operator intervention,
  # not by terragrunt. Ignore drift so a scale-to-zero for cost control
  # doesn't get immediately reversed by the next apply.
  lifecycle {
    ignore_changes = [desired_count]
  }

  tags = {
    Name = "phoenix-${var.env}"
  }
}
