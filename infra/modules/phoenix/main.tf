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
# Phoenix must reach Aurora (via the cluster's internal routing) and pull its
# Docker image from the internet (egress). It must also accept inbound TCP
# 6006 from the shared internal-service client SG that botnim-api / librechat
# tasks attach to — the Service Connect sidecar still routes traffic to the
# upstream task's ENI:6006 directly, and AWS enforces SG ingress at the ENI
# layer regardless of the service mesh on top. Without this rule the SC
# sidecar reports "no healthy upstream" (HTTP 503) for every request.
#
# CODE-REVIEW RED FLAG: do NOT add a public CIDR (0.0.0.0/0) ingress rule.
# Phoenix must remain unreachable from the public internet. Ingress is
# scoped to the in-VPC client SG only.
################################################################################

resource "aws_security_group" "phoenix" {
  name        = "phoenix-${var.env}"
  # AWS EC2 SG description must be ASCII-only (no em dashes, no Unicode).
  description = "Phoenix LLM-tracing: ingress 6006 from internal-service-clients SG only; egress all."
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

# Required for Service Connect: the SC sidecar in the calling task forwards
# traffic to phoenix's task ENI on port 6006 (the upstream side of the
# proxy). Without this ingress rule the calls hit the SG and time out;
# the sidecar reports "no healthy upstream" and the trace-fetch route
# returns 502 ("phoenix unreachable"). Source is the cluster-wide
# internal-service-clients SG (org-infra contract:
# /buildup/shared/<env>/contract → internal_services.client_security_group_id).
resource "aws_security_group_rule" "phoenix_ingress_from_clients" {
  type                     = "ingress"
  from_port                = 6006
  to_port                  = 6006
  protocol                 = "tcp"
  security_group_id        = aws_security_group.phoenix.id
  source_security_group_id = var.internal_service_clients_sg_id
  description              = "Allow Service Connect callers (LibreChat / botnim-api) to reach Phoenix:6006"
}

# Workaround ingress rules — needed while some app tasks use an older
# org-infra modules/app ref that doesn't auto-attach the cluster-wide
# internal-service-clients SG to their ENIs. Without this, those tasks
# can't reach phoenix:6006 over Service Connect (Phoenix logs see only
# the other client tasks' traffic). See var.extra_client_security_group_ids.
resource "aws_security_group_rule" "phoenix_ingress_from_extra_clients" {
  for_each = toset(var.extra_client_security_group_ids)

  type                     = "ingress"
  from_port                = 6006
  to_port                  = 6006
  protocol                 = "tcp"
  security_group_id        = aws_security_group.phoenix.id
  source_security_group_id = each.value
  description              = "Allow ${each.value} (legacy client SG) to reach Phoenix:6006"
}

# Open Aurora's SG to phoenix on 5432. Org-infra's modules/app does this
# automatically for `enable_aurora_access = true` callers (e.g. botnim-api,
# whose task SG is on Aurora's ingress allowlist). Our hand-rolled phoenix
# module isn't on that path, so we add the rule directly here.
#
# An earlier attempt assumed Aurora trusts the cluster-wide
# internal-service-clients SG — it does not. Aurora's allowlist is a
# specific set of task SGs; phoenix needs its OWN entry.
#
# Cross-state safety: this resource lives in the phoenix terraform state
# but writes a rule onto a SG owned by org-infra. It's additive (a discrete
# aws_security_group_rule resource, not an inline `ingress` block), so it
# does not conflict with org-infra's other managed rules on the same SG.
resource "aws_security_group_rule" "aurora_accept_phoenix" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = var.aurora_security_group_id
  source_security_group_id = aws_security_group.phoenix.id
  description              = "Phoenix to Aurora (LLM trace store, phoenix DB + role)"
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
    sid     = "GetPhoenixDbSecret"
    effect  = "Allow"
    actions = ["secretsmanager:GetSecretValue"]
    # Secrets Manager appends a 6-char random suffix to every secret it
    # creates (e.g. "botnim/staging/phoenix-db-url-qcgWIN"), so callers
    # that reference the bare-name ARN miss with AccessDenied. Allow both
    # forms: the exact bare ARN AND the suffix-wildcard variant.
    resources = [
      var.phoenix_db_secret_arn,
      "${var.phoenix_db_secret_arn}-*",
    ]
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
      image = "${var.phoenix_image_repository}:${var.phoenix_image_tag}"

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
    subnets = var.subnet_ids
    # Two SGs on the task ENI:
    #   1. phoenix-<env>: owns ingress 6006 from internal-service-clients SG
    #      (the upstream side of Service Connect — see aws_security_group_rule
    #      "phoenix_ingress_from_clients" above).
    #   2. internal-service-clients SG: makes phoenix a recognized client of
    #      cluster-internal services (Aurora, future siblings). Aurora's SG
    #      grants ingress only to members of this SG, so without it phoenix's
    #      psycopg connect to the writer endpoint times out and the container
    #      exits 1 with PhoenixMigrationError. Mirrors what org-infra's
    #      modules/app does when enable_aurora_access=true.
    security_groups  = [aws_security_group.phoenix.id, var.internal_service_clients_sg_id]
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
        # Explicit bare alias so sibling tasks in the same Service Connect
        # namespace can reach Phoenix as `phoenix:6006`. Without dns_name,
        # ECS publishes only the namespace-qualified FQDN
        # (`phoenix.<namespace>.local`); bare-name DNS lookups from the
        # LibreChat / botnim-api tasks then fail and the trace-fetch route
        # returns 502 ("phoenix unreachable: fetch failed"). Mirrors the
        # bare alias org-infra's modules/app sets for botnim-api.
        dns_name = "phoenix"
      }

      # AWS ECS Service Connect's default perRequestTimeout is 15 seconds
      # (configurable since Jan 2024). Phoenix's GraphQL endpoint —
      # used by LibreChat's admin trace UI at /api/botnim/traces/<id> —
      # routinely takes longer than that on first hit (cold cache, schema
      # loading, multi-project scan). With the default 15s in effect,
      # OTLP POST /v1/traces (fast, sub-second) ingested fine while every
      # request to /graphql, /healthz, / returned 504 at exactly 15.0s
      # before reaching Phoenix at all (Phoenix logs showed zero
      # non-OTLP traffic). Raising to 60s mirrors the inbound timeout
      # already in place on botnim-api's SC mapping.
      timeout {
        per_request_timeout_seconds = 60
        idle_timeout_seconds        = 120
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
