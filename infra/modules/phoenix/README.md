# modules/phoenix

Terraform module for the Phoenix LLM-tracing collector — an internal-only
ECS Fargate service that stores OpenInference / OTLP traces from botnim-api
and LibreChat.

## What this deploys

- **ECS Fargate service** running `arizephoenix/phoenix` (pinned tag).
- **CloudWatch log group** `/ecs/phoenix-<env>` with 14-day retention.
- **IAM roles**: task execution role (ECR pull + Secrets Manager read) and
  task role (SSM Messages for operator port-forward access).
- **Security group**: egress-only. No ingress rules — see below.

## No-public-surface invariant

Phoenix must never be reachable from the public internet. There is no ALB,
no NLB, no DNS record, no public IP. The `expose_publicly` input variable
is hard-pinned to `false` via a `validation` block:

```hcl
variable "expose_publicly" {
  default = false
  validation {
    condition     = var.expose_publicly == false
    error_message = "expose_publicly is hard-pinned to false..."
  }
}
```

Flipping this is intentionally a code change requiring human review — you
cannot override it at call-site without removing the validation block.

The security group has **no ingress rules**. Intra-cluster reachability is
provided entirely by Service Connect; adding an SG ingress rule is a
code-review red flag.

## Required inputs

See `variables.tf` for full descriptions. Summary:

| Variable | Description |
|---|---|
| `env` | `staging` or `prod` |
| `cluster_name` | ECS cluster name (`buildup-shared`) |
| `vpc_id` | VPC ID |
| `subnet_ids` | Private subnet IDs for the task ENI |
| `service_connect_namespace_arn` | Service Connect namespace ARN |
| `phoenix_db_secret_arn` | Secrets Manager ARN for the DB URL secret |

Optional: `task_cpu` (default 512), `task_memory` (default 1024),
`image_tag` (default `version-7.0.0`).

## Secrets Manager secret format

The secret at `phoenix_db_secret_arn` must be a plain-string value:

```
postgresql://phoenix_app:<password>@<aurora-writer-host>:5432/phoenix
```

The `phoenix` database and `phoenix_app` role are created by alembic
migration `0013_phoenix_db`. The secret value must be set out-of-band
(AWS Console or CLI) — it is never written to git.

## Client endpoints (Service Connect)

Other ECS tasks in the same cluster namespace reach Phoenix via:

| Purpose | URL |
|---|---|
| OTLP trace ingest | `http://phoenix:6006/v1/traces` |
| GraphQL API / UI | `http://phoenix:6006/graphql` |

The `service_connect_dns` output (`phoenix:6006`) is the canonical value to
paste into `PHOENIX_COLLECTOR_ENDPOINT` / `OTEL_EXPORTER_OTLP_ENDPOINT`
environment variables in sibling services.

## Data retention

Phoenix is started with `PHOENIX_DATA_RETENTION_DAYS=7`. Traces older than
7 days are pruned by Phoenix's internal cleanup job. This value is set per
the security review finding I4 (the spec originally said 30 days).

## Operator access

Phoenix has no public URL. The two access patterns below serve different
purposes — use the right one for the task at hand.

### Debugging the running container

Use `aws ecs execute-command` to open an interactive shell **inside** the
Phoenix container. This is useful for inspecting the process, running
one-off commands, or checking environment variables.

```bash
# 1. Find the running task ARN
TASK=$(aws --profile anubanu-<env> ecs list-tasks \
  --cluster buildup-shared \
  --service-name phoenix-<env> \
  --desired-status RUNNING \
  --query 'taskArns[0]' --output text)

# 2. Open an interactive shell in the container
aws --profile anubanu-<env> ecs execute-command \
  --cluster buildup-shared \
  --task "$TASK" \
  --container phoenix \
  --interactive \
  --command "sh"
```

### Accessing the raw Phoenix UI

Use `aws ssm start-session` with `AWS-StartPortForwardingSession` to open a
**TCP tunnel** from your local machine's port 16006 into the container's port
6006. Once the tunnel is up, open your browser — the Phoenix UI is available
at `http://localhost:16006`.

```bash
# 1. Find the running task ARN (same as above)
TASK=$(aws --profile anubanu-<env> ecs list-tasks \
  --cluster buildup-shared \
  --service-name phoenix-<env> \
  --desired-status RUNNING \
  --query 'taskArns[0]' --output text)

# 2. Open the TCP tunnel (local :16006 → container :6006)
aws --profile anubanu-<env> ssm start-session \
  --target "ecs:buildup-shared_${TASK##*/}_phoenix" \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["6006"],"localPortNumber":["16006"]}'

# 3. Open http://localhost:16006 in your browser
```

The task role includes the four `ssmmessages:*` actions required for
`enable_execute_command = true` to work (used by both patterns above).
