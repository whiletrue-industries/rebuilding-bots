# botnim-api — prod infra

Terraform configuration deploying `botnim-api` to the shared ECS Fargate cluster
in `Build-Up-IL/org-infra`.

## Topology

One Fargate task with two containers:

| Container | Image | Port | Purpose |
|---|---|---|---|
| `api` | (this repo's ECR) | 8000 | FastAPI — handles `/botnim/retrieve/*` |
| `elasticsearch` | `docker.elastic.co/elasticsearch/elasticsearch:8.11.0` | 9200 (localhost only) | Single-node ES for vector + BM25 search |

Routing: shared ALB listener rule on `botnim.build-up.team` with path pattern
`/botnim/*` → this task's target group (priority 100).

Persistence: an EFS filesystem (`botnim-api-es-efs`) with one access point
mounted at `/usr/share/elasticsearch/data` in the sidecar. Data survives task
restarts. The task is fixed at `desired_count = 1` — no horizontal scaling.

Backups: an S3 bucket (`botnim-api-es-backups-prod`) with lifecycle rules
(30-day IA → 90-day expire). The task role has write access; ES snapshot
scheduling is handled separately (cron job inside the task, or scheduled
EventBridge trigger).

Secrets:
- `botnim-api/prod/openai-api-key` — must be populated out-of-band
- `botnim-api/prod/elasticsearch-password` — must be populated out-of-band

## Prereqs

1. The `Build-Up-IL/org-infra` platform-contract layer has been applied
   (there must be a JSON document at SSM `/buildup/shared/prod/contract`).
2. The `feat/ecs-efs-and-sidecars` branch of `Build-Up-IL/org-infra` has been
   merged to `main`, and this file's `?ref=...` references have been updated to
   a stable tag or commit SHA.

## First-time deploy

```bash
# 1. AWS login
aws sso login --profile shared-production
export AWS_PROFILE=shared-production

# 2. Bootstrap (creates ECR, EFS, SGs, log group, secrets; desired_count=0)
cd infra/prod
terraform init
terraform plan -out=bootstrap.tfplan
terraform apply bootstrap.tfplan

# 3. Populate secrets (one-time, values never in git)
aws secretsmanager put-secret-value \
  --secret-id botnim-api/prod/openai-api-key \
  --secret-string "sk-..."
aws secretsmanager put-secret-value \
  --secret-id botnim-api/prod/elasticsearch-password \
  --secret-string "$(openssl rand -base64 32)"

# 4. Build & push the first image
ECR_URL=$(terraform output -raw ecr_repository_url)
cd ../..
aws ecr get-login-password --region il-central-1 \
  | docker login --username AWS --password-stdin "$ECR_URL"
docker build -f backend/api/Dockerfile -t "$ECR_URL:v1" .
docker push "$ECR_URL:v1"

# 5. Real deploy
cd infra/prod
terraform apply -var image_tag=v1 -var desired_count=1
```

Subsequent deploys only need a new image tag:

```bash
./scripts/build_and_push.sh <SHA>
cd infra/prod && terraform apply -var image_tag=<SHA> -var desired_count=1
```
