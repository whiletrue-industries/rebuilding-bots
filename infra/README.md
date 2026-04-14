# botnim-api infra

Terragrunt-managed Terraform deploying `botnim-api` to the shared ECS Fargate
cluster in `Build-Up-IL/org-infra`.

## Layout

```
infra/
├── root.hcl                  # shared terragrunt config (backend + provider generation)
├── live/prod/                # terragrunt invocation roots (one per env)
│   └── terragrunt.hcl
├── envs/prod/                # raw Terraform code (one per env)
│   ├── main.tf               # modules/app invocation w/ Elasticsearch sidecar
│   ├── data.tf               # reads shared platform contract from SSM
│   ├── variables.tf
│   ├── secrets.tf            # Secrets Manager entries
│   ├── efs.tf                # EFS filesystem for ES data
│   ├── backups.tf            # S3 bucket for ES snapshots
│   └── outputs.tf
└── README.md
```

`live/prod/terragrunt.hcl` is the apply point; it includes `root.hcl` for backend
config and sets `source = envs/prod`. State key in the shared bucket:
`projects/botnim-api/prod/terraform.tfstate`.

## Topology

One Fargate task with two containers:

| Container | Image | Port | Purpose |
|---|---|---|---|
| `api` | (this repo's ECR) | 8000 | FastAPI — handles `/botnim/retrieve/*` |
| `elasticsearch` | `docker.elastic.co/elasticsearch/elasticsearch:8.11.0` | 9200 (localhost only) | Single-node ES for vector + BM25 search |

Routing: shared ALB on `botnim.build-up.team` with path pattern `/botnim/*`
at priority 100. Task is fixed at `desired_count = 1` — no horizontal scaling.

Persistence: EFS filesystem (`botnim-api-es-efs`) with one access point mounted
at `/usr/share/elasticsearch/data` in the sidecar. Snapshots can be pushed to
the S3 bucket `botnim-api-es-backups-prod` (30-day IA → 90-day expire).

Secrets (populated out-of-band):
- `botnim-api/prod/openai-api-key`
- `botnim-api/prod/elasticsearch-password`

## Prereqs

1. `Build-Up-IL/org-infra` platform-contract layer applied (SSM `/buildup/shared/prod/contract` exists)
2. `Build-Up-IL/org-infra#40` (EFS + sidecar support) merged — then update the
   `?ref=...` in `envs/prod/main.tf` and `envs/prod/efs.tf` to a stable tag/SHA

## First-time deploy

```bash
aws sso login --profile shared-production
export AWS_PROFILE=shared-production

cd infra/live/prod
terragrunt init
terragrunt plan
terragrunt apply

# Populate secrets (values never in git)
aws secretsmanager put-secret-value \
  --secret-id botnim-api/prod/openai-api-key \
  --secret-string "sk-..."
aws secretsmanager put-secret-value \
  --secret-id botnim-api/prod/elasticsearch-password \
  --secret-string "$(openssl rand -base64 32)"

# Build and push first image
ECR_URL=$(terragrunt output -raw ecr_repository_url)
cd ../../..
aws ecr get-login-password --region il-central-1 \
  | docker login --username AWS --password-stdin "$ECR_URL"
docker build -f backend/api/Dockerfile -t "$ECR_URL:v1" .
docker push "$ECR_URL:v1"

# Real deploy
cd infra/live/prod
terragrunt apply -var image_tag=v1 -var desired_count=1
```

## Subsequent deploys

```bash
cd infra/live/prod
terragrunt apply -var image_tag=<new-sha> -var desired_count=1
```

CI workflow template in `infra/envs/prod/deploy-ecs.yml.template` — copy to
`.github/workflows/deploy-ecs.yml` manually (requires a PAT with `workflow` scope).
