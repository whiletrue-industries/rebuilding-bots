# Terragrunt root config for botnim-api app state.
#
# Apps share the same S3 state bucket as buildup-org-infra but live under
# their own project-scoped key prefix. Each environment under live/<env>/
# gets its own state file at projects/botnim-api/<env>/terraform.tfstate.

locals {
  region             = "il-central-1"
  state_bucket       = "buildup-org-tfstate-prod"
  project_state_name = "botnim-api"
}

remote_state {
  backend = "s3"

  generate = {
    path      = "backend_generated.tf"
    if_exists = "overwrite_terragrunt"
  }

  config = {
    bucket                 = local.state_bucket
    key                    = "projects/${local.project_state_name}/${path_relative_to_include()}/terraform.tfstate"
    region                 = local.region
    encrypt                = true
    skip_region_validation = true
    use_lockfile           = true
  }
}

generate "provider" {
  path      = "provider_generated.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<-EOF
    terraform {
      required_version = ">= 1.7.0"
      required_providers {
        aws = {
          source  = "hashicorp/aws"
          version = "~> 6.37"
        }
      }
    }

    provider "aws" {
      region = "${local.region}"

      default_tags {
        tags = {
          Project     = "${local.project_state_name}"
          Environment = "${basename(path_relative_to_include())}"
          ManagedBy   = "terragrunt"
        }
      }
    }
  EOF
}

inputs = {
  region = local.region
}
