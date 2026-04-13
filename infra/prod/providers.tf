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
  region = "il-central-1"

  default_tags {
    tags = {
      Project     = "botnim-api"
      Environment = "prod"
      ManagedBy   = "terraform"
    }
  }
}
