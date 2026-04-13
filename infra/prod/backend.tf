terraform {
  backend "s3" {
    bucket                 = "buildup-org-tfstate-prod"
    key                    = "projects/botnim-api/prod/terraform.tfstate"
    region                 = "il-central-1"
    encrypt                = true
    skip_region_validation = true
    use_lockfile           = true
  }
}
