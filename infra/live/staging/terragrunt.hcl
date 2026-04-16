include "root" {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "${get_repo_root()}//infra/envs/staging"
}

inputs = {
  environment = "staging"

  # Listener rule priority for /botnim/* on the shared staging ALB. Keycloak
  # owns priority 100 (host: auth.staging.build-up.team), messaging-service
  # is at 110. Keeping botnim-api at 300 avoids churn on the existing rule
  # and leaves room for siblings. The variable defaults to 100 (the prod
  # value), so we override explicitly here.
  listener_priority = 300

  # Pin image_tag so terragrunt apply doesn't reset it to the `bootstrap`
  # default and hand ECS a tag that doesn't exist in ECR. Deploy pipelines
  # override this on each run; this is the last-known-good for ad-hoc
  # terragrunt applies (like service-connect infra changes).
  image_tag = "f95a32d"
}
