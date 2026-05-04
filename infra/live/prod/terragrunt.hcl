include "root" {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "${get_repo_root()}//infra/envs/prod"
}

inputs = {
  environment = "prod"

  # Listener rule priority on the shared prod ALB. Priorities 100 / 200 / 300
  # / 310 are claimed by tigburzfoni / safegan / checkup / checkup-staging.
  # 400 leaves headroom (LibreChat sits at 410 right behind us so /botnim/*
  # is matched before /* catch-all).
  listener_priority = 400
}
