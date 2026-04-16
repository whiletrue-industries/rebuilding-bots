data "aws_ssm_parameter" "platform_contract" {
  name = "/buildup/shared/${var.environment}/contract"
}

locals {
  # The platform contract SSM parameter is marked sensitive end-to-end; the
  # subfields we read here (zone name, subnet IDs) aren't actually secret, so
  # unwrap to avoid sensitive-tainting derived values used as host headers,
  # for_each keys, etc.
  contract    = jsondecode(data.aws_ssm_parameter.platform_contract.value)
  zone_name   = nonsensitive(trimsuffix(local.contract.dns.zone_name, "."))
  botnim_fqdn = "botnim.${local.zone_name}"
}
