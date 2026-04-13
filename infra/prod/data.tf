data "aws_region" "current" {}

data "aws_ssm_parameter" "platform_contract" {
  name = "/buildup/shared/prod/contract"
}

locals {
  contract = jsondecode(data.aws_ssm_parameter.platform_contract.value)
}
