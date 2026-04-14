################################################################################
# Secrets Manager entries for botnim-api
#
# The Terraform creates the secret resources, but the VALUES must be set
# out-of-band (AWS console or CLI) so they never touch git. The task execution
# role is granted read access to each ARN.
################################################################################

resource "aws_secretsmanager_secret" "openai_api_key" {
  name        = "botnim-api/${var.environment}/openai-api-key"
  description = "OpenAI API key for botnim-api embedding generation"
  kms_key_id  = local.contract.ecs.kms_key_arn
}

resource "aws_secretsmanager_secret" "elasticsearch_password" {
  name        = "botnim-api/${var.environment}/elasticsearch-password"
  description = "Password for the elasticsearch 'elastic' superuser"
  kms_key_id  = local.contract.ecs.kms_key_arn
}
