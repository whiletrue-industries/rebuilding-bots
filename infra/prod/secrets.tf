################################################################################
# Secrets Manager entries for botnim-api
#
# The Terraform creates the secret resources, but the VALUES must be set out-of-band
# (e.g. via the AWS console, CLI, or a separate bootstrap script) so they never
# touch git. The task execution role is granted read access to each ARN below.
################################################################################

resource "aws_secretsmanager_secret" "openai_api_key" {
  name        = "botnim-api/prod/openai-api-key"
  description = "OpenAI API key for botnim-api embedding generation and (unused) assistants management"
  kms_key_id  = local.contract.ecs.kms_key_arn
}

resource "aws_secretsmanager_secret" "elasticsearch_password" {
  name        = "botnim-api/prod/elasticsearch-password"
  description = "Password for the elasticsearch 'elastic' superuser"
  kms_key_id  = local.contract.ecs.kms_key_arn
}
