# ─── root/terraform_resources.tf ─────────────────────────────────────────────
# Wires all modules together into a complete CDC pipeline

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ── Secrets Manager — DB credentials ─────────────────────────────────────────
resource "aws_secretsmanager_secret" "db_creds" {
  name                    = "${local.name_prefix}-db-creds-${local.suffix}"
  description             = "Source DB credentials for Debezium CDC connector"
  recovery_window_in_days = 7
  kms_key_id              = var.environment == "prod" ? aws_kms_key.pipeline[0].id : null
}

resource "aws_secretsmanager_secret_version" "db_creds" {
  secret_id = aws_secretsmanager_secret.db_creds.id
  secret_string = jsonencode({
    username = var.db_username
    password = var.db_password
    host     = var.db_host
    port     = var.db_port
    dbname   = var.db_name
  })
}

# ── KMS Key (prod only) ───────────────────────────────────────────────────────
resource "aws_kms_key" "pipeline" {
  count                   = var.environment == "prod" ? 1 : 0
  description             = "KMS key for ${local.name_prefix} CDC pipeline"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "pipeline" {
  count         = var.environment == "prod" ? 1 : 0
  name          = "alias/${local.name_prefix}-pipeline"
  target_key_id = aws_kms_key.pipeline[0].key_id
}

locals {
  kms_key_arn = var.environment == "prod" ? aws_kms_key.pipeline[0].arn : ""
}

# ── Networking ────────────────────────────────────────────────────────────────
module "networking" {
  source = "./modules/networking"

  name_prefix        = local.name_prefix
  vpc_id             = var.vpc_id
  vpc_cidr           = var.vpc_cidr
  private_subnet_ids = var.private_subnet_ids
  availability_zones = var.availability_zones
}

# ── S3 Buckets ────────────────────────────────────────────────────────────────
module "s3" {
  source = "./modules/s3"

  name_prefix = local.name_prefix
  suffix      = local.suffix
  kms_key_arn = local.kms_key_arn
}

# ── IAM Roles ─────────────────────────────────────────────────────────────────
module "iam" {
  source = "./modules/iam"

  name_prefix        = local.name_prefix
  aws_region         = var.aws_region
  account_id         = data.aws_caller_identity.current.account_id
  msk_cluster_arn    = module.msk.msk_cluster_arn
  s3_bucket_arns     = [
    module.s3.scripts_bucket_arn,
    module.s3.output_bucket_arn,
    module.s3.checkpoints_bucket_arn,
    module.s3.logs_bucket_arn,
    module.s3.dlq_bucket_arn,
  ]
  scripts_bucket_arn = module.s3.scripts_bucket_arn
  logs_bucket_arn    = module.s3.logs_bucket_arn
  secret_arns        = [aws_secretsmanager_secret.db_creds.arn]
  kms_key_arn        = local.kms_key_arn
  glue_database_name = var.glue_database_name

  depends_on = [module.msk]
}

# ── MSK Cluster + Debezium Connector ─────────────────────────────────────────
module "msk" {
  source = "./modules/msk"

  name_prefix           = local.name_prefix
  kafka_version         = var.msk_kafka_version
  broker_instance_type  = var.msk_broker_instance_type
  broker_count          = var.msk_broker_count
  broker_storage_gb     = var.msk_broker_storage_gb
  subnet_ids            = module.networking.private_subnet_ids
  msk_sg_id             = module.networking.msk_sg_id
  msk_connect_sg_id     = module.networking.msk_connect_sg_id
  kms_key_arn           = local.kms_key_arn
  logs_bucket_id        = module.s3.logs_bucket_id
  logs_bucket_arn       = module.s3.logs_bucket_arn
  scripts_bucket_id     = module.s3.scripts_bucket_id
  scripts_bucket_arn    = module.s3.scripts_bucket_arn
  msk_connect_role_arn  = module.iam.msk_connect_role_arn
  db_host               = var.db_host
  db_port               = var.db_port
  db_name               = var.db_name
  db_username           = var.db_username
  db_password           = var.db_password
  db_tables             = var.db_tables
  debezium_server_name  = var.debezium_server_name

  depends_on = [module.networking, module.s3, module.iam]
}

# ── SNS Topic ─────────────────────────────────────────────────────────────────
resource "aws_sns_topic" "alerts" {
  count = var.alert_email != "" ? 1 : 0
  name  = "${local.name_prefix}-alerts-${local.suffix}"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ── Glue Job ──────────────────────────────────────────────────────────────────
module "glue" {
  source = "./modules/glue"

  name_prefix            = local.name_prefix
  aws_region             = var.aws_region
  glue_role_arn          = module.iam.glue_role_arn
  glue_database_name     = var.glue_database_name
  glue_script_s3_path    = module.s3.glue_script_s3_path
  subnet_ids             = module.networking.private_subnet_ids
  availability_zones     = var.availability_zones
  glue_sg_id             = module.networking.glue_sg_id
  worker_type            = var.glue_worker_type
  number_of_workers      = var.glue_number_of_workers
  timeout_minutes        = var.glue_timeout_minutes
  max_concurrent_runs    = var.glue_max_concurrent_runs
  spark_ui_enabled       = var.glue_spark_ui_enabled
  spark_ui_bucket_id     = module.s3.spark_ui_bucket_id
  scripts_bucket_id      = module.s3.scripts_bucket_id
  output_bucket_id       = module.s3.output_bucket_id
  checkpoints_bucket_id  = module.s3.checkpoints_bucket_id
  dlq_bucket_id          = module.s3.dlq_bucket_id
  msk_bootstrap_servers  = module.msk.bootstrap_brokers_iam
  kafka_topics           = join(",", [for t in var.msk_topics : t.name if !startswith(t.name, "__") && !startswith(t.name, "debezium")])
  kafka_starting_offsets = "latest"
  target_format          = var.target_table_format
  debezium_server_name   = var.debezium_server_name
  db_secret_arn          = aws_secretsmanager_secret.db_creds.arn
  kms_key_arn            = local.kms_key_arn
  alert_email            = var.alert_email
  alert_sns_arn          = var.alert_email != "" ? aws_sns_topic.alerts[0].arn : ""

  depends_on = [module.msk, module.iam, module.s3]
}

# ── Glue Catalog Database ─────────────────────────────────────────────────────
resource "aws_glue_catalog_database" "output" {
  count       = var.enable_glue_catalog ? 1 : 0
  name        = var.glue_database_name
  description = "CDC output tables — ${var.environment}"
}
