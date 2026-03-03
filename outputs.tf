output "msk_bootstrap_brokers_iam" {
  description = "MSK bootstrap brokers (IAM auth)"
  value       = module.msk.bootstrap_brokers_iam
}

output "msk_cluster_arn" {
  description = "MSK cluster ARN"
  value       = module.msk.msk_cluster_arn
}

output "glue_job_name" {
  description = "Glue streaming job name"
  value       = module.glue.glue_job_name
}

output "glue_job_arn" {
  description = "Glue streaming job ARN"
  value       = module.glue.glue_job_arn
}

output "output_s3_bucket" {
  description = "S3 bucket for CDC output (Delta tables)"
  value       = module.s3.output_bucket_id
}

output "checkpoints_s3_bucket" {
  description = "S3 bucket for Spark streaming checkpoints"
  value       = module.s3.checkpoints_bucket_id
}

output "scripts_s3_bucket" {
  description = "S3 bucket for Glue scripts and JARs"
  value       = module.s3.scripts_bucket_id
}

output "glue_database_name" {
  description = "Glue catalog database for CDC output tables"
  value       = var.glue_database_name
}

output "vpc_id" {
  description = "VPC ID used by the pipeline"
  value       = module.networking.vpc_id
}

output "db_secret_arn" {
  description = "Secrets Manager ARN for DB credentials"
  value       = aws_secretsmanager_secret.db_creds.arn
  sensitive   = true
}

output "run_glue_job_command" {
  description = "AWS CLI command to start the Glue streaming job"
  value       = "aws glue start-job-run --job-name ${module.glue.glue_job_name} --region ${var.aws_region}"
}
