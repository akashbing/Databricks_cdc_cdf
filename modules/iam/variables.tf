variable "name_prefix"        { type = string }
variable "aws_region"         { type = string }
variable "account_id"         { type = string }
variable "msk_cluster_arn"    { type = string }
variable "s3_bucket_arns"     { type = list(string) }
variable "scripts_bucket_arn" { type = string }
variable "logs_bucket_arn"    { type = string }
variable "secret_arns"        { type = list(string); default = [] }
variable "kms_key_arn"        { type = string; default = "" }
variable "glue_database_name" { type = string }
