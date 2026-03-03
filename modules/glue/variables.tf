variable "name_prefix"              { type = string }
variable "aws_region"               { type = string }
variable "glue_role_arn"            { type = string }
variable "glue_database_name"       { type = string }
variable "glue_script_s3_path"      { type = string }
variable "subnet_ids"               { type = list(string) }
variable "availability_zones"       { type = list(string) }
variable "glue_sg_id"               { type = string }
variable "worker_type"              { type = string; default = "G.2X" }
variable "number_of_workers"        { type = number; default = 4 }
variable "timeout_minutes"          { type = number; default = 2880 }
variable "max_concurrent_runs"      { type = number; default = 1 }
variable "spark_ui_enabled"         { type = bool;   default = true }
variable "spark_ui_bucket_id"       { type = string }
variable "scripts_bucket_id"        { type = string }
variable "output_bucket_id"         { type = string }
variable "checkpoints_bucket_id"    { type = string }
variable "dlq_bucket_id"            { type = string }
variable "msk_bootstrap_servers"    { type = string }
variable "kafka_topics"             { type = string }
variable "kafka_starting_offsets"   { type = string; default = "latest" }
variable "target_format"            { type = string; default = "delta" }
variable "debezium_server_name"     { type = string }
variable "db_secret_arn"            { type = string; default = "" }
variable "kms_key_arn"              { type = string; default = "" }
variable "alert_email"              { type = string; default = "" }
variable "alert_sns_arn"            { type = string; default = "" }
