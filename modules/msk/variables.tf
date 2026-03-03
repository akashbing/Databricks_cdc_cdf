variable "name_prefix"           { type = string }
variable "kafka_version"          { type = string }
variable "broker_instance_type"   { type = string }
variable "broker_count"           { type = number }
variable "broker_storage_gb"      { type = number }
variable "subnet_ids"             { type = list(string) }
variable "msk_sg_id"              { type = string }
variable "msk_connect_sg_id"      { type = string }
variable "kms_key_arn"            { type = string; default = "" }
variable "logs_bucket_id"         { type = string }
variable "logs_bucket_arn"        { type = string }
variable "scripts_bucket_id"      { type = string }
variable "scripts_bucket_arn"     { type = string }
variable "msk_connect_role_arn"   { type = string }
variable "db_host"                { type = string }
variable "db_port"                { type = number; default = 5432 }
variable "db_name"                { type = string }
variable "db_username"            { type = string }
variable "db_password"            { type = string; sensitive = true }
variable "db_tables"              { type = string }
variable "debezium_server_name"   { type = string }
