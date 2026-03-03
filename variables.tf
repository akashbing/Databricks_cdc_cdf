# ─── General ──────────────────────────────────────────────────────────────────
variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used as prefix for all resources"
  type        = string
  default     = "cdc-pipeline"
}

variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "owner" {
  description = "Team or individual who owns these resources"
  type        = string
  default     = "data-engineering"
}

# ─── Networking ───────────────────────────────────────────────────────────────
variable "vpc_id" {
  description = "Existing VPC ID. Leave empty to create a new VPC."
  type        = string
  default     = ""
}

variable "vpc_cidr" {
  description = "CIDR block for the new VPC (only used if vpc_id is empty)"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_ids" {
  description = "List of existing private subnet IDs (must be in same VPC). Leave empty to create."
  type        = list(string)
  default     = []
}

variable "availability_zones" {
  description = "AZs to use for subnets"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

# ─── MSK ──────────────────────────────────────────────────────────────────────
variable "msk_kafka_version" {
  description = "Apache Kafka version for MSK"
  type        = string
  default     = "3.5.1"
}

variable "msk_broker_instance_type" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.m5.large"
}

variable "msk_broker_count" {
  description = "Number of MSK brokers (must be a multiple of AZ count)"
  type        = number
  default     = 3
}

variable "msk_broker_storage_gb" {
  description = "EBS storage per broker in GB"
  type        = number
  default     = 100
}

variable "msk_topics" {
  description = "Kafka topics to pre-create"
  type = list(object({
    name               = string
    partitions         = number
    replication_factor = number
    retention_ms       = number
  }))
  default = [
    {
      name               = "dbserver1.public.orders"
      partitions         = 6
      replication_factor = 3
      retention_ms       = 604800000  # 7 days
    },
    {
      name               = "dbserver1.public.customers"
      partitions         = 6
      replication_factor = 3
      retention_ms       = 604800000
    },
    {
      name               = "debezium-dlq"
      partitions         = 3
      replication_factor = 3
      retention_ms       = 2592000000  # 30 days
    },
    {
      name               = "__debezium-heartbeat.dbserver1"
      partitions         = 1
      replication_factor = 3
      retention_ms       = 3600000  # 1 hour
    }
  ]
}

# ─── Source Database ──────────────────────────────────────────────────────────
variable "db_host" {
  description = "Source RDS / Aurora PostgreSQL hostname"
  type        = string
}

variable "db_port" {
  description = "Source database port"
  type        = number
  default     = 5432
}

variable "db_name" {
  description = "Source database name"
  type        = string
  default     = "myapp"
}

variable "db_username" {
  description = "Debezium replication user"
  type        = string
  default     = "debezium_user"
}

variable "db_password" {
  description = "Debezium replication user password"
  type        = string
  sensitive   = true
}

variable "db_tables" {
  description = "Comma-separated list of tables to capture (schema.table format)"
  type        = string
  default     = "public.orders,public.customers,public.products"
}

variable "debezium_server_name" {
  description = "Logical name prefix for Debezium connector (used as Kafka topic prefix)"
  type        = string
  default     = "dbserver1"
}

# ─── Glue ─────────────────────────────────────────────────────────────────────
variable "glue_worker_type" {
  description = "Glue worker type: G.1X | G.2X | G.4X | G.8X"
  type        = string
  default     = "G.2X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers"
  type        = number
  default     = 4
}

variable "glue_max_concurrent_runs" {
  description = "Max concurrent Glue job runs"
  type        = number
  default     = 1
}

variable "glue_timeout_minutes" {
  description = "Glue streaming job timeout in minutes (2880 = 48 hrs)"
  type        = number
  default     = 2880
}

variable "glue_spark_ui_enabled" {
  description = "Enable Spark UI logs for the Glue job"
  type        = bool
  default     = true
}

# ─── Target / Output ──────────────────────────────────────────────────────────
variable "target_table_format" {
  description = "Target table format: delta | iceberg | parquet"
  type        = string
  default     = "delta"
}

variable "enable_glue_catalog" {
  description = "Register output tables in AWS Glue Data Catalog"
  type        = bool
  default     = true
}

variable "glue_database_name" {
  description = "Glue catalog database name for output tables"
  type        = string
  default     = "cdc_output"
}

# ─── Alerting ─────────────────────────────────────────────────────────────────
variable "alert_email" {
  description = "Email address for pipeline failure alerts (SNS)"
  type        = string
  default     = ""
}
