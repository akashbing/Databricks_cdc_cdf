# ─── modules/glue/main.tf ─────────────────────────────────────────────────────

# ── Glue Data Catalog Database ────────────────────────────────────────────────
resource "aws_glue_catalog_database" "cdc_output" {
  name        = var.glue_database_name
  description = "CDC pipeline output tables — managed by Terraform"
}

# ── Glue Connection (VPC / MSK access) ───────────────────────────────────────
resource "aws_glue_connection" "msk_vpc" {
  name            = "${var.name_prefix}-msk-vpc-connection"
  connection_type = "NETWORK"
  description     = "VPC network connection allowing Glue to reach MSK"

  physical_connection_requirements {
    subnet_id              = var.subnet_ids[0]
    security_group_id_list = [var.glue_sg_id]
    availability_zone      = var.availability_zones[0]
  }
}

# ── CloudWatch Log Group for Glue ─────────────────────────────────────────────
resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws/glue/jobs/${var.name_prefix}-cdc-streaming"
  retention_in_days = 30
}

# ── Glue Streaming Job ────────────────────────────────────────────────────────
resource "aws_glue_job" "cdc_streaming" {
  name              = "${var.name_prefix}-cdc-streaming"
  description       = "CDC pipeline: reads from MSK (Debezium) → writes to Delta Lake on S3"
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = var.timeout_minutes
  max_retries       = 0   # Streaming jobs should not auto-retry

  command {
    name            = "gluestreaming"          # ← key for streaming jobs
    script_location = var.glue_script_s3_path
    python_version  = "3"
  }

  connections = [aws_glue_connection.msk_vpc.name]

  default_arguments = {
    # ── Glue built-ins ──────────────────────────────────────────────────────
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_job.name
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = tostring(var.spark_ui_enabled)
    "--spark-event-logs-path"            = "s3://${var.spark_ui_bucket_id}/spark-ui/"
    "--enable-auto-scaling"              = "true"
    "--enable-glue-datacatalog"          = "true"
    "--TempDir"                          = "s3://${var.checkpoints_bucket_id}/glue-temp/"

    # ── Extra JARs for Kafka + Delta ────────────────────────────────────────
    "--extra-jars" = join(",", [
      "s3://${var.scripts_bucket_id}/jars/aws-msk-iam-auth-1.1.9-all.jar",
      "s3://${var.scripts_bucket_id}/jars/delta-core_2.12-2.4.0.jar",
      "s3://${var.scripts_bucket_id}/jars/delta-storage-2.4.0.jar",
    ])

    # ── Spark config ────────────────────────────────────────────────────────
    "--conf" = join(" --conf ", [
      "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
      "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
      "spark.databricks.delta.schema.autoMerge.enabled=true",
      "spark.sql.shuffle.partitions=200",
      "spark.streaming.stopGracefullyOnShutdown=true",
      "spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    ])

    # ── Pipeline arguments (passed to the Glue script) ──────────────────────
    "--MSK_BOOTSTRAP_SERVERS"   = var.msk_bootstrap_servers
    "--KAFKA_TOPICS"            = var.kafka_topics
    "--KAFKA_STARTING_OFFSETS"  = var.kafka_starting_offsets
    "--TARGET_S3_PATH"          = "s3://${var.output_bucket_id}/cdc/"
    "--CHECKPOINT_S3_PATH"      = "s3://${var.checkpoints_bucket_id}/streaming/"
    "--DLQ_S3_PATH"             = "s3://${var.dlq_bucket_id}/failed-events/"
    "--GLUE_DATABASE"           = var.glue_database_name
    "--TARGET_FORMAT"           = var.target_format
    "--DEBEZIUM_SERVER_NAME"    = var.debezium_server_name
    "--TRIGGER_INTERVAL"        = "30 seconds"
    "--MAX_OFFSETS_PER_TRIGGER" = "10000"
    "--AWS_REGION"              = var.aws_region
    "--SECRET_ARN"              = var.db_secret_arn
  }

  execution_property {
    max_concurrent_runs = var.max_concurrent_runs
  }

  # Spark UI + history
  dynamic "notification_property" {
    for_each = var.alert_sns_arn != "" ? [1] : []
    content {
      notify_delay_after = 60
    }
  }

  tags = { Name = "${var.name_prefix}-cdc-streaming" }
}

# ── EventBridge rule — restart Glue job if it fails ──────────────────────────
resource "aws_cloudwatch_event_rule" "glue_failure" {
  name        = "${var.name_prefix}-glue-failure-rule"
  description = "Triggers SNS alert when CDC Glue job fails"

  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.cdc_streaming.name]
      state   = ["FAILED", "STOPPED", "ERROR", "TIMEOUT"]
    }
  })
}

resource "aws_cloudwatch_event_target" "glue_failure_sns" {
  count     = var.alert_sns_arn != "" ? 1 : 0
  rule      = aws_cloudwatch_event_rule.glue_failure.name
  target_id = "GlueFailureSNS"
  arn       = var.alert_sns_arn
}

# ── CloudWatch Alarms ─────────────────────────────────────────────────────────
resource "aws_cloudwatch_metric_alarm" "glue_failed_runs" {
  alarm_name          = "${var.name_prefix}-glue-failed-runs"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.ALL.jvm.heap.used"
  namespace           = "Glue"
  period              = 300
  statistic           = "Maximum"
  threshold           = 1
  alarm_description   = "Glue CDC job has failed runs"
  alarm_actions       = var.alert_sns_arn != "" ? [var.alert_sns_arn] : []

  dimensions = {
    JobName = aws_glue_job.cdc_streaming.name
    Type    = "gauge"
  }
}

# ── SNS Topic for alerts ──────────────────────────────────────────────────────
resource "aws_sns_topic" "pipeline_alerts" {
  count             = var.alert_email != "" ? 1 : 0
  name              = "${var.name_prefix}-pipeline-alerts"
  kms_master_key_id = var.kms_key_arn != "" ? var.kms_key_arn : null
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.pipeline_alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "glue_job_name"       { value = aws_glue_job.cdc_streaming.name }
output "glue_job_arn"        { value = aws_glue_job.cdc_streaming.arn }
output "glue_connection_name"{ value = aws_glue_connection.msk_vpc.name }
output "glue_database_name"  { value = aws_glue_catalog_database.cdc_output.name }
output "alert_sns_arn"       { value = var.alert_email != "" ? aws_sns_topic.pipeline_alerts[0].arn : "" }
