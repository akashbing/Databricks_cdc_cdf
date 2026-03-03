# ─── modules/msk/main.tf ──────────────────────────────────────────────────────

# ── MSK Cluster ───────────────────────────────────────────────────────────────
resource "aws_msk_cluster" "this" {
  cluster_name           = "${var.name_prefix}-msk"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.subnet_ids
    security_groups = [var.msk_sg_id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_storage_gb
      }
    }
  }

  # ── Authentication — IAM (recommended) ──────────────────────────────────
  client_authentication {
    sasl {
      iam = true
    }
    # Uncomment for SCRAM-SHA-512 in addition to IAM:
    # sasl {
    #   scram = true
    # }
    tls {}
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
    encryption_at_rest_kms_key_arn = var.kms_key_arn
  }

  # ── Enhanced monitoring ──────────────────────────────────────────────────
  enhanced_monitoring = "PER_TOPIC_PER_BROKER"

  open_monitoring {
    prometheus {
      jmx_exporter  { enabled_in_broker = true }
      node_exporter { enabled_in_broker = true }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk_broker.name
      }
      s3 {
        enabled = true
        bucket  = var.logs_bucket_id
        prefix  = "msk-broker-logs/"
      }
    }
  }

  # ── MSK configuration ────────────────────────────────────────────────────
  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  tags = { Name = "${var.name_prefix}-msk" }
}

# ── MSK Custom Configuration ──────────────────────────────────────────────────
resource "aws_msk_configuration" "this" {
  name           = "${var.name_prefix}-msk-config"
  kafka_versions = [var.kafka_version]

  server_properties = <<-EOT
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.io.threads=8
    num.network.threads=5
    num.partitions=6
    num.replica.fetchers=2
    replica.lag.time.max.ms=30000
    socket.receive.buffer.bytes=102400
    socket.request.max.bytes=104857600
    socket.send.buffer.bytes=102400
    unclean.leader.election.enable=true
    zookeeper.session.timeout.ms=18000
    log.retention.hours=168
    log.segment.bytes=1073741824
    log.retention.check.interval.ms=300000
    message.max.bytes=10485760
  EOT
}

# ── CloudWatch Log Group for MSK brokers ──────────────────────────────────────
resource "aws_cloudwatch_log_group" "msk_broker" {
  name              = "/aws/msk/${var.name_prefix}/broker"
  retention_in_days = 14
}

# ── MSK Connect — Debezium Custom Plugin ──────────────────────────────────────
resource "aws_mskconnect_custom_plugin" "debezium" {
  name         = "${var.name_prefix}-debezium-postgres"
  content_type = "ZIP"
  description  = "Debezium PostgreSQL CDC connector plugin"

  location {
    s3 {
      bucket_arn = var.scripts_bucket_arn
      file_key   = aws_s3_object.debezium_plugin.key
    }
  }

  depends_on = [aws_s3_object.debezium_plugin]
}

# Upload the Debezium plugin ZIP to S3 (download it separately)
resource "aws_s3_object" "debezium_plugin" {
  bucket = var.scripts_bucket_id
  key    = "msk-plugins/debezium-connector-postgres-2.4.0.zip"
  source = "${path.root}/plugins/debezium-connector-postgres-2.4.0.zip"
  etag   = filemd5("${path.root}/plugins/debezium-connector-postgres-2.4.0.zip")
}

# ── MSK Connect Worker Configuration ─────────────────────────────────────────
resource "aws_mskconnect_worker_configuration" "debezium" {
  name = "${var.name_prefix}-debezium-worker-config"

  properties_file_content = <<-EOT
    key.converter=org.apache.kafka.connect.json.JsonConverter
    key.converter.schemas.enable=true
    value.converter=org.apache.kafka.connect.json.JsonConverter
    value.converter.schemas.enable=true
    offset.storage.topic=__debezium-offsets
    config.storage.topic=__debezium-configs
    status.storage.topic=__debezium-status
    offset.storage.replication.factor=3
    config.storage.replication.factor=3
    status.storage.replication.factor=3
  EOT
}

# ── MSK Connect Connector ─────────────────────────────────────────────────────
resource "aws_mskconnect_connector" "debezium_postgres" {
  name = "${var.name_prefix}-debezium-postgres-connector"

  kafkaconnect_version = "2.7.1"

  capacity {
    autoscaling {
      mcu_count        = 1
      min_worker_count = 1
      max_worker_count = 4

      scale_in_policy {
        cpu_utilization_percentage = 20
      }
      scale_out_policy {
        cpu_utilization_percentage = 80
      }
    }
  }

  connector_configuration = {
    "connector.class"                        = "io.debezium.connector.postgresql.PostgresConnector"
    "tasks.max"                              = "1"
    "database.hostname"                      = var.db_host
    "database.port"                          = tostring(var.db_port)
    "database.user"                          = var.db_username
    "database.password"                      = var.db_password
    "database.dbname"                        = var.db_name
    "database.server.name"                   = var.debezium_server_name
    "plugin.name"                            = "pgoutput"
    "slot.name"                              = "debezium_slot"
    "publication.name"                       = "debezium_publication"
    "table.include.list"                     = var.db_tables
    "topic.prefix"                           = var.debezium_server_name
    "snapshot.mode"                          = "initial"
    "snapshot.isolation.mode"                = "read_committed"
    "decimal.handling.mode"                  = "string"
    "time.precision.mode"                    = "connect"
    "timestamp.timezone"                     = "UTC"
    "tombstones.on.delete"                   = "true"
    "heartbeat.interval.ms"                  = "10000"
    "heartbeat.topics.prefix"                = "__debezium-heartbeat"
    "transforms"                             = "unwrap"
    "transforms.unwrap.type"                 = "io.debezium.transforms.ExtractNewRecordState"
    "transforms.unwrap.drop.tombstones"      = "false"
    "transforms.unwrap.delete.handling.mode" = "rewrite"
    "transforms.unwrap.add.fields"           = "op,table,lsn,source.ts_ms"
    "errors.tolerance"                       = "all"
    "errors.log.enable"                      = "true"
    "errors.log.include.messages"            = "true"
    "errors.deadletterqueue.topic.name"      = "debezium-dlq"
    "errors.deadletterqueue.topic.replication.factor" = "3"
    "database.ssl.mode"                      = "require"
    "max.batch.size"                         = "2048"
    "max.queue.size"                         = "16384"
    "poll.interval.ms"                       = "500"
  }

  kafka_cluster {
    apache_kafka_cluster {
      bootstrap_servers = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
      vpc {
        security_groups = [var.msk_connect_sg_id]
        subnets         = var.subnet_ids
      }
    }
  }

  kafka_cluster_client_authentication {
    authentication_type = "IAM"
  }

  kafka_cluster_encryption_in_transit {
    encryption_type = "TLS"
  }

  plugin {
    custom_plugin {
      arn      = aws_mskconnect_custom_plugin.debezium.arn
      revision = aws_mskconnect_custom_plugin.debezium.latest_revision
    }
  }

  worker_configuration {
    arn      = aws_mskconnect_worker_configuration.debezium.arn
    revision = aws_mskconnect_worker_configuration.debezium.latest_revision
  }

  service_execution_role_arn = var.msk_connect_role_arn

  log_delivery {
    worker_log_delivery {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk_connect.name
      }
      s3 {
        enabled = true
        bucket  = var.logs_bucket_id
        prefix  = "msk-connect-logs/"
      }
    }
  }

  depends_on = [aws_msk_cluster.this, aws_mskconnect_custom_plugin.debezium]
}

resource "aws_cloudwatch_log_group" "msk_connect" {
  name              = "/aws/msk-connect/${var.name_prefix}/debezium"
  retention_in_days = 14
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "msk_cluster_arn"           { value = aws_msk_cluster.this.arn }
output "msk_cluster_name"          { value = aws_msk_cluster.this.cluster_name }
output "bootstrap_brokers_iam"     { value = aws_msk_cluster.this.bootstrap_brokers_sasl_iam }
output "bootstrap_brokers_tls"     { value = aws_msk_cluster.this.bootstrap_brokers_tls }
output "zookeeper_connect_string"  { value = aws_msk_cluster.this.zookeeper_connect_string }
output "connector_arn"             { value = aws_mskconnect_connector.debezium_postgres.arn }
