"""
CDC PySpark Script — AWS Glue Streaming Job
============================================
Reads Debezium CDC events from Amazon MSK, parses the Debezium JSON envelope,
and writes to Delta Lake on S3 (raw archive + current-state MERGE table).

Deploy with Terraform. All runtime config is injected via Glue job arguments.

Glue version : 4.0
Python        : 3
Worker type   : G.2X (or G.4X for high throughput)
Job type      : gluestreaming
"""

import sys
import json
import logging
from datetime import datetime

from awsglue.transforms   import *
from awsglue.utils        import getResolvedOptions
from awsglue.context      import GlueContext
from awsglue.job          import Job
from pyspark.context       import SparkContext
from pyspark.sql           import SparkSession
from pyspark.sql           import functions as F
from pyspark.sql.types     import (
    BooleanType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("cdc_glue_job")

# ─── Parse Glue job arguments ─────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "MSK_BOOTSTRAP_SERVERS",
    "KAFKA_TOPICS",
    "KAFKA_STARTING_OFFSETS",
    "TARGET_S3_PATH",
    "CHECKPOINT_S3_PATH",
    "DLQ_S3_PATH",
    "GLUE_DATABASE",
    "TARGET_FORMAT",
    "DEBEZIUM_SERVER_NAME",
    "TRIGGER_INTERVAL",
    "MAX_OFFSETS_PER_TRIGGER",
    "AWS_REGION",
    "SECRET_ARN",
])

# ─── Glue / Spark context setup ───────────────────────────────────────────────
sc          = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Spark config for Delta Lake
spark.conf.set("spark.sql.extensions",
               "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog",
               "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")

logger.info("Glue job started: %s | Spark %s", args["JOB_NAME"], spark.version)

# ─── Runtime config ───────────────────────────────────────────────────────────
MSK_BOOTSTRAP      = args["MSK_BOOTSTRAP_SERVERS"]
KAFKA_TOPICS       = args["KAFKA_TOPICS"]           # comma-separated
STARTING_OFFSETS   = args["KAFKA_STARTING_OFFSETS"]
TARGET_S3          = args["TARGET_S3_PATH"].rstrip("/")
CHECKPOINT_S3      = args["CHECKPOINT_S3_PATH"].rstrip("/")
DLQ_S3             = args["DLQ_S3_PATH"].rstrip("/")
GLUE_DB            = args["GLUE_DATABASE"]
TARGET_FORMAT      = args["TARGET_FORMAT"]          # delta | parquet
SERVER_NAME        = args["DEBEZIUM_SERVER_NAME"]
TRIGGER_INTERVAL   = args["TRIGGER_INTERVAL"]
MAX_OFFSETS        = int(args["MAX_OFFSETS_PER_TRIGGER"])
AWS_REGION         = args["AWS_REGION"]
SECRET_ARN         = args["SECRET_ARN"]


# ─── Helper: fetch credentials from Secrets Manager ──────────────────────────
def get_secret(secret_arn: str, region: str) -> dict:
    import boto3
    client = boto3.client("secretsmanager", region_name=region)
    resp   = client.get_secret_value(SecretId=secret_arn)
    return json.loads(resp["SecretString"])


# ─── Debezium envelope schema ─────────────────────────────────────────────────
def build_debezium_schema(row_schema: StructType) -> StructType:
    """Wrap a row schema into the full Debezium CDC envelope."""
    source_schema = StructType([
        StructField("version",   StringType(), True),
        StructField("connector", StringType(), True),
        StructField("name",      StringType(), True),
        StructField("ts_ms",     LongType(),   True),
        StructField("snapshot",  StringType(), True),
        StructField("db",        StringType(), True),
        StructField("schema",    StringType(), True),
        StructField("table",     StringType(), True),
        StructField("txId",      LongType(),   True),
        StructField("lsn",       LongType(),   True),
    ])
    tx_schema = StructType([
        StructField("id",                    StringType(), True),
        StructField("total_order",           LongType(),   True),
        StructField("data_collection_order", LongType(),   True),
    ])
    payload_schema = StructType([
        StructField("before",      row_schema,   True),
        StructField("after",       row_schema,   True),
        StructField("source",      source_schema, True),
        StructField("op",          StringType(), True),
        StructField("ts_ms",       LongType(),   True),
        StructField("transaction", tx_schema,    True),
    ])
    return StructType([StructField("payload", payload_schema, True)])


# ─── Table schemas — add your tables here ─────────────────────────────────────
ORDERS_SCHEMA = StructType([
    StructField("id",          IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id",  IntegerType(), True),
    StructField("quantity",    IntegerType(), True),
    StructField("total_price", StringType(),  True),
    StructField("status",      StringType(),  True),
    StructField("created_at",  LongType(),    True),
    StructField("updated_at",  LongType(),    True),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("id",         IntegerType(), False),
    StructField("name",       StringType(),  True),
    StructField("email",      StringType(),  True),
    StructField("phone",      StringType(),  True),
    StructField("created_at", LongType(),    True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("id",          IntegerType(), False),
    StructField("name",        StringType(),  True),
    StructField("category",    StringType(),  True),
    StructField("price",       StringType(),  True),
    StructField("in_stock",    BooleanType(), True),
    StructField("updated_at",  LongType(),    True),
])

# Map topic suffix → (row_schema, primary_key)
TABLE_CONFIG = {
    "orders":    (ORDERS_SCHEMA,    "id"),
    "customers": (CUSTOMERS_SCHEMA, "id"),
    "products":  (PRODUCTS_SCHEMA,  "id"),
}


# ─── Read from MSK (Structured Streaming) ─────────────────────────────────────
def read_from_msk():
    """
    Create a Structured Streaming DataFrame from Amazon MSK using IAM auth.
    Glue 4.0 includes the Kafka connector — no extra JARs needed for the reader.
    """
    kafka_opts = {
        "kafka.bootstrap.servers":   MSK_BOOTSTRAP,
        "subscribe":                 KAFKA_TOPICS,
        "startingOffsets":           STARTING_OFFSETS,
        "maxOffsetsPerTrigger":      str(MAX_OFFSETS),
        "failOnDataLoss":            "false",
        "kafka.security.protocol":   "SASL_SSL",
        "kafka.sasl.mechanism":      "AWS_MSK_IAM",
        "kafka.sasl.jaas.config":    (
            "software.amazon.msk.auth.iam.IAMLoginModule required;"
        ),
        "kafka.sasl.client.callback.handler.class": (
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        ),
        # Glue-specific: keep connections alive
        "kafka.reconnect.backoff.ms":     "1000",
        "kafka.reconnect.backoff.max.ms": "10000",
        "kafka.session.timeout.ms":       "45000",
        "kafka.request.timeout.ms":       "60000",
    }

    logger.info("Subscribing to MSK topics: %s", KAFKA_TOPICS)
    return (
        spark.readStream
        .format("kafka")
        .options(**kafka_opts)
        .load()
    )


# ─── Parse Debezium JSON envelope ─────────────────────────────────────────────
def parse_cdc_events(raw_df, row_schema: StructType):
    """
    Decode the raw Kafka binary payload into a structured CDC DataFrame.
    Returns one row per CDC event with before/after/op/metadata columns.
    """
    envelope_schema = build_debezium_schema(row_schema)

    return (
        raw_df
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("value").cast(StringType()).alias("raw_value"),
        )
        # Tombstone messages (Kafka deletes) have null value — route to DLQ
        .withColumn("is_tombstone", F.col("raw_value").isNull())
        # Parse JSON
        .withColumn("parsed", F.when(
            ~F.col("is_tombstone"),
            F.from_json(F.col("raw_value"), envelope_schema)
        ))
        # Flatten payload
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("kafka_ts"),
            F.col("raw_value"),
            F.col("is_tombstone"),
            F.col("parsed.payload.op").alias("cdc_op"),
            F.col("parsed.payload.ts_ms").alias("cdc_ts_ms"),
            F.col("parsed.payload.source.db").alias("src_db"),
            F.col("parsed.payload.source.table").alias("src_table"),
            F.col("parsed.payload.source.lsn").alias("src_lsn"),
            F.col("parsed.payload.source.txId").alias("src_tx_id"),
            F.col("parsed.payload.before").alias("before"),
            F.col("parsed.payload.after").alias("after"),
            F.col("parsed.payload.transaction.id").alias("tx_id"),
        )
        # Derived / audit columns
        .withColumn("event_time",
            (F.col("cdc_ts_ms") / 1000).cast(TimestampType()))
        .withColumn("op_label",
            F.when(F.col("cdc_op") == "c", "INSERT")
            .when(F.col("cdc_op") == "u", "UPDATE")
            .when(F.col("cdc_op") == "d", "DELETE")
            .when(F.col("cdc_op") == "r", "SNAPSHOT")
            .otherwise("UNKNOWN"))
        .withColumn("processed_at", F.current_timestamp())
        # Date partitions
        .withColumn("year",  F.year("event_time"))
        .withColumn("month", F.month("event_time"))
        .withColumn("day",   F.dayofmonth("event_time"))
    )


# ─── Delta MERGE — upsert + hard delete ───────────────────────────────────────
def apply_delta_merge(micro_batch_df, batch_id: int, target_path: str, pk: str):
    """
    foreachBatch handler.
    - INSERTs / UPDATEs / SNAPSHOTs → MERGE into Delta table
    - DELETEs                        → hard DELETE from Delta table
    Deduplicates within each micro-batch by keeping the latest LSN per PK.
    """
    from delta.tables import DeltaTable
    from pyspark.sql.window import Window

    batch_count = micro_batch_df.count()
    if batch_count == 0:
        logger.info("Batch %d — empty, skipping", batch_id)
        return

    logger.info("Batch %d — %d events for table at %s", batch_id, batch_count, target_path)

    # ── Upserts (c, u, r) ────────────────────────────────────────────────────
    upserts = (
        micro_batch_df
        .filter(F.col("cdc_op").isin("c", "u", "r"))
        .filter(F.col("after").isNotNull())
        .withColumn("_rn",
            F.row_number().over(
                Window.partitionBy(F.col(f"after.{pk}"))
                      .orderBy(F.col("src_lsn").desc_nulls_last())
            )
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        # Flatten the `after` struct into top-level columns
        .select(
            F.col(f"after.{pk}").alias(pk),
            F.col("after.*"),
            F.col("event_time").alias("_cdc_event_time"),
            F.col("cdc_op").alias("_cdc_op"),
            F.col("src_lsn").alias("_cdc_lsn"),
            F.col("processed_at").alias("_cdc_processed_at"),
        )
    )

    # ── Hard deletes (d) ──────────────────────────────────────────────────────
    deletes = (
        micro_batch_df
        .filter(F.col("cdc_op") == "d")
        .filter(F.col("before").isNotNull())
        .select(F.col(f"before.{pk}").alias(pk))
        .distinct()
    )

    # ── Write to Delta ────────────────────────────────────────────────────────
    if DeltaTable.isDeltaTable(spark, target_path):
        delta_tbl = DeltaTable.forPath(spark, target_path)

        # MERGE upserts
        if upserts.count() > 0:
            (
                delta_tbl.alias("target")
                .merge(
                    upserts.alias("src"),
                    f"target.{pk} = src.{pk}",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info("Batch %d — merged %d upserts", batch_id, upserts.count())

        # Hard deletes
        delete_ids = [r[pk] for r in deletes.collect()]
        if delete_ids:
            delta_tbl.delete(F.col(pk).isin(delete_ids))
            logger.info("Batch %d — deleted %d rows", batch_id, len(delete_ids))

    else:
        # First run — bootstrap the Delta table
        logger.info("Batch %d — bootstrapping new Delta table at %s", batch_id, target_path)
        if upserts.count() > 0:
            (
                upserts.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(target_path)
            )

    # ── Register / update Glue Data Catalog ──────────────────────────────────
    table_name = target_path.rstrip("/").split("/")[-1]
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {GLUE_DB}.{table_name}
            USING DELTA
            LOCATION '{target_path}'
        """)
    except Exception as e:
        logger.warning("Could not register table in Glue catalog: %s", e)


# ─── Write raw CDC archive ────────────────────────────────────────────────────
def write_raw_archive(parsed_df, table_name: str):
    """Append every CDC event to a partitioned S3 path — full audit trail."""
    path  = f"{TARGET_S3}/raw/{table_name}/"
    chk   = f"{CHECKPOINT_S3}/raw/{table_name}/"

    return (
        parsed_df
        .drop("before", "after")       # flatten structs not needed in archive
        .writeStream
        .format(TARGET_FORMAT)
        .outputMode("append")
        .partitionBy("year", "month", "day")
        .option("checkpointLocation", chk)
        .option("path", path)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


# ─── Write current-state Delta table ─────────────────────────────────────────
def write_current_state(parsed_df, table_name: str, pk: str):
    """Maintain latest-state Delta table via MERGE in foreachBatch."""
    path = f"{TARGET_S3}/current/{table_name}/"
    chk  = f"{CHECKPOINT_S3}/current/{table_name}/"

    return (
        parsed_df.writeStream
        .outputMode("update")
        .option("checkpointLocation", chk)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .foreachBatch(
            lambda df, bid: apply_delta_merge(df, bid, path, pk)
        )
        .start()
    )


# ─── Write DLQ (failed / tombstone messages) ──────────────────────────────────
def write_dlq(raw_df):
    """Route un-parseable or tombstone messages to the dead-letter S3 path."""
    failed = (
        raw_df
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("value").cast(StringType()).alias("raw_value"),
            F.current_timestamp().alias("failed_at"),
            F.lit("null_value_or_parse_error").alias("failure_reason"),
        )
        .filter(F.col("raw_value").isNull())
    )

    return (
        failed.writeStream
        .format("json")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_S3}/dlq/")
        .option("path", DLQ_S3)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("CDC Glue Streaming Job starting")
    logger.info("MSK:     %s", MSK_BOOTSTRAP)
    logger.info("Topics:  %s", KAFKA_TOPICS)
    logger.info("Target:  %s", TARGET_S3)
    logger.info("Format:  %s", TARGET_FORMAT)
    logger.info("=" * 60)

    # ── 1. Read raw Kafka stream ──────────────────────────────────────────────
    raw_df = read_from_msk()

    # ── 2. Start DLQ writer for bad/tombstone messages ────────────────────────
    dlq_query = write_dlq(raw_df)

    # ── 3. Per-table processing ───────────────────────────────────────────────
    all_queries = [dlq_query]

    for table_name, (row_schema, pk) in TABLE_CONFIG.items():
        # Filter to this table's topic
        topic = f"{SERVER_NAME}.public.{table_name}"

        # Check if this topic is in the subscribed list
        if table_name not in KAFKA_TOPICS and topic not in KAFKA_TOPICS:
            logger.info("Skipping table %s (not in KAFKA_TOPICS)", table_name)
            continue

        table_df = (
            raw_df
            .filter(F.col("topic") == topic)
            .transform(lambda df: parse_cdc_events(df, row_schema))
            .filter(~F.col("is_tombstone"))
        )

        # 3a. Raw append archive
        raw_q = write_raw_archive(table_df, table_name)
        all_queries.append(raw_q)
        logger.info("Raw archive query started for: %s", table_name)

        # 3b. Current-state Delta table (MERGE)
        state_q = write_current_state(table_df, table_name, pk)
        all_queries.append(state_q)
        logger.info("Current-state query started for: %s → pk=%s", table_name, pk)

    logger.info("All %d streaming queries running. Awaiting termination…", len(all_queries))

    # ── 4. Block until all queries finish (or job is stopped) ─────────────────
    for q in all_queries:
        try:
            q.awaitTermination()
        except Exception as exc:
            logger.error("Query %s terminated with error: %s", getattr(q, "id", "?"), exc)
            raise

    # Glue job commit (for bookmarking — not typical for streaming but good practice)
    job.commit()


if __name__ == "__main__":
    main()
