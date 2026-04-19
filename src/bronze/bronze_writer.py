# src/bronze/bronze_writer.py
"""
Bronze layer writer.

Bronze = raw ingestion, append-only, no transformations.
Goal: get data into Iceberg as quickly as possible, preserving everything.

Writes to an Iceberg table partitioned by ingestion_date so that:
  - time-travel queries are efficient
  - old partitions can be expired independently
  - Silver can process incremental slices via partition filters
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Optional
from src.utils.logger import PipelineLogger

log = PipelineLogger("bronze_writer")


# ──────────────────────────────────────────────────────────────────────────────
# DDL — Create Bronze tables
# ──────────────────────────────────────────────────────────────────────────────

ORDERS_BRONZE_DDL = """
    order_id        STRING       COMMENT 'Source order identifier',
    customer_id     STRING,
    product_id      STRING,
    quantity        INT,
    unit_price      DOUBLE,
    order_status    STRING,
    order_ts        TIMESTAMP,
    -- Audit columns appended by reader
    _source_path    STRING,
    _file_format    STRING,
    _ingestion_ts   TIMESTAMP,
    ingestion_date  DATE         COMMENT 'Partition column — day data landed'
"""

CUSTOMERS_BRONZE_DDL = """
    customer_id     STRING,
    first_name      STRING,
    last_name       STRING,
    email           STRING,
    country         STRING,
    signup_date     DATE,
    _source_path    STRING,
    _file_format    STRING,
    _ingestion_ts   TIMESTAMP,
    ingestion_date  DATE
"""


def create_bronze_tables(spark: SparkSession, catalog: str = "spark_catalog") -> None:
    """Create Bronze Iceberg databases and tables (idempotent)."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.bronze")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.bronze.orders_raw (
            {ORDERS_BRONZE_DDL}
        )
        USING iceberg
        PARTITIONED BY (days(ingestion_date))
        TBLPROPERTIES (
            'format-version'                  = '2',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes'    = '134217728'
        )
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.bronze.customers_raw (
            {CUSTOMERS_BRONZE_DDL}
        )
        USING iceberg
        PARTITIONED BY (days(ingestion_date))
        TBLPROPERTIES (
            'format-version'                  = '2',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes'    = '134217728'
        )
    """)
    log.info("Bronze tables ready", catalog=catalog)


# ──────────────────────────────────────────────────────────────────────────────
# Batch write
# ──────────────────────────────────────────────────────────────────────────────

def write_bronze_batch(
    df: DataFrame,
    table_name: str,
    mode: str = "append",
) -> None:
    """
    Write a batch DataFrame to a Bronze Iceberg table.

    mode="append"    — standard incremental load (default)
    mode="overwrite" — full reload for a given partition (use with care)
    """
    row_count = df.count()
    (
        df.write
        .format("iceberg")
        .mode(mode)
        .option("write.distribution-mode", "hash")  # hash-distribute by partition
        .saveAsTable(table_name)
    )
    log.info("Bronze batch write complete", table=table_name, rows=row_count, mode=mode)


# ──────────────────────────────────────────────────────────────────────────────
# Streaming write (used with Auto Loader readStream)
# ──────────────────────────────────────────────────────────────────────────────

def write_bronze_stream(
    df: DataFrame,
    table_name: str,
    checkpoint_path: str,
    trigger_interval: str = "5 minutes",
    output_mode: str = "append",
) -> "StreamingQuery":
    """
    Write a streaming DataFrame to a Bronze Iceberg table using foreachBatch.

    foreachBatch gives us:
      - Exactly-once semantics via Iceberg's ACID guarantees + checkpoint
      - Ability to count rows per micro-batch for monitoring
      - Easy retry logic without downstream duplication
    """
    from pyspark.sql.streaming import StreamingQuery

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            log.info("Empty micro-batch skipped", table=table_name, batch_id=batch_id)
            return
        count = batch_df.count()
        (
            batch_df.write
            .format("iceberg")
            .mode("append")
            .option("write.distribution-mode", "hash")
            .saveAsTable(table_name)
        )
        log.info("Bronze stream batch written",
                 table=table_name, batch_id=batch_id, rows=count)

    query: StreamingQuery = (
        df.writeStream
        .foreachBatch(write_batch)
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
    )
    log.info("Bronze stream started", table=table_name, trigger=trigger_interval)
    return query
