# Databricks notebook source
# notebooks/01_ingest_bronze.py
"""
Stage 1: Source → Bronze

Two modes
─────────
STREAMING  — Auto Loader; keeps running, picks up new files automatically.
BATCH      — One-shot read of all files currently in the source path.

Toggle via the 'run_mode' widget below.
"""

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<your-repo>/databricks_medallion")

from pyspark.sql import SparkSession
from src.utils.spark_session import get_spark
from src.ingestion.reader import read_avro_batch, read_parquet_batch, \
                                  read_avro_stream, read_parquet_stream
from src.bronze.bronze_writer import write_bronze_batch, write_bronze_stream
from config.pipeline_config import *

spark = get_spark()

# COMMAND ----------
# DBTITLE 1,Widget — choose run mode

dbutils.widgets.dropdown("run_mode", "BATCH", ["BATCH", "STREAMING"])
run_mode = dbutils.widgets.get("run_mode")
print(f"Run mode: {run_mode}")

# COMMAND ----------
# DBTITLE 1,Ingest Orders (Avro)

if run_mode == "BATCH":
    # ── Batch ─────────────────────────────────────────────────────────
    orders_raw = read_avro_batch(spark, AVRO_SOURCE_PATH)
    write_bronze_batch(orders_raw, ORDERS_BRONZE_TABLE)

else:
    # ── Streaming (Auto Loader) ────────────────────────────────────────
    orders_stream = read_avro_stream(
        spark,
        source_path=AVRO_SOURCE_PATH,
        schema_location=f"{CHECKPOINT_BASE}/orders_schema",
    )
    orders_query = write_bronze_stream(
        orders_stream,
        table_name=ORDERS_BRONZE_TABLE,
        checkpoint_path=ORDERS_CHECKPOINT,
        trigger_interval="2 minutes",
    )
    print(f"Streaming query ID: {orders_query.id}")

# COMMAND ----------
# DBTITLE 1,Ingest Customers (Parquet)

if run_mode == "BATCH":
    customers_raw = read_parquet_batch(spark, PARQUET_SOURCE_PATH)
    write_bronze_batch(customers_raw, CUSTOMERS_BRONZE_TABLE)
else:
    customers_stream = read_parquet_stream(
        spark,
        source_path=PARQUET_SOURCE_PATH,
        schema_location=f"{CHECKPOINT_BASE}/customers_schema",
    )
    customers_query = write_bronze_stream(
        customers_stream,
        table_name=CUSTOMERS_BRONZE_TABLE,
        checkpoint_path=CUSTOMERS_CHECKPOINT,
        trigger_interval="2 minutes",
    )

# COMMAND ----------
# DBTITLE 1,Quick validation — Bronze counts

spark.sql(f"""
    SELECT ingestion_date, COUNT(*) AS row_count, _file_format
    FROM   {ORDERS_BRONZE_TABLE}
    GROUP  BY 1, 3
    ORDER  BY 1 DESC
""").show()

spark.sql(f"""
    SELECT ingestion_date, COUNT(*) AS row_count
    FROM   {CUSTOMERS_BRONZE_TABLE}
    GROUP  BY 1
    ORDER  BY 1 DESC
""").show()

# COMMAND ----------
# DBTITLE 1,Iceberg snapshot history (Bronze orders)

spark.sql(f"SELECT * FROM {ORDERS_BRONZE_TABLE}.snapshots ORDER BY committed_at DESC").show(5)
