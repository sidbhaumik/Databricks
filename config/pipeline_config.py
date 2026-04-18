# config/pipeline_config.py
"""
Central configuration for the Medallion pipeline.
Override values via environment variables or Databricks Widgets in notebooks.
"""

import os

# ---------------------------------------------------------------------------
# Catalog & database names
# ---------------------------------------------------------------------------
CATALOG = os.getenv("CATALOG", "spark_catalog")

BRONZE_DB  = f"{CATALOG}.bronze"
SILVER_DB  = f"{CATALOG}.silver"
GOLD_DB    = f"{CATALOG}.gold"

# ---------------------------------------------------------------------------
# Source paths  (DBFS, S3, ADLS, or GCS)
# ---------------------------------------------------------------------------
BASE_PATH        = os.getenv("BASE_PATH", "/mnt/datalake")
AVRO_SOURCE_PATH  = f"{BASE_PATH}/raw/avro/orders/"
PARQUET_SOURCE_PATH = f"{BASE_PATH}/raw/parquet/customers/"

# Auto Loader checkpoint locations
CHECKPOINT_BASE   = f"{BASE_PATH}/checkpoints"
ORDERS_CHECKPOINT = f"{CHECKPOINT_BASE}/orders_bronze"
CUSTOMERS_CHECKPOINT = f"{CHECKPOINT_BASE}/customers_bronze"

# ---------------------------------------------------------------------------
# Iceberg table names
# ---------------------------------------------------------------------------
ORDERS_BRONZE_TABLE   = f"{BRONZE_DB}.orders_raw"
ORDERS_SILVER_TABLE   = f"{SILVER_DB}.orders"
ORDERS_GOLD_TABLE     = f"{GOLD_DB}.orders_daily_agg"

CUSTOMERS_BRONZE_TABLE = f"{BRONZE_DB}.customers_raw"
CUSTOMERS_SILVER_TABLE = f"{SILVER_DB}.customers"

# ---------------------------------------------------------------------------
# Iceberg write options (applied at every layer)
# ---------------------------------------------------------------------------
ICEBERG_WRITE_OPTIONS = {
    "write.format.default":         "parquet",
    "write.parquet.compression-codec": "zstd",
    "write.metadata.compression-codec": "gzip",
    "write.target-file-size-bytes": str(128 * 1024 * 1024),  # 128 MB
}

# ---------------------------------------------------------------------------
# Partition spec
# ---------------------------------------------------------------------------
BRONZE_PARTITION_COL  = "ingestion_date"   # date the record landed
SILVER_PARTITION_COL  = "order_date"
GOLD_PARTITION_COL    = "report_date"

# ---------------------------------------------------------------------------
# Merge / upsert keys
# ---------------------------------------------------------------------------
ORDERS_MERGE_KEYS    = ["order_id"]
CUSTOMERS_MERGE_KEYS = ["customer_id"]

# ---------------------------------------------------------------------------
# Data quality thresholds
# ---------------------------------------------------------------------------
NULL_TOLERANCE_PCT   = 5.0    # max % nulls allowed in key columns
DUPE_TOLERANCE_PCT   = 1.0    # max % duplicate primary keys allowed
