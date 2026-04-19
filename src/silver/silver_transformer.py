# src/silver/silver_transformer.py
"""
Silver layer transformer.

Bronze → Silver = cleanse, deduplicate, type-cast, and upsert via MERGE INTO.

Key patterns
────────────
1. Read incremental Bronze data for a given processing date.
2. Apply business rules (null handling, type casts, standardisation).
3. Deduplicate within the batch using a row_number window.
4. Upsert to Silver via Iceberg MERGE INTO — Silver is NOT append-only.
   Existing rows are updated; new rows are inserted.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List
from src.utils.logger import PipelineLogger

log = PipelineLogger("silver_transformer")


# ──────────────────────────────────────────────────────────────────────────────
# DDL — Create Silver tables
# ──────────────────────────────────────────────────────────────────────────────

def create_silver_tables(spark: SparkSession, catalog: str = "spark_catalog") -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.silver")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.silver.orders (
            order_id        STRING        NOT NULL COMMENT 'Business key',
            customer_id     STRING        NOT NULL,
            product_id      STRING,
            quantity        INT,
            unit_price      DECIMAL(18,4),
            total_amount    DECIMAL(18,4) COMMENT 'Derived: quantity * unit_price',
            order_status    STRING,
            order_date      DATE          COMMENT 'Partition column',
            order_ts        TIMESTAMP,
            -- Lineage
            _bronze_path    STRING,
            _silver_ts      TIMESTAMP,
            -- DQ flags
            dq_passed       BOOLEAN
        )
        USING iceberg
        PARTITIONED BY (months(order_date))
        TBLPROPERTIES (
            'format-version'                  = '2',
            'write.parquet.compression-codec' = 'zstd',
            'read.parquet.vectorization.enabled' = 'true',
            'write.merge.mode'                = 'merge-on-read'
        )
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.silver.customers (
            customer_id     STRING        NOT NULL,
            full_name       STRING,
            email           STRING,
            email_domain    STRING        COMMENT 'Derived from email',
            country         STRING,
            country_code    STRING,
            signup_date     DATE,
            _silver_ts      TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version'                  = '2',
            'write.parquet.compression-codec' = 'zstd',
            'write.merge.mode'                = 'merge-on-read'
        )
    """)
    log.info("Silver tables ready", catalog=catalog)


# ──────────────────────────────────────────────────────────────────────────────
# Orders: Bronze → Silver
# ──────────────────────────────────────────────────────────────────────────────

def transform_orders(df: DataFrame) -> DataFrame:
    """
    Apply cleansing and business rules to raw orders data.

    Rules applied
    ─────────────
    - Drop rows with null order_id or customer_id (business key nulls)
    - Cast unit_price to DECIMAL; replace negatives with null (bad data)
    - Compute total_amount = quantity * unit_price
    - Standardise order_status to upper-case
    - Derive order_date from order_ts
    - Flag rows that pass all quality checks
    """
    return (
        df
        # ── Drop critical nulls ──────────────────────────────────────────
        .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())

        # ── Type casts ───────────────────────────────────────────────────
        .withColumn("unit_price",
            F.when(F.col("unit_price").cast("decimal(18,4)") < 0, F.lit(None))
             .otherwise(F.col("unit_price").cast("decimal(18,4)"))
        )
        .withColumn("quantity",
            F.when(F.col("quantity") < 0, F.lit(None))
             .otherwise(F.col("quantity").cast("int"))
        )

        # ── Derived columns ──────────────────────────────────────────────
        .withColumn("total_amount",
            F.round(F.col("quantity") * F.col("unit_price"), 4)
        )
        .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
        .withColumn("order_date",   F.to_date(F.col("order_ts")))

        # ── DQ flag ──────────────────────────────────────────────────────
        .withColumn("dq_passed",
            F.col("unit_price").isNotNull() &
            F.col("quantity").isNotNull() &
            F.col("order_status").isin("PENDING","CONFIRMED","SHIPPED","DELIVERED","CANCELLED")
        )

        # ── Audit ────────────────────────────────────────────────────────
        .withColumn("_bronze_path", F.col("_source_path"))
        .withColumn("_silver_ts",   F.current_timestamp())

        # ── Select final Silver schema ────────────────────────────────────
        .select(
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "total_amount",
            "order_status", "order_date", "order_ts",
            "_bronze_path", "_silver_ts", "dq_passed",
        )
    )


def transform_customers(df: DataFrame) -> DataFrame:
    """
    Apply cleansing and enrichment to raw customers data.
    """
    return (
        df
        .filter(F.col("customer_id").isNotNull())
        .withColumn("full_name",
            F.trim(F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
        )
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("email_domain",
            F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        )
        .withColumn("country",      F.initcap(F.trim(F.col("country"))))
        .withColumn("country_code", F.upper(F.col("country")).substr(1, 2))
        .withColumn("_silver_ts",   F.current_timestamp())
        .select(
            "customer_id", "full_name", "email", "email_domain",
            "country", "country_code", "signup_date", "_silver_ts",
        )
    )


# ──────────────────────────────────────────────────────────────────────────────
# Deduplication
# ──────────────────────────────────────────────────────────────────────────────

def deduplicate(
    df: DataFrame,
    key_cols: List[str],
    order_col: str = "_silver_ts",
) -> DataFrame:
    """
    Keep the latest record per business key within the batch.

    Uses a row_number window rather than dropDuplicates so that the ordering
    is deterministic — the newest record (highest order_col) wins.
    """
    window = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


# ──────────────────────────────────────────────────────────────────────────────
# MERGE INTO (upsert)
# ──────────────────────────────────────────────────────────────────────────────

def upsert_to_silver(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    merge_keys: List[str],
    temp_view_name: str = "_silver_source",
) -> None:
    """
    Upsert source_df into an Iceberg Silver table using MERGE INTO.

    Iceberg MERGE INTO (v2) supports:
      - UPDATE existing rows when the merge key matches
      - INSERT new rows when no match is found
      - DELETE (not used here — soft deletes are preferred for Silver)

    The source DataFrame is registered as a temp view so we can reference it
    in SQL. Spark's query planner handles the join efficiently via
    Iceberg's partition pruning.
    """
    source_df.createOrReplaceTempView(temp_view_name)

    # Build the ON clause from merge keys
    on_clause = " AND ".join(
        f"target.{k} = source.{k}" for k in merge_keys
    )

    # Build UPDATE SET clause — update all non-key columns
    sample_row = source_df.limit(1)
    all_cols = source_df.columns
    update_cols = [c for c in all_cols if c not in merge_keys]
    update_set = ", ".join(f"target.{c} = source.{c}" for c in update_cols)

    # Build INSERT clause
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join(f"source.{c}" for c in all_cols)

    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {temp_view_name} AS source
        ON ({on_clause})
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    spark.sql(merge_sql)
    log.info("MERGE INTO complete", table=target_table, keys=merge_keys)


# ──────────────────────────────────────────────────────────────────────────────
# Orchestration: Bronze → Silver (orders)
# ──────────────────────────────────────────────────────────────────────────────

def run_orders_bronze_to_silver(
    spark: SparkSession,
    bronze_table: str,
    silver_table: str,
    processing_date: str,          # "YYYY-MM-DD" — process one day at a time
    merge_keys: List[str] = ("order_id",),
) -> None:
    """
    Read today's Bronze slice, transform, dedup, and upsert to Silver.

    Incremental reads are done via partition filter on ingestion_date — only
    today's Bronze records are loaded into memory.
    """
    log.info("Orders Bronze → Silver", date=processing_date)

    raw_df = spark.table(bronze_table).filter(
        F.col("ingestion_date") == processing_date
    )

    if raw_df.isEmpty():
        log.warn("No Bronze records for date", date=processing_date)
        return

    transformed = transform_orders(raw_df)
    deduped     = deduplicate(transformed, list(merge_keys))
    upsert_to_silver(spark, deduped, silver_table, list(merge_keys))

    log.info("Orders Silver upsert done",
             date=processing_date, rows=deduped.count())


def run_customers_bronze_to_silver(
    spark: SparkSession,
    bronze_table: str,
    silver_table: str,
    processing_date: str,
    merge_keys: List[str] = ("customer_id",),
) -> None:
    log.info("Customers Bronze → Silver", date=processing_date)

    raw_df = spark.table(bronze_table).filter(
        F.col("ingestion_date") == processing_date
    )
    if raw_df.isEmpty():
        log.warn("No Bronze customer records", date=processing_date)
        return

    transformed = transform_customers(raw_df)
    deduped     = deduplicate(transformed, list(merge_keys))
    upsert_to_silver(spark, deduped, silver_table, list(merge_keys))
    log.info("Customers Silver upsert done", date=processing_date)
