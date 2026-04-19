# src/utils/iceberg_utils.py
"""
Helpers for Iceberg DDL, time travel, and table maintenance.

All functions accept a SparkSession and a fully-qualified table name such as
    spark_catalog.bronze.orders_raw
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional
from src.utils.logger import PipelineLogger

log = PipelineLogger("iceberg_utils")


# ──────────────────────────────────────────────────────────────────────────────
# Table creation
# ──────────────────────────────────────────────────────────────────────────────

def create_iceberg_table_if_not_exists(
    spark: SparkSession,
    table_name: str,
    ddl_schema: str,
    partition_by: Optional[str] = None,
    location: Optional[str] = None,
    extra_properties: Optional[dict] = None,
) -> None:
    """
    Issue a CREATE TABLE IF NOT EXISTS for an Iceberg table.

    Parameters
    ----------
    table_name    : fully-qualified name, e.g. spark_catalog.bronze.orders_raw
    ddl_schema    : column definitions, e.g. "id BIGINT, name STRING, ..."
    partition_by  : optional partition expression, e.g. "days(order_date)"
    location      : optional explicit storage path
    extra_properties : dict of Iceberg table properties to set
    """
    partition_clause = f"PARTITIONED BY ({partition_by})" if partition_by else ""
    location_clause  = f"LOCATION '{location}'" if location else ""

    default_props = {
        "format-version":               "2",          # Iceberg v2 (row-level deletes)
        "write.parquet.compression-codec": "zstd",
        "write.metadata.compression-codec": "gzip",
        "write.target-file-size-bytes": "134217728",  # 128 MB
        "history.expire.max-snapshot-age-ms": "604800000",  # 7 days
    }
    if extra_properties:
        default_props.update(extra_properties)

    tblproperties = ", ".join(
        f"'{k}' = '{v}'" for k, v in default_props.items()
    )

    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {ddl_schema}
        )
        USING iceberg
        {partition_clause}
        {location_clause}
        TBLPROPERTIES ({tblproperties})
    """
    spark.sql(sql)
    log.info("Table ready", table=table_name)


# ──────────────────────────────────────────────────────────────────────────────
# Time travel
# ──────────────────────────────────────────────────────────────────────────────

def read_at_snapshot(
    spark: SparkSession, table_name: str, snapshot_id: int
) -> DataFrame:
    """Read an Iceberg table at a specific snapshot ID."""
    return spark.read.option("snapshot-id", snapshot_id).table(table_name)


def read_at_timestamp(
    spark: SparkSession, table_name: str, ts: str
) -> DataFrame:
    """
    Read an Iceberg table as it existed at a given timestamp.

    Parameters
    ----------
    ts : ISO-8601 string, e.g. "2024-01-15 08:00:00"
    """
    return spark.read.option("as-of-timestamp", ts).table(table_name)


def list_snapshots(spark: SparkSession, table_name: str) -> DataFrame:
    """Return the snapshot history of an Iceberg table."""
    return spark.sql(f"SELECT * FROM {table_name}.snapshots ORDER BY committed_at DESC")


# ──────────────────────────────────────────────────────────────────────────────
# Schema evolution
# ──────────────────────────────────────────────────────────────────────────────

def add_column(
    spark: SparkSession, table_name: str, col_name: str, col_type: str
) -> None:
    """Add a new nullable column to an existing Iceberg table."""
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
    log.info("Column added", table=table_name, column=col_name, type=col_type)


def rename_column(
    spark: SparkSession, table_name: str, old_name: str, new_name: str
) -> None:
    """Rename a column (Iceberg v2 metadata-only operation)."""
    spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}")
    log.info("Column renamed", table=table_name, old=old_name, new=new_name)


# ──────────────────────────────────────────────────────────────────────────────
# Maintenance (expire snapshots, remove orphans, rewrite data files)
# ──────────────────────────────────────────────────────────────────────────────

def expire_snapshots(
    spark: SparkSession,
    table_name: str,
    older_than_days: int = 7,
) -> None:
    """
    Remove snapshots older than `older_than_days` to reclaim storage.
    Iceberg retains the table's current state — this only removes old versions.
    """
    spark.sql(f"""
        CALL spark_catalog.system.expire_snapshots(
            table => '{table_name}',
            older_than => TIMESTAMP '{older_than_days} days ago',
            retain_last => 3
        )
    """)
    log.info("Snapshots expired", table=table_name, older_than_days=older_than_days)


def remove_orphan_files(
    spark: SparkSession,
    table_name: str,
    older_than_days: int = 3,
) -> None:
    """Delete data files that are no longer referenced by any snapshot."""
    spark.sql(f"""
        CALL spark_catalog.system.remove_orphan_files(
            table => '{table_name}',
            older_than => TIMESTAMP '{older_than_days} days ago'
        )
    """)
    log.info("Orphan files removed", table=table_name)


def rewrite_data_files(
    spark: SparkSession,
    table_name: str,
    strategy: str = "sort",
    sort_order: Optional[str] = None,
) -> None:
    """
    Compact small files and optionally re-sort data for better read performance.

    Parameters
    ----------
    strategy   : "binpack" (compact only) or "sort" (compact + sort)
    sort_order : e.g. "order_date, customer_id" — required when strategy="sort"
    """
    sort_clause = f"sort_order => '{sort_order}'," if sort_order else ""
    spark.sql(f"""
        CALL spark_catalog.system.rewrite_data_files(
            table => '{table_name}',
            strategy => '{strategy}',
            {sort_clause}
            options => map('min-input-files','5')
        )
    """)
    log.info("Data files rewritten", table=table_name, strategy=strategy)


def run_full_maintenance(spark: SparkSession, table_name: str) -> None:
    """Convenience wrapper: expire → orphan cleanup → compact."""
    log.info("Starting maintenance", table=table_name)
    expire_snapshots(spark, table_name)
    remove_orphan_files(spark, table_name)
    rewrite_data_files(spark, table_name, strategy="binpack")
    log.info("Maintenance complete", table=table_name)
