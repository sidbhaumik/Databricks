# src/utils/iceberg_utils.py
"""
Backward-compatible table utility helpers.

NOTE:
- This module name is kept for compatibility with older notebooks/jobs.
- Implementations are Delta Lake oriented so they run on Databricks Community Edition.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from src.utils.logger import PipelineLogger

log = PipelineLogger("table_utils")


def create_iceberg_table_if_not_exists(
    spark: SparkSession,
    table_name: str,
    ddl_schema: str,
    partition_by: Optional[str] = None,
    location: Optional[str] = None,
    extra_properties: Optional[dict] = None,
) -> None:
    """Create table using Delta Lake (legacy function name retained)."""
    partition_clause = f"PARTITIONED BY ({partition_by})" if partition_by else ""
    location_clause = f"LOCATION '{location}'" if location else ""

    props = {"delta.autoOptimize.optimizeWrite": "true"}
    if extra_properties:
        props.update(extra_properties)
    tblproperties = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {ddl_schema}
        )
        USING delta
        {partition_clause}
        {location_clause}
        TBLPROPERTIES ({tblproperties})
        """
    )
    log.info("Table ready", table=table_name)


def read_at_snapshot(spark: SparkSession, table_name: str, snapshot_id: int) -> DataFrame:
    """Read Delta table by version (legacy arg name snapshot_id)."""
    return spark.read.option("versionAsOf", snapshot_id).table(table_name)


def read_at_timestamp(spark: SparkSession, table_name: str, ts: str) -> DataFrame:
    """Read Delta table as-of timestamp."""
    return spark.read.option("timestampAsOf", ts).table(table_name)


def list_snapshots(spark: SparkSession, table_name: str) -> DataFrame:
    """Return Delta history (legacy function name retained)."""
    return spark.sql(f"DESCRIBE HISTORY {table_name}")


def add_column(spark: SparkSession, table_name: str, col_name: str, col_type: str) -> None:
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({col_name} {col_type})")
    log.info("Column added", table=table_name, column=col_name, type=col_type)


def rename_column(spark: SparkSession, table_name: str, old_name: str, new_name: str) -> None:
    spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}")
    log.info("Column renamed", table=table_name, old=old_name, new=new_name)


def expire_snapshots(spark: SparkSession, table_name: str, older_than_days: int = 7) -> None:
    """Delta equivalent: VACUUM retention (hours)."""
    hours = older_than_days * 24
    spark.sql(f"VACUUM {table_name} RETAIN {hours} HOURS")
    log.info("Vacuum completed", table=table_name, retain_hours=hours)


def remove_orphan_files(spark: SparkSession, table_name: str, older_than_days: int = 3) -> None:
    """Alias to VACUUM for backwards compatibility."""
    expire_snapshots(spark, table_name, older_than_days=older_than_days)


def rewrite_data_files(
    spark: SparkSession,
    table_name: str,
    strategy: str = "binpack",
    sort_order: Optional[str] = None,
) -> None:
    """Delta equivalent compaction/optimization."""
    if sort_order:
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({sort_order})")
    else:
        spark.sql(f"OPTIMIZE {table_name}")
    log.info("Optimize completed", table=table_name, strategy=strategy)


def run_full_maintenance(spark: SparkSession, table_name: str) -> None:
    log.info("Starting maintenance", table=table_name)
    rewrite_data_files(spark, table_name)
    expire_snapshots(spark, table_name)
    log.info("Maintenance complete", table=table_name)
