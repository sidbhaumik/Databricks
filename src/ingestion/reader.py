# src/ingestion/reader.py
"""
Unified reader for Avro and Parquet source files.

Supports both batch (spark.read) and streaming (spark.readStream via Auto Loader).
Auto Loader (cloudFiles) is the recommended approach on Databricks — it tracks
which files have already been processed and scales to billions of files.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from typing import Optional
from src.utils.logger import PipelineLogger

log = PipelineLogger("reader")


# ──────────────────────────────────────────────────────────────────────────────
# Batch readers
# ──────────────────────────────────────────────────────────────────────────────

def read_avro_batch(
    spark: SparkSession,
    path: str,
    schema: Optional[StructType] = None,
) -> DataFrame:
    """
    Read Avro files from a path (or glob) in batch mode.

    Avro files embed their schema, so passing schema= is optional but
    recommended in production to reject schema drift at ingest time.
    """
    reader = spark.read.format("avro")
    if schema:
        reader = reader.schema(schema)
    df = reader.load(path)
    log.info("Avro batch read", path=path, rows=df.count())
    return _add_audit_columns(df, source_path=path, file_format="avro")


def read_parquet_batch(
    spark: SparkSession,
    path: str,
    schema: Optional[StructType] = None,
    merge_schema: bool = True,
) -> DataFrame:
    """
    Read Parquet files from a path (or glob) in batch mode.

    merge_schema=True allows reading directories where different Parquet files
    have slightly different schemas (schema evolution across partitions).
    """
    reader = (
        spark.read.format("parquet")
        .option("mergeSchema", str(merge_schema).lower())
    )
    if schema:
        reader = reader.schema(schema)
    df = reader.load(path)
    log.info("Parquet batch read", path=path, rows=df.count())
    return _add_audit_columns(df, source_path=path, file_format="parquet")


# ──────────────────────────────────────────────────────────────────────────────
# Streaming readers (Auto Loader — Databricks cloudFiles)
# ──────────────────────────────────────────────────────────────────────────────

def read_avro_stream(
    spark: SparkSession,
    source_path: str,
    schema_location: str,
    schema: Optional[StructType] = None,
) -> DataFrame:
    """
    Stream new Avro files via Databricks Auto Loader.

    Auto Loader detects new files using cloud notifications (SQS/Event Grid/Pub Sub)
    or directory listing, and checkpoints progress so restarts are idempotent.

    Parameters
    ----------
    schema_location : DBFS/cloud path where Auto Loader persists inferred schema.
                      Required so schema is stable across restarts.
    """
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "avro")
        .option("cloudFiles.schemaLocation", schema_location)
        # Infer schema from first batch, then enforce it on subsequent batches
        .option("cloudFiles.inferColumnTypes", "true")
        # Surface the source file path as a column for lineage
        .option("cloudFiles.includeExistingFiles", "true")
    )
    if schema:
        reader = reader.schema(schema)
    df = reader.load(source_path)
    log.info("Avro stream opened", path=source_path)
    return _add_audit_columns(df, source_path=source_path, file_format="avro")


def read_parquet_stream(
    spark: SparkSession,
    source_path: str,
    schema_location: str,
    schema: Optional[StructType] = None,
) -> DataFrame:
    """
    Stream new Parquet files via Databricks Auto Loader.

    Parquet supports schema evolution — Auto Loader handles new columns
    automatically when cloudFiles.schemaEvolutionMode = 'addNewColumns'.
    """
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
    )
    if schema:
        reader = reader.schema(schema)
    df = reader.load(source_path)
    log.info("Parquet stream opened", path=source_path)
    return _add_audit_columns(df, source_path=source_path, file_format="parquet")


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────

def _add_audit_columns(df: DataFrame, source_path: str, file_format: str) -> DataFrame:
    """
    Append standard audit/lineage columns to every ingested DataFrame.

    These columns propagate through Bronze and are preserved in Silver/Gold
    for full data lineage.
    """
    return (
        df
        .withColumn("_source_path",   F.lit(source_path))
        .withColumn("_file_format",   F.lit(file_format))
        .withColumn("_ingestion_ts",  F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())   # used as partition column
    )
