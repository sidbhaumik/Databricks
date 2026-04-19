# src/utils/spark_session.py
"""
SparkSession factory.

On Databricks the session already exists — get_or_create() returns it.
Locally, this builds a session configured with the Iceberg Spark runtime.
"""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "MedallionPipeline") -> SparkSession:
    """
    Return (or create) a SparkSession with Apache Iceberg support.

    Key configs explained
    ─────────────────────
    spark.sql.extensions
        Registers the IcebergSparkSessionExtensions which add support for
        Iceberg DDL (CREATE TABLE … USING iceberg) and DML (MERGE INTO).

    spark.sql.catalog.spark_catalog
        Replaces the default Hive session catalog with Iceberg's
        SparkSessionCatalog so that existing spark_catalog.db.table
        references resolve to Iceberg tables stored in the Databricks
        metastore (or Glue / REST catalog in other environments).

    write.parquet.compression-codec = zstd
        ZSTD gives better compression than snappy with similar read speed.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        # ── Iceberg extensions ──────────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # ── Catalog — use Iceberg session catalog as drop-in replacement ─
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        # ── Optional: add a dedicated iceberg catalog (Unity Catalog etc.) ─
        # .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        # .config("spark.sql.catalog.iceberg.type", "rest")
        # .config("spark.sql.catalog.iceberg.uri", "https://<catalog-endpoint>")
        # ── Performance ─────────────────────────────────────────────────
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        # ── Schema evolution — allow adding new columns to Iceberg tables ─
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
