"""
SparkSession factory.

Designed to run both in Databricks Community Edition and local environments
without requiring Iceberg runtime jars.
"""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "MedallionPipeline") -> SparkSession:
    """
    Return (or create) a SparkSession tuned for Delta-based medallion pipelines.

    Community Edition compatibility:
    - Avoids Iceberg-specific extensions/catalog plugins.
    - Uses default Spark catalog (`spark_catalog`) and Delta SQL features.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
