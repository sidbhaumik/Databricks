# Databricks Medallion Architecture with Apache Iceberg

End-to-end PySpark data pipeline: Avro/Parquet → Bronze → Silver → Gold using Apache Iceberg tables on Databricks.

## Project Structure

```
databricks_medallion/
├── config/
│   └── pipeline_config.py          # Centralised config (paths, table names, options)
├── data/
│   └── raw/
│       ├── avro/                    # Sample Avro source files
│       └── parquet/                 # Sample Parquet source files
├── notebooks/
│   ├── 00_setup.py                  # Cluster/catalog setup & sample data generation
│   ├── 01_ingest_bronze.py          # Ingestion → Bronze (Auto Loader / batch)
│   ├── 02_bronze_to_silver.py       # Bronze → Silver transformation
│   ├── 03_silver_to_gold.py         # Silver → Gold aggregation
│   └── 04_validate_pipeline.py      # End-to-end quality checks
├── src/
│   ├── ingestion/
│   │   └── reader.py                # Avro & Parquet readers (stream + batch)
│   ├── bronze/
│   │   └── bronze_writer.py         # Write raw data to Bronze Iceberg table
│   ├── silver/
│   │   └── silver_transformer.py    # Cleanse, dedup, upsert to Silver Iceberg
│   ├── gold/
│   │   └── gold_aggregator.py       # Aggregate & write to Gold Iceberg table
│   └── utils/
│       ├── iceberg_utils.py         # Iceberg DDL helpers, time-travel, maintenance
│       ├── spark_session.py         # SparkSession factory
│       └── logger.py                # Structured logging
└── tests/
    ├── test_bronze.py
    ├── test_silver.py
    └── test_gold.py
```

## Architecture

| Layer  | Iceberg Table                        | Description                                      |
|--------|--------------------------------------|--------------------------------------------------|
| Bronze | `spark_catalog.bronze.<entity>`      | Raw ingestion, append-only, audit columns        |
| Silver | `spark_catalog.silver.<entity>`      | Cleansed, deduped, MERGE INTO upsert             |
| Gold   | `spark_catalog.gold.<entity>_agg`    | Consumption-ready aggregates & KPI tables        |

## Key Technologies

- **Apache Spark / PySpark** — transformations at every layer
- **Apache Iceberg** — ACID-compliant open table format with schema evolution & time travel
- **Databricks Auto Loader** — incremental ingestion with schema inference
- **Delta / Iceberg MERGE INTO** — efficient upserts at Silver & Gold

## Quickstart

1. Run `notebooks/00_setup.py` on your Databricks cluster to create catalogs and sample data.
2. Run notebooks 01 → 04 in order, or wire them into a Databricks Workflow.
3. Query Gold tables from your BI tool or SQL warehouse.
