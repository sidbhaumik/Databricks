# Databricks notebook source
# notebooks/02_bronze_to_silver.py
"""
Stage 2: Bronze → Silver

Runs incremental processing for a given date window.
In production this is typically driven by a Databricks Workflow
with processing_date = yesterday ({{ds}} in Airflow / task value in Workflows).
"""

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<your-repo>/databricks_medallion")

from datetime import date, timedelta
from src.utils.spark_session import get_spark
from src.silver.silver_transformer import (
    run_orders_bronze_to_silver,
    run_customers_bronze_to_silver,
)
from config.pipeline_config import *

spark = get_spark()

# COMMAND ----------
# DBTITLE 1,Widgets

dbutils.widgets.text("processing_date", str(date.today() - timedelta(days=1)))
processing_date = dbutils.widgets.get("processing_date")
print(f"Processing date: {processing_date}")

# COMMAND ----------
# DBTITLE 1,Orders: Bronze → Silver (MERGE INTO)

run_orders_bronze_to_silver(
    spark,
    bronze_table=ORDERS_BRONZE_TABLE,
    silver_table=ORDERS_SILVER_TABLE,
    processing_date=processing_date,
    merge_keys=ORDERS_MERGE_KEYS,
)

# COMMAND ----------
# DBTITLE 1,Customers: Bronze → Silver (MERGE INTO)

run_customers_bronze_to_silver(
    spark,
    bronze_table=CUSTOMERS_BRONZE_TABLE,
    silver_table=CUSTOMERS_SILVER_TABLE,
    processing_date=processing_date,
    merge_keys=CUSTOMERS_MERGE_KEYS,
)

# COMMAND ----------
# DBTITLE 1,Silver validation

print("=== Orders Silver sample ===")
spark.sql(f"""
    SELECT order_date, order_status, COUNT(*) AS cnt,
           SUM(total_amount) AS revenue,
           AVG(dq_passed::double) AS dq_rate
    FROM   {ORDERS_SILVER_TABLE}
    WHERE  order_date = '{processing_date}'
    GROUP  BY 1, 2
    ORDER  BY 1, 2
""").show()

print("=== Customers Silver sample ===")
spark.sql(f"""
    SELECT country, COUNT(*) AS cnt
    FROM   {CUSTOMERS_SILVER_TABLE}
    GROUP  BY 1
    ORDER  BY 2 DESC
    LIMIT  10
""").show()

# COMMAND ----------
# DBTITLE 1,Iceberg time travel — compare today's Silver to snapshot before merge

snapshots = spark.sql(
    f"SELECT snapshot_id, committed_at FROM {ORDERS_SILVER_TABLE}.snapshots ORDER BY committed_at DESC"
).collect()

if len(snapshots) >= 2:
    before_snapshot = snapshots[1].snapshot_id
    print(f"Rows before today's merge (snapshot {before_snapshot}):")
    spark.read.option("snapshot-id", before_snapshot).table(ORDERS_SILVER_TABLE).count()

# COMMAND ----------
# DBTITLE 1,Schema evolution demo — add a new column to Silver orders

# Uncomment to demonstrate zero-downtime schema evolution:
# spark.sql(f"ALTER TABLE {ORDERS_SILVER_TABLE} ADD COLUMN discount_pct DECIMAL(5,2)")
# spark.sql(f"DESCRIBE {ORDERS_SILVER_TABLE}").show(30)
