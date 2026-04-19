# Databricks notebook source
# notebooks/03_silver_to_gold.py
"""
Stage 3: Silver → Gold

Produces three consumption-ready tables:
  1. orders_daily_agg   — KPIs per customer per day
  2. customer_ltv       — rolling lifetime value per customer
  3. product_daily_perf — revenue/units per product per day

Also runs OPTIMIZE (ZORDER) on the written partitions.
"""

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<your-repo>/databricks_medallion")

from datetime import date, timedelta
from src.utils.spark_session import get_spark
from src.gold.gold_aggregator import run_silver_to_gold
from config.pipeline_config import *

spark = get_spark()

# COMMAND ----------

dbutils.widgets.text("report_date", str(date.today() - timedelta(days=1)))
report_date = dbutils.widgets.get("report_date")
print(f"Report date: {report_date}")

# COMMAND ----------
# DBTITLE 1,Run Silver → Gold

run_silver_to_gold(
    spark,
    orders_silver   = ORDERS_SILVER_TABLE,
    customers_silver= CUSTOMERS_SILVER_TABLE,
    gold_daily_agg  = ORDERS_GOLD_TABLE,
    gold_ltv        = f"{GOLD_DB}.customer_ltv",
    gold_product    = f"{GOLD_DB}.product_daily_perf",
    report_date     = report_date,
)

# COMMAND ----------
# DBTITLE 1,Gold validation

print("=== Daily order KPIs (top 10 customers by revenue) ===")
spark.sql(f"""
    SELECT customer_id, full_name, country,
           total_orders, completed_orders, total_revenue, dq_pass_rate
    FROM   {ORDERS_GOLD_TABLE}
    WHERE  report_date = '{report_date}'
    ORDER  BY total_revenue DESC NULLS LAST
    LIMIT  10
""").show()

print("=== Customer LTV segments ===")
spark.sql(f"""
    SELECT customer_segment, COUNT(*) AS customers,
           SUM(lifetime_revenue) AS segment_revenue,
           AVG(lifetime_orders)  AS avg_orders
    FROM   {GOLD_DB}.customer_ltv
    GROUP  BY 1
    ORDER  BY segment_revenue DESC
""").show()

print("=== Top 10 products by revenue ===")
spark.sql(f"""
    SELECT product_id, units_sold, gross_revenue, revenue_rank
    FROM   {GOLD_DB}.product_daily_perf
    WHERE  report_date = '{report_date}'
    ORDER  BY revenue_rank
    LIMIT  10
""").show()

# COMMAND ----------
# DBTITLE 1,Iceberg metadata — Gold files

spark.sql(f"SELECT * FROM {ORDERS_GOLD_TABLE}.files LIMIT 10").show(truncate=False)
