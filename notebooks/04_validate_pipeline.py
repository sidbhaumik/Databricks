# Databricks notebook source
# notebooks/04_validate_pipeline.py
"""
End-to-end data quality validation notebook.

Run after notebooks 01–03 to verify:
  - Row counts propagate correctly across layers
  - No business key nulls in Silver/Gold
  - DQ pass rates are above threshold
  - Delta Lake metadata is healthy
"""

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<your-repo>/databricks_medallion")

from datetime import date, timedelta
from src.utils.spark_session import get_spark
from config.pipeline_config import *

spark = get_spark()

dbutils.widgets.text("report_date", str(date.today() - timedelta(days=1)))
report_date = dbutils.widgets.get("report_date")

failures = []

# ──────────────────────────────────────────────────────────────────────────────
# Helper
# ──────────────────────────────────────────────────────────────────────────────

def check(name: str, condition: bool, detail: str = ""):
    status = "PASS ✓" if condition else "FAIL ✗"
    print(f"  [{status}] {name} {detail}")
    if not condition:
        failures.append(name)

# ──────────────────────────────────────────────────────────────────────────────
# 1. Bronze layer
# ──────────────────────────────────────────────────────────────────────────────

print(f"\n{'─'*60}")
print(f"BRONZE LAYER — {report_date}")
print(f"{'─'*60}")

bronze_orders_count = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {ORDERS_BRONZE_TABLE}
    WHERE ingestion_date = '{report_date}'
""").first().n

bronze_customers_count = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {CUSTOMERS_BRONZE_TABLE}
    WHERE ingestion_date = '{report_date}'
""").first().n

check("Bronze orders loaded",    bronze_orders_count > 0,    f"({bronze_orders_count:,} rows)")
check("Bronze customers loaded", bronze_customers_count > 0, f"({bronze_customers_count:,} rows)")

# Null check on critical Bronze columns
bronze_null_rate = spark.sql(f"""
    SELECT AVG(CASE WHEN order_id IS NULL THEN 1.0 ELSE 0.0 END) AS null_rate
    FROM {ORDERS_BRONZE_TABLE}
    WHERE ingestion_date = '{report_date}'
""").first().null_rate or 0.0

check("Bronze order_id null rate within tolerance",
      (bronze_null_rate * 100) <= NULL_TOLERANCE_PCT,
      f"({bronze_null_rate*100:.2f}% nulls, threshold={NULL_TOLERANCE_PCT}%)")

# ──────────────────────────────────────────────────────────────────────────────
# 2. Silver layer
# ──────────────────────────────────────────────────────────────────────────────

print(f"\n{'─'*60}")
print(f"SILVER LAYER — {report_date}")
print(f"{'─'*60}")

silver_orders_count = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {ORDERS_SILVER_TABLE}
    WHERE order_date = '{report_date}'
""").first().n

check("Silver orders populated", silver_orders_count > 0, f"({silver_orders_count:,} rows)")

# Silver should never have null order_id (dropped in transform)
silver_null_keys = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {ORDERS_SILVER_TABLE}
    WHERE order_date = '{report_date}' AND order_id IS NULL
""").first().n

check("Silver has no null order_ids", silver_null_keys == 0, f"({silver_null_keys} nulls)")

# DQ pass rate
dq_pass_rate = spark.sql(f"""
    SELECT AVG(CAST(dq_passed AS DOUBLE)) AS rate FROM {ORDERS_SILVER_TABLE}
    WHERE order_date = '{report_date}'
""").first().rate or 0.0

check("Silver DQ pass rate >= 90%", dq_pass_rate >= 0.90,
      f"({dq_pass_rate*100:.1f}%)")

# Duplicate check
silver_dupes = spark.sql(f"""
    SELECT COUNT(*) - COUNT(DISTINCT order_id) AS dupes
    FROM {ORDERS_SILVER_TABLE}
    WHERE order_date = '{report_date}'
""").first().dupes

dupe_pct = (silver_dupes / silver_orders_count * 100) if silver_orders_count > 0 else 0
check("Silver duplicate rate within tolerance",
      dupe_pct <= DUPE_TOLERANCE_PCT,
      f"({silver_dupes} dupes, {dupe_pct:.2f}%)")

# Customers Silver
silver_customers_count = spark.sql(f"SELECT COUNT(*) AS n FROM {CUSTOMERS_SILVER_TABLE}").first().n
check("Silver customers populated", silver_customers_count > 0, f"({silver_customers_count:,} rows)")

# ──────────────────────────────────────────────────────────────────────────────
# 3. Gold layer
# ──────────────────────────────────────────────────────────────────────────────

print(f"\n{'─'*60}")
print(f"GOLD LAYER — {report_date}")
print(f"{'─'*60}")

gold_count = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {ORDERS_GOLD_TABLE}
    WHERE report_date = '{report_date}'
""").first().n

check("Gold daily agg populated", gold_count > 0, f"({gold_count:,} rows)")

# Revenue sanity: Gold revenue should not exceed Silver revenue
silver_rev = spark.sql(f"""
    SELECT COALESCE(SUM(total_amount), 0) AS rev FROM {ORDERS_SILVER_TABLE}
    WHERE order_date = '{report_date}' AND order_status = 'DELIVERED'
""").first().rev or 0.0

gold_rev = spark.sql(f"""
    SELECT COALESCE(SUM(total_revenue), 0) AS rev FROM {ORDERS_GOLD_TABLE}
    WHERE report_date = '{report_date}'
""").first().rev or 0.0

# Allow 0.01% tolerance for decimal rounding
rev_diff_pct = abs(float(gold_rev) - float(silver_rev)) / max(float(silver_rev), 1) * 100
check("Gold revenue matches Silver (within 0.01%)", rev_diff_pct <= 0.01,
      f"(silver={silver_rev:.2f}, gold={gold_rev:.2f}, diff={rev_diff_pct:.4f}%)")

# LTV
ltv_count = spark.sql(f"SELECT COUNT(*) AS n FROM {GOLD_DB}.customer_ltv").first().n
check("Customer LTV table populated", ltv_count > 0, f"({ltv_count:,} rows)")

# Product perf
product_count = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {GOLD_DB}.product_daily_perf
    WHERE report_date = '{report_date}'
""").first().n
check("Product daily perf populated", product_count > 0, f"({product_count:,} rows)")

# ──────────────────────────────────────────────────────────────────────────────
# 4. Delta Lake health
# ──────────────────────────────────────────────────────────────────────────────

print(f"\n{'─'*60}")
print("DELTA TABLE HEALTH")
print(f"{'─'*60}")

for tbl in [ORDERS_BRONZE_TABLE, ORDERS_SILVER_TABLE, ORDERS_GOLD_TABLE]:
    snapshot_count = spark.sql(f"DESCRIBE HISTORY {tbl}").count()
    check(f"History versions exist: {tbl.split('.')[-1]}", snapshot_count > 0,
          f"({snapshot_count} versions)")

# ──────────────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────────────

print(f"\n{'═'*60}")
if failures:
    print(f"VALIDATION FAILED — {len(failures)} check(s) failed:")
    for f in failures:
        print(f"  ✗ {f}")
    dbutils.notebook.exit("FAILED")
else:
    print(f"ALL CHECKS PASSED ✓  ({report_date})")
    dbutils.notebook.exit("SUCCESS")
