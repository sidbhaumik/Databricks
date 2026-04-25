# Databricks notebook source
# notebooks/00_setup.py
"""
Run this notebook once to:
  1. Create catalog databases (bronze / silver / gold)
  2. Generate sample Avro and Parquet source files on DBFS
  3. Verify Delta Lake is correctly configured
"""

# COMMAND ----------
# MAGIC %pip install faker  # only needed for sample data generation

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<your-repo>/databricks_medallion")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
from datetime import date, timedelta

from src.utils.spark_session import get_spark
from src.bronze.bronze_writer import create_bronze_tables
from src.silver.silver_transformer import create_silver_tables
from src.gold.gold_aggregator import create_gold_tables

spark = get_spark()

# COMMAND ----------
# DBTITLE 1,Create databases

catalog = "spark_catalog"
create_bronze_tables(spark, catalog)
create_silver_tables(spark, catalog)
create_gold_tables(spark, catalog)

print("✓ All databases and tables created")

# COMMAND ----------
# DBTITLE 1,Generate sample customer data → Parquet

from faker import Faker
fake = Faker()

CUSTOMER_COUNT = 500
ORDERS_PER_DAY = 2000
BASE_DATE = date.today() - timedelta(days=7)

customers = [
    {
        "customer_id": f"CUST-{i:05d}",
        "first_name":  fake.first_name(),
        "last_name":   fake.last_name(),
        "email":       fake.email(),
        "country":     fake.country(),
        "signup_date": (BASE_DATE - timedelta(days=random.randint(0, 365))).isoformat(),
    }
    for i in range(CUSTOMER_COUNT)
]

customers_df = spark.createDataFrame(customers)
output_path = "dbfs:/FileStore/datalake/raw/parquet/customers/"
customers_df.write.mode("overwrite").parquet(output_path)
print(f"✓ {CUSTOMER_COUNT} customers written to {output_path}")

# COMMAND ----------
# DBTITLE 1,Generate sample orders data → Avro

customer_ids = [f"CUST-{i:05d}" for i in range(CUSTOMER_COUNT)]
statuses     = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
products     = [f"PROD-{i:04d}" for i in range(100)]

from pyspark.sql.avro.functions import to_avro   # noqa — available on Databricks

orders = []
for day_offset in range(7):   # 7 days of history
    order_date = BASE_DATE + timedelta(days=day_offset)
    for _ in range(ORDERS_PER_DAY):
        orders.append({
            "order_id":    f"ORD-{day_offset:02d}-{random.randint(100000, 999999)}",
            "customer_id": random.choice(customer_ids),
            "product_id":  random.choice(products),
            "quantity":    random.randint(1, 20),
            "unit_price":  round(random.uniform(5.0, 500.0), 2),
            "order_status": random.choice(statuses),
            "order_ts":    f"{order_date.isoformat()} {random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
        })

orders_df = spark.createDataFrame(orders)
avro_path = "dbfs:/FileStore/datalake/raw/avro/orders/"
(
    orders_df.write.format("avro")
    .mode("overwrite")
    .save(avro_path)
)
print(f"✓ {len(orders)} orders written to {avro_path}")

# COMMAND ----------
# DBTITLE 1,Verify Delta Lake

spark.sql("SHOW DATABASES IN spark_catalog").show()
spark.sql("SHOW TABLES IN spark_catalog.bronze").show()
spark.sql("SHOW TABLES IN spark_catalog.silver").show()
spark.sql("SHOW TABLES IN spark_catalog.gold").show()

# COMMAND ----------
# DBTITLE 1,Verify Delta Lake table properties

spark.sql("DESCRIBE EXTENDED spark_catalog.bronze.orders_raw").show(50, truncate=False)
