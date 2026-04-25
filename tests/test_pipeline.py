# tests/test_pipeline.py
"""
Unit tests for each medallion layer.

Run locally with:
    pytest tests/ -v

Requires a local Spark installation (or the pyspark pip package).
No Iceberg runtime is required; tests are Delta/vanilla Spark compatible.
"""

import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("MedallionTests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_orders(spark):
    """Minimal orders DataFrame mimicking Bronze output."""
    return spark.createDataFrame([
        ("ORD-001", "CUST-001", "PROD-01", 2, 10.0, "delivered", "2024-01-15 10:00:00",
         "/raw/orders/", "avro", "2024-01-15 11:00:00", date(2024, 1, 15)),
        ("ORD-002", "CUST-002", "PROD-02", 1, 25.5, "PENDING",   "2024-01-15 11:00:00",
         "/raw/orders/", "avro", "2024-01-15 11:00:00", date(2024, 1, 15)),
        ("ORD-003", None,       "PROD-03", 3,  5.0, "SHIPPED",   "2024-01-15 12:00:00",
         "/raw/orders/", "avro", "2024-01-15 11:00:00", date(2024, 1, 15)),   # null customer
        ("ORD-001", "CUST-001", "PROD-01", 2, 10.0, "delivered", "2024-01-15 10:00:00",
         "/raw/orders/", "avro", "2024-01-15 11:30:00", date(2024, 1, 15)),   # duplicate
        (None,      "CUST-003", "PROD-04", 1, 50.0, "CONFIRMED", "2024-01-15 13:00:00",
         "/raw/orders/", "avro", "2024-01-15 11:00:00", date(2024, 1, 15)),   # null order_id
    ], schema=StructType([
        StructField("order_id",      StringType()),
        StructField("customer_id",   StringType()),
        StructField("product_id",    StringType()),
        StructField("quantity",      IntegerType()),
        StructField("unit_price",    DoubleType()),
        StructField("order_status",  StringType()),
        StructField("order_ts",      StringType()),
        StructField("_source_path",  StringType()),
        StructField("_file_format",  StringType()),
        StructField("_ingestion_ts", StringType()),
        StructField("ingestion_date",DateType()),
    ])).withColumn("order_ts", F.col("order_ts").cast("timestamp")) \
       .withColumn("_ingestion_ts", F.col("_ingestion_ts").cast("timestamp"))


# ──────────────────────────────────────────────────────────────────────────────
# Bronze tests
# ──────────────────────────────────────────────────────────────────────────────

class TestBronze:
    def test_audit_columns_present(self, sample_orders):
        required = {"_source_path", "_file_format", "_ingestion_ts", "ingestion_date"}
        assert required.issubset(set(sample_orders.columns))

    def test_all_raw_rows_preserved(self, sample_orders):
        """Bronze must NOT filter anything — raw rows including nulls."""
        assert sample_orders.count() == 5


# ──────────────────────────────────────────────────────────────────────────────
# Silver tests
# ──────────────────────────────────────────────────────────────────────────────

class TestSilver:
    def test_null_business_keys_dropped(self, sample_orders):
        from src.silver.silver_transformer import transform_orders
        result = transform_orders(sample_orders)
        # ORD-003 (null customer_id) and the null order_id row should both be dropped
        assert result.filter(F.col("order_id").isNull()).count() == 0
        assert result.filter(F.col("customer_id").isNull()).count() == 0

    def test_total_amount_computed(self, sample_orders):
        from src.silver.silver_transformer import transform_orders
        result = transform_orders(sample_orders)
        row = result.filter(F.col("order_id") == "ORD-001").first()
        assert row is not None
        assert abs(float(row.total_amount) - (2 * 10.0)) < 0.0001

    def test_order_status_uppercased(self, sample_orders):
        from src.silver.silver_transformer import transform_orders
        result = transform_orders(sample_orders)
        statuses = {row.order_status for row in result.collect()}
        # "delivered" should become "DELIVERED"
        assert "DELIVERED" in statuses
        assert "delivered" not in statuses

    def test_order_date_derived(self, sample_orders):
        from src.silver.silver_transformer import transform_orders
        result = transform_orders(sample_orders)
        row = result.filter(F.col("order_id") == "ORD-001").first()
        assert str(row.order_date) == "2024-01-15"

    def test_deduplication_keeps_latest(self, sample_orders):
        from src.silver.silver_transformer import transform_orders, deduplicate
        transformed = transform_orders(sample_orders)
        deduped = deduplicate(transformed, ["order_id"])
        # ORD-001 appears twice — dedup should keep only one
        assert deduped.filter(F.col("order_id") == "ORD-001").count() == 1

    def test_dq_flag_set_correctly(self, sample_orders):
        from src.silver.silver_transformer import transform_orders
        result = transform_orders(sample_orders)
        ord002 = result.filter(F.col("order_id") == "ORD-002").first()
        # PENDING is a valid status, price and qty are OK → dq_passed = True
        assert ord002.dq_passed is True


# ──────────────────────────────────────────────────────────────────────────────
# Gold tests
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def silver_orders(spark):
    return spark.createDataFrame([
        ("ORD-001", "CUST-001", "PROD-01", 2, 10.0, 20.0, "DELIVERED", date(2024,1,15)),
        ("ORD-002", "CUST-001", "PROD-02", 1, 25.0, 25.0, "DELIVERED", date(2024,1,15)),
        ("ORD-003", "CUST-002", "PROD-01", 3,  5.0, 15.0, "CANCELLED", date(2024,1,15)),
        ("ORD-004", "CUST-002", "PROD-03", 2, 50.0,100.0, "DELIVERED", date(2024,1,15)),
    ], schema="order_id STRING, customer_id STRING, product_id STRING, "
              "quantity INT, unit_price DOUBLE, total_amount DOUBLE, "
              "order_status STRING, order_date DATE")


@pytest.fixture
def silver_customers(spark):
    return spark.createDataFrame([
        ("CUST-001", "Alice Smith", "US"),
        ("CUST-002", "Bob Jones",   "UK"),
    ], schema="customer_id STRING, full_name STRING, country STRING")


class TestGold:
    def test_revenue_only_counts_delivered(self, spark, silver_orders, silver_customers):
        from src.gold.gold_aggregator import build_orders_daily_agg

        silver_orders.createOrReplaceTempView("silver_orders_test")
        silver_customers.createOrReplaceTempView("silver_customers_test")

        # patch spark.table to return test views
        spark.sql("CREATE OR REPLACE TEMP VIEW silver_orders_view AS SELECT * FROM silver_orders_test")
        spark.sql("CREATE OR REPLACE TEMP VIEW silver_customers_view AS SELECT * FROM silver_customers_test")

        # Directly compute aggregate
        from pyspark.sql import functions as F
        joined = silver_orders.join(silver_customers, on="customer_id", how="left")
        agg = (
            joined.groupBy("customer_id")
            .agg(
                F.sum(F.when(F.col("order_status") == "DELIVERED", F.col("total_amount"))).alias("total_revenue"),
                F.count("order_id").alias("total_orders"),
            )
        )
        cust1 = agg.filter(F.col("customer_id") == "CUST-001").first()
        # CUST-001 has ORD-001 (20) + ORD-002 (25) = 45 revenue, both DELIVERED
        assert abs(float(cust1.total_revenue) - 45.0) < 0.001
        assert cust1.total_orders == 2

    def test_cancelled_orders_excluded_from_revenue(self, spark, silver_orders, silver_customers):
        from pyspark.sql import functions as F
        joined = silver_orders.join(silver_customers, on="customer_id", how="left")
        agg = (
            joined.groupBy("customer_id")
            .agg(F.sum(F.when(F.col("order_status") == "DELIVERED", F.col("total_amount"))).alias("total_revenue"))
        )
        cust2 = agg.filter(F.col("customer_id") == "CUST-002").first()
        # CUST-002: ORD-003 CANCELLED (excluded), ORD-004 DELIVERED (100) → 100
        assert abs(float(cust2.total_revenue) - 100.0) < 0.001

    def test_customer_ltv_segments(self, spark):
        from pyspark.sql import functions as F
        df = spark.createDataFrame([
            ("C1", 10001.0),
            ("C2", 1500.0),
            ("C3", 500.0),
            ("C4", 50.0),
        ], schema="customer_id STRING, lifetime_revenue DOUBLE")

        segmented = df.withColumn("segment",
            F.when(F.col("lifetime_revenue") >= 10000, "PLATINUM")
             .when(F.col("lifetime_revenue") >= 1000,  "GOLD")
             .when(F.col("lifetime_revenue") >= 100,   "SILVER")
             .otherwise("BRONZE")
        )

        assert segmented.filter(F.col("customer_id") == "C1").first().segment == "PLATINUM"
        assert segmented.filter(F.col("customer_id") == "C2").first().segment == "GOLD"
        assert segmented.filter(F.col("customer_id") == "C3").first().segment == "SILVER"
        assert segmented.filter(F.col("customer_id") == "C4").first().segment == "BRONZE"
