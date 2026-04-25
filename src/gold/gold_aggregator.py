# src/gold/gold_aggregator.py
"""
Gold layer aggregator.

Silver → Gold = join, aggregate, and produce consumption-ready tables.

Design decisions
────────────────
- Gold tables are OVERWRITE per report_date partition — they are derived and
  fully reproducible from Silver, so idempotent overwrite is safe and simpler
  than MERGE INTO.
- ZORDER is applied on high-cardinality filter columns to speed up BI queries.
- All monetary values are DECIMAL(18,4) — never DOUBLE — to avoid float drift.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Optional
from src.utils.logger import PipelineLogger

log = PipelineLogger("gold_aggregator")


# ──────────────────────────────────────────────────────────────────────────────
# DDL — Create Gold tables
# ──────────────────────────────────────────────────────────────────────────────

def create_gold_tables(spark: SparkSession, catalog: str = "spark_catalog") -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")

    # ── Daily orders aggregate (one row per day per customer) ──────────
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.gold.orders_daily_agg (
            report_date         DATE          NOT NULL COMMENT 'Partition column',
            customer_id         STRING        NOT NULL,
            full_name           STRING,
            country             STRING,
            total_orders        LONG          COMMENT 'Count of orders placed',
            completed_orders    LONG,
            cancelled_orders    LONG,
            total_revenue       DECIMAL(18,4) COMMENT 'Sum of total_amount for completed orders',
            avg_order_value     DECIMAL(18,4),
            top_product_id      STRING        COMMENT 'Most ordered product on this day',
            dq_pass_rate        DOUBLE        COMMENT '% of orders passing DQ checks'
        )
        USING delta
        PARTITIONED BY (report_date)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)

    # ── Customer lifetime value (rolling, one row per customer) ────────
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.gold.customer_ltv (
            customer_id         STRING        NOT NULL,
            full_name           STRING,
            country             STRING,
            email_domain        STRING,
            first_order_date    DATE,
            last_order_date     DATE,
            lifetime_orders     LONG,
            lifetime_revenue    DECIMAL(18,4),
            avg_order_value     DECIMAL(18,4),
            customer_segment    STRING        COMMENT 'BRONZE / SILVER / GOLD / PLATINUM',
            last_updated_ts     TIMESTAMP
        )
        USING delta
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)

    # ── Product performance (daily, one row per product per day) ───────
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.gold.product_daily_perf (
            report_date         DATE          NOT NULL,
            product_id          STRING        NOT NULL,
            units_sold          LONG,
            gross_revenue       DECIMAL(18,4),
            order_count         LONG,
            avg_unit_price      DECIMAL(18,4),
            revenue_rank        INT           COMMENT 'Rank by revenue within the day'
        )
        USING delta
        PARTITIONED BY (report_date)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)
    log.info("Gold tables ready", catalog=catalog)


# ──────────────────────────────────────────────────────────────────────────────
# Aggregation: Daily orders per customer
# ──────────────────────────────────────────────────────────────────────────────

def build_orders_daily_agg(
    spark: SparkSession,
    orders_silver_table: str,
    customers_silver_table: str,
    report_date: str,
) -> DataFrame:
    """
    Build the daily order aggregate joined with customer dimension.

    Includes
    --------
    - Order counts by status
    - Revenue KPIs (only for DELIVERED orders)
    - DQ pass rate
    - Top product by order count (window rank)
    """
    orders = (
        spark.table(orders_silver_table)
        .filter(F.col("order_date") == report_date)
    )
    customers = spark.table(customers_silver_table)

    # ── Customer join ─────────────────────────────────────────────────
    joined = orders.join(
        customers.select("customer_id", "full_name", "country"),
        on="customer_id",
        how="left",
    )

    # ── Top product per customer (window rank) ─────────────────────────
    product_window = Window.partitionBy("customer_id").orderBy(
        F.count("product_id").desc()
    )
    product_counts = (
        orders.groupBy("customer_id", "product_id")
              .agg(F.count("*").alias("cnt"))
    )
    top_product = (
        product_counts
        .withColumn("rnk", F.row_number().over(
            Window.partitionBy("customer_id").orderBy(F.col("cnt").desc())
        ))
        .filter(F.col("rnk") == 1)
        .select("customer_id", F.col("product_id").alias("top_product_id"))
    )

    # ── Main aggregation ───────────────────────────────────────────────
    agg_df = (
        joined
        .groupBy("customer_id", "full_name", "country")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.count(
                F.when(F.col("order_status") == "DELIVERED", 1)
            ).alias("completed_orders"),
            F.count(
                F.when(F.col("order_status") == "CANCELLED", 1)
            ).alias("cancelled_orders"),
            F.sum(
                F.when(F.col("order_status") == "DELIVERED", F.col("total_amount"))
            ).alias("total_revenue"),
            F.avg(
                F.when(F.col("order_status") == "DELIVERED", F.col("total_amount"))
            ).alias("avg_order_value"),
            F.avg(F.col("dq_passed").cast("double")).alias("dq_pass_rate"),
        )
        .join(top_product, on="customer_id", how="left")
        .withColumn("report_date", F.lit(report_date).cast("date"))
        .withColumn("total_revenue",
            F.col("total_revenue").cast("decimal(18,4)")
        )
        .withColumn("avg_order_value",
            F.round(F.col("avg_order_value"), 4).cast("decimal(18,4)")
        )
    )
    return agg_df


# ──────────────────────────────────────────────────────────────────────────────
# Aggregation: Customer lifetime value
# ──────────────────────────────────────────────────────────────────────────────

def build_customer_ltv(
    spark: SparkSession,
    orders_silver_table: str,
    customers_silver_table: str,
) -> DataFrame:
    """
    Compute rolling customer lifetime value across all history.

    Segments
    ────────
    PLATINUM : revenue >= 10,000
    GOLD     : revenue >= 1,000
    SILVER   : revenue >= 100
    BRONZE   : everything else
    """
    customers = spark.table(customers_silver_table)
    orders    = spark.table(orders_silver_table).filter(
        F.col("order_status") == "DELIVERED"
    )

    ltv_df = (
        orders
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("lifetime_orders"),
            F.sum("total_amount").cast("decimal(18,4)").alias("lifetime_revenue"),
            F.avg("total_amount").cast("decimal(18,4)").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        .join(
            customers.select("customer_id", "full_name", "country", "email_domain"),
            on="customer_id",
            how="left",
        )
        .withColumn("customer_segment",
            F.when(F.col("lifetime_revenue") >= 10000, "PLATINUM")
             .when(F.col("lifetime_revenue") >= 1000,  "GOLD")
             .when(F.col("lifetime_revenue") >= 100,   "SILVER")
             .otherwise("BRONZE")
        )
        .withColumn("last_updated_ts", F.current_timestamp())
    )
    return ltv_df


# ──────────────────────────────────────────────────────────────────────────────
# Aggregation: Product daily performance
# ──────────────────────────────────────────────────────────────────────────────

def build_product_daily_perf(
    spark: SparkSession,
    orders_silver_table: str,
    report_date: str,
) -> DataFrame:
    orders = (
        spark.table(orders_silver_table)
        .filter(
            (F.col("order_date") == report_date) &
            (F.col("order_status") == "DELIVERED")
        )
    )

    daily_df = (
        orders
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("total_amount").cast("decimal(18,4)").alias("gross_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("unit_price").cast("decimal(18,4)").alias("avg_unit_price"),
        )
        .withColumn("revenue_rank",
            F.rank().over(Window.orderBy(F.col("gross_revenue").desc()))
        )
        .withColumn("report_date", F.lit(report_date).cast("date"))
    )
    return daily_df


# ──────────────────────────────────────────────────────────────────────────────
# Writers
# ──────────────────────────────────────────────────────────────────────────────

def write_gold_partition(
    df: DataFrame,
    table_name: str,
    partition_col: str,
    partition_val: str,
) -> None:
    """
    Overwrite a single date partition in a Gold Delta Lake table.

    Dynamic overwrite mode replaces only the matching partition — other
    partitions are untouched. This is safe for idempotent daily runs.
    """
    count = df.count()
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .saveAsTable(table_name)
    )
    log.info("Gold partition written",
             table=table_name, partition=partition_val, rows=count)


def upsert_customer_ltv(
    spark: SparkSession,
    ltv_df: DataFrame,
    target_table: str,
) -> None:
    """MERGE INTO for the non-partitioned customer_ltv table."""
    ltv_df.createOrReplaceTempView("_gold_ltv_source")

    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING _gold_ltv_source AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    log.info("Customer LTV upserted", table=target_table)


# ──────────────────────────────────────────────────────────────────────────────
# Optimise Gold (ZORDER)
# ──────────────────────────────────────────────────────────────────────────────

def optimize_gold_table(
    spark: SparkSession,
    table_name: str,
    zorder_cols: Optional[List[str]] = None,
) -> None:
    """
    Run Delta Lake's rewrite_data_files with sort strategy for optimal read perf.
    On Databricks you can also run OPTIMIZE … ZORDER BY directly.
    """
    if zorder_cols:
        cols = ", ".join(zorder_cols)
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({cols})")
        log.info("ZORDER applied", table=table_name, cols=zorder_cols)
    else:
        spark.sql(f"OPTIMIZE {table_name}")
        log.info("Optimised", table=table_name)


# ──────────────────────────────────────────────────────────────────────────────
# Orchestration: Silver → Gold (one report date)
# ──────────────────────────────────────────────────────────────────────────────

def run_silver_to_gold(
    spark: SparkSession,
    orders_silver:   str,
    customers_silver: str,
    gold_daily_agg:  str,
    gold_ltv:        str,
    gold_product:    str,
    report_date:     str,
) -> None:
    log.info("Silver → Gold started", date=report_date)

    # 1. Daily orders aggregate
    daily_agg = build_orders_daily_agg(spark, orders_silver, customers_silver, report_date)
    write_gold_partition(daily_agg, gold_daily_agg, "report_date", report_date)

    # 2. Product daily performance
    product_perf = build_product_daily_perf(spark, orders_silver, report_date)
    write_gold_partition(product_perf, gold_product, "report_date", report_date)

    # 3. Customer LTV (full recompute + upsert)
    ltv = build_customer_ltv(spark, orders_silver, customers_silver)
    upsert_customer_ltv(spark, ltv, gold_ltv)

    # 4. Optimise the daily partition
    optimize_gold_table(spark, gold_daily_agg, zorder_cols=["customer_id"])
    optimize_gold_table(spark, gold_product,   zorder_cols=["product_id"])

    log.info("Silver → Gold complete", date=report_date)
