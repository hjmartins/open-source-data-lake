"""
gold_layer.py — Silver → Gold Aggregation
──────────────────────────────────────────
Reads from Silver, produces business-ready aggregates:
  - Daily revenue by product/region
  - Customer lifetime value (CLV)
  - Return rate analysis
  - Rolling 7-day metrics
"""

import sys
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

SILVER_BUCKET   = "de-project-dev-silver"
GOLD_BUCKET     = "de-project-dev-gold"
LOCALSTACK_ENDPOINT = "http://localstack:4566"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("GoldLayerAggregation")
        .config("spark.hadoop.fs.s3a.endpoint", LOCALSTACK_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def read_silver(spark: SparkSession) -> DataFrame:
    """Read all Silver data (leverages partition pruning)."""
    return (
        spark.read
        .parquet(f"s3a://{SILVER_BUCKET}/sales/")
        .filter(F.col("is_valid_status") & F.col("is_valid_region"))
    )


# ─── Aggregation 1: Daily revenue summary ────────────────────────────────────

def daily_revenue_summary(df: DataFrame) -> DataFrame:
    """Revenue breakdown by date × product × region."""
    return (
        df
        .filter(F.col("status") == "completed")
        .groupBy("order_date", "product", "region")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_revenue").alias("total_revenue"),
            F.avg("unit_price").alias("avg_unit_price"),
            F.sum("quantity").alias("total_units_sold"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("revenue_per_order",
            F.round(F.col("total_revenue") / F.col("order_count"), 2)
        )
        .withColumn("processed_at", F.current_timestamp())
    )


# ─── Aggregation 2: Customer Lifetime Value ───────────────────────────────────

def customer_lifetime_value(df: DataFrame) -> DataFrame:
    """CLV metrics per customer."""
    return (
        df
        .filter(F.col("status") == "completed")
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_revenue").alias("lifetime_revenue"),
            F.avg("total_revenue").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("product").alias("unique_products_bought"),
        )
        # CLV tier segmentation
        .withColumn("clv_tier",
            F.when(F.col("lifetime_revenue") >= 10_000, "platinum")
            .when(F.col("lifetime_revenue") >= 5_000,  "gold")
            .when(F.col("lifetime_revenue") >= 1_000,  "silver")
            .otherwise("bronze")
        )
        .withColumn("processed_at", F.current_timestamp())
    )


# ─── Aggregation 3: Return rate analysis ─────────────────────────────────────

def return_rate_analysis(df: DataFrame) -> DataFrame:
    """Return rate per product with risk flag."""
    total = df.groupBy("product").agg(
        F.count("order_id").alias("total_orders")
    )
    returns = (
        df
        .filter(F.col("status") == "returned")
        .groupBy("product")
        .agg(F.count("order_id").alias("returned_orders"))
    )

    return (
        total.join(returns, on="product", how="left")
        .fillna(0, subset=["returned_orders"])
        .withColumn("return_rate_pct",
            F.round(F.col("returned_orders") / F.col("total_orders") * 100, 2)
        )
        .withColumn("high_return_risk",
            F.col("return_rate_pct") > 15.0
        )
        .withColumn("processed_at", F.current_timestamp())
    )


# ─── Aggregation 4: Rolling 7-day revenue ────────────────────────────────────

def rolling_7day_revenue(df: DataFrame) -> DataFrame:
    """Rolling window revenue metrics (requires date ordering)."""
    daily = (
        df
        .filter(F.col("status") == "completed")
        .groupBy("order_date", "region")
        .agg(F.sum("total_revenue").alias("daily_revenue"))
    )

    window_7d = (
        Window
        .partitionBy("region")
        .orderBy(F.col("order_date").cast("long"))
        .rangeBetween(-6 * 86400, 0)  # 7 days in seconds
    )

    return (
        daily
        .withColumn("revenue_7d_rolling", F.sum("daily_revenue").over(window_7d))
        .withColumn("revenue_7d_avg",     F.avg("daily_revenue").over(window_7d))
        .withColumn("processed_at", F.current_timestamp())
    )


# ─── Write Gold ───────────────────────────────────────────────────────────────

def write_gold(df: DataFrame, table_name: str) -> None:
    """Write Gold table as Parquet."""
    path = f"s3a://{GOLD_BUCKET}/{table_name}/"
    df.write.mode("overwrite").parquet(path)
    print(f"Written Gold table: {table_name} ({df.count()} rows)")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df_silver = read_silver(spark)

    # Build and write all Gold tables
    write_gold(daily_revenue_summary(df_silver),   "daily_revenue_summary")
    write_gold(customer_lifetime_value(df_silver), "customer_lifetime_value")
    write_gold(return_rate_analysis(df_silver),    "return_rate_analysis")
    write_gold(rolling_7day_revenue(df_silver),    "rolling_7day_revenue")

    print("Gold layer aggregation complete.")
    spark.stop()


if __name__ == "__main__":
    main()
