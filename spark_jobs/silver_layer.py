"""
silver_layer.py — Bronze → Silver Transformation
─────────────────────────────────────────────────
Reads raw CSV from Bronze, applies:
  - Schema enforcement & type casting
  - Null handling & deduplication
  - Data quality scoring
  - Partitioned Parquet write to Silver bucket

Run:
  spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 silver_layer.py
"""

import logging
import sys
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType, TimestampType
)

log = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────

BRONZE_BUCKET = "de-project-dev-bronze"
SILVER_BUCKET = "de-project-dev-silver"
LOCALSTACK_ENDPOINT = "http://localstack:4566"

# Expected schema — enforces contract
SALES_SCHEMA = StructType([
    StructField("order_id",    StringType(),  nullable=False),
    StructField("customer_id", StringType(),  nullable=False),
    StructField("product",     StringType(),  nullable=True),
    StructField("quantity",    IntegerType(), nullable=True),
    StructField("unit_price",  DoubleType(),  nullable=True),
    StructField("region",      StringType(),  nullable=True),
    StructField("status",      StringType(),  nullable=True),
    StructField("order_date",  DateType(),    nullable=True),
])

VALID_STATUSES  = ["completed", "returned", "pending"]
VALID_REGIONS   = ["North", "South", "East", "West", "Central"]
MAX_UNIT_PRICE  = 10_000.0
MAX_QUANTITY    = 1_000


# ─── Spark Session ────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """Build Spark session configured for LocalStack S3."""
    return (
        SparkSession.builder
        .appName("SilverLayerTransformation")
        .config("spark.hadoop.fs.s3a.endpoint", LOCALSTACK_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Performance: columnar read
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Cost optimisation: adaptive query execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


# ─── Transformation functions ─────────────────────────────────────────────────

def read_bronze(spark: SparkSession, execution_date: str) -> DataFrame:
    """Read partitioned CSV from Bronze layer."""
    year, month, day = execution_date.split("-")
    path = f"s3a://{BRONZE_BUCKET}/sales/year={year}/month={month}/day={day}/*.csv"

    log.info("Reading Bronze data from: %s", path)
    return (
        spark.read
        .schema(SALES_SCHEMA)
        .option("header", "true")
        .option("mode", "PERMISSIVE")        # Don't crash on bad rows
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(path)
    )


def clean_data(df: DataFrame) -> DataFrame:
    """Apply cleaning transformations."""

    # 1. Remove completely null rows
    df = df.dropna(how="all")

    # 2. Deduplicate on order_id (keep first occurrence)
    df = df.dropDuplicates(["order_id"])

    # 3. Normalise string columns
    df = (
        df
        .withColumn("product", F.trim(F.upper(F.col("product"))))
        .withColumn("region",  F.trim(F.initcap(F.col("region"))))
        .withColumn("status",  F.trim(F.lower(F.col("status"))))
    )

    # 4. Clamp numeric outliers
    df = (
        df
        .withColumn("unit_price", F.when(
            F.col("unit_price") > MAX_UNIT_PRICE, MAX_UNIT_PRICE
        ).otherwise(F.col("unit_price")))
        .withColumn("quantity", F.when(
            F.col("quantity") > MAX_QUANTITY, MAX_QUANTITY
        ).when(F.col("quantity") < 0, 0)
        .otherwise(F.col("quantity")))
    )

    # 5. Fill nulls with sensible defaults
    df = df.fillna({
        "product":    "UNKNOWN",
        "region":     "UNKNOWN",
        "status":     "pending",
        "quantity":   0,
        "unit_price": 0.0,
    })

    return df


def enrich_data(df: DataFrame) -> DataFrame:
    """Add derived / business columns."""

    return (
        df
        # Revenue calculation
        .withColumn("total_revenue",
            F.round(F.col("quantity") * F.col("unit_price"), 2)
        )
        # Valid flags (used in Gold aggregations)
        .withColumn("is_valid_status",
            F.col("status").isin(VALID_STATUSES)
        )
        .withColumn("is_valid_region",
            F.col("region").isin(VALID_REGIONS)
        )
        # Data quality score (0–100)
        .withColumn("quality_score",
            (
                F.when(F.col("order_id").isNotNull(), 25).otherwise(0) +
                F.when(F.col("customer_id").isNotNull(), 25).otherwise(0) +
                F.when(F.col("is_valid_status"), 25).otherwise(0) +
                F.when(F.col("unit_price") > 0, 25).otherwise(0)
            ).cast(IntegerType())
        )
        # Audit columns
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("pipeline_version", F.lit("1.0.0"))
        # Partition columns
        .withColumn("year",  F.year("order_date").cast(StringType()))
        .withColumn("month", F.month("order_date").cast(StringType()))
    )


def write_silver(df: DataFrame, execution_date: str) -> int:
    """Write cleaned data to Silver as partitioned Parquet."""

    output_path = f"s3a://{SILVER_BUCKET}/sales/"

    # Only write high-quality records (score >= 75)
    df_quality = df.filter(F.col("quality_score") >= 75)
    count = df_quality.count()

    log.info("Writing %d quality records to Silver", count)

    (
        df_quality
        .write
        .mode("overwrite")
        .partitionBy("year", "month")      # Partition pruning for cost efficiency
        .parquet(output_path)
    )

    return count


def log_metrics(spark: SparkSession, df_raw: DataFrame,
                df_silver: DataFrame, execution_date: str) -> None:
    """Log transformation metrics for monitoring."""
    raw_count    = df_raw.count()
    silver_count = df_silver.count()
    drop_rate    = round((raw_count - silver_count) / raw_count * 100, 2)

    metrics = {
        "execution_date":       execution_date,
        "raw_record_count":     raw_count,
        "silver_record_count":  silver_count,
        "drop_rate_pct":        drop_rate,
        "avg_quality_score":    df_silver.agg(F.avg("quality_score")).first()[0],
        "avg_revenue":          df_silver.agg(F.avg("total_revenue")).first()[0],
    }

    log.info("Transformation metrics: %s", metrics)

    # Write metrics to S3 for Grafana
    metrics_df = spark.createDataFrame([metrics])
    (
        metrics_df
        .write
        .mode("append")
        .json(f"s3a://{SILVER_BUCKET}/metrics/pipeline/date={execution_date}/")
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    execution_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    log.info("Starting Silver transformation for: %s", execution_date)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df_raw    = read_bronze(spark, execution_date)
        df_clean  = clean_data(df_raw)
        df_silver = enrich_data(df_clean)

        log_metrics(spark, df_raw, df_silver, execution_date)
        count = write_silver(df_silver, execution_date)

        log.info("Silver transformation complete: %d records written", count)
        return 0

    except Exception as e:
        log.exception("Silver transformation FAILED: %s", e)
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
