"""
bronze_layer.py — Raw Ingestion → Bronze
─────────────────────────────────────────
First stage of the Medallion Architecture.
Reads raw CSV from the landing zone, applies minimal validation
(no business logic), and writes to Bronze as-is with audit metadata.

Responsibilities:
  - Schema detection & corrupt record isolation
  - Duplicate order_id detection (flag only, don't drop)
  - Partition by year/month/day
  - Write audit log with ingestion stats

Run:
  spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 bronze_layer.py 2024-01-15
"""

import logging
import sys
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)

log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

BRONZE_BUCKET       = "de-project-dev-bronze"
LOCALSTACK_ENDPOINT = "http://localstack:4566"

# Loose schema — Bronze accepts everything, bad rows go to _corrupt_record
RAW_SCHEMA = StructType([
    StructField("order_id",          StringType(), nullable=True),
    StructField("customer_id",       StringType(), nullable=True),
    StructField("product",           StringType(), nullable=True),
    StructField("quantity",          StringType(), nullable=True),  # kept as string
    StructField("unit_price",        StringType(), nullable=True),  # kept as string
    StructField("region",            StringType(), nullable=True),
    StructField("status",            StringType(), nullable=True),
    StructField("order_date",        StringType(), nullable=True),
    StructField("_corrupt_record",   StringType(), nullable=True),
])


# ─── Spark Session ────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BronzeLayerIngestion")
        .config("spark.hadoop.fs.s3a.endpoint", LOCALSTACK_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ─── Read raw landing files ───────────────────────────────────────────────────

def read_raw_csv(spark: SparkSession, execution_date: str) -> DataFrame:
    """Read raw CSV from landing zone. PERMISSIVE mode keeps corrupt rows."""
    year, month, day = execution_date.split("-")
    path = f"s3a://{BRONZE_BUCKET}/sales/year={year}/month={month}/day={day}/*.csv"

    log.info("Reading raw CSV from: %s", path)

    return (
        spark.read
        .schema(RAW_SCHEMA)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("multiLine", "false")
        .csv(path)
    )


# ─── Bronze validation (flag only, never drop) ───────────────────────────────

def add_bronze_metadata(df: DataFrame, execution_date: str) -> DataFrame:
    """
    Bronze layer principle: preserve everything, just annotate.
    No data is dropped here — bad rows are flagged for Silver to handle.
    """
    return (
        df
        # Ingestion audit columns
        .withColumn("_ingested_at",       F.current_timestamp())
        .withColumn("_execution_date",    F.lit(execution_date))
        .withColumn("_source",            F.lit("s3_landing_zone"))
        .withColumn("_pipeline_stage",    F.lit("bronze"))

        # Flag corrupt records (parsing failures)
        .withColumn("_is_corrupt",
            F.col("_corrupt_record").isNotNull()
        )

        # Flag null critical fields (don't drop, just mark)
        .withColumn("_has_null_key",
            F.col("order_id").isNull() | F.col("customer_id").isNull()
        )

        # Flag duplicate order_ids within this batch
        .withColumn("_is_duplicate",
            F.count("order_id").over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window.partitionBy("order_id")
            ) > 1
        )

        # Partition helpers
        .withColumn("year",  F.lit(execution_date[:4]))
        .withColumn("month", F.lit(execution_date[5:7]))
        .withColumn("day",   F.lit(execution_date[8:10]))
    )


# ─── Write Bronze (partitioned Parquet) ──────────────────────────────────────

def write_bronze_parquet(df: DataFrame) -> int:
    """
    Overwrite the day partition in Bronze.
    Bronze retains ALL rows — corrupt, duplicate, null.
    Silver will decide what to keep.
    """
    output_path = f"s3a://{BRONZE_BUCKET}/sales_parquet/"

    (
        df
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(output_path)
    )

    count = df.count()
    log.info("Wrote %d rows to Bronze Parquet", count)
    return count


# ─── Audit log ────────────────────────────────────────────────────────────────

def write_audit_log(spark: SparkSession, df: DataFrame, execution_date: str) -> None:
    """Write ingestion stats to audit table for lineage tracking."""
    total      = df.count()
    corrupt    = df.filter(F.col("_is_corrupt")).count()
    null_key   = df.filter(F.col("_has_null_key")).count()
    duplicates = df.filter(F.col("_is_duplicate")).count()

    audit = {
        "execution_date":    execution_date,
        "ingested_at":       datetime.utcnow().isoformat() + "Z",
        "pipeline_stage":    "bronze",
        "total_rows":        total,
        "corrupt_rows":      corrupt,
        "null_key_rows":     null_key,
        "duplicate_rows":    duplicates,
        "clean_rows":        total - corrupt - null_key,
        "corrupt_pct":       round(corrupt / total * 100, 2) if total > 0 else 0.0,
    }

    log.info("Audit: %s", audit)

    audit_df = spark.createDataFrame([audit])
    (
        audit_df
        .write
        .mode("append")
        .json(
            f"s3a://{BRONZE_BUCKET}/audit/date={execution_date}/"
        )
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    execution_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    log.info("Starting Bronze ingestion for: %s", execution_date)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df_raw    = read_raw_csv(spark, execution_date)
        df_bronze = add_bronze_metadata(df_raw, execution_date)

        write_bronze_parquet(df_bronze)
        write_audit_log(spark, df_bronze, execution_date)

        log.info("Bronze layer complete for %s", execution_date)
        return 0

    except Exception as e:
        log.exception("Bronze ingestion FAILED: %s", e)
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())