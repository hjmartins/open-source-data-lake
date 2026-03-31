"""
bronze_layer.py — Weather RAW → Bronze
──────────────────────────────────────
Responsável por estruturar dados brutos da API Open-Meteo.

Princípios:
- NÃO remove dados
- NÃO aplica regra de negócio
- Apenas estrutura, enriquece e audita

Uso:
spark-submit bronze_layer.py 2026-03-31
"""

import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ─── Config ────────────────────────────────────────────────────────────────

BRONZE_BUCKET = "de-project-dev-bronze"
LOCALSTACK_ENDPOINT = "http://localstack:4566"


# ─── Spark Session ─────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BronzeWeatherLayer")
        .config("spark.hadoop.fs.s3a.endpoint", LOCALSTACK_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


# ─── Read RAW ──────────────────────────────────────────────────────────────

def read_raw(spark: SparkSession, execution_date: str) -> DataFrame:
    year, month, day = execution_date.split("-")

    path = f"s3a://{BRONZE_BUCKET}/weather/raw/year={year}/month={month}/day={day}/"

    log.info("Reading RAW data from: %s", path)

    df = spark.read.parquet(path)

    return df


# ─── Flatten + Normalize ───────────────────────────────────────────────────

def normalize_weather(df: DataFrame) -> DataFrame:
    """
    Flatten nested JSON structure from Open-Meteo.
    """

    return df.select(
        F.col("current_weather.temperature").alias("temperature"),
        F.col("current_weather.windspeed").alias("windspeed"),
        F.col("current_weather.winddirection").alias("wind_direction"),
        F.col("current_weather.weathercode").alias("weather_code"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("generationtime_ms"),
        F.col("ingested_at").alias("raw_ingested_at")
    )


# ─── Add Metadata ──────────────────────────────────────────────────────────

def add_metadata(df: DataFrame, execution_date: str) -> DataFrame:
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_execution_date", F.lit(execution_date))
        .withColumn("_source", F.lit("open-meteo"))
        .withColumn("_pipeline_stage", F.lit("bronze"))

        # flags (não remover!)
        .withColumn("_has_null_temp", F.col("temperature").isNull())
        .withColumn("_has_null_wind", F.col("windspeed").isNull())

        # partição
        .withColumn("year", F.lit(execution_date[:4]))
        .withColumn("month", F.lit(execution_date[5:7]))
        .withColumn("day", F.lit(execution_date[8:10]))
    )


# ─── Write Bronze ──────────────────────────────────────────────────────────

def write_bronze(df: DataFrame) -> int:
    output_path = f"s3a://{BRONZE_BUCKET}/weather/bronze/"

    (
        df
        .write
        .mode("append")
        .partitionBy("year", "month", "day")
        .parquet(output_path)
    )

    count = df.count()
    log.info("Wrote %d rows to Bronze", count)

    return count


# ─── Audit Log ─────────────────────────────────────────────────────────────

def write_audit(df: DataFrame, execution_date: str, spark: SparkSession):
    total = df.count()
    null_temp = df.filter(F.col("_has_null_temp")).count()
    null_wind = df.filter(F.col("_has_null_wind")).count()

    audit_data = {
        "execution_date": execution_date,
        "ingested_at": datetime.utcnow().isoformat() + "Z",
        "pipeline_stage": "bronze",
        "total_rows": total,
        "null_temp_rows": null_temp,
        "null_wind_rows": null_wind,
        "valid_rows": total - null_temp,
    }

    log.info("Audit: %s", audit_data)

    audit_df = spark.createDataFrame([audit_data])

    (
        audit_df
        .write
        .mode("append")
        .json(f"s3a://{BRONZE_BUCKET}/weather/audit/date={execution_date}/")
    )


# ─── Main ──────────────────────────────────────────────────────────────────

def main():
    execution_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")

    log.info("Starting Bronze Layer for %s", execution_date)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df_raw = read_raw(spark, execution_date)

        df_norm = normalize_weather(df_raw)

        df_bronze = add_metadata(df_norm, execution_date)

        df_bronze.cache()

        write_bronze(df_bronze)

        write_audit(df_bronze, execution_date, spark)

        log.info("Bronze processing SUCCESS")

    except Exception as e:
        log.exception("Bronze processing FAILED: %s", e)
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()