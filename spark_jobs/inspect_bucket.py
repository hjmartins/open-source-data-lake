from pyspark.sql import SparkSession

# ─── CONFIG ───────────────────────────────────────────────

BUCKET = "de-project-dev-bronze"
PATH = f"s3a://{BUCKET}/weather/bronze/"

# ─── SPARK SESSION ────────────────────────────────────────
#sales/year=2026/month=03/day=30/sales_2026-03-30.csv
spark = (
    SparkSession.builder
    .appName("Read Bronze Layer")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ─── LEITURA ──────────────────────────────────────────────

df = spark.read.parquet("s3a://de-project-dev-bronze/weather/bronze/")
df.show()