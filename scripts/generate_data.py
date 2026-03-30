"""
generate_data.py — Synthetic Data Generator
─────────────────────────────────────────────
Generates realistic e-commerce sales CSV and uploads to S3 Bronze.

Usage:
  python scripts/generate_data.py --date 2024-01-15 --records 5000
  python scripts/generate_data.py --days 30          # backfill 30 days
"""

import argparse
import csv
import io
import logging
import random
from datetime import datetime, timedelta

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

AWS_CONFIG = dict(
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)
BRONZE_BUCKET = "de-project-dev-bronze"

PRODUCTS = [
    ("Laptop",    499.99, 1999.99),
    ("Phone",     299.99,  999.99),
    ("Tablet",    199.99,  799.99),
    ("Monitor",   149.99,  699.99),
    ("Keyboard",   29.99,  199.99),
    ("Mouse",      19.99,   99.99),
    ("Headset",    49.99,  299.99),
    ("Webcam",     39.99,  199.99),
    ("SSD",        59.99,  349.99),
    ("RAM",        49.99,  249.99),
]

REGIONS   = ["North", "South", "East", "West", "Central"]
STATUSES  = [("completed", 0.75), ("returned", 0.10), ("pending", 0.15)]

# ─── Data generation ──────────────────────────────────────────────────────────

def weighted_choice(choices: list[tuple]) -> str:
    """Pick from weighted (value, weight) pairs."""
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


def generate_records(execution_date: str, num_records: int) -> str:
    """Return CSV string with `num_records` synthetic sales rows."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=[
        "order_id", "customer_id", "product",
        "quantity", "unit_price", "region", "status", "order_date"
    ])
    writer.writeheader()

    for i in range(num_records):
        product_name, price_min, price_max = random.choice(PRODUCTS)
        unit_price = round(random.uniform(price_min, price_max), 2)

        writer.writerow({
            "order_id":    f"ORD-{execution_date.replace('-','')}-{i:06d}",
            "customer_id": f"CUST-{random.randint(1000, 9999):04d}",
            "product":     product_name,
            "quantity":    random.randint(1, 10),
            "unit_price":  unit_price,
            "region":      random.choice(REGIONS),
            "status":      weighted_choice(STATUSES),
            "order_date":  execution_date,
        })

    return buffer.getvalue()


def upload_to_s3(execution_date: str, csv_data: str) -> str:
    """Upload CSV to Bronze bucket with partitioning and encryption."""
    s3  = boto3.client("s3", **AWS_CONFIG)
    y, m, d = execution_date.split("-")
    key = f"sales/year={y}/month={m}/day={d}/sales_{execution_date}.csv"

    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=csv_data.encode("utf-8"),
        ServerSideEncryption="AES256",
        ContentType="text/csv",
        Metadata={"source": "generate_data.py", "date": execution_date},
    )

    uri = f"s3://{BRONZE_BUCKET}/{key}"
    log.info("Uploaded → %s", uri)
    return uri


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic sales data")
    parser.add_argument("--date",    default=datetime.now().strftime("%Y-%m-%d"),
                        help="Target date YYYY-MM-DD (default: today)")
    parser.add_argument("--records", type=int, default=2000,
                        help="Records per day (default: 2000)")
    parser.add_argument("--days",    type=int, default=0,
                        help="Backfill N days ending at --date")
    args = parser.parse_args()

    base_date = datetime.strptime(args.date, "%Y-%m-%d")
    dates = [
        (base_date - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(max(args.days, 1))
    ]

    for date in sorted(dates):
        n    = random.randint(int(args.records * 0.8), int(args.records * 1.2))
        data = generate_records(date, n)
        upload_to_s3(date, data)
        log.info("Date %s: %d records generated", date, n)

    log.info("Done. Generated data for %d date(s).", len(dates))


if __name__ == "__main__":
    main()