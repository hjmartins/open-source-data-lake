"""
ingestion_dag.py
────────────────
Ingestion pipeline DAG:
  1. Generate synthetic sales data
  2. Upload raw CSV to S3 Bronze bucket
  3. Trigger SQS event for downstream processing
  4. Validate data quality (row count, schema)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ─── LocalStack connection config ────────────────────────────────────────────
AWS_CONFIG = dict(
    endpoint_url="http://localstack:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

BRONZE_BUCKET = "de-project-dev-bronze"
INGESTION_QUEUE = "de-project-dev-ingestion"

# ─── Task functions ──────────────────────────────────────────────────────────

def generate_data(**context) -> dict:
    """Generate synthetic e-commerce sales data."""
    import csv
    import io
    import random

    execution_date = context["ds"]
    num_records = random.randint(1000, 5000)

    products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Headset"]
    regions  = ["North", "South", "East", "West", "Central"]
    statuses = ["completed", "returned", "pending"]

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=[
        "order_id", "customer_id", "product", "quantity",
        "unit_price", "region", "status", "order_date"
    ])
    writer.writeheader()

    for i in range(num_records):
        qty   = random.randint(1, 10)
        price = round(random.uniform(9.99, 999.99), 2)
        writer.writerow({
            "order_id":    f"ORD-{execution_date}-{i:05d}",
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "product":     random.choice(products),
            "quantity":    qty,
            "unit_price":  price,
            "region":      random.choice(regions),
            "status":      random.choice(statuses),
            "order_date":  execution_date,
        })

    data_str = buffer.getvalue()
    context["task_instance"].xcom_push(key="csv_data", value=data_str)
    context["task_instance"].xcom_push(key="record_count", value=num_records)

    log.info("Generated %d records for %s", num_records, execution_date)
    return {"record_count": num_records, "date": execution_date}


def upload_to_bronze(**context) -> str:
    """Upload raw CSV to S3 Bronze bucket with partitioning."""
    execution_date = context["ds"]
    year, month, day = execution_date.split("-")

    csv_data = context["task_instance"].xcom_pull(
        task_ids="generate_raw_data", key="csv_data"
    )

    s3_client = boto3.client("s3", **AWS_CONFIG)
    s3_key = f"sales/year={year}/month={month}/day={day}/sales_{execution_date}.csv"

    # Upload with server-side encryption
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=s3_key,
        Body=csv_data.encode("utf-8"),
        ServerSideEncryption="AES256",          # Security: enforce encryption
        ContentType="text/csv",
        Metadata={
            "pipeline":       "ingestion",
            "source":         "synthetic-generator",
            "execution_date": execution_date,
        }
    )

    s3_uri = f"s3://{BRONZE_BUCKET}/{s3_key}"
    context["task_instance"].xcom_push(key="s3_uri", value=s3_uri)
    log.info("Uploaded to %s", s3_uri)
    return s3_uri


def trigger_sqs_event(**context) -> None:
    """Send SQS event to notify downstream ETL pipeline."""
    s3_uri = context["task_instance"].xcom_pull(
        task_ids="upload_bronze", key="s3_uri"
    )
    record_count = context["task_instance"].xcom_pull(
        task_ids="generate_raw_data", key="record_count"
    )

    sqs_client = boto3.client("sqs", **AWS_CONFIG)

    # Get queue URL
    queue_url = sqs_client.get_queue_url(QueueName=INGESTION_QUEUE)["QueueUrl"]

    message = {
        "event_type":     "new_data_available",
        "s3_uri":         s3_uri,
        "record_count":   record_count,
        "execution_date": context["ds"],
        "pipeline":       "sales_ingestion",
    }

    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message),
        MessageAttributes={
            "source": {
                "StringValue": "airflow",
                "DataType": "String"
            }
        }
    )
    log.info("Sent SQS event: %s", message)


def validate_data_quality(**context) -> str:
    """Check row count and schema — branch on pass/fail."""
    record_count = context["task_instance"].xcom_pull(
        task_ids="generate_raw_data", key="record_count"
    )

    # Quality checks
    checks = {
        "min_records": record_count >= 100,
        "max_records": record_count <= 100_000,
    }

    failed = [k for k, v in checks.items() if not v]

    if failed:
        log.warning("Quality checks FAILED: %s", failed)
        return "quality_failed"

    log.info("All quality checks PASSED (%d records)", record_count)
    return "quality_passed"


# ─── DAG definition ──────────────────────────────────────────────────────────

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="sales_ingestion",
    description="Ingest synthetic sales data to S3 Bronze layer",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",   # Daily at 02:00 UTC
    default_args=default_args,
    catchup=False,
    tags=["ingestion", "sales", "bronze"],
    doc_md="""
    ## Sales Ingestion DAG
    Generates synthetic sales data and lands it in the **Bronze** S3 layer.

    ### Flow
    `Generate → Upload → Quality Check → SQS Event`

    ### Monitoring
    - Check XCom for `record_count` to track daily volume
    - SQS queue depth monitors downstream lag
    """,
) as dag:

    generate_task = PythonOperator(
        task_id="generate_raw_data",
        python_callable=generate_data,
    )

    upload_task = PythonOperator(
        task_id="upload_bronze",
        python_callable=upload_to_bronze,
    )

    quality_check = BranchPythonOperator(
        task_id="data_quality_check",
        python_callable=validate_data_quality,
    )

    quality_passed = EmptyOperator(task_id="quality_passed")
    quality_failed = EmptyOperator(task_id="quality_failed")

    trigger_task = PythonOperator(
        task_id="trigger_etl_event",
        python_callable=trigger_sqs_event,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ─── DAG flow ────────────────────────────────────────────────────────────
    generate_task >> upload_task >> quality_check
    quality_check >> [quality_passed, quality_failed]
    quality_passed >> trigger_task
