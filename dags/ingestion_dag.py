from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import boto3
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from src.extract.extract_weather import run_daily

log = logging.getLogger(__name__)

AWS_CONFIG = dict(
    endpoint_url="http://localstack:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

BRONZE_BUCKET = "de-project-dev-bronze"
INGESTION_QUEUE = "de-project-dev-ingestion"


# ─── TASK 1: Extract API ─────────────────────────────────────

def extract_weather(**context):
    data = run_daily()

    context["ti"].xcom_push(key="weather_data", value=data)
    return data


# ─── TASK 2: Upload Bronze ───────────────────────────────────

def upload_bronze(**context):
    execution_date = context["ds"]
    year, month, day = execution_date.split("-")

    data = context["ti"].xcom_pull(
        task_ids="extract_weather", key="weather_data"
    )

    df = pd.json_normalize(data)

    # salva como parquet em memória
    parquet_buffer = df.to_parquet(index=False)

    s3 = boto3.client("s3", **AWS_CONFIG)

    key = f"weather/raw/year={year}/month={month}/day={day}/data.parquet"

    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=parquet_buffer,
        ContentType="application/octet-stream"
    )

    s3_uri = f"s3://{BRONZE_BUCKET}/{key}"
    context["ti"].xcom_push(key="s3_uri", value=s3_uri)

    return s3_uri


# ─── TASK 3: Quality Check ───────────────────────────────────

def validate_data(**context):
    data = context["ti"].xcom_pull(
        task_ids="extract_weather", key="weather_data"
    )

    checks = {
        "has_current": "current" in data,
        "has_temperature": "temperature" in data.get("current", {}).get("current_weather", {})
    }

    failed = [k for k, v in checks.items() if not v]

    if failed:
        log.warning("Quality FAILED: %s", failed)
        return "quality_failed"

    return "quality_passed"


# ─── TASK 4: Send SQS ───────────────────────────────────────

def send_sqs(**context):
    s3_uri = context["ti"].xcom_pull(task_ids="upload_bronze", key="s3_uri")

    sqs = boto3.client("sqs", **AWS_CONFIG)
    queue_url = sqs.get_queue_url(QueueName=INGESTION_QUEUE)["QueueUrl"]

    message = {
        "event_type": "weather_data",
        "layer": "bronze",
        "s3_uri": s3_uri,
        "execution_date": context["ds"]
    }

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message)
    )

    log.info("SQS event sent: %s", message)


# ─── DAG ────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["weather", "bronze", "api"],
) as dag:

    extract = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,

    )

    upload = PythonOperator(
        task_id="upload_bronze",
        python_callable=upload_bronze,
    )

    quality = BranchPythonOperator(
        task_id="quality_check",
        python_callable=validate_data,
    )

    passed = EmptyOperator(task_id="quality_passed")
    failed = EmptyOperator(task_id="quality_failed")

    sqs_trigger = PythonOperator(
        task_id="send_sqs",
        python_callable=send_sqs,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # flow
    extract >> upload >> quality
    quality >> [passed, failed]
    passed >> sqs_trigger