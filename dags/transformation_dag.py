"""
transformation_dag.py
─────────────────────
ETL pipeline DAG:
  Bronze → Silver (clean) → Gold (aggregate)
  Triggered by SQS event from ingestion_dag
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

AWS_CONFIG = dict(
    endpoint_url="http://localstack:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

INGESTION_QUEUE = "de-project-dev-ingestion"
SPARK_MASTER    = "spark://spark-master:7077"


def poll_sqs_trigger(**context) -> dict:
    """Poll SQS for ingestion completion event."""
    sqs = boto3.client("sqs", **AWS_CONFIG)
    queue_url = sqs.get_queue_url(QueueName=INGESTION_QUEUE)["QueueUrl"]

    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10,
    )

    messages = response.get("Messages", [])
    if not messages:
        log.info("No messages in SQS — nothing to process")
        return {}

    msg  = messages[0]
    body = json.loads(msg["Body"])
    log.info("Received SQS event: %s", body)

    # Delete message (acknowledge processing)
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=msg["ReceiptHandle"]
    )

    context["task_instance"].xcom_push(key="s3_uri", value=body.get("s3_uri"))
    context["task_instance"].xcom_push(key="record_count", value=body.get("record_count"))
    return body


def validate_bronze_exists(**context) -> bool:
    """Confirm Bronze data exists before running Spark."""
    s3_uri = context["task_instance"].xcom_pull(
        task_ids="poll_sqs", key="s3_uri"
    )
    if not s3_uri:
        raise ValueError("No S3 URI found — ingestion may not have completed")

    # Parse bucket + key from s3://bucket/key
    path = s3_uri.replace("s3://", "")
    bucket, key = path.split("/", 1)

    s3 = boto3.client("s3", **AWS_CONFIG)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        log.info("Bronze file confirmed: %s", s3_uri)
        return True
    except Exception as e:
        raise FileNotFoundError(f"Bronze file not found: {s3_uri}") from e


default_args = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "email_on_failure":  False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="sales_etl_pipeline",
    description="Transform Bronze → Silver → Gold via PySpark",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 4 * * *",   # 2 hours after ingestion
    default_args=default_args,
    catchup=False,
    tags=["etl", "spark", "silver", "gold"],
) as dag:

    poll_task = PythonOperator(
        task_id="poll_sqs",
        python_callable=poll_sqs_trigger,
    )

    validate_task = PythonOperator(
        task_id="validate_bronze",
        python_callable=validate_bronze_exists,
    )

    # Run PySpark Silver job on the Spark cluster
    silver_task = BashOperator(
        task_id="run_silver_job",
        bash_command="""
            spark-submit \
              --master {{ var.value.get("spark_master_url", "local[*]") }} \
              --packages org.apache.hadoop:hadoop-aws:3.3.4 \
              --conf spark.hadoop.fs.s3a.endpoint=http://localstack:4566 \
              --conf spark.hadoop.fs.s3a.access.key=test \
              --conf spark.hadoop.fs.s3a.secret.key=test \
              --conf spark.hadoop.fs.s3a.path.style.access=true \
              /opt/airflow/spark_jobs/silver_layer.py {{ ds }}
        """,
    )

    gold_task = BashOperator(
        task_id="run_gold_job",
        bash_command="""
            spark-submit \
              --master {{ var.value.get("spark_master_url", "local[*]") }} \
              --packages org.apache.hadoop:hadoop-aws:3.3.4 \
              --conf spark.hadoop.fs.s3a.endpoint=http://localstack:4566 \
              --conf spark.hadoop.fs.s3a.access.key=test \
              --conf spark.hadoop.fs.s3a.secret.key=test \
              --conf spark.hadoop.fs.s3a.path.style.access=true \
              /opt/airflow/spark_jobs/gold_layer.py {{ ds }}
        """,
    )

    poll_task >> validate_task >> silver_task >> gold_task
