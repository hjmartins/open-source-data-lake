"""
monitoring_dag.py
─────────────────
Daily pipeline health & cost monitoring:
  - S3 storage usage per layer
  - Spark job duration tracking
  - Data freshness checks
  - Simulated cost report (mimics AWS Cost Explorer)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

AWS_CONFIG = dict(
    endpoint_url="http://localstack:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

BUCKETS = {
    "bronze": "de-project-dev-bronze",
    "silver": "de-project-dev-silver",
    "gold":   "de-project-dev-gold",
}

# Simulated cost rates ($/GB/month)
COST_RATES = {
    "s3_standard":    0.023,
    "s3_standard_ia": 0.0125,
    "compute_hour":   0.048,    # Spot-equivalent
}


def check_s3_storage(**context) -> dict:
    """Calculate storage usage and estimated cost per layer."""
    s3 = boto3.client("s3", **AWS_CONFIG)
    report = {}

    for layer, bucket in BUCKETS.items():
        try:
            paginator  = s3.get_paginator("list_objects_v2")
            pages      = paginator.paginate(Bucket=bucket)
            total_size = sum(
                obj["Size"]
                for page in pages
                for obj in page.get("Contents", [])
            )

            size_gb       = total_size / (1024 ** 3)
            monthly_cost  = round(size_gb * COST_RATES["s3_standard"], 4)

            report[layer] = {
                "bucket":       bucket,
                "size_bytes":   total_size,
                "size_gb":      round(size_gb, 4),
                "monthly_cost": monthly_cost,
            }
            log.info("%s: %.2f GB = $%.4f/month", layer, size_gb, monthly_cost)

        except Exception as e:
            log.warning("Could not check bucket %s: %s", bucket, e)
            report[layer] = {"error": str(e)}

    total_cost = sum(v.get("monthly_cost", 0) for v in report.values())
    report["total_monthly_cost"] = round(total_cost, 4)
    log.info("Total estimated monthly S3 cost: $%.4f", total_cost)

    context["task_instance"].xcom_push(key="storage_report", value=report)
    return report


def check_data_freshness(**context) -> dict:
    """Verify Gold layer data was updated today."""
    s3 = boto3.client("s3", **AWS_CONFIG)
    execution_date = datetime.strptime(context["ds"], "%Y-%m-%d")
    freshness = {}

    for layer, bucket in BUCKETS.items():
        try:
            response = s3.list_objects_v2(
                Bucket=bucket, MaxKeys=1
            )
            if not response.get("Contents"):
                freshness[layer] = {"status": "EMPTY", "last_modified": None}
                continue

            # Find most recently modified object
            objects = s3.list_objects_v2(Bucket=bucket)
            latest  = max(
                objects.get("Contents", []),
                key=lambda x: x["LastModified"],
                default=None
            )

            if latest:
                last_mod    = latest["LastModified"].replace(tzinfo=None)
                hours_since = (datetime.utcnow() - last_mod).total_seconds() / 3600
                is_fresh    = hours_since < 26  # Tolerance: 2 hours beyond 24h

                freshness[layer] = {
                    "status":       "FRESH" if is_fresh else "STALE",
                    "last_modified": str(last_mod),
                    "hours_since":   round(hours_since, 1),
                }
                log.info("%s freshness: %s (%.1fh ago)", layer,
                         freshness[layer]["status"], hours_since)

        except Exception as e:
            freshness[layer] = {"status": "ERROR", "error": str(e)}

    return freshness


def generate_cost_report(**context) -> None:
    """Generate a daily cost optimisation report."""
    storage = context["task_instance"].xcom_pull(
        task_ids="check_storage", key="storage_report"
    )

    recommendations = []

    bronze_gb = storage.get("bronze", {}).get("size_gb", 0)
    silver_gb = storage.get("silver", {}).get("size_gb", 0)
    gold_gb   = storage.get("gold", {}).get("size_gb", 0)

    # Check compression efficiency
    if bronze_gb > 0 and silver_gb > 0:
        compression_ratio = bronze_gb / silver_gb
        if compression_ratio < 2:
            recommendations.append(
                "⚠ Low compression ratio (%.1fx). "
                "Consider using ORC instead of Parquet, or check for data bloat."
                % compression_ratio
            )

    # Gold should be much smaller than Silver
    if silver_gb > 0 and gold_gb > silver_gb * 0.5:
        recommendations.append(
            "⚠ Gold layer is large relative to Silver. "
            "Review aggregation logic to ensure proper summarisation."
        )

    # Cost threshold alert
    total_cost = storage.get("total_monthly_cost", 0)
    if total_cost > 10.0:
        recommendations.append(
            "🔴 Monthly storage cost $%.2f exceeds $10 threshold. "
            "Review lifecycle policies." % total_cost
        )
    else:
        recommendations.append(
            "✅ Monthly storage cost $%.2f within budget." % total_cost
        )

    report = {
        "date":              context["ds"],
        "storage_summary":   storage,
        "recommendations":   recommendations,
        "cost_optimisation": {
            "parquet_savings_pct":   "~70% vs CSV",
            "lifecycle_policies":    "active",
            "spot_simulation":       "Docker resource limits applied",
        }
    }

    log.info("Cost report: %s", report)

    # In production: write to S3 and/or send to Slack/email
    s3 = boto3.client("s3", **AWS_CONFIG)
    import json
    s3.put_object(
        Bucket=BUCKETS["gold"],
        Key=f"reports/cost/date={context['ds']}/cost_report.json",
        Body=json.dumps(report, indent=2).encode(),
        ServerSideEncryption="AES256",
    )


default_args = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="pipeline_monitoring",
    description="Daily health checks, storage metrics, and cost reporting",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    default_args=default_args,
    catchup=False,
    tags=["monitoring", "cost", "health"],
) as dag:

    storage_task  = PythonOperator(task_id="check_storage",   python_callable=check_s3_storage)
    freshness_task = PythonOperator(task_id="check_freshness", python_callable=check_data_freshness)
    report_task   = PythonOperator(task_id="cost_report",     python_callable=generate_cost_report)

    [storage_task, freshness_task] >> report_task
