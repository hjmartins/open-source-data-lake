"""
cost_report.py — Cost Monitoring Simulation
─────────────────────────────────────────────
Mimics AWS Cost Explorer by calculating storage + compute costs
from LocalStack S3 usage and Docker resource stats.

Usage:
  python scripts/cost_report.py
  python scripts/cost_report.py --output report.json
"""

import argparse
import json
import logging
import subprocess
from datetime import datetime

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

BUCKETS = {
    "bronze": "de-project-dev-bronze",
    "silver": "de-project-dev-silver",
    "gold":   "de-project-dev-gold",
}

# AWS pricing reference ($/GB/month, $/vCPU-hour)
PRICING = {
    "s3_standard":    0.023,
    "s3_standard_ia": 0.0125,
    "s3_glacier":     0.004,
    "ec2_spot_vcpu":  0.012,   # ~m5.xlarge spot equivalent
    "emr_overhead":   1.20,    # EMR adds ~20% over EC2
    "data_transfer":  0.09,    # $/GB egress
}

# ─── S3 storage analysis ──────────────────────────────────────────────────────

def get_bucket_stats(bucket: str) -> dict:
    """Return object count, total size, and estimated monthly cost."""
    s3 = boto3.client("s3", **AWS_CONFIG)

    try:
        paginator   = s3.get_paginator("list_objects_v2")
        total_size  = 0
        total_files = 0
        formats     = {}

        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                total_size  += obj["Size"]
                total_files += 1
                ext = obj["Key"].rsplit(".", 1)[-1].lower()
                formats[ext] = formats.get(ext, 0) + 1

        size_gb      = total_size / (1024 ** 3)
        monthly_cost = round(size_gb * PRICING["s3_standard"], 6)

        return {
            "object_count":     total_files,
            "size_bytes":       total_size,
            "size_gb":          round(size_gb, 6),
            "size_mb":          round(total_size / (1024 ** 2), 2),
            "monthly_cost_usd": monthly_cost,
            "file_formats":     formats,
            "status":           "ok",
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}


# ─── Docker compute cost simulation ──────────────────────────────────────────

def get_docker_resource_usage() -> dict:
    """Read CPU/memory from running Docker containers."""
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format",
             "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"],
            capture_output=True, text=True, timeout=10
        )
        containers = {}
        for line in result.stdout.strip().splitlines():
            parts = line.split("\t")
            if len(parts) == 3:
                name, cpu, mem = parts
                cpu_pct = float(cpu.replace("%", "")) / 100
                containers[name] = {"cpu_pct": round(cpu_pct, 4), "memory": mem}
        return containers
    except Exception as e:
        log.warning("Could not read Docker stats: %s", e)
        return {}


def estimate_compute_cost(containers: dict, runtime_hours: float = 8.0) -> dict:
    """Estimate equivalent AWS compute cost for the pipeline runtime."""
    spark_containers = [k for k in containers if "spark" in k.lower()]
    total_cpu_pct    = sum(containers[c]["cpu_pct"] for c in spark_containers)

    # Assume 4 vCPUs allocated per Spark container
    vcpus_used          = total_cpu_pct * 4
    compute_cost        = round(vcpus_used * PRICING["ec2_spot_vcpu"] * runtime_hours, 4)
    emr_equivalent_cost = round(compute_cost * PRICING["emr_overhead"], 4)

    return {
        "spark_containers":      spark_containers,
        "vcpus_simulated":       round(vcpus_used, 2),
        "runtime_hours":         runtime_hours,
        "spot_cost_usd":         compute_cost,
        "emr_equivalent_usd":    emr_equivalent_cost,
        "savings_vs_emr_usd":    round(emr_equivalent_cost - compute_cost, 4),
    }


# ─── Recommendations ──────────────────────────────────────────────────────────

def generate_recommendations(storage: dict, compute: dict) -> list[str]:
    """Produce actionable cost-optimisation suggestions."""
    recs = []

    bronze = storage.get("bronze", {})
    silver = storage.get("silver", {})
    gold   = storage.get("gold", {})

    # Parquet vs CSV check
    for layer, stats in storage.items():
        fmts = stats.get("file_formats", {})
        if fmts.get("csv", 0) > 0 and fmts.get("parquet", 0) == 0:
            recs.append(
                f"⚠ {layer.upper()}: only CSV detected. "
                "Converting to Parquet saves ~70% storage."
            )

    # Compression ratio
    b_gb = bronze.get("size_gb", 0)
    s_gb = silver.get("size_gb", 0)
    if b_gb > 0 and s_gb > 0:
        ratio = b_gb / s_gb if s_gb > 0 else 1
        if ratio < 2.0:
            recs.append(
                f"⚠ Compression ratio is {ratio:.1f}x (Bronze→Silver). "
                "Target ≥ 3x. Check Parquet codec (try ZSTD over Snappy)."
            )
        else:
            recs.append(f"✅ Good compression ratio: {ratio:.1f}x (Bronze→Silver).")

    # Gold should be tiny
    if s_gb > 0 and gold.get("size_gb", 0) > s_gb * 0.3:
        recs.append(
            "⚠ Gold layer is >30% the size of Silver. "
            "Aggregations may not be summarising enough."
        )

    # Total monthly cost
    total = sum(v.get("monthly_cost_usd", 0) for v in storage.values() if isinstance(v, dict))
    if total < 1.0:
        recs.append(f"✅ Monthly storage cost ${total:.4f} — well within free tier.")
    elif total < 10.0:
        recs.append(f"✅ Monthly storage cost ${total:.4f} — within budget.")
    else:
        recs.append(
            f"🔴 Monthly storage ${total:.2f} exceeds $10. "
            "Review S3 lifecycle policies."
        )

    # Lifecycle reminder
    recs.append(
        "💡 Lifecycle tip: Bronze → Standard-IA at 30d, Glacier at 90d, "
        "delete at 365d (configured in Terraform)."
    )

    return recs


# ─── Report assembly ──────────────────────────────────────────────────────────

def build_report() -> dict:
    log.info("Collecting S3 storage stats...")
    storage = {layer: get_bucket_stats(bucket) for layer, bucket in BUCKETS.items()}

    log.info("Collecting Docker compute stats...")
    containers = get_docker_resource_usage()
    compute    = estimate_compute_cost(containers)

    total_monthly = sum(
        v.get("monthly_cost_usd", 0) for v in storage.values() if isinstance(v, dict)
    )

    report = {
        "generated_at":          datetime.utcnow().isoformat() + "Z",
        "storage": {
            **{layer: stats for layer, stats in storage.items()},
            "total_monthly_cost_usd": round(total_monthly, 4),
        },
        "compute":                compute,
        "total_estimated_daily":  round(total_monthly / 30 + compute["spot_cost_usd"], 4),
        "recommendations":        generate_recommendations(storage, compute),
        "cost_optimisation_applied": {
            "parquet_format":           "saves ~70% vs CSV",
            "spark_resource_limits":    "2 vCPU / 2 GB per worker (Docker)",
            "s3_lifecycle_policies":    "Bronze expires 30d→IA, 90d→Glacier",
            "spot_equivalent_pricing":  "LocalStack = $0, EMR equivalent saved",
        },
    }

    return report


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Simulate AWS cost report")
    parser.add_argument("--output", default=None, help="Save JSON report to file")
    args = parser.parse_args()

    report = build_report()

    print("\n" + "═" * 52)
    print("  DATA ENGINEERING — COST REPORT")
    print("  " + report["generated_at"])
    print("═" * 52)

    print("\n📦 Storage (S3 / LocalStack)")
    for layer in BUCKETS:
        stats = report["storage"].get(layer, {})
        if stats.get("status") == "ok":
            print(f"  {layer:<8} {stats['size_mb']:>8.2f} MB  "
                  f"${stats['monthly_cost_usd']:.6f}/month")
        else:
            print(f"  {layer:<8} [error: {stats.get('error', '?')}]")
    print(f"  {'TOTAL':<8}              "
          f"${report['storage']['total_monthly_cost_usd']:.4f}/month")

    print("\n⚙  Compute (Docker / Spark)")
    c = report["compute"]
    print(f"  Spark vCPUs used:   {c['vcpus_simulated']}")
    print(f"  Spot estimate:     ${c['spot_cost_usd']:.4f}  ({c['runtime_hours']}h)")
    print(f"  EMR equivalent:    ${c['emr_equivalent_usd']:.4f}")
    print(f"  Savings vs EMR:    ${c['savings_vs_emr_usd']:.4f}")

    print("\n💰 Estimated daily total: ${:.4f}".format(report["total_estimated_daily"]))

    print("\n📋 Recommendations")
    for rec in report["recommendations"]:
        print(f"  {rec}")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2)
        log.info("Report saved to %s", args.output)

    print()


if __name__ == "__main__":
    main()