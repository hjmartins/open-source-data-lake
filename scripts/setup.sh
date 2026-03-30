#!/usr/bin/env bash
# setup.sh — One-command project bootstrap
# Usage: ./scripts/setup.sh

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

log()  { echo -e "${BLUE}[INFO]${NC} $1"; }
ok()   { echo -e "${GREEN}[OK]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
err()  { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# ─── Prerequisites ────────────────────────────────────────────────────────────
command -v docker      >/dev/null 2>&1 || err "Docker not installed"
command -v docker-compose >/dev/null 2>&1 || command -v "docker compose" >/dev/null 2>&1 || err "Docker Compose not installed"
command -v terraform   >/dev/null 2>&1 || warn "Terraform not installed — skipping IaC step"

log "Starting Data Engineering Project setup..."

# ─── Start Docker stack ───────────────────────────────────────────────────────
log "Starting Docker services..."
docker compose up -d --build

log "Waiting for LocalStack to be healthy..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:4566/_localstack/health | grep -q '"s3": "available"'; then
        ok "LocalStack is ready"
        break
    fi
    echo -n "."
    sleep 3
    [ "$i" -eq 30 ] && err "LocalStack failed to start"
done

# ─── Terraform infrastructure ─────────────────────────────────────────────────
if command -v terraform >/dev/null 2>&1; then
    log "Applying Terraform infrastructure..."
    cd terraform
    terraform init -input=false
    terraform apply -auto-approve -input=false
    cd ..
    ok "Infrastructure provisioned"
else
    log "Provisioning S3 buckets manually via AWS CLI..."
    for bucket in de-project-dev-bronze de-project-dev-silver de-project-dev-gold; do
        aws --endpoint-url=http://localhost:4566 \
            s3api create-bucket \
            --bucket "$bucket" \
            --region us-east-1 \
            2>/dev/null && ok "Created bucket: $bucket" || warn "Bucket $bucket may already exist"

        # Enable encryption
        aws --endpoint-url=http://localhost:4566 \
            s3api put-bucket-encryption \
            --bucket "$bucket" \
            --server-side-encryption-configuration '{
              "Rules": [{
                "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}
              }]
            }' 2>/dev/null

        # Block public access
        aws --endpoint-url=http://localhost:4566 \
            s3api put-public-access-block \
            --bucket "$bucket" \
            --public-access-block-configuration \
            "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true" \
            2>/dev/null
    done

    # Create SQS queue
    aws --endpoint-url=http://localhost:4566 \
        sqs create-queue \
        --queue-name de-project-dev-ingestion \
        --region us-east-1 2>/dev/null || warn "Queue may already exist"
    ok "Manual infrastructure setup complete"
fi

# ─── Wait for Airflow ─────────────────────────────────────────────────────────
log "Waiting for Airflow webserver..."
for i in $(seq 1 20); do
    if curl -sf http://localhost:8080/health | grep -q '"healthy"'; then
        ok "Airflow is ready"
        break
    fi
    echo -n "."
    sleep 5
    [ "$i" -eq 20 ] && warn "Airflow not yet ready — check logs with: docker logs airflow-webserver"
done

# ─── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}══════════════════════════════════════════${NC}"
echo -e "${GREEN}  Setup Complete!${NC}"
echo -e "${GREEN}══════════════════════════════════════════${NC}"
echo ""
echo "  Airflow UI:      http://localhost:8080  (admin/admin)"
echo "  Spark Master UI: http://localhost:8081"
echo "  Grafana:         http://localhost:3000  (admin/admin)"
echo "  Prometheus:      http://localhost:9090"
echo "  LocalStack:      http://localhost:4566"
echo ""
echo "  Trigger a pipeline run:"
echo "  $ docker exec airflow-scheduler airflow dags trigger sales_ingestion"
echo ""
