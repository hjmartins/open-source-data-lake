#!/usr/bin/env bash
# dev.sh — Sobe só o que você precisa
# Uso: ./scripts/dev.sh

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

log()  { echo -e "${BLUE}▸${NC} $1"; }
ok()   { echo -e "${GREEN}✔${NC} $1"; }
warn() { echo -e "${YELLOW}⚠${NC} $1"; }
err()  { echo -e "${RED}✖${NC} $1"; exit 1; }
hr()   { echo -e "${BLUE}────────────────────────────────────────${NC}"; }

# ─── Perfis ───────────────────────────────────────────────────────────────────
# Cada perfil lista os serviços do docker-compose.yml a subir

profile_ingestion() {
  # Testar DAGs de ingestão e S3
  # RAM: ~2 GB | CPU: baixo
  SERVICES="localstack postgres airflow-init"
  POST_SERVICES="airflow-webserver airflow-scheduler"
  LABEL="Ingestão  (LocalStack + Airflow)"
  URLS=(
    "Airflow  → http://localhost:8080  (admin/admin)"
    "LocalStack → http://localhost:4566"
  )
}

profile_etl() {
  # Testar jobs Spark completos com S3
  # RAM: ~4 GB | CPU: médio
  SERVICES="localstack postgres airflow-init spark-master spark-worker"
  POST_SERVICES="airflow-webserver airflow-scheduler"
  LABEL="ETL completo  (LocalStack + Airflow + Spark)"
  URLS=(
    "Airflow     → http://localhost:8080  (admin/admin)"
    "Spark UI    → http://localhost:8081"
    "LocalStack  → http://localhost:4566"
  )
}

profile_spark() {
  # Rodar e testar jobs Spark sem Airflow
  # RAM: ~2.5 GB | CPU: médio-alto durante jobs
  SERVICES="localstack spark-master spark-worker"
  POST_SERVICES=""
  LABEL="Spark standalone  (LocalStack + Spark)"
  URLS=(
    "Spark UI   → http://localhost:8081"
    "LocalStack → http://localhost:4566"
  )
}

profile_monitoring() {
  # Ver dashboards e métricas (não roda pipeline)
  # RAM: ~1 GB | CPU: muito baixo
  SERVICES="prometheus grafana"
  POST_SERVICES=""
  LABEL="Monitoramento  (Prometheus + Grafana)"
  URLS=(
    "Grafana    → http://localhost:3000  (admin/admin)"
    "Prometheus → http://localhost:9090"
  )
}

profile_full() {
  # Stack completo — precisa de 8 GB RAM
  SERVICES="localstack postgres airflow-init spark-master spark-worker prometheus grafana"
  POST_SERVICES="airflow-webserver airflow-scheduler"
  LABEL="Stack completo"
  URLS=(
    "Airflow    → http://localhost:8080  (admin/admin)"
    "Spark UI   → http://localhost:8081"
    "Grafana    → http://localhost:3000  (admin/admin)"
    "Prometheus → http://localhost:9090"
    "LocalStack → http://localhost:4566"
  )
}

# ─── Menu ─────────────────────────────────────────────────────────────────────

show_menu() {
  clear
  echo -e "${BOLD}${CYAN}"
  echo "  ╔══════════════════════════════════════════╗"
  echo "  ║     Data Engineering — Dev Launcher      ║"
  echo "  ╚══════════════════════════════════════════╝"
  echo -e "${NC}"
  hr
  echo -e "  ${BOLD}1)${NC} Ingestão       LocalStack + Airflow          ${YELLOW}~2 GB${NC}"
  echo -e "  ${BOLD}2)${NC} ETL completo   LocalStack + Airflow + Spark   ${YELLOW}~4 GB${NC}"
  echo -e "  ${BOLD}3)${NC} Spark solo     LocalStack + Spark             ${YELLOW}~2.5 GB${NC}"
  echo -e "  ${BOLD}4)${NC} Monitoramento  Prometheus + Grafana           ${YELLOW}~1 GB${NC}"
  echo -e "  ${BOLD}5)${NC} Stack completo tudo                           ${YELLOW}~7 GB${NC}"
  hr
  echo -e "  ${BOLD}d)${NC} Derrubar tudo (docker compose down)"
  echo -e "  ${BOLD}s)${NC} Status dos containers"
  echo -e "  ${BOLD}l)${NC} Logs de um serviço"
  echo -e "  ${BOLD}q)${NC} Sair"
  hr
  echo -ne "  Escolha: "
}

# ─── Funções utilitárias ──────────────────────────────────────────────────────

wait_healthy() {
  local service=$1
  local url=$2
  local label=${3:-$service}
  local retries=${4:-20}

  log "Aguardando $label..."
  for i in $(seq 1 "$retries"); do
    if curl -sf "$url" > /dev/null 2>&1; then
      ok "$label pronto"
      return 0
    fi
    sleep 3
    echo -ne "  ${i}/${retries}\r"
  done
  warn "$label demorou mais que o esperado — verifique: docker logs $service"
}

bootstrap_localstack() {
  # Cria buckets e fila SQS se ainda não existirem
  log "Provisionando infraestrutura LocalStack..."
  sleep 5  # deixa o LocalStack inicializar

  for bucket in de-project-dev-bronze de-project-dev-silver de-project-dev-gold; do
    aws --endpoint-url=http://localhost:4566 \
        s3api create-bucket --bucket "$bucket" --region us-east-1 \
        2>/dev/null && echo "  bucket: $bucket" || true

    aws --endpoint-url=http://localhost:4566 \
        s3api put-public-access-block --bucket "$bucket" \
        --public-access-block-configuration \
        "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true" \
        2>/dev/null || true
  done

  aws --endpoint-url=http://localhost:4566 \
      sqs create-queue --queue-name de-project-dev-ingestion --region us-east-1 \
      2>/dev/null || true

  ok "Infraestrutura pronta"
}

start_profile() {
  local profile_fn=$1

  # Carrega variáveis do perfil
  SERVICES=""; POST_SERVICES=""; LABEL=""; URLS=()
  $profile_fn

  echo ""
  log "Subindo: ${BOLD}$LABEL${NC}"
  hr

  # Sobe serviços base
  docker compose up -d $SERVICES

  # Aguarda LocalStack se estiver no perfil
  if echo "$SERVICES" | grep -q "localstack"; then
    wait_healthy "localstack" "http://localhost:4566/_localstack/health" "LocalStack" 30
    bootstrap_localstack
  fi

  # Aguarda init do Airflow antes de subir webserver/scheduler
  if echo "$SERVICES" | grep -q "airflow-init"; then
    log "Executando airflow db init..."
    docker compose wait airflow-init 2>/dev/null || sleep 15
  fi

  # Sobe serviços dependentes (webserver, scheduler)
  if [ -n "$POST_SERVICES" ]; then
    docker compose up -d $POST_SERVICES
    if echo "$POST_SERVICES" | grep -q "airflow-webserver"; then
      wait_healthy "airflow-webserver" "http://localhost:8080/health" "Airflow" 25
    fi
  fi

  # Aguarda Spark se presente
  if echo "$SERVICES" | grep -q "spark-master"; then
    wait_healthy "spark-master" "http://localhost:8081" "Spark Master" 15
  fi

  echo ""
  hr
  echo -e "${GREEN}${BOLD}  Tudo pronto! Acesse:${NC}"
  for url in "${URLS[@]}"; do
    echo -e "  ${CYAN}▸${NC} $url"
  done
  hr

  # Dica rápida de uso
  if echo "$SERVICES" | grep -q "airflow"; then
    echo ""
    echo -e "  ${YELLOW}Dicas:${NC}"
    echo "  Acionar DAG:  docker exec airflow-scheduler airflow dags trigger sales_ingestion"
    echo "  Ver logs:     docker logs -f airflow-scheduler"
    echo "  Gerar dados:  python scripts/generate_data.py --days 7"
  fi
  if echo "$SERVICES" | grep -q "spark"; then
    echo ""
    echo -e "  ${YELLOW}Rodar job Spark diretamente:${NC}"
    echo "  docker exec spark-master spark-submit /opt/bitnami/spark/jobs/silver_layer.py 2024-01-15"
  fi
  echo ""
}

action_down() {
  echo ""
  log "Derrubando todos os containers..."
  docker compose down
  ok "Ambiente encerrado"
  echo ""
}

action_status() {
  echo ""
  docker compose ps
  echo ""
}

action_logs() {
  echo ""
  echo -ne "  Nome do serviço (ex: airflow-scheduler, spark-master): "
  read -r svc
  echo ""
  docker logs --tail=50 -f "$svc"
}

# ─── Loop principal ───────────────────────────────────────────────────────────

# Suporte a argumento direto: ./dev.sh etl
case "${1:-}" in
  ingestion)   start_profile profile_ingestion; exit 0 ;;
  etl)         start_profile profile_etl;       exit 0 ;;
  spark)       start_profile profile_spark;     exit 0 ;;
  monitoring)  start_profile profile_monitoring; exit 0 ;;
  full)        start_profile profile_full;      exit 0 ;;
  down)        action_down;                     exit 0 ;;
esac

# Menu interativo
while true; do
  show_menu
  read -r choice
  case "$choice" in
    1) start_profile profile_ingestion  ;;
    2) start_profile profile_etl        ;;
    3) start_profile profile_spark      ;;
    4) start_profile profile_monitoring ;;
    5) start_profile profile_full       ;;
    d) action_down    ;;
    s) action_status  ;;
    l) action_logs    ;;
    q) echo ""; exit 0 ;;
    *) warn "Opção inválida" ;;
  esac
  echo -ne "${BLUE}  [Enter para voltar ao menu]${NC}"
  read -r
done