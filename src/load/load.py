import logging
import os
from datetime import datetime
from typing import List
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

RAW_BASE_PATH = "/opt/airflow/data/raw/weather"

POSTGRES_CONN = {
    'host': os.environ.get("POSTGRES_HOST", "postgres"),
    'port': int(os.environ.get("POSTGRES_PORT", 5432)),
    'database': os.environ.get("POSTGRES_DB", "weather_db"),
    'user': os.environ.get("POSTGRES_USER", "admin"),
    'password': os.environ.get("POSTGRES_PASSWORD", "admin"),
}

COLUMNS_ORDER = [
    'time', 'temperature_2m', 'relative_humidity_2m', 'precipitation',
    'wind_speed_10m', 'city', 'latitude', 'longitude',
    'ingestion_timestamp', 'source', 'execution_date'
]


def get_target_year(execution_date):
    if isinstance(execution_date, str):
        return execution_date[:4]
    elif isinstance(execution_date, datetime):
        return str(execution_date.year)
    else:
        raise ValueError(f"Formato de data desconhecido: {type(execution_date)}")


def _find_parquet_files(years: List[str]):
    """Localiza todos os arquivos parquet para os anos indicados."""
    try:
        cities = [
            d for d in os.listdir(RAW_BASE_PATH)
            if os.path.isdir(os.path.join(RAW_BASE_PATH, d))
        ]
    except FileNotFoundError:
        raise FileNotFoundError(f"Diretório base não encontrado: {RAW_BASE_PATH}")

    files_found = []
    for year in years:
        for city in cities:
            city_year_path = os.path.join(RAW_BASE_PATH, city, f"year={year}")
            if not os.path.exists(city_year_path):
                logger.info(f"Sem dados de {year} para {city}")
                continue
            # Fix: busca recursiva para suportar partição /date=YYYY-MM-DD/
            for root, _, files in os.walk(city_year_path):
                for f in files:
                    if f.endswith(".parquet"):
                        files_found.append(os.path.join(root, f))

    return files_found


def load_to_postgres(execution_date, is_historical: bool = False, **kwargs):
    """Carrega arquivos parquet na tabela de staging do PostgreSQL."""
    execution_date_str = (
        execution_date if isinstance(execution_date, str) else execution_date.isoformat()
    )

    if is_historical:
        years = [str(y) for y in range(2021, datetime.now().year + 1)]
        logger.info("Modo histórico: carregando todos os anos.")
    else:
        years = [get_target_year(execution_date)]
        logger.info(f"Modo incremental: carregando ano {years[0]}.")

    files_found = _find_parquet_files(years)

    if not files_found:
        logger.warning("Nenhum arquivo parquet encontrado. Encerrando sem erro.")
        return

    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    # Garante que a tabela de controle existe (idempotente)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ingest_control (
            file_path TEXT PRIMARY KEY,
            loaded_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()

    for file_path in files_found:
        cur.execute("SELECT 1 FROM ingest_control WHERE file_path = %s", (file_path,))
        if cur.fetchone():
            logger.info(f"[SKIP] Já processado: {file_path}")
            continue

        logger.info(f"[LOAD] {file_path}")
        df = pd.read_parquet(file_path)
        df["execution_date"] = execution_date_str

        for col in COLUMNS_ORDER:
            if col not in df.columns:
                df[col] = None

        data_tuples = [tuple(x) for x in df[COLUMNS_ORDER].to_numpy()]

        insert_sql = """
            INSERT INTO stg_weather (
                time, temperature_2m, relative_humidity_2m, precipitation,
                wind_speed_10m, city, latitude, longitude,
                ingestion_timestamp, source, execution_date
            ) VALUES %s
            ON CONFLICT DO NOTHING
        """

        try:
            execute_values(cur, insert_sql, data_tuples)
            cur.execute("INSERT INTO ingest_control (file_path) VALUES (%s)", (file_path,))
            conn.commit()
            logger.info(f"[SUCCESS] {len(df)} registros de {file_path}")
        except Exception as e:
            conn.rollback()
            logger.error(f"[ERROR] Falha ao inserir {file_path}: {e}")
            raise

    cur.close()
    conn.close()
