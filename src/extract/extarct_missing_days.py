"""
Módulo para extração de dias faltantes no banco de dados.
Nota: O nome do arquivo mantém o typo original (extarct) para não quebrar imports existentes.
"""
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine

from src.utils.request import safe_request

logger = logging.getLogger(__name__)

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
RAW_BASE_PATH = "/opt/airflow/data/raw/weather"

# Fix: conexão centralizada via env vars (não hardcoded)
POSTGRES_URL = os.environ.get(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://admin:admin@postgres:5432/weather_db"
)

CITIES = [
    {"name": "New_York", "latitude": 40.7128, "longitude": -74.0060},
    {"name": "Los_Angeles", "latitude": 34.0522, "longitude": -118.2437},
    {"name": "Chicago", "latitude": 41.8781, "longitude": -87.6298},
]


def extract_historical_weather(city: dict, start_date: str, end_date: str) -> pd.DataFrame:
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
        ],
        "timezone": "UTC",
    }

    data = safe_request(BASE_URL, params)
    df = pd.DataFrame(data["hourly"])
    df["city"] = city["name"]
    df["latitude"] = city["latitude"]
    df["longitude"] = city["longitude"]
    df["ingestion_timestamp"] = datetime.now(timezone.utc)
    return df


def save_historical_weather_data(df: pd.DataFrame, city_name: str, missing_date: str) -> None:
    year = missing_date.split("-")[0]
    output_dir = os.path.join(RAW_BASE_PATH, f"{city_name}/year={year}/date={missing_date}")
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"weather_{city_name}_{missing_date}.parquet")

    if os.path.exists(file_path):
        logger.info(f"Já existe: {file_path}")
        return

    df.to_parquet(file_path, index=False)
    logger.info(f"Salvo: {file_path}")


def get_missing_days() -> list[str]:
    """Compara datas no banco com o range esperado e retorna as faltantes."""
    engine = create_engine(POSTGRES_URL)
    try:
        df = pd.read_sql("SELECT DISTINCT date FROM fact_weather", engine)
        df["date"] = pd.to_datetime(df["date"])
        start_date = df["date"].min()
    except Exception:
        logger.warning("Não foi possível ler fact_weather. Usando 2024-01-01 como início.")
        start_date = datetime(2024, 1, 1)
        df = pd.DataFrame(columns=["date"])

    full_range = pd.date_range(start=start_date, end=datetime.today(), freq="D")

    if not df.empty:
        missing = full_range.difference(df["date"])
    else:
        missing = full_range

    return [d.strftime("%Y-%m-%d") for d in missing]


def run_missing_days(**kwargs) -> None:  # Fix: **kwargs para compatibilidade com Airflow
    missing_dates = get_missing_days()
    if not missing_dates:
        logger.info("Nenhuma data faltante para processar.")
        return

    logger.info(f"{len(missing_dates)} datas faltantes encontradas.")

    for city in CITIES:
        for missing_date in missing_dates:
            try:
                df = extract_historical_weather(city, missing_date, missing_date)
                save_historical_weather_data(df, city["name"], missing_date)
            except Exception as e:
                logger.error(f"Erro ao processar {city['name']} em {missing_date}: {e}")
                raise

    logger.info("Preenchimento de dias faltantes concluído.")
