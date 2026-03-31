import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
RAW_BASE_PATH = "/opt/airflow/data/raw/weather"

CITIES = [
    {"name": "New_York", "latitude": 40.7128, "longitude": -74.0060},
    {"name": "Los_Angeles", "latitude": 34.0522, "longitude": -118.2437},
    {"name": "Chicago", "latitude": 41.8781, "longitude": -87.6298},
]


def extract_historical_weather(city: dict, start_date: str, end_date: str) -> pd.DataFrame:
    """Busca dados históricos da Open-Meteo API para uma cidade e período."""
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

    response = requests.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data["hourly"])

    df["city"] = city["name"]
    df["latitude"] = city["latitude"]
    df["longitude"] = city["longitude"]
    df["ingestion_timestamp"] = datetime.now(timezone.utc)
    df["start_date"] = start_date
    df["end_date"] = end_date

    return df


def save_historical_weather_data(df: pd.DataFrame, city_name: str, year: int) -> None:
    """Salva os dados em parquet particionado por cidade e ano. Não sobrescreve."""
    output_dir = os.path.join(RAW_BASE_PATH, f"{city_name}/year={year}")

    # Fix: verificava o diretório mas não o arquivo — agora verifica o arquivo diretamente
    file_path = os.path.join(output_dir, f"weather_{city_name}_{year}.parquet")
    if Path(file_path).exists():
        logger.info(f"Arquivo já existe, pulando: {file_path}")
        return

    os.makedirs(output_dir, exist_ok=True)
    df.to_parquet(file_path, index=False)
    logger.info(f"Salvo: {file_path}")


def run_historical(**kwargs) -> None:  # Fix: aceita **kwargs para compatibilidade com Airflow
    """Extrai dados históricos dos últimos 5 anos para todas as cidades."""
    today = datetime.now().date()
    start_year = today.year - 5

    for city in CITIES:
        for year in range(start_year, today.year):
            start_date = date(year, 1, 1).isoformat()
            end_date = date(year, 12, 31).isoformat()
            logger.info(f"Extraindo {city['name']} — {year}")
            try:
                df = extract_historical_weather(city, start_date, end_date)
                save_historical_weather_data(df, city["name"], year)
            except Exception as e:
                logger.error(f"Erro ao processar {city['name']} {year}: {e}")
                raise

    logger.info("Extração histórica concluída.")
