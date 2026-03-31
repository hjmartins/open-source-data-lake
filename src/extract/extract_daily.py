import logging
import os
from datetime import date, datetime, timezone

import pandas as pd
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.open-meteo.com/v1/forecast"
RAW_BASE_PATH = "/opt/airflow/data/raw/weather"

CITIES = [
    {"name": "New_York", "latitude": 40.7128, "longitude": -74.0060},
    {"name": "Los_Angeles", "latitude": 34.0522, "longitude": -118.2437},
    {"name": "Chicago", "latitude": 41.8781, "longitude": -87.6298},
]


def fetch_weather_data(city: dict) -> pd.DataFrame:
    """Busca dados de previsão da Open-Meteo API para uma cidade."""
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
        ],
        "timezone": "UTC",
    }

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data["hourly"])

    df["city"] = city["name"]
    df["latitude"] = city["latitude"]
    df["longitude"] = city["longitude"]
    df["ingestion_timestamp"] = datetime.now(timezone.utc)

    return df


def filter_by_date(df: pd.DataFrame, target_date: str) -> pd.DataFrame:
    """Filtra o DataFrame para apenas a data de execução."""
    df["time"] = pd.to_datetime(df["time"])
    target = pd.to_datetime(target_date).date()
    return df[df["time"].dt.date == target].copy()





def run_daily(): 
    """Extrai dados do dia de execução para todas as cidades."""
    hoje = date.today()
    all_data = []
    for city in CITIES:
        logger.info(f"Extraindo {city['name']} para {hoje}")
        df = fetch_weather_data(city)
        df_day = filter_by_date(df, hoje.isoformat())  
        all_data.append(df_day)
        if df_day.empty:
            logger.warning(f"Nenhum dado encontrado para {city['name']} em {hoje}")
            continue

    return pd.concat(all_data, ignore_index=True)
