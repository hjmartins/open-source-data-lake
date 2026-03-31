import logging
import os

import pandas as pd
from sqlalchemy import create_engine

from src.config import POSTGRES_URL, ANALYTICS_BASE_PATH
from src.utils.new_df import new_df
from src.utils.feature_transform import build_training_dataset

logger = logging.getLogger(__name__)

FEATURE_PARQUET = os.path.join(ANALYTICS_BASE_PATH, "weather_data_feature.parquet")
QUERY = "SELECT * FROM fact_weather ORDER BY date, hour"


def extract_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    """Recebe DataFrame bruto do banco e retorna features prontas para ML."""
    logger.info("Gerando agregações diárias...")
    df_daily = new_df(df)
    logger.info("Criando features temporais e targets...")
    df_features = build_training_dataset(df_daily)
    return df_features


def extract_and_transform_from_db(**kwargs) -> None:
    """Extrai do banco, gera features e salva em parquet para uso pelo modelo."""
    logger.info("Extraindo dados de fact_weather...")
    engine = create_engine(POSTGRES_URL)
    df = pd.read_sql(QUERY, engine)

    df_features = extract_and_transform(df)

    os.makedirs(ANALYTICS_BASE_PATH, exist_ok=True)
    df_features.to_parquet(FEATURE_PARQUET, index=False)
    logger.info("Features salvas em %s (%d linhas)", FEATURE_PARQUET, len(df_features))
