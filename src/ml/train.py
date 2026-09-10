"""
Módulo de treinamento dos modelos de ML climático.
Extraído do script standalone train.py do Streamlit para rodar no Airflow.
"""
import logging
import os

import joblib
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sqlalchemy import create_engine
import pandas as pd

from src.config import POSTGRES_URL, ANALYTICS_BASE_PATH, MODELS_PATH
from src.utils.feature_transform import build_training_dataset
from src.utils.new_df import new_df

logger = logging.getLogger(__name__)

QUERY = "SELECT * FROM fact_weather ORDER BY date, hour"
FEATURE_PARQUET = os.path.join(ANALYTICS_BASE_PATH, "weather_data_feature.parquet")


def run_training(**kwargs) -> None:
    """Treina e persiste modelos RandomForest para temperatura e precipitação."""
    logger.info("Carregando features de %s", FEATURE_PARQUET)

    if not os.path.exists(FEATURE_PARQUET):
        raise FileNotFoundError(
            f"Arquivo de features não encontrado: {FEATURE_PARQUET}. "
            "Execute a task feature_engineering primeiro."
        )

    df = pd.read_parquet(FEATURE_PARQUET)

    feature_cols = [c for c in df.columns if "target" not in c]
    X = df[feature_cols]
    y_temp   = df["target_temp_tomorrow"]
    y_precip = df["target_precip_tomorrow"]

    split = int(len(X) * 0.8)
    X_train, X_test     = X.iloc[:split],      X.iloc[split:]
    yt_train, yt_test   = y_temp.iloc[:split],  y_temp.iloc[split:]
    yp_train, yp_test   = y_precip.iloc[:split], y_precip.iloc[split:]

    logger.info("Treinando modelo de temperatura (%d amostras)...", len(X_train))
    model_temp = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    model_temp.fit(X_train, yt_train)

    logger.info("Treinando modelo de precipitação...")
    model_precip = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    model_precip.fit(X_train, yp_train)

    mae_temp  = mean_absolute_error(yt_test, model_temp.predict(X_test))
    rmse_temp = np.sqrt(mean_squared_error(yt_test, model_temp.predict(X_test)))
    mae_precip = mean_absolute_error(yp_test, model_precip.predict(X_test))

    logger.info("Temperatura → MAE: %.2f °C | RMSE: %.2f °C", mae_temp, rmse_temp)
    logger.info("Precipitação → MAE: %.2f mm", mae_precip)

    os.makedirs(MODELS_PATH, exist_ok=True)
    joblib.dump(model_temp,  os.path.join(MODELS_PATH, "modelo_temperatura.pkl"))
    joblib.dump(model_precip, os.path.join(MODELS_PATH, "modelo_precipitacao.pkl"))
    logger.info("Modelos salvos em %s", MODELS_PATH)
