"""
Transforma dados do PostgreSQL em parquet enriquecido e envia ao MinIO.
Fix: credenciais via config centralizado; logging em vez de print.
"""
import io
import logging

import pandas as pd
from minio import Minio
from sqlalchemy import create_engine

from src.config import POSTGRES_URL, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
from src.utils.extract_and_transform import extract_and_transform

logger = logging.getLogger(__name__)


def tranform(**kwargs) -> None:  # mantém nome original para não quebrar a DAG
    logger.info("Lendo fact_weather...")
    engine = create_engine(POSTGRES_URL)
    df = pd.read_sql("SELECT * FROM fact_weather", engine)

    logger.info("Aplicando feature engineering...")
    df_transformed = extract_and_transform(df)

    buffer = io.BytesIO()
    df_transformed.to_parquet(buffer, index=False)
    buffer.seek(0)

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    client.put_object(
        MINIO_BUCKET,
        "processed/data.parquet",
        buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream",
    )
    logger.info("Parquet enviado ao MinIO: %s/processed/data.parquet", MINIO_BUCKET)
