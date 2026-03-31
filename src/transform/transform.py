import logging
import os

import psycopg2

logger = logging.getLogger(__name__)

POSTGRES_CONN = {
    'host': os.environ.get("POSTGRES_HOST", "postgres"),
    'port': int(os.environ.get("POSTGRES_PORT", 5432)),
    'database': os.environ.get("POSTGRES_DB", "weather_db"),
    'user': os.environ.get("POSTGRES_USER", "admin"),
    'password': os.environ.get("POSTGRES_PASSWORD", "admin"),
}

TRANSFORM_SQL = """
    INSERT INTO fact_weather (
        city, date, hour,
        temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m
    )
    SELECT
        city,
        DATE(time)                        AS date,
        EXTRACT(HOUR FROM time)::INT      AS hour,
        AVG(temperature_2m)               AS temperature_2m,
        AVG(relative_humidity_2m)         AS relative_humidity_2m,
        SUM(precipitation)                AS precipitation,
        AVG(wind_speed_10m)               AS wind_speed_10m
    FROM stg_weather
    WHERE execution_date = %s
    GROUP BY city, DATE(time), EXTRACT(HOUR FROM time)
    ON CONFLICT (city, date, hour) DO NOTHING;
"""


def transform_to_fact(execution_date: str, **kwargs) -> None:  # Fix: **kwargs para compat Airflow
    """Agrega dados da staging e insere na tabela fact (idempotente via ON CONFLICT)."""
    logger.info(f"Transformando dados para execution_date={execution_date}")

    conn = psycopg2.connect(**POSTGRES_CONN)
    try:
        with conn:  # Fix: context manager garante commit/rollback automático
            with conn.cursor() as cur:
                cur.execute(TRANSFORM_SQL, (execution_date,))
                logger.info(f"Transform concluído para {execution_date}. Rows afetadas: {cur.rowcount}")
    finally:
        conn.close()
