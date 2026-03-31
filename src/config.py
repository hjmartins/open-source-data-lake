"""
Configurações centralizadas do pipeline.
"""
import os

# --- Cidades monitoradas ---
CITIES = [
    {"name": "New_York",     "latitude": 40.7128,  "longitude": -74.0060},
    {"name": "Los_Angeles",  "latitude": 34.0522,  "longitude": -118.2437},
    {"name": "Chicago",      "latitude": 41.8781,  "longitude": -87.6298},
]

# --- Paths ---
RAW_BASE_PATH      = os.environ.get("RAW_BASE_PATH", "/opt/airflow/data/raw/weather")
PROCESSED_BASE_PATH = os.environ.get("PROCESSED_BASE_PATH", "/opt/airflow/data/processed")
ANALYTICS_BASE_PATH = os.environ.get("ANALYTICS_BASE_PATH", "/opt/airflow/data/analytics")
MODELS_PATH        = os.environ.get("MODELS_PATH", "/opt/airflow/data/models")

# ---banco ---
POSTGRES_CONN = {
    "host":     os.environ.get("POSTGRES_HOST",     "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "database": os.environ.get("POSTGRES_DB",       "weather_db"),
    "user":     os.environ.get("POSTGRES_USER",     "admin"),
    "password": os.environ.get("POSTGRES_PASSWORD", "admin"),
}

POSTGRES_URL = (
    f"postgresql+psycopg2://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}"
    f"@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['database']}"
)

# --- MinIO ---
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "admin123")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET",     "weather-data")

# --- Open-Meteo ---
ARCHIVE_API_URL  = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
]
