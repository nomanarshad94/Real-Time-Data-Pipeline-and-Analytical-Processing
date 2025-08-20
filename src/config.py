import os
from dotenv import load_dotenv
from pydantic.v1 import BaseSettings
import logging

load_dotenv()

class Config(BaseSettings):
    # Database Configuration
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "iot_sensor_db")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "default_password")
    DATABASE_URL: str = os.getenv("DATABASE_URL", f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:5432/{DB_NAME}")

    # Directory Paths
    PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR: str = os.path.join(PROJECT_ROOT, "data")
    QUARANTINE_DIR: str = os.path.join(PROJECT_ROOT, "quarantine")
    PROCESSED_DIR: str = os.path.join(PROJECT_ROOT, "processed")
    LOGS_DIR: str = os.path.join(PROJECT_ROOT, "logs")
    
    # Kaggle Dataset
    KAGGLE_DATASET: str = "ziya07/iot-based-environmental-dataset"
    
    # File Monitoring
    MONITORING_INTERVAL: int = 10  # seconds
    FILE_EXTENSIONS: list = [".csv"]
    
    # Data Validation Ranges for IoT Environmental Dataset
    TEMP_MIN = -50.0
    TEMP_MAX = 60.0
    HUMIDITY_MIN = 0.0
    HUMIDITY_MAX = 100.0
    AIR_QUALITY_MIN = 0
    AIR_QUALITY_MAX = 500
    NOISE_MIN = 0.0
    NOISE_MAX = 130.0
    LIGHTING_MIN = 0.0
    LIGHTING_MAX = 100000.0
    CROWD_DENSITY_MIN = 0
    CROWD_DENSITY_MAX = 1000
    STRESS_LEVEL_MIN = 0
    STRESS_LEVEL_MAX = 100
    SLEEP_HOURS_MIN = 0.0
    SLEEP_HOURS_MAX = 24.0
    MOOD_SCORE_MIN = 0.0
    MOOD_SCORE_MAX = 5.0

    # Pipeline Configuration

    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 5
    BATCH_SIZE = 1000

    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


# Global configuration instance
config = Config()

# Create directories if they don't exist
for directory in [config.DATA_DIR, config.QUARANTINE_DIR, config.PROCESSED_DIR, config.LOGS_DIR]:
    os.makedirs(directory, exist_ok=True)

# Logging will be configured by logger.py module
logging.info("Configuration loaded successfully.")