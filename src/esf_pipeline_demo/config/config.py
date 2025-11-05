# src/esf_pipeline/config/config.py

import logging.config
import os
from datetime import datetime
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Pathing and environment variables
PROJECT_ROOT = Path(__file__).parents[3]
load_dotenv(PROJECT_ROOT / ".env")

SRC_DIR = PROJECT_ROOT / "src"
DATA_DIR = PROJECT_ROOT / "data"
PACKAGE_DIR = Path(__file__).parents[1]
CONFIG_DIR = Path(__file__).parent

LOCAL_RAW_DIR = DATA_DIR / "raw"
LOCAL_PROCESSED_DIR = DATA_DIR / "processed"
LOCAL_MODEL_DIR = DATA_DIR / "model"
LOCAL_RESULTS_DIR = DATA_DIR / "results"

with open(PROJECT_ROOT / "data_schema.yaml") as f:
    DATA_SCHEMA = yaml.safe_load(f)

with open(PROJECT_ROOT / "recall.yaml") as f:
    RECALL_REF = yaml.safe_load(f)

# Azure Blob Storage
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "esfonlinesafety")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY", "<your-storage-account-key>")
AZURE_RAW_CONTAINER = "raw-scraped"
AZURE_PROCESSED_CONTAINER = "processed"
AZURE_MODELS_CONTAINER = "models-training"

# Azure SQL Database
AZURE_SQL_SERVER = os.getenv(
    "AZURE_SQL_SERVER", "esfonlinesafety-sqlserver-001.database.windows.net"
)
AZURE_SQL_DATABASE = os.getenv("AZURE_SQL_DATABASE", "esfmetadata")
AZURE_SQL_USERNAME = os.getenv("AZURE_SQL_USERNAME", "sqladmin")
AZURE_SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD", "<you-sql-password>")
AZURE_SQL_DRIVER = os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 17 for SQL Server")

# Scraping Settings
USERNAME = os.getenv("OYX_USERNAME")
PASSWORD = os.getenv("OYX_PASSWORD")

MAX_PRODUCTS = 200
MAX_NON_RENDER_WORKERS = 50
MAX_RENDER_WORKERS = 15

SCRAPE_DATE = os.getenv("ESF_SCRAPE_DATE", "")
if not SCRAPE_DATE:
    SCRAPE_DATE = datetime.now().strftime("%Y-%m-%d")


def setup_logging():
    """Load logging configuration from YAML file."""
    with open(CONFIG_DIR / "logging_config.yaml") as f:
        config = yaml.safe_load(f)
    current_date = datetime.now().strftime("%Y%m%d")

    for _, handler_config in config.get("handlers", {}).items():
        if "filename" in handler_config:
            filename = handler_config["filename"]
            if not Path(filename).is_absolute():
                log_path = Path(filename)
                date_based_path = log_path.parent / current_date / log_path.name
                full_path = PROJECT_ROOT / date_based_path
                full_path.parent.mkdir(parents=True, exist_ok=True)
                handler_config["filename"] = str(full_path)

    logging.config.dictConfig(config)
