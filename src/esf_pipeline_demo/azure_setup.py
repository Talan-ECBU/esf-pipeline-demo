# run_init.py
"""Script to initialize the database by creating necessary tables."""

from logging import getLogger
from pathlib import Path

import pyodbc

from .config.config import (
    AZURE_SQL_DATABASE,
    AZURE_SQL_DRIVER,
    AZURE_SQL_PASSWORD,
    AZURE_SQL_SERVER,
    AZURE_SQL_USERNAME,
)

logger = getLogger(__name__)


def run_sql_script(cursor, path: Path):
    with open(path) as f:
        sql = f.read()
    for stmt in sql.split("GO"):
        _stmt = stmt.strip()
        if _stmt:
            cursor.execute(_stmt)


def setup_azure():
    package_root = Path(__file__).resolve().parents[1]
    sql_path = package_root / "sql" / "init_tables.sql"

    if not sql_path.exists():
        logger.error(f"Could not find {sql_path}")
        return

    conn_str = (
        f"DRIVER={AZURE_SQL_DRIVER};"
        f"SERVER={AZURE_SQL_SERVER};"
        f"DATABASE={AZURE_SQL_DATABASE};"
        f"UID={AZURE_SQL_USERNAME};"
        f"PWD={AZURE_SQL_PASSWORD}"
    )

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    logger.info(f"Running SQL script: {sql_path}")
    run_sql_script(cursor, sql_path)
    conn.commit()
    conn.close()
    logger.info("Tables created (if they did not already exist).")
