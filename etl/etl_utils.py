from datetime import datetime, timezone
from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv
import json
from pathlib import Path

# Load environment variables from .env
BASE_DIR = Path(__file__).resolve().parent.parent
dotenv_path = BASE_DIR/".env"
load_dotenv(dotenv_path=dotenv_path)

# Read DB connection params
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_PORT = os.getenv("POSTGRES_PORT", "5433") 
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")

if not all([DB_USER, DB_PASS, DB_NAME]):
    # Fail early if credentials are missing
    raise RuntimeError("❌ Database credentials are missing. Check your .env file.")

# Shared SQLAlchemy engine (Postgres)
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Centralized access to API keys
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")


def load_to_db(df: pd.DataFrame, table: str, source: str, schema: str = "core"):
    """
    Insert a DataFrame into a Postgres table with upsert behavior.

    Args:
        df (pd.DataFrame): Data to insert. Must contain:
            - date, indicator, region, value, meta
        table (str): Target table name.
        source (str): Source label (used in table + logs).
        schema (str): Target schema. Defaults to "core".

    Notes:
        - Uses `ON CONFLICT` to update existing rows (deduplication).
        - Automatically updates the `updated_at` timestamp.
    """
    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = text(f"""
                INSERT INTO {schema}.{table} (date, indicator, region, value, source, meta)
                VALUES (:date, :indicator, :region, :value, :source, :meta)
                ON CONFLICT (date, indicator, region) DO UPDATE
                SET value = EXCLUDED.value,
                    source = EXCLUDED.source,
                    meta = EXCLUDED.meta,
                    updated_at = :updated_at
            """)
            conn.execute(stmt, {
                "date": row["date"],
                "indicator": row["indicator"],
                "region": row["region"],
                "value": row["value"],
                "source": source,
                "meta": json.dumps(row.get("meta", {})),
                "updated_at": datetime.now(timezone.utc)
            })


def log_ingestion(source: str, status: str, records: int, message: str = ""):
    """
    Log the outcome of a data ingestion run into `ops.ingestion_log`.

    Args:
        source (str): Data source name (e.g. "World Bank").
        status (str): "success" or "fail".
        records (int): Number of records processed.
        message (str): Optional extra details.

    Notes:
        - Helps track pipeline runs across different data sources.
        - `ops.ingestion_log` should exist with proper schema.
    """
    with engine.begin() as conn:
        stmt = text("""
            INSERT INTO ops.ingestion_log (source, status, records, message)
            VALUES (:source, :status, :records, :message)
        """)
        conn.execute(stmt, {
            "source": source,
            "status": status,
            "records": records,
            "message": message
        })