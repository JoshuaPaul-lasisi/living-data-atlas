import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from sqlalchemy import create_engine, text
from urllib3.util import Retry

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    # Neon (or any Postgres) connection string; force the psycopg2 driver.
    if DATABASE_URL.startswith("postgresql://"):
        DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)
else:
    # Local docker-compose fallback for dev without touching the cloud DB.
    db_user = os.getenv("POSTGRES_USER")
    db_pass = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5433")
    if not all([db_user, db_pass, db_name]):
        raise RuntimeError(
            "Set DATABASE_URL (Neon) or POSTGRES_USER/PASSWORD/DB (local dev) in .env"
        )
    DATABASE_URL = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
NGXPULSE_API_KEY = os.getenv("NGXPULSE_API_KEY")


def http_session() -> requests.Session:
    """
    Shared requests.Session with retry/backoff for transient failures
    (rate limits, 5xx, connection resets) so a flaky upstream API doesn't
    kill an entire loader run.
    """
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


UPSERT_OBSERVATIONS = text("""
    INSERT INTO core.observations (date, indicator, region, value, source, meta, updated_at)
    VALUES (:date, :indicator, :region, :value, :source, :meta, :updated_at)
    ON CONFLICT (date, indicator, region, source) DO UPDATE
    SET value = EXCLUDED.value,
        meta = EXCLUDED.meta,
        updated_at = EXCLUDED.updated_at
""")


def load_observations(df: pd.DataFrame, source: str) -> int:
    """
    Upsert rows into core.observations, the single fact table every loader writes to.

    Args:
        df: must contain columns date, indicator, region, value, and optionally meta (dict).
        source: label identifying the loader/API this data came from.

    Returns:
        Number of rows upserted.
    """
    if df.empty:
        return 0

    now = datetime.now(timezone.utc)
    records = [
        {
            "date": row["date"],
            "indicator": row["indicator"],
            "region": row["region"],
            "value": None if pd.isna(row["value"]) else float(row["value"]),
            "source": source,
            "meta": json.dumps(row.get("meta") or {}),
            "updated_at": now,
        }
        for _, row in df.iterrows()
    ]

    with engine.begin() as conn:
        conn.execute(UPSERT_OBSERVATIONS, records)

    return len(records)


def log_ingestion(source: str, status: str, records: int, message: str = ""):
    """Record the outcome of an ingestion run into ops.ingestion_log."""
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO ops.ingestion_log (source, status, records, message)
                VALUES (:source, :status, :records, :message)
            """),
            {"source": source, "status": status, "records": records, "message": message[:2000]},
        )
