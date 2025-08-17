from datetime import datetime, timezone
from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv
import json

# Load environment variables from .env
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_HOST = os.getenv("DB_HOST", "localhost")

if not all([DB_USER, DB_PASS, DB_NAME]):
    raise RuntimeError("❌ Database credentials are missing. Check your .env file.")

# Shared DB engine
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

def load_to_db(df: pd.DataFrame, table: str, source: str, schema: str = "core"):
    """
    Generic loader with upsert (date, indicator, region as PK).
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
    Logs ingestion attempts into ops.ingestion_log
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