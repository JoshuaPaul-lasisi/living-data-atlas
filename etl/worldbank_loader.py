import os
import requests
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, text
from dotenv import load_dotenv
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import insert
import json


# Load environment variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- Step 1: Fetch Data from World Bank API ---
def fetch_indicator(indicator, country="NG"):
    """
    Fetch a World Bank indicator for Nigeria.
    Example indicators:
    - NY.GDP.MKTP.CD = GDP (current US$)
    - FP.CPI.TOTL.ZG = Inflation, consumer prices (% annual)
    - SL.UEM.TOTL.ZS = Unemployment (% of total labor force)
    """
    url = f"http://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&per_page=1000"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()[1]  # 0=metadata, 1=actual data
    return data

# --- Step 2: Normalize into DataFrame ---
def normalize(data, indicator_name):
    records = []
    for d in data:
        if d["value"] is not None:
            records.append({
                "date": f"{d['date']}-01-01",
                "indicator": indicator_name,
                "region": d["country"]["id"],
                "value": d["value"],
                "source": "World Bank",
                "meta": json.dumps({"indicator": d["indicator"]["id"]})
            })
    return pd.DataFrame(records)

# --- Step 3: Load into Postgres ---
metadata = MetaData(schema="core")
econ_daily = Table("econ_daily", metadata, autoload_with=engine)

def load_to_db(df, table=econ_daily):
    try:
        with engine.begin() as conn:
            for _, row in df.iterrows():
                stmt = insert(table).values(
                    date=row["date"],
                    indicator=row["indicator"],
                    region=row["region"],
                    value=row["value"],
                    source=row["source"],
                    meta=row["meta"]
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["date", "indicator", "region"],
                    set_={
                        "value": row["value"],
                        "source": row["source"],
                        "meta": row["meta"],
                        "updated_at": datetime.now(timezone.utc)
                    }
                )
                conn.execute(stmt)

            # ✅ success log
            conn.execute(
                text("INSERT INTO ops.ingestion_log (source, status, records, message) VALUES (:source, :status, :records, :message)"),
                {
                    "source": "World Bank",
                    "status": "success",
                    "records": len(df),
                    "message": "Upsert completed"
                }
            )
    except Exception as e:
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO ops.ingestion_log (source, status, records, message) VALUES (:source, :status, :records, :message)"),
                {
                    "source": "World Bank",
                    "status": "failed",
                    "records": 0,
                    "message": str(e)
                }
            )
        raise



if __name__ == "__main__":
    indicators = {
        "NY.GDP.MKTP.CD": "gdp_usd",
        "FP.CPI.TOTL.ZG": "inflation_yoy",
        "SL.UEM.TOTL.ZS": "unemployment_rate"
    }

    for wb_code, our_name in indicators.items():
        raw = fetch_indicator(wb_code)
        df = normalize(raw, our_name)
        load_to_db(df)

    print("✅ World Bank data loaded into core.econ_daily")