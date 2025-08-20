# etl/airquality_loader.py
import requests
import pandas as pd
from datetime import datetime, timezone
from etl.etl_utils import load_to_db, log_ingestion

source = "OpenAQ"

def fetch_air_quality(city="Lagos", limit=100):
    url = f"https://api.openaq.org/v2/measurements?city={city}&limit={limit}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])

def normalize(rows, region="NG"):
    out = []
    for r in rows:
        out.append({
            "date": r["date"]["utc"].split("T")[0],
            "indicator": r["parameter"],   # e.g. "pm25", "pm10", "no2"
            "region": region,
            "value": float(r["value"]),
            "meta": {"unit": r["unit"], "location": r.get("location")},
        })
    return pd.DataFrame(out)

if __name__ == "__main__":
    try:
        raw = fetch_air_quality(city="Lagos", limit=500)
        df = normalize(raw, "NG")
        load_to_db(df, table="air_quality_daily", source=source)
        log_ingestion(source, "ok", len(df))
        print("✅ Air Quality data loaded")
    except Exception as e:
        log_ingestion(source, "fail", 0, str(e))
        raise