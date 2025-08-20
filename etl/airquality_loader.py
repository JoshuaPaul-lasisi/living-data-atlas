import requests
import pandas as pd
from datetime import date
from etl_utils import load_to_db, log_ingestion

SOURCE = "OpenAQ"

def fetch_air_quality(lat: float, lon: float, radius: int = 10000, limit: int = 100, page: int = 1) -> dict:
    """
    Fetch latest air quality measurements near given coordinates.
    """
    url = "https://api.openaq.org/v3/latest"
    params = {
        "coordinates": f"{lat},{lon}",
        "radius": radius, 
        "limit": limit,
        "page": page,
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def normalize(raw: dict, region: str) -> pd.DataFrame:
    """
    Normalize OpenAQ 'latest' JSON into our standard format.
    """
    records = []

    for loc in raw.get("results", []):
        for m in loc.get("measurements", []):
            records.append({
                "date": m["lastUpdated"][:10],   # just YYYY-MM-DD
                "indicator": m["parameter"],
                "region": region,
                "value": m["value"],
                "meta": {
                    "unit": m["unit"],
                    "location": loc.get("location"),
                    "coordinates": loc.get("coordinates")
                }
            })

    return pd.DataFrame(records)

if __name__ == "__main__":
    raw = fetch_air_quality(lat=6.5244, lon=3.3792)
    df = normalize(raw, "NG-LAG")

    try:
        load_to_db(df, table="airquality_daily", source=SOURCE, schema="core")
        log_ingestion(SOURCE, "success", len(df))
        print(f"✅ Air quality data loaded ({len(df)} rows)")
    except Exception as e:
        log_ingestion(SOURCE, "failed", len(df), str(e))
        raise