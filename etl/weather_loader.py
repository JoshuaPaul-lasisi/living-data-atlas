import requests
import pandas as pd
from datetime import date, timedelta
from etl_utils import load_to_db, log_ingestion

SOURCE = "Open-Meteo"

def fetch_weather(lat: float, lon: float, start: str, end: str) -> dict:
    """
    Fetch historical daily weather data from Open-Meteo archive API.
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "daily": ["temperature_2m_max","temperature_2m_min","precipitation_sum"],
        "timezone": "UTC",
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def normalize(raw: dict, region: str) -> pd.DataFrame:
    """
    Normalize Open-Meteo weather JSON into our econ_daily-like format.
    """
    dates = raw["daily"]["time"]
    records = []

    for i, d in enumerate(dates):
        records.append({
            "date": d,
            "indicator": "temp_max_c",
            "region": region,
            "value": raw["daily"]["temperature_2m_max"][i],
            "meta": {"source_var": "temperature_2m_max"}
        })
        records.append({
            "date": d,
            "indicator": "temp_min_c",
            "region": region,
            "value": raw["daily"]["temperature_2m_min"][i],
            "meta": {"source_var": "temperature_2m_min"}
        })
        records.append({
            "date": d,
            "indicator": "precip_mm",
            "region": region,
            "value": raw["daily"]["precipitation_sum"][i],
            "meta": {"source_var": "precipitation_sum"}
        })

    return pd.DataFrame(records)

if __name__ == "__main__":
    # Pull for Jan 2024 Abuja (lat/lon)
    start = date(2024, 1, 1)
    end = date(2024, 1, 31)
    raw = fetch_weather(lat=9.05785, lon=7.49508, start=start.isoformat(), end=end.isoformat())
    df = normalize(raw, "NG-ABJ")

    try:
        load_to_db(df, table="weather_daily", source=SOURCE, schema="core")
        log_ingestion(SOURCE, "success", len(df))
        print(f"✅ Weather data loaded ({len(df)} rows)")
    except Exception as e:
        log_ingestion(SOURCE, "failed", len(df), str(e))
        raise