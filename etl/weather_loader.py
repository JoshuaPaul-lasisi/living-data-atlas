# etl/weather_loader.py
import requests
import pandas as pd
from datetime import datetime, timezone
from etl.etl_utils import load_to_db, log_ingestion

source = "Open-Meteo"

def fetch_weather(lat: float, lon: float, start: str, end: str):
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}&start_date={start}&end_date={end}"
        "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=UTC"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json().get("daily", {})

def normalize(data, region="NG"):
    dates = data.get("time", [])
    out = []

    for i, d in enumerate(dates):
        for metric in ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"]:
            out.append({
                "date": d,
                "indicator": metric,
                "region": region,
                "value": float(data[metric][i]) if data[metric][i] is not None else None,
                "meta": {"units": "°C" if "temperature" in metric else "mm"},
            })

    return pd.DataFrame(out)

if __name__ == "__main__":
    try:
        raw = fetch_weather(lat=9.05785, lon=7.49508, start="2024-01-01", end="2024-12-31")  # Abuja coords
        df = normalize(raw, "NG")
        load_to_db(df, table="weather_daily", source=source)
        log_ingestion(source, "ok", len(df))
        print("✅ Weather data loaded")
    except Exception as e:
        log_ingestion(source, "fail", 0, str(e))
        raise