from datetime import date, timedelta
import requests
import pandas as pd
from etl_utils import load_to_db, log_ingestion

source = "Open-Meteo"

def fetch_weather(lat: float, lon: float, start: str, end: str) -> dict:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
        "timezone": "UTC",
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def normalize_weather(raw: dict, source: str) -> pd.DataFrame:
    daily = raw["daily"]
    df = pd.DataFrame({
        "date": daily["time"],
        "temp_max": daily["temperature_2m_max"],
        "temp_min": daily["temperature_2m_min"],
        "precipitation": daily["precipitation_sum"]
    })
    df = df.melt(id_vars=["date"], var_name="indicator", value_name="value")
    df["region"] = "Abuja"
    df["source"] = source
    df["meta"] = None
    return df

def month_chunks(start: date, end: date):
    """Yield month ranges between two dates."""
    current = start
    while current <= end:
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        yield current, min(next_month - timedelta(days=1), end)
        current = next_month

if __name__ == "__main__":
    try:
        all_data = []
        for s, e in month_chunks(date(2024,1,1), date(2024,12,31)):
            raw = fetch_weather(lat=9.05785, lon=7.49508, start=s.isoformat(), end=e.isoformat())
            df = normalize_weather(raw, source)
            all_data.append(df)

        final_df = pd.concat(all_data, ignore_index=True)
        load_to_db(final_df, table="weather_daily", source=source, schema="core")
        log_ingestion(source, "success", len(final_df))
        print(f"✅ Weather data loaded: {len(final_df)} rows")

    except Exception as e:
        log_ingestion(source, "fail", 0, str(e))
        raise