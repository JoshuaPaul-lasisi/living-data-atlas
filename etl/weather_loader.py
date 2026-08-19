from datetime import date, timedelta

import pandas as pd
import requests

from config import CITIES
from etl_utils import load_observations, log_ingestion

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
SOURCE = "Open-Meteo"
DAILY_VARS = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"]

# Open-Meteo's archive has a short reporting lag; pulling the last N days on
# every run (rather than just "yesterday") means a late-arriving day still
# gets picked up on the next scheduled run, via upsert.
ROLLING_WINDOW_DAYS = 10


def fetch_weather(lat: float, lon: float, start: str, end: str) -> dict:
    """Fetch historical daily weather data from the Open-Meteo archive API."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "daily": DAILY_VARS,
        "timezone": "UTC",
    }
    r = requests.get(ARCHIVE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def normalize(raw: dict, region: str) -> pd.DataFrame:
    """Normalize Open-Meteo weather JSON into core.observations rows."""
    daily = raw.get("daily", {})
    dates = daily.get("time", [])
    rows = []
    for i, d in enumerate(dates):
        for var in DAILY_VARS:
            values = daily.get(var, [])
            if i >= len(values) or values[i] is None:
                continue
            indicator = {
                "temperature_2m_max": "temp_max_c",
                "temperature_2m_min": "temp_min_c",
                "precipitation_sum": "precip_mm",
            }[var]
            rows.append({
                "date": d,
                "indicator": indicator,
                "region": region,
                "value": values[i],
                "meta": {"source_var": var},
            })
    return pd.DataFrame(rows)


def run(days_back: int = ROLLING_WINDOW_DAYS) -> int:
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days_back)

    frames = []
    failures = []
    for c in CITIES:
        try:
            raw = fetch_weather(c["lat"], c["lon"], start.isoformat(), end.isoformat())
            frames.append(normalize(raw, c["region"]))
        except Exception as e:
            failures.append(f"{c['city']}: {e}")

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    n = load_observations(df, source=SOURCE)

    if failures:
        log_ingestion(SOURCE, "partial" if n else "fail", n, "; ".join(failures)[:2000])
    else:
        log_ingestion(SOURCE, "success", n, f"{len(CITIES)} cities, {start} to {end}")

    return n


if __name__ == "__main__":
    count = run()
    print(f"Weather: {count} rows upserted into core.observations")
