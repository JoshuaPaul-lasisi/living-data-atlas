import argparse
from datetime import date, timedelta

import pandas as pd

from config import CITIES
from etl_utils import http_session, load_observations, log_ingestion
from quality import flag_out_of_range

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
SOURCE = "Open-Meteo"
DAILY_VARS = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"]
SESSION = http_session()
MAX_CHUNK_DAYS = 31  # keep each request small per the original process-log note

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
    r = SESSION.get(ARCHIVE_URL, params=params, timeout=30)
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


def fetch_weather_range(lat: float, lon: float, start: date, end: date, region: str) -> pd.DataFrame:
    """Fetch and normalize a date range, splitting it into <= MAX_CHUNK_DAYS requests."""
    frames = []
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + timedelta(days=MAX_CHUNK_DAYS - 1), end)
        raw = fetch_weather(lat, lon, chunk_start.isoformat(), chunk_end.isoformat())
        frames.append(normalize(raw, region))
        chunk_start = chunk_end + timedelta(days=1)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def run(start: date = None, end: date = None, days_back: int = ROLLING_WINDOW_DAYS) -> int:
    """
    Default (no args): rolling window covering the last `days_back` days, for
    the scheduled daily run. Pass explicit start/end for a one-off backfill.
    """
    if end is None:
        end = date.today() - timedelta(days=1)
    if start is None:
        start = end - timedelta(days=days_back)

    frames = []
    failures = []
    for c in CITIES:
        try:
            frames.append(fetch_weather_range(c["lat"], c["lon"], start, end, c["region"]))
        except Exception as e:
            failures.append(f"{c['city']}: {e}")

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    n = load_observations(df, source=SOURCE)
    n_alerts = flag_out_of_range(df, source=SOURCE)

    note = f"{len(CITIES)} cities, {start} to {end}"
    if n_alerts:
        note += f"; {n_alerts} quality alerts raised"
    if failures:
        log_ingestion(SOURCE, "partial" if n else "fail", n, "; ".join(failures)[:2000])
    else:
        log_ingestion(SOURCE, "success", n, note)

    return n


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load weather data into core.observations")
    parser.add_argument("--days-back", type=int, default=ROLLING_WINDOW_DAYS,
                         help="Rolling window size for a normal (non-backfill) run")
    parser.add_argument("--start", type=date.fromisoformat, help="Backfill start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=date.fromisoformat, help="Backfill end date (YYYY-MM-DD)")
    args = parser.parse_args()

    count = run(start=args.start, end=args.end, days_back=args.days_back)
    print(f"Weather: {count} rows upserted into core.observations")
