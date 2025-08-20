from datetime import date, timedelta
import requests
import pandas as pd
from etl_utils import load_to_db, log_ingestion

source = "OpenAQ"

def fetch_air_quality(city: str, start: str, end: str, page: int = 1, limit: int = 100) -> dict:
    url = "https://api.openaq.org/v2/measurements"
    params = {
        "city": city,
        "date_from": start,
        "date_to": end,
        "limit": limit,
        "page": page
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def normalize_air_quality(raw: dict, source: str) -> pd.DataFrame:
    results = raw.get("results", [])
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame([{
        "date": r["date"]["utc"][:10],
        "indicator": r["parameter"],
        "region": r["city"],
        "value": r["value"],
        "source": source,
        "meta": {"unit": r["unit"], "location": r["location"]}
    } for r in results])
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
            page = 1
            while True:
                raw = fetch_air_quality("Lagos", start=s.isoformat(), end=e.isoformat(), page=page)
                df = normalize_air_quality(raw, source)
                if df.empty:
                    break
                all_data.append(df)
                page += 1

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            load_to_db(final_df, table="airquality_daily", source=source, schema="core")
            log_ingestion(source, "success", len(final_df))
            print(f"✅ Air quality data loaded: {len(final_df)} rows")
        else:
            log_ingestion(source, "success", 0, "No data")
            print("⚠️ No air quality data found.")

    except Exception as e:
        log_ingestion(source, "fail", 0, str(e))
        raise