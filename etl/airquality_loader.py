import os
import requests
import pandas as pd
from etl_utils import load_to_db, log_ingestion, OPENAQ_API_KEY
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SOURCE = "OpenAQ"
BASE_URL = "https://api.openaq.org/v3"

print("Using API key:", os.getenv("OPENAQ_API_KEY"))


def fetch_locations(lat, lon, distance=10000, limit=100):
    """
    Fetch all monitoring stations near given coordinates.
    """
    headers = {"X-API-Key": OPENAQ_API_KEY}
    params = {"coordinates": f"{lat},{lon}", "distance": distance, "limit": limit}
    r = requests.get(f"{BASE_URL}/locations", params=params, headers=headers)
    r.raise_for_status()
    return r.json().get("results", [])


def fetch_measurements(location_id, limit=100):
    """
    Fetch paginated measurements for a given location ID.
    """
    headers = {"X-API-Key": OPENAQ_API_KEY}
    all_results, page = [], 1

    while True:
        params = {"location_id": location_id, "limit": limit, "page": page}
        r = requests.get(f"{BASE_URL}/measurements", params=params, headers=headers)
        r.raise_for_status()
        data = r.json().get("results", [])
        if not data:
            break

        all_results.extend(data)
        if len(data) < limit:  # last page
            break
        page += 1

    return all_results


def normalize_measurements(raw: list, region="NG-LAG"):
    """
    Normalize OpenAQ measurements into rows for DB.
    Each pollutant becomes a row: (date, indicator, value, region, meta).
    """
    rows = []
    for m in raw:
        rows.append({
            "date": m.get("date", {}).get("utc", "").split("T")[0],
            "indicator": m.get("parameter"),
            "region": region,
            "value": m.get("value"),
            "meta": {
                "unit": m.get("unit"),
                "location": m.get("location", {}).get("name"),
                "coords": m.get("coordinates", {})
            }
        })
    return pd.DataFrame(rows)


if __name__ == "__main__":
    try:
        # Step 1: Find all monitoring stations near Lagos
        locations = fetch_locations(6.5244, 3.3792)  # Lagos coords
        print(f"📍 Found {len(locations)} monitoring stations near Lagos")

        # Step 2: Fetch measurements for each station
        all_measurements = []
        for loc in locations:
            loc_id = loc["id"]
            print(f"⏳ Fetching measurements for station {loc_id} ({loc['name']})")
            ms = fetch_measurements(loc_id)
            all_measurements.extend(ms)

        # Step 3: Normalize
        df = normalize_measurements(all_measurements, region="NG-LAG")

        # Step 4: Load into DB
        if not df.empty:
            load_to_db(df, table="air_quality_daily", source=SOURCE, schema="core")
            log_ingestion(SOURCE, "success", len(df), "Air quality data ingested")
            print(f"✅ {len(df)} air quality records loaded")
        else:
            log_ingestion(SOURCE, "empty", 0, "No data returned")
            print("⚠️ No air quality data returned")

    except Exception as e:
        log_ingestion(SOURCE, "failure", 0, str(e))
        raise