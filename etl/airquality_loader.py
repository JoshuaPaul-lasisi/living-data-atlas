import time
from datetime import datetime, timezone

import pandas as pd
import requests

from config import CITIES, OPENAQ_RADIUS_M
from etl_utils import OPENAQ_API_KEY, load_observations, log_ingestion

BASE_URL = "https://api.openaq.org/v3"
SOURCE = "OpenAQ"
REQUEST_PAUSE_S = 0.3  # be polite to the free-tier rate limit


def _headers():
    return {"X-API-Key": OPENAQ_API_KEY}


def fetch_locations(lat: float, lon: float, radius: int = OPENAQ_RADIUS_M, limit: int = 50) -> list:
    """Fetch monitoring stations near given coordinates, following pagination."""
    results, page = [], 1
    while True:
        params = {"coordinates": f"{lat},{lon}", "radius": radius, "limit": limit, "page": page}
        r = requests.get(f"{BASE_URL}/locations", params=params, headers=_headers(), timeout=30)
        r.raise_for_status()
        batch = r.json().get("results", [])
        results.extend(batch)
        if len(batch) < limit:
            break
        page += 1
        time.sleep(REQUEST_PAUSE_S)
    return results


def fetch_latest(location_id: int) -> list:
    """Fetch the latest reading per sensor for a given location."""
    r = requests.get(f"{BASE_URL}/locations/{location_id}/latest", headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])


def normalize_location(location: dict, latest: list, region: str) -> pd.DataFrame:
    """
    Normalize one location's latest sensor readings into core.observations rows.
    Each location's `sensors` list maps sensor id -> parameter, which `latest`
    readings reference only by sensorsId.
    """
    sensor_meta = {
        s["id"]: {"parameter": s.get("parameter", {}).get("name"), "unit": s.get("parameter", {}).get("units")}
        for s in location.get("sensors", [])
    }

    rows = []
    for reading in latest:
        sensor_id = reading.get("sensorsId")
        meta = sensor_meta.get(sensor_id, {})
        parameter = meta.get("parameter")
        value = reading.get("value")
        dt = reading.get("datetime", {}).get("utc")
        if not parameter or value is None or not dt:
            continue
        rows.append({
            "date": dt.split("T")[0],
            "indicator": parameter,
            "region": region,
            "value": value,
            "meta": {
                "unit": meta.get("unit"),
                "location": location.get("name"),
                "location_id": location.get("id"),
                "sensor_id": sensor_id,
            },
        })
    return pd.DataFrame(rows)


def run() -> int:
    frames = []
    failures = []

    for c in CITIES:
        try:
            locations = fetch_locations(c["lat"], c["lon"])
        except Exception as e:
            failures.append(f"{c['city']} (locations): {e}")
            continue

        for loc in locations:
            try:
                latest = fetch_latest(loc["id"])
                frames.append(normalize_location(loc, latest, c["region"]))
                time.sleep(REQUEST_PAUSE_S)
            except Exception as e:
                failures.append(f"{c['city']}/{loc.get('name')}: {e}")

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    n = load_observations(df, source=SOURCE)

    if failures:
        log_ingestion(SOURCE, "partial" if n else "fail", n, "; ".join(failures)[:2000])
    else:
        log_ingestion(SOURCE, "success", n, f"{len(CITIES)} cities scanned")

    return n


if __name__ == "__main__":
    count = run()
    print(f"Air quality: {count} rows upserted into core.observations")
