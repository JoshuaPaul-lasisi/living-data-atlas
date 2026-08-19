import pandas as pd
import requests

from config import WORLDBANK_INDICATORS
from etl_utils import load_observations, log_ingestion

WORLD_BANK_API = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&per_page=20000"
SOURCE = "World Bank"


def fetch_worldbank(indicator: str, country: str = "NG") -> list:
    """Fetch raw indicator data directly from the World Bank API."""
    url = WORLD_BANK_API.format(country=country, indicator=indicator)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    if len(payload) < 2 or payload[1] is None:
        return []
    return payload[1]


def normalize(data: list, indicator_name: str) -> pd.DataFrame:
    """Transform raw World Bank JSON into core.observations rows (one per year)."""
    rows = []
    for d in data:
        if d["value"] is None:
            continue
        rows.append({
            "date": f"{d['date']}-01-01",
            "indicator": indicator_name,
            "region": d["country"]["id"],
            "value": float(d["value"]),
            "meta": {"wb_indicator_code": d["indicator"]["id"]},
        })
    return pd.DataFrame(rows)


def run(country: str = "NG") -> int:
    frames = []
    failures = []
    for wb_code, indicator_name in WORLDBANK_INDICATORS.items():
        try:
            raw = fetch_worldbank(wb_code, country=country)
            frames.append(normalize(raw, indicator_name))
        except Exception as e:
            failures.append(f"{wb_code}: {e}")

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    n = load_observations(df, source=SOURCE)

    if failures:
        log_ingestion(SOURCE, "partial" if n else "fail", n, "; ".join(failures)[:2000])
    else:
        log_ingestion(SOURCE, "success", n, f"{len(WORLDBANK_INDICATORS)} indicators loaded")

    return n


if __name__ == "__main__":
    count = run()
    print(f"World Bank: {count} rows upserted into core.observations")
