import requests
import pandas as pd
from etl_utils import load_to_db, log_ingestion

WORLD_BANK_API = "http://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&per_page=10000"

def fetch_worldbank(country="NG", indicator="NY.GDP.MKTP.CD"):
    url = WORLD_BANK_API.format(country=country, indicator=indicator)
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()[1]  # World Bank API returns [metadata, data]
    return data

def normalize(data, indicator_name):
    rows = []
    for d in data:
        if d["value"] is not None:
            rows.append({
                "date": f"{d['date']}-01-01",
                "indicator": indicator_name,
                "region": d["country"]["id"],
                "value": float(d["value"]),
                "meta": {"indicator": d["indicator"]["id"]}
            })
    return pd.DataFrame(rows)

if __name__ == "__main__":
    source = "World Bank"
    try:
        raw = fetch_worldbank(country="NG", indicator="NY.GDP.MKTP.CD")
        df = normalize(raw, "gdp_usd")

        load_to_db(df, table="econ_daily", source=source, schema="core")
        log_ingestion(source, "success", len(df), "GDP loaded")

        print(f"✅ {source} data loaded into core.econ_daily")

    except Exception as e:
        log_ingestion(source, "fail", 0, str(e))
        raise