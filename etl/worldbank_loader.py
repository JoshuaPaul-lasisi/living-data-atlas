import requests
import pandas as pd
from etl_utils import load_to_db, log_ingestion

# Base World Bank API template (REST endpoint)
WORLD_BANK_API = "http://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&per_page=10000"

def fetch_worldbank(country="NG", indicator="NY.GDP.MKTP.CD"):
    """
    Fetch raw indicator data directly from the World Bank API.

    Args:
        country (str): ISO country code. Defaults to Nigeria ("NG").
        indicator (str): Indicator code to pull. Defaults to GDP in current USD.

    Returns:
        list: JSON payload (second element of response) containing data points.

    Notes:
        - The API returns a list of yearly values with country metadata.
        - If request fails, this will raise an HTTPError.
    """
    url = WORLD_BANK_API.format(country=country, indicator=indicator)
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()[1]
    return data


def normalize(data, indicator_name):
    """
    Transform raw World Bank JSON into a structured DataFrame.

    Args:
        data (list): Raw data pulled from World Bank API.
        indicator_name (str): Short label for indicator (used in DB).

    Returns:
        pd.DataFrame: Cleaned data with standardized schema:
            - date: YYYY-01-01 (as string, to make it daily-compatible)
            - indicator: name we assign (e.g. "gdp_usd")
            - region: country code
            - value: float numeric value
            - meta: JSON with extra context (e.g. indicator ID)
    """
    rows = []
    for d in data:
        if d["value"] is not None:  # Skip missing years
            rows.append({
                "date": f"{d['date']}-01-01",  # fake daily granularity (January 1st)
                "indicator": indicator_name,
                "region": d["country"]["id"],
                "value": float(d["value"]),
                "meta": {"indicator": d["indicator"]["id"]}
            })
    return pd.DataFrame(rows)


if __name__ == "__main__":
    # Ingestion routine: fetch → normalize → load → log
    source = "World Bank"
    try:
        # Step 1: Pull GDP data for Nigeria
        raw = fetch_worldbank(country="NG", indicator="NY.GDP.MKTP.CD")
        
        # Step 2: Normalize into a tabular DataFrame
        df = normalize(raw, "gdp_usd")

        # Step 3: Push into Postgres (core.econ_daily)
        load_to_db(df, table="econ_daily", source=source, schema="core")

        # Step 4: Log the success
        log_ingestion(source, "success", len(df), "GDP loaded")

        print(f"✅ {source} data loaded into core.econ_daily")

    except Exception as e:
        # Catch errors, log as fail, and bubble it up
        log_ingestion(source, "fail", 0, str(e))
        raise