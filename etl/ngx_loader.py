import pandas as pd

from etl_utils import http_session, load_observations, log_ingestion
from quality import flag_out_of_range

BASE_URL = "https://ngxpulse.ng"
SOURCE = "NGX Pulse"
SESSION = http_session()

# NGX Pulse's index-history endpoint is public (no key) for indices/ETFs.
# code -> our indicator name. Add more NGX index codes here as needed.
INDEX_CODES = {
    "ASI": "ngx_asi",
}


def fetch_index_history(code: str) -> dict:
    """Fetch the full daily history for one NGX index."""
    r = SESSION.get(f"{BASE_URL}/api/ngxdata/indices/{code}/history", timeout=30)
    r.raise_for_status()
    return r.json()


def normalize(data: dict, indicator_name: str) -> pd.DataFrame:
    """Transform NGX Pulse index-history JSON into core.observations rows."""
    if not data.get("success"):
        return pd.DataFrame()
    rows = []
    for point in data.get("history", []):
        if point.get("value") is None or not point.get("date"):
            continue
        rows.append({
            "date": point["date"],
            "indicator": indicator_name,
            "region": "NG",
            "value": float(point["value"]),
            "meta": {"ngx_index_code": data.get("code"), "ngx_index_name": data.get("name")},
        })
    return pd.DataFrame(rows)


def run() -> int:
    frames = []
    failures = []
    for code, indicator_name in INDEX_CODES.items():
        try:
            data = fetch_index_history(code)
            frames.append(normalize(data, indicator_name))
        except Exception as e:
            failures.append(f"{code}: {e}")

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    n = load_observations(df, source=SOURCE)
    n_alerts = flag_out_of_range(df, source=SOURCE)

    note = f"{len(INDEX_CODES)} indices"
    if n_alerts:
        note += f"; {n_alerts} quality alerts raised"
    if failures:
        log_ingestion(SOURCE, "partial" if n else "fail", n, "; ".join(failures)[:2000])
    else:
        log_ingestion(SOURCE, "success", n, note)

    return n


if __name__ == "__main__":
    count = run()
    print(f"NGX: {count} rows upserted into core.observations")
