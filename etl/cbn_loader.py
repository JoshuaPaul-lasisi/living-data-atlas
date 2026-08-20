"""
CBN has no documented public JSON API -- this scrapes their official
exchange-rate page. Column names/structure aren't guaranteed stable; if this
loader starts failing, check the page manually and update _find_rate_table /
_find_col below. See docs/PROCESS.md for why this one is higher-risk than
the other loaders.
"""
from datetime import date

import pandas as pd

from etl_utils import http_session, load_observations, log_ingestion
from quality import flag_out_of_range

URL = "https://www.cbn.gov.ng/rates/ExchRateByCurrency.html"
SOURCE = "CBN"
SESSION = http_session()


def fetch_rate_tables() -> list:
    """Fetch and parse all HTML tables on the CBN exchange-rate page."""
    r = SESSION.get(URL, timeout=30)
    r.raise_for_status()
    return pd.read_html(r.text)


def _find_rate_table(tables: list) -> pd.DataFrame | None:
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if any("currency" in c for c in cols) and any("rate" in c for c in cols):
            return t
    return None


def _find_col(table: pd.DataFrame, *keywords: str) -> str | None:
    for c in table.columns:
        lc = str(c).strip().lower()
        if all(k in lc for k in keywords):
            return c
    return None


def normalize(tables: list) -> pd.DataFrame:
    """Extract the USD/NGN rate from the CBN rate table, if the expected shape is found."""
    table = _find_rate_table(tables)
    if table is None:
        return pd.DataFrame()

    currency_col = _find_col(table, "currency")
    rate_col = _find_col(table, "central") or _find_col(table, "rate")
    if currency_col is None or rate_col is None:
        return pd.DataFrame()

    usd_rows = table[table[currency_col].astype(str).str.contains(r"US DOLLAR|USD", case=False, regex=True, na=False)]
    if usd_rows.empty:
        return pd.DataFrame()

    value = pd.to_numeric(usd_rows.iloc[0][rate_col], errors="coerce")
    if pd.isna(value):
        return pd.DataFrame()

    date_col = _find_col(table, "date")
    rate_date = date.today().isoformat()
    if date_col is not None:
        try:
            rate_date = pd.to_datetime(usd_rows.iloc[0][date_col]).date().isoformat()
        except Exception:
            pass

    return pd.DataFrame([{
        "date": rate_date,
        "indicator": "cbn_fx_usd_ngn",
        "region": "NG",
        "value": float(value),
        "meta": {"currency_col": currency_col, "rate_col": rate_col},
    }])


def run() -> int:
    try:
        tables = fetch_rate_tables()
        df = normalize(tables)
    except Exception as e:
        log_ingestion(SOURCE, "fail", 0, str(e)[:2000])
        raise

    n = load_observations(df, source=SOURCE)
    n_alerts = flag_out_of_range(df, source=SOURCE)

    if df.empty:
        log_ingestion(SOURCE, "fail", 0,
                       "Could not locate a recognizable USD rate row -- CBN page structure may have changed")
    else:
        note = "USD/NGN rate loaded"
        if n_alerts:
            note += f"; {n_alerts} quality alerts raised"
        log_ingestion(SOURCE, "success", n, note)

    return n


if __name__ == "__main__":
    count = run()
    print(f"CBN: {count} rows upserted into core.observations")
