"""Lightweight data-quality checks: flag implausible values into core.alerts."""
import json

import pandas as pd
from sqlalchemy import text

from config import QUALITY_BOUNDS
from etl_utils import engine


def flag_out_of_range(df: pd.DataFrame, source: str) -> int:
    """
    Check df rows (date, indicator, region, value) against QUALITY_BOUNDS and
    write one core.alerts row per violation. Returns the number of alerts raised.
    """
    if df.empty:
        return 0

    violations = []
    for _, row in df.iterrows():
        bounds = QUALITY_BOUNDS.get(row["indicator"])
        value = row["value"]
        if bounds is None or value is None or pd.isna(value):
            continue
        low, high = bounds
        if value < low or value > high:
            violations.append({
                "signal": "out_of_range",
                "severity": "warning",
                "details": json.dumps({
                    "indicator": row["indicator"],
                    "region": row["region"],
                    "date": str(row["date"]),
                    "value": float(value),
                    "expected_range": [low, high],
                    "source": source,
                }),
            })

    if not violations:
        return 0

    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO core.alerts (signal, severity, details) VALUES (:signal, :severity, :details)"),
            violations,
        )
    return len(violations)
