"""Unit tests for the anomaly-detection logic.

These tests target the pure-function pieces that don't need Postgres.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from src.analysis.detect_anomalies import _compute_anomalies


def _flat_series(n: int = 200, base: float = 100.0) -> pd.DataFrame:
    """Build a perfectly flat price series with one obvious spike at the end."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = []
    rng = np.random.default_rng(0)
    for i in range(n):
        # Tiny noise to give std a non-zero value.
        price = base + rng.normal(0, 0.01)
        rows.append({"open_time": start + timedelta(minutes=i), "close_price": price})
    # Inject a 5% jump at the last point.
    rows[-1]["close_price"] = base * 1.05
    return pd.DataFrame(rows)


def test_obvious_spike_is_flagged():
    df = _flat_series()
    flagged = _compute_anomalies(df, window=60, threshold=2.5)
    assert not flagged.empty
    # The injected spike is the very last row.
    assert flagged.iloc[-1].open_time == df.iloc[-1].open_time
    assert flagged.iloc[-1].direction == "up"


def test_calm_series_has_no_anomalies():
    rng = np.random.default_rng(1)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    base = 100.0
    rows = []
    for i in range(300):
        # Small symmetric noise; no return should be 2.5+ std away.
        price = base + rng.normal(0, 0.1)
        rows.append({"open_time": start + timedelta(minutes=i), "close_price": price})
    df = pd.DataFrame(rows)
    flagged = _compute_anomalies(df, window=60, threshold=4.0)
    assert flagged.empty


def test_too_short_history_returns_empty():
    df = _flat_series(n=10)
    flagged = _compute_anomalies(df, window=60, threshold=2.5)
    assert flagged.empty
