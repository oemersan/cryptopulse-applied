"""Fetch minute-level OHLCV candles for crypto pairs.

Two modes:
  - live   : pull from Binance public REST API
  - sample : read from data/sample/prices.csv (offline / demo fallback)

Run from inside a container, e.g.:
  docker compose run --rm pipeline python -m src.ingestion.ingest_prices
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import text

from src.common.config import settings
from src.common.db import get_engine
from src.common.es_index import INDEX_PRICES, bulk_index, ensure_indices

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

# Airflow mounts ./data at /opt/airflow/data; pipeline container at /app/data.
_DATA_ROOTS = [Path("/opt/airflow/data"), Path("/app/data")]
SAMPLE_FILE = next(
    (r / "sample/prices.csv" for r in _DATA_ROOTS if r.exists()),
    Path("/app/data/sample/prices.csv"),
)


def _fetch_live(symbol: str, limit: int = 1000) -> pd.DataFrame:
    """Fetch the most recent `limit` 1-minute candles for `symbol`."""
    log.info("Fetching live candles for %s (limit=%d)", symbol, limit)
    response = requests.get(
        BINANCE_KLINES_URL,
        params={"symbol": symbol, "interval": "1m", "limit": limit},
        timeout=15,
    )
    response.raise_for_status()
    raw = response.json()
    df = pd.DataFrame(
        raw,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "n_trades",
            "taker_buy_base", "taker_buy_quote", "ignore",
        ],
    )
    df["symbol"] = symbol
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = df[col].astype(float)
    return df[["symbol", "open_time", "open", "high", "low", "close", "volume"]]


def _read_sample(symbol: str) -> pd.DataFrame:
    if not SAMPLE_FILE.exists():
        raise FileNotFoundError(
            f"Sample file not found: {SAMPLE_FILE}. "
            "Run the sample-data generator or switch DATA_MODE to 'live'."
        )
    df = pd.read_csv(SAMPLE_FILE, parse_dates=["open_time"])
    df = df[df["symbol"] == symbol].copy()
    if df.empty:
        log.warning("No sample rows for %s", symbol)
    return df


def _upsert(df: pd.DataFrame) -> int:
    """Insert rows; ignore duplicates on (symbol, open_time)."""
    if df.empty:
        return 0
    rows = df.rename(
        columns={
            "open": "open_price",
            "high": "high_price",
            "low": "low_price",
            "close": "close_price",
        }
    ).to_dict(orient="records")

    sql = text(
        """
        INSERT INTO prices
            (symbol, open_time, open_price, high_price, low_price, close_price, volume)
        VALUES
            (:symbol, :open_time, :open_price, :high_price, :low_price, :close_price, :volume)
        ON CONFLICT (symbol, open_time) DO NOTHING
        """
    )
    with get_engine().begin() as conn:
        result = conn.execute(sql, rows)
        return result.rowcount or 0


def _index_es(df: pd.DataFrame) -> int:
    """Mirror prices into Elasticsearch so Kibana can chart them."""
    if df.empty:
        return 0
    docs = df.rename(
        columns={
            "open": "open_price",
            "high": "high_price",
            "low": "low_price",
            "close": "close_price",
        }
    ).to_dict(orient="records")
    # Stable id = symbol + timestamp -> idempotent re-runs.
    for d in docs:
        d["_id"] = f"{d['symbol']}_{int(d['open_time'].timestamp())}"
    return bulk_index(INDEX_PRICES, docs, id_field="_id")


def run() -> None:
    cfg = settings()
    log.info("ingest_prices starting in %s mode for %s", cfg.data_mode, cfg.crypto_symbols)
    ensure_indices()
    total_pg = 0
    total_es = 0
    for symbol in cfg.crypto_symbols:
        try:
            if cfg.data_mode == "sample":
                df = _read_sample(symbol)
            else:
                df = _fetch_live(symbol)
            inserted = _upsert(df)
            indexed = _index_es(df)
            total_pg += inserted
            total_es += indexed
            log.info("symbol=%s rows=%d pg_inserted=%d es_indexed=%d",
                     symbol, len(df), inserted, indexed)
        except Exception as exc:
            log.exception("Failed to ingest %s: %s", symbol, exc)
    log.info("ingest_prices done. pg_inserted=%d es_indexed=%d", total_pg, total_es)


if __name__ == "__main__":
    run()
