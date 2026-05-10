"""Detect price anomalies and link them to temporally-nearby news articles."""
from __future__ import annotations

import logging
from datetime import timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.common.config import settings
from src.common.db import get_engine
from src.common.es_index import (
    INDEX_ANOMALIES,
    INDEX_ANOMALY_NEWS_CONTEXT,
    bulk_index,
    ensure_indices,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def _load_recent_prices(symbol: str, lookback_minutes: int) -> pd.DataFrame:
    # Use the latest timestamp in the table as the reference point so that
    # sample-mode data (which has fixed past timestamps) is not excluded by
    # a NOW()-based filter.
    query = text(
        """
        SELECT open_time, close_price
        FROM prices
        WHERE symbol = :symbol
          AND open_time >= (
              SELECT MAX(open_time) FROM prices WHERE symbol = :symbol
          ) - make_interval(mins => :lookback)
        ORDER BY open_time
        """
    )

    with get_engine().connect() as conn:
        result = conn.execute(
            query,
            {
                "symbol": symbol,
                "lookback": lookback_minutes,
            },
        )

        rows = result.mappings().all()

    return pd.DataFrame(rows)


def _compute_anomalies(df: pd.DataFrame, window: int, threshold: float) -> pd.DataFrame:
    """Return rows whose 1-min log-return |z-score| exceeds the threshold."""
    if len(df) < window + 2:
        return df.iloc[0:0]

    df = df.sort_values("open_time").reset_index(drop=True)

    # PostgreSQL NUMERIC/DECIMAL values may arrive as object/Decimal.
    # Convert to float so numpy operations such as np.log work correctly.
    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    df = df.dropna(subset=["close_price"])

    df["return"] = np.log(df["close_price"] / df["close_price"].shift(1))
    df["rolling_mean"] = df["return"].rolling(window=window, min_periods=window).mean()
    df["rolling_std"] = df["return"].rolling(window=window, min_periods=window).std()

    # Avoid division by zero if the rolling std is exactly zero.
    df["rolling_std"] = df["rolling_std"].replace(0, np.nan)

    df["z_score"] = (df["return"] - df["rolling_mean"]) / df["rolling_std"]
    flagged = df[df["z_score"].abs() >= threshold].copy()
    flagged["direction"] = np.where(flagged["z_score"] > 0, "up", "down")

    return flagged


def _save_anomalies(symbol: str, flagged: pd.DataFrame) -> int:
    if flagged.empty:
        return 0

    rows = [
        {
            "symbol": symbol,
            "detected_at": row["open_time"].to_pydatetime(),
            "z_score": float(row["z_score"]),
            "return_pct": float(row["return"]),
            "rolling_mean": float(row["rolling_mean"]),
            "rolling_std": float(row["rolling_std"]),
            "direction": row["direction"],
        }
        for _, row in flagged.iterrows()
    ]

    sql = text(
        """
        INSERT INTO anomalies
            (symbol, detected_at, z_score, return_pct, rolling_mean, rolling_std, direction)
        VALUES
            (:symbol, :detected_at, :z_score, :return_pct, :rolling_mean, :rolling_std, :direction)
        ON CONFLICT (symbol, detected_at) DO NOTHING
        """
    )

    with get_engine().begin() as conn:
        result = conn.execute(sql, rows)
        return result.rowcount or 0


def _index_anomalies_es(symbol: str, flagged: pd.DataFrame) -> int:
    if flagged.empty:
        return 0

    docs = []

    for _, row in flagged.iterrows():
        docs.append(
            {
                "_id": f"{symbol}_{int(row['open_time'].timestamp())}",
                "symbol": symbol,
                "detected_at": row["open_time"].to_pydatetime(),
                "z_score": float(row["z_score"]),
                "return_pct": float(row["return"]),
                "rolling_mean": float(row["rolling_mean"]),
                "rolling_std": float(row["rolling_std"]),
                "direction": row["direction"],
            }
        )

    return bulk_index(INDEX_ANOMALIES, docs, id_field="_id")


def _link_news() -> int:
    """For every anomaly without links yet, find news within the configured
    time window mentioning the anomaly's asset symbol and create link rows.
    """
    cfg = settings()
    base_symbol_expr = "REPLACE(REPLACE(a.symbol, 'USDT', ''), 'USD', '')"
    sql = text(
        f"""
        WITH unlinked AS (
            SELECT a.id, a.symbol, a.detected_at,
                   {base_symbol_expr} AS base
            FROM anomalies a
            LEFT JOIN anomaly_news_links l ON l.anomaly_id = a.id
            WHERE l.id IS NULL
        ),
        candidates AS (
            SELECT u.id AS anomaly_id,
                   n.id AS news_id,
                   EXTRACT(EPOCH FROM (n.published_at - u.detected_at)) / 60.0
                       AS time_offset_min
            FROM unlinked u
            JOIN news n
              ON n.mentioned_symbols @> ARRAY[u.base]
             AND n.published_at BETWEEN u.detected_at - (:win || ' minutes')::interval
                                    AND u.detected_at + (:win || ' minutes')::interval
        )
        INSERT INTO anomaly_news_links (anomaly_id, news_id, time_offset_min)
        SELECT anomaly_id, news_id, time_offset_min::int FROM candidates
        ON CONFLICT (anomaly_id, news_id) DO NOTHING
        """
    )
    with get_engine().begin() as conn:
        result = conn.execute(sql, {"win": cfg.news_link_window_minutes})
        return result.rowcount or 0


def _index_anomaly_news_context_es() -> int:
    """Index joined anomaly-news context rows for Kibana dashboards."""
    sql = text(
        """
        SELECT
            l.id AS link_id,
            a.id AS anomaly_id,
            n.id AS news_id,
            a.symbol,
            REPLACE(REPLACE(a.symbol, 'USDT', ''), 'USD', '') AS base_symbol,
            a.detected_at,
            a.direction,
            a.z_score,
            a.return_pct,
            n.title AS news_title,
            n.source AS news_source,
            n.link AS news_link,
            n.published_at,
            n.sentiment_score,
            n.sentiment_label,
            l.time_offset_min
        FROM anomaly_news_links l
        JOIN anomalies a ON a.id = l.anomaly_id
        JOIN news n ON n.id = l.news_id
        ORDER BY a.detected_at DESC
        """
    )

    with get_engine().connect() as conn:
        rows = conn.execute(sql).mappings().all()

    docs = []
    for row in rows:
        docs.append(
            {
                "_id": row["link_id"],
                "anomaly_id": row["anomaly_id"],
                "news_id": row["news_id"],
                "symbol": row["symbol"],
                "base_symbol": row["base_symbol"],
                "detected_at": row["detected_at"],
                "direction": row["direction"],
                "z_score": float(row["z_score"]),
                "return_pct": float(row["return_pct"]),
                "news_title": row["news_title"],
                "news_source": row["news_source"],
                "news_link": row["news_link"],
                "published_at": row["published_at"],
                "sentiment_score": float(row["sentiment_score"]) if row["sentiment_score"] is not None else None,
                "sentiment_label": row["sentiment_label"],
                "time_offset_min": row["time_offset_min"],
            }
        )

    return bulk_index(INDEX_ANOMALY_NEWS_CONTEXT, docs, id_field="_id")


def run() -> None:
    cfg = settings()
    log.info("detect_anomalies starting (threshold=%.2f, window=%d min)",
             cfg.zscore_threshold, cfg.zscore_window_minutes)
    ensure_indices()
    # Load enough history to fill the rolling window plus buffer.
    lookback = max(cfg.zscore_window_minutes * 4, 240)
    total_new = 0
    total_es = 0
    for symbol in cfg.crypto_symbols:
        df = _load_recent_prices(symbol, lookback)
        flagged = _compute_anomalies(df, cfg.zscore_window_minutes, cfg.zscore_threshold)
        inserted = _save_anomalies(symbol, flagged)
        indexed = _index_anomalies_es(symbol, flagged)
        log.info("symbol=%s candles=%d flagged=%d pg_inserted=%d es_indexed=%d",
                 symbol, len(df), len(flagged), inserted, indexed)
        total_new += inserted
        total_es += indexed
    linked = _link_news()
    context_indexed = _index_anomaly_news_context_es()
    log.info(
        "detect_anomalies done. new_anomalies=%d es_indexed=%d new_links=%d context_indexed=%d",
        total_new,
        total_es,
        linked,
        context_indexed,
    )


if __name__ == "__main__":
    run()
