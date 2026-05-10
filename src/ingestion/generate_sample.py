"""Generate sample price and news data for offline / demo runs.

Usage (from inside the pipeline container):
  docker compose run --rm pipeline python -m src.ingestion.generate_sample
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OUT_DIR = Path("/app/data/sample")
PRICES_OUT = OUT_DIR / "prices.csv"
NEWS_OUT = OUT_DIR / "news.json"

# Synthetic-data parameters: 5 symbols x ~3 days x 1440 min/day ~= 21,600 rows.
SYMBOLS_AND_BASE_PRICES: dict[str, float] = {
    "BTCUSDT": 60_000.0,
    "ETHUSDT": 3_000.0,
    "SOLUSDT": 150.0,
    "XRPUSDT": 0.50,
    "ADAUSDT": 0.45,
}
DAYS_OF_HISTORY = 3
SEED = 42


def _generate_prices() -> pd.DataFrame:
    rng = np.random.default_rng(SEED)
    end = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    minutes = DAYS_OF_HISTORY * 24 * 60
    timestamps = [end - timedelta(minutes=i) for i in range(minutes - 1, -1, -1)]

    rows = []
    for symbol, base in SYMBOLS_AND_BASE_PRICES.items():
        # Geometric Brownian motion-ish walk with rare large jumps (anomalies).
        returns = rng.normal(loc=0.0, scale=0.0015, size=minutes)
        # Inject ~6 spikes per symbol so the analysis DAG has anomalies to find.
        spike_idx = rng.choice(minutes, size=6, replace=False)
        returns[spike_idx] += rng.choice([-1, 1], size=6) * rng.uniform(0.01, 0.03, size=6)
        prices = base * np.exp(np.cumsum(returns))

        for ts, price, ret in zip(timestamps, prices, returns):
            high = price * (1 + abs(rng.normal(0, 0.0005)))
            low = price * (1 - abs(rng.normal(0, 0.0005)))
            open_p = price * (1 - ret / 2)
            volume = abs(rng.normal(100, 30))
            rows.append(
                {
                    "symbol": symbol,
                    "open_time": ts,
                    "open": round(open_p, 8),
                    "high": round(max(open_p, price, high), 8),
                    "low": round(min(open_p, price, low), 8),
                    "close": round(price, 8),
                    "volume": round(volume, 4),
                }
            )
    return pd.DataFrame(rows)


def _generate_news(price_df: pd.DataFrame) -> list[dict]:
    """Generate a few hundred fake-but-plausible articles, some near anomalies."""
    rng = np.random.default_rng(SEED + 1)

    headlines_pos = [
        "{coin} rallies as institutional inflows accelerate",
        "Analysts call {coin} breakout 'historic' after volume surge",
        "{coin} ETF approval fuels broad market optimism",
    ]
    headlines_neg = [
        "{coin} slides as regulators signal new restrictions",
        "Sell-off hits {coin} amid macro uncertainty",
        "{coin} drops sharply as exchange outflows climb",
    ]
    headlines_neutral = [
        "{coin} weekly recap: range-bound trading continues",
        "Developers publish update on {coin} roadmap",
        "{coin} community votes on governance proposal",
    ]

    coin_names = {
        "BTCUSDT": "Bitcoin", "ETHUSDT": "Ethereum",
        "SOLUSDT": "Solana", "XRPUSDT": "XRP", "ADAUSDT": "Cardano",
    }

    end = price_df["open_time"].max()
    start = price_df["open_time"].min()
    n_articles = 300
    articles = []
    for i in range(n_articles):
        symbol = rng.choice(list(coin_names.keys()))
        coin = coin_names[symbol]
        bucket = rng.choice(["pos", "neg", "neutral"], p=[0.35, 0.35, 0.30])
        templates = {"pos": headlines_pos, "neg": headlines_neg, "neutral": headlines_neutral}[bucket]
        title = rng.choice(templates).format(coin=coin)
        summary = (
            f"{coin} traders weigh latest developments. "
            f"This story is part of a synthetic dataset generated for offline demo use."
        )
        # Spread articles uniformly in time.
        offset_min = rng.integers(0, int((end - start).total_seconds() / 60))
        published = start + timedelta(minutes=int(offset_min))
        articles.append(
            {
                "source": rng.choice(["coindesk", "cointelegraph", "decrypt"]),
                "title": title,
                "summary": summary,
                "link": f"https://example.com/sample/{i}-{coin.lower()}",
                "published_at": published.isoformat(),
            }
        )
    return articles


def run() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Generating prices ...")
    prices = _generate_prices()
    prices.to_csv(PRICES_OUT, index=False)
    log.info("Wrote %d price rows to %s", len(prices), PRICES_OUT)

    log.info("Generating news ...")
    news = _generate_news(prices)
    NEWS_OUT.write_text(json.dumps(news, indent=2))
    log.info("Wrote %d news rows to %s", len(news), NEWS_OUT)


if __name__ == "__main__":
    run()
