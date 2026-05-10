"""Centralized configuration loaded from environment variables.

All other modules import from this file instead of reading os.environ
directly, so we have a single place to validate values and provide
sensible defaults.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    # Pipeline mode
    data_mode: str  # 'live' or 'sample'

    # Data sources
    crypto_symbols: list[str]
    news_rss_feeds: list[str]

    # Analysis parameters
    zscore_threshold: float
    zscore_window_minutes: int
    news_link_window_minutes: int

    # Postgres
    pg_host: str
    pg_port: int
    pg_user: str
    pg_password: str
    pg_db: str

    # Elasticsearch
    es_host: str
    es_port: int

    @property
    def postgres_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )

    @property
    def elasticsearch_url(self) -> str:
        return f"http://{self.es_host}:{self.es_port}"


def load_settings() -> Settings:
    return Settings(
        data_mode=os.environ.get("DATA_MODE", "live").lower(),
        crypto_symbols=_split_csv(os.environ.get("CRYPTO_SYMBOLS", "BTCUSDT")),
        news_rss_feeds=_split_csv(os.environ.get("NEWS_RSS_FEEDS", "")),
        zscore_threshold=float(os.environ.get("ZSCORE_THRESHOLD", "2.5")),
        zscore_window_minutes=int(os.environ.get("ZSCORE_WINDOW_MINUTES", "60")),
        news_link_window_minutes=int(os.environ.get("NEWS_LINK_WINDOW_MINUTES", "30")),
        pg_host=os.environ.get("POSTGRES_HOST", "postgres"),
        pg_port=int(os.environ.get("POSTGRES_PORT", "5432")),
        pg_user=os.environ["POSTGRES_USER"],
        pg_password=os.environ["POSTGRES_PASSWORD"],
        pg_db=os.environ["POSTGRES_DB"],
        es_host=os.environ.get("ELASTICSEARCH_HOST", "elasticsearch"),
        es_port=int(os.environ.get("ELASTICSEARCH_PORT", "9200")),
    )


# Lazy singleton so importing the module doesn't crash if env is incomplete.
_settings: Settings | None = None


def settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings
