"""Fetch crypto-related news articles from public RSS feeds.

Stores metadata + sentiment in Postgres and indexes the searchable
text fields in Elasticsearch in a single pass (dual-write pattern).
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import feedparser
from elasticsearch.helpers import bulk
from sqlalchemy import text
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.common.config import settings
from src.common.db import get_engine, get_es
from src.common.es_index import INDEX_NEWS, ensure_indices

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Airflow mounts ./data at /opt/airflow/data; pipeline container at /app/data.
_DATA_ROOTS = [Path("/opt/airflow/data"), Path("/app/data")]
SAMPLE_FILE = next(
    (r / "sample/news.json" for r in _DATA_ROOTS if r.exists()),
    Path("/app/data/sample/news.json"),
)

# Map ticker symbols (uppercase, no quote currency) to keywords we'll search for
# in the article text. Extend as needed.
SYMBOL_KEYWORDS: dict[str, list[str]] = {
    "BTC": ["bitcoin", "btc"],
    "ETH": ["ethereum", "eth", "ether"],
    "SOL": ["solana", "sol"],
    "XRP": ["xrp", "ripple"],
    "ADA": ["cardano", "ada"],
}

_analyzer = SentimentIntensityAnalyzer()


def _label(score: float) -> str:
    if score >= 0.05:
        return "positive"
    if score <= -0.05:
        return "negative"
    return "neutral"


def _detect_symbols(text_blob: str) -> list[str]:
    text_low = text_blob.lower()
    found: list[str] = []
    for sym, kws in SYMBOL_KEYWORDS.items():
        if any(re.search(rf"\b{kw}\b", text_low) for kw in kws):
            found.append(sym)
    return found


def _article_uid(link: str) -> str:
    return hashlib.sha256(link.encode("utf-8")).hexdigest()[:32]


def _parse_feed(url: str) -> list[dict]:
    log.info("Parsing feed: %s", url)
    parsed = feedparser.parse(url)
    source = parsed.feed.get("title", url).lower().split()[0][:64] if parsed.feed else url
    articles: list[dict] = []
    for entry in parsed.entries:
        link = entry.get("link", "").strip()
        title = entry.get("title", "").strip()
        if not link or not title:
            continue
        summary = entry.get("summary", "") or entry.get("description", "")
        # feedparser exposes published_parsed as time.struct_time
        if entry.get("published_parsed"):
            published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        else:
            published = datetime.now(tz=timezone.utc)
        articles.append(
            {
                "article_uid": _article_uid(link),
                "source": source,
                "title": title,
                "summary": summary,
                "link": link,
                "published_at": published,
            }
        )
    return articles


def _read_sample() -> list[dict]:
    if not SAMPLE_FILE.exists():
        raise FileNotFoundError(f"Sample news file not found: {SAMPLE_FILE}")
    with SAMPLE_FILE.open() as f:
        raw = json.load(f)
    out = []
    for item in raw:
        item["article_uid"] = _article_uid(item["link"])
        item["published_at"] = datetime.fromisoformat(item["published_at"])
        out.append(item)
    return out


def _enrich(articles: Iterable[dict]) -> list[dict]:
    enriched: list[dict] = []
    for art in articles:
        text_blob = f"{art['title']} {art.get('summary', '')}"
        score = _analyzer.polarity_scores(text_blob)["compound"]
        art["sentiment_score"] = round(score, 4)
        art["sentiment_label"] = _label(score)
        art["mentioned_symbols"] = _detect_symbols(text_blob)
        enriched.append(art)
    return enriched


def _upsert_postgres(articles: list[dict]) -> int:
    if not articles:
        return 0
    sql = text(
        """
        INSERT INTO news
            (article_uid, source, title, summary, link, published_at,
             sentiment_score, sentiment_label, mentioned_symbols)
        VALUES
            (:article_uid, :source, :title, :summary, :link, :published_at,
             :sentiment_score, :sentiment_label, :mentioned_symbols)
        ON CONFLICT (article_uid) DO NOTHING
        """
    )
    with get_engine().begin() as conn:
        result = conn.execute(sql, articles)
        return result.rowcount or 0


def _index_elasticsearch(articles: list[dict]) -> int:
    if not articles:
        return 0
    es = get_es()
    actions = [
        {
            "_op_type": "index",
            "_index": INDEX_NEWS,
            "_id": art["article_uid"],
            "_source": {
                **art,
                "published_at": art["published_at"].isoformat(),
            },
        }
        for art in articles
    ]
    success, _ = bulk(es, actions, raise_on_error=False)
    return success


def run() -> None:
    cfg = settings()
    log.info("ingest_news starting in %s mode (%d feeds)", cfg.data_mode, len(cfg.news_rss_feeds))
    ensure_indices()

    raw: list[dict] = []
    if cfg.data_mode == "sample":
        raw = _read_sample()
    else:
        for url in cfg.news_rss_feeds:
            try:
                raw.extend(_parse_feed(url))
            except Exception as exc:
                log.exception("Feed failed: %s -- %s", url, exc)

    articles = _enrich(raw)
    pg_inserted = _upsert_postgres(articles)
    es_indexed = _index_elasticsearch(articles)
    log.info(
        "ingest_news done. parsed=%d pg_inserted=%d es_indexed=%d",
        len(articles), pg_inserted, es_indexed,
    )


if __name__ == "__main__":
    run()
