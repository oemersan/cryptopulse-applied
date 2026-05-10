"""Elasticsearch index management and bulk-write helpers.

Centralizing this here keeps index names and mappings in one place,
so the ingestion and analysis modules don't drift apart.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable

from elasticsearch.helpers import bulk

from src.common.db import get_es

log = logging.getLogger(__name__)

# Index names. Kept simple and lowercase (ES requirement).
INDEX_NEWS = "news"
INDEX_PRICES = "prices"
INDEX_ANOMALIES = "anomalies"
INDEX_ANOMALY_NEWS_CONTEXT = "anomaly_news_context"

# Minimal explicit mappings. ES would auto-detect these, but being explicit
# avoids surprises (e.g. numbers being indexed as strings).
_MAPPINGS = {
    INDEX_NEWS: {
        "properties": {
            "article_uid": {"type": "keyword"},
            "source": {"type": "keyword"},
            "title": {"type": "text"},
            "summary": {"type": "text"},
            "link": {"type": "keyword"},
            "published_at": {"type": "date"},
            "sentiment_score": {"type": "float"},
            "sentiment_label": {"type": "keyword"},
            "mentioned_symbols": {"type": "keyword"},
        }
    },
    INDEX_PRICES: {
        "properties": {
            "symbol": {"type": "keyword"},
            "open_time": {"type": "date"},
            "open_price": {"type": "float"},
            "high_price": {"type": "float"},
            "low_price": {"type": "float"},
            "close_price": {"type": "float"},
            "volume": {"type": "float"},
        }
    },
    INDEX_ANOMALIES: {
        "properties": {
            "symbol": {"type": "keyword"},
            "detected_at": {"type": "date"},
            "z_score": {"type": "float"},
            "return_pct": {"type": "float"},
            "rolling_mean": {"type": "float"},
            "rolling_std": {"type": "float"},
            "direction": {"type": "keyword"},
        }
    },
    INDEX_ANOMALY_NEWS_CONTEXT: {
        "properties": {
            "anomaly_id": {"type": "long"},
            "news_id": {"type": "long"},
            "symbol": {"type": "keyword"},
            "base_symbol": {"type": "keyword"},
            "detected_at": {"type": "date"},
            "direction": {"type": "keyword"},
            "z_score": {"type": "float"},
            "return_pct": {"type": "float"},
            "news_title": {"type": "text"},
            "news_source": {"type": "keyword"},
            "news_link": {"type": "keyword"},
            "published_at": {"type": "date"},
            "sentiment_score": {"type": "float"},
            "sentiment_label": {"type": "keyword"},
            "time_offset_min": {"type": "integer"},
        }
    },
}


def ensure_indices() -> None:
    """Create indices with the right mapping if they don't exist yet.

    Safe to call on every DAG run; the existence check is cheap.
    """
    es = get_es()
    for name, mapping in _MAPPINGS.items():
        if not es.indices.exists(index=name):
            es.indices.create(index=name, mappings=mapping)
            log.info("Created ES index: %s", name)


def _serialize_value(v):
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def bulk_index(index: str, docs: Iterable[dict], id_field: str | None = None) -> int:
    """Bulk-index a list of dicts into ``index``.

    If ``id_field`` is given, that field is used as the document _id so re-runs
    are idempotent. Otherwise ES generates a random _id.
    """
    actions = []
    for doc in docs:
        action = {"_op_type": "index", "_index": index,
                  "_source": {k: _serialize_value(v) for k, v in doc.items() if k != id_field}}
        if id_field and id_field in doc:
            action["_id"] = str(doc[id_field])
        actions.append(action)

    if not actions:
        return 0
    success, errors = bulk(get_es(), actions, raise_on_error=False, stats_only=False)
    if errors:
        log.error("Bulk index errors: %s", errors[:3])
    return success
