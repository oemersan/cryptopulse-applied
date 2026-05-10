"""Database and search-engine connection helpers."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from elasticsearch import Elasticsearch
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.common.config import settings


_engine: Engine | None = None
_es_client: Elasticsearch | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings().postgres_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
    return _engine


@contextmanager
def get_session() -> Iterator[Session]:
    """Yield a SQLAlchemy session with automatic commit/rollback."""
    SessionLocal = sessionmaker(bind=get_engine(), expire_on_commit=False)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_es() -> Elasticsearch:
    global _es_client
    if _es_client is None:
        _es_client = Elasticsearch(
            settings().elasticsearch_url,
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=3,
        )
    return _es_client
