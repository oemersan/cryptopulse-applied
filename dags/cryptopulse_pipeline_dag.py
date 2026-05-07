"""CryptoPulse end-to-end pipeline DAG.

Three tasks, scheduled hourly:

    ingest_prices ──┐
                    ├──> detect_and_link
    ingest_news  ───┘

Price and news ingestion run in parallel; the analysis task runs only
after both have succeeded. This is the dependency that makes Airflow
worth using here in the first place: the analysis is meaningless until
both upstream sources have produced rows for the current window.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.analysis.detect_anomalies import run as run_detect_and_link
from src.ingestion.ingest_news import run as run_ingest_news
from src.ingestion.ingest_prices import run as run_ingest_prices

default_args = {
    "owner": "cryptopulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}

with DAG(
    dag_id="cryptopulse_pipeline",
    description=(
        "End-to-end pipeline: ingest prices and news in parallel, "
        "then detect anomalies and link them to nearby news."
    ),
    start_date=datetime(2026, 5, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["cryptopulse", "pipeline"],
) as dag:

    ingest_prices = PythonOperator(
        task_id="ingest_prices",
        python_callable=run_ingest_prices,
        doc_md=(
            "Pull minute-level OHLCV candles from the Binance public API "
            "(or from `data/sample/prices.csv` in sample mode) and write "
            "them to Postgres and Elasticsearch."
        ),
    )

    ingest_news = PythonOperator(
        task_id="ingest_news",
        python_callable=run_ingest_news,
        doc_md=(
            "Parse the configured RSS feeds, score each article with VADER, "
            "tag it with mentioned crypto symbols, and dual-write to "
            "Postgres and Elasticsearch."
        ),
    )

    detect_and_link = PythonOperator(
        task_id="detect_and_link",
        python_callable=run_detect_and_link,
        doc_md=(
            "Compute rolling z-scores on the price series, flag anomalies "
            "above the configured threshold, and link each anomaly to news "
            "articles published in the same time window. Requires both "
            "ingestion tasks to have succeeded."
        ),
    )

    # The dependency that justifies using Airflow here:
    # detect_and_link cannot run until both ingestion tasks have produced data.
    [ingest_prices, ingest_news] >> detect_and_link
