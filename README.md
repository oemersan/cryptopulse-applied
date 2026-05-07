# CryptoPulse

> An end-to-end, fully containerized data engineering pipeline that detects
> short-term cryptocurrency price anomalies and links them to financial news
> articles published in the same time window.
>
> **Course:** YZV 322E — Applied Data Engineering · Spring 2026
> **Institution:** Istanbul Technical University

---

## What it does

1. **Ingests minute-level price candles** for a configurable set of crypto
   pairs from the Binance public REST API.
2. **Ingests financial news** (titles, summaries, metadata) from public RSS
   feeds (CoinDesk, Cointelegraph, Decrypt, ...).
3. **Detects price anomalies** using a rolling z-score of 1-minute log returns.
4. **Scores news sentiment** with VADER and detects which crypto each article
   mentions.
5. **Links each anomaly to news articles** published within a configurable
   time window around it.
6. **Visualizes** prices, anomaly markers, and surrounding news sentiment in
   Kibana dashboards.

The system makes **no causal claim**. It produces context that an analyst
can read alongside the chart.

## Architecture

```
                Binance API           RSS feeds (CoinDesk, ...)
                     |                          |
                     v                          v
            +-------------------+      +-------------------+
            | price_ingestion   |      | news_ingestion    |
            | DAG (Airflow)     |      | DAG (Airflow)     |
            +-------------------+      +-------------------+
                     |                          |   \
                     v                          v    \---> Elasticsearch
                 PostgreSQL (prices,  news, anomalies, anomaly_news_links)
                     |                                            |
                     v                                            v
            +-------------------+                          +-------------+
            | analysis DAG      |                          |   Kibana    |
            | (z-score + link)  |                          | dashboards  |
            +-------------------+                          +-------------+
```

A more detailed diagram is included in the technical report.

## Tech stack

| Layer | Tool | Why |
|---|---|---|
| Orchestration | **Apache Airflow** | DAGs make ingestion + analysis schedulable, observable, retryable |
| Relational store | **PostgreSQL** | Structured time series, anomalies, and many-to-many links |
| DB admin | **pgAdmin** | Visual inspection of tables during demo |
| Search index | **Elasticsearch** | Full-text and asset-aware retrieval over news |
| Dashboards | **Kibana** | Built-in time-series + filter UI on top of ES |
| Containerization | **Docker + Compose** | Single-command bring-up, no host dependencies |
| Anomaly detection | rolling z-score (NumPy/Pandas) | Simple, transparent, easy to defend |
| Sentiment | **VADER** | Lexicon-based, CPU-only, lightweight container |

## Requirements

- Docker + Docker Compose (Compose V2)
- ~8 GB free RAM, ~10 GB free disk
- Internet access for live mode (sample mode works fully offline)

## Quick start

```bash
# 1. Clone and configure
git clone <repo-url>
cd cryptopulse
cp .env.example .env

# 2. Build and bring everything up
docker compose up --build -d

# 3. (First time only) generate sample data so you can demo without internet
docker compose run --rm pipeline python -m src.ingestion.generate_sample
```

After ~2 minutes the following UIs are reachable:

| Service | URL | Default credentials |
|---|---|---|
| Airflow | http://localhost:8088 | admin / admin |
| pgAdmin | http://localhost:5050 | admin@cryptopulse.local / admin |
| Kibana  | http://localhost:5601 | (none) |
| Elasticsearch | http://localhost:9200 | (none) |
| Postgres | localhost:5433 | see `.env` |

Once Airflow is up, the three DAGs (`price_ingestion`, `news_ingestion`,
`analysis`) are unpaused by default and run hourly. To trigger them
immediately, use the "play" button in the Airflow UI.

Kibana data views (`prices`, `anomalies`, `news`) are created automatically
by the `kibana-init` container on first startup. To build the demo
dashboards on top of them, follow [`docs/kibana_dashboard_guide.md`](docs/kibana_dashboard_guide.md).

## Running modes

The `DATA_MODE` variable in `.env` switches the pipeline between live and
offline sources:

- `DATA_MODE=live` — fetch from Binance + RSS feeds (default)
- `DATA_MODE=sample` — read from `data/sample/*` (deterministic, offline)

Switching does not require a rebuild; the DAGs read the variable at runtime.

## Manual operation

You can run individual scripts inside the dedicated `pipeline` container,
which carries all the team-authored code:

```bash
docker compose run --rm pipeline python -m src.ingestion.ingest_prices
docker compose run --rm pipeline python -m src.ingestion.ingest_news
docker compose run --rm pipeline python -m src.analysis.detect_anomalies
docker compose run --rm pipeline python -m src.ingestion.generate_sample
```

## Repository layout

```
.
├── docker-compose.yml
├── .env.example
├── docker/
│   ├── airflow/         # Airflow custom image (deps for our DAGs)
│   └── pipeline/        # Standalone team-code container
├── dags/                # Airflow DAG definitions
├── src/
│   ├── common/          # config, db helpers
│   ├── ingestion/       # price + news ingestion + sample generator
│   └── analysis/        # anomaly detection + linking
├── sql/init.sql         # Postgres schema, loaded on first start
├── kibana/              # Saved objects (dashboards) for import
├── data/sample/         # Offline fallback dataset
├── docs/                # Diagrams, screenshots
└── tests/               # Unit tests
```

## Known limitations

- Anomalies are flagged on a single asset; cross-asset effects are out of
  scope.
- News sources are limited to English-language RSS feeds. Turkish-language
  coverage is not modeled.
- VADER is a general-purpose lexicon. A domain-specific finance model
  (e.g. FinBERT) would likely give better sentiment signal but was excluded
  for resource reasons.
- Linking is **temporal co-occurrence only**. The system does not claim
  the news caused the move.

## Team

| Member | Role | Main areas |
|---|---|---|
| [Name 1] | Team Lead | Ingestion (`src/ingestion`), Postgres schema |
| [Name 2] | — | Airflow DAGs, anomaly detection logic |
| [Name 3] | — | Docker/Compose, Elasticsearch, Kibana dashboards |

A detailed individual-contribution table is provided in the technical report.

## License

MIT — see [`LICENSE`](LICENSE).
