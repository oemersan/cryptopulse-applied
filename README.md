# CryptoPulse

**An end-to-end, fully containerized data engineering pipeline that detects
sudden cryptocurrency price movements and links each flagged anomaly to
financial news published in the same time window.**

> Course: YZV 322E — Applied Data Engineering · Spring 2026  
> Istanbul Technical University · Department of Artificial Intelligence and Data Engineering

---

## Table of Contents

1. [What It Does](#what-it-does)
2. [Architecture](#architecture)
3. [Tech Stack](#tech-stack)
4. [Requirements](#requirements)
5. [Quick Start](#quick-start)
6. [Running Modes](#running-modes)
7. [Service URLs](#service-urls)
8. [Manual Operation](#manual-operation)
9. [Repository Layout](#repository-layout)
10. [Known Limitations](#known-limitations)
11. [Team](#team)

---

## What It Does

CryptoPulse automates the correlation between **cryptocurrency price anomalies**
and **financial news**. When a sharp price move happens, analysts typically open
a chart, spot the spike, then manually search news sites for context. This
pipeline does that automatically:

1. **Fetches minute-level OHLCV candles** for five cryptocurrency pairs from the
   Binance public REST API (BTC, ETH, SOL, XRP, ADA).
2. **Fetches and enriches news articles** from public RSS feeds (CoinDesk,
   Cointelegraph). Each article gets a VADER sentiment score and is tagged with
   the crypto symbols it mentions.
3. **Detects price anomalies** using a rolling z-score on 1-minute log returns.
   Any candle whose z-score exceeds the configured threshold is flagged.
4. **Links each anomaly to nearby news** — articles published within a
   configurable time window that mention the same asset.
5. **Visualizes everything** in Kibana dashboards: price charts, anomaly markers,
   sentiment trends, and the anomaly-news context table.

> **Scope note:** The system finds *temporal co-occurrence*, not causation.
> We're not claiming that news caused the price move or vice versa — we're
> just surfacing which news appeared around the same time. The interpretation
> is left to the analyst.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                       External Data Sources                       │
│         Binance REST API               RSS Feeds                  │
│      (1-min OHLCV candles)     (CoinDesk, Cointelegraph)          │
└────────────┬─────────────────────────────┬───────────────────────┘
             │                             │
    ┌────────▼──────────┐       ┌──────────▼──────────┐
    │   ingest_prices   │       │    ingest_news       │
    │   (Python)        │       │    (Python + VADER)  │
    └────────┬──────────┘       └──────────┬───────────┘
             │    run in parallel           │
             └──────────────┬──────────────┘
                            │  dual-write
              ┌─────────────▼──────────────────────┐
              │  PostgreSQL          Elasticsearch  │
              │  (prices, news,      (prices, news, │
              │   anomalies,          anomalies,    │
              │   links)              context)      │
              └─────────────┬──────────────────────┘
                            │
                 ┌──────────▼──────────┐
                 │   detect_and_link   │
                 │  rolling z-score +  │
                 │  SQL news linking   │
                 └──────────┬──────────┘
                            │
                 ┌──────────▼──────────┐
                 │   Kibana Dashboards  │
                 │  · Price timeseries  │
                 │  · Anomaly counts    │
                 │  · Sentiment trends  │
                 │  · Anomaly-news ctx  │
                 └─────────────────────┘
```

**Apache Airflow** orchestrates everything as a single hourly DAG
(`cryptopulse_pipeline`). `ingest_prices` and `ingest_news` run in parallel;
`detect_and_link` only starts after both succeed. That dependency is the main
reason we use Airflow — without it, the analysis could run before the data
is ready and produce wrong or incomplete results.

A more detailed architecture diagram is in the technical report.

---

## Tech Stack

| Layer | Tool | Why We Chose It |
|---|---|---|
| Orchestration | **Apache Airflow** | Task dependency management; automatic retries; monitoring UI |
| Relational store | **PostgreSQL 15** | ACID guarantees; native `TEXT[]` + GIN index for symbol matching; `ON CONFLICT` for idempotent writes |
| DB admin | **pgAdmin 4** | Easy table inspection during development and demo |
| Search + index | **Elasticsearch 8.13** | Kibana-native queries; asset-aware full-text search with minimal code |
| Dashboards | **Kibana 8.13** | Built-in Lens visualizations; no custom frontend needed |
| Containerization | **Docker + Compose** | Single-command startup; no host dependencies |
| Anomaly detection | rolling z-score (NumPy/Pandas) | Transparent math; no labeled training data needed; fast |
| Sentiment | **VADER** | Offline, CPU-only, 50 KB — works in any container without GPU |

---

## Requirements

| Requirement | Minimum |
|---|---|
| Docker Engine | 24+ |
| Docker Compose | V2 (`docker compose`, not `docker-compose`) |
| Free RAM | 6 GB |
| Free Disk | 10 GB |
| Internet | Only needed for live mode; sample mode works fully offline |

---

## Quick Start

```bash
# 1. Clone the repository
git clone <repo-url> cryptopulse
cd cryptopulse

# 2. Create your local environment file
cp .env.example .env
# The defaults work out of the box for local development.
# Change passwords before deploying anywhere outside your laptop.

# 3. On Linux: set the Airflow UID to your own user (avoids volume permission issues)
echo "AIRFLOW_UID=$(id -u)" >> .env

# 4. Build images and start all services
docker compose up --build -d

# 5. Watch the startup logs (Ctrl+C exits the view; containers keep running)
docker compose logs -f
```

After about **2 minutes** all services are healthy and the UIs are reachable
(see [Service URLs](#service-urls) below).

The Airflow DAG `cryptopulse_pipeline` is unpaused by default and runs on an
hourly schedule. To trigger a run immediately without waiting:

```bash
# Option A — click the play button in the Airflow UI at http://localhost:8088
# Option B — trigger from the terminal
docker compose exec airflow-webserver airflow dags trigger cryptopulse_pipeline
```

---

## Running Modes

Set `DATA_MODE` in `.env` before starting. No rebuild is needed when changing
this value — just restart the affected containers.

| Mode | Value | Data source | Use case |
|---|---|---|---|
| **Live** | `DATA_MODE=live` | Binance REST API + RSS feeds | Default; pulls real market data |
| **Sample** | `DATA_MODE=sample` | `data/sample/*.csv` / `*.json` | Offline demo; fully deterministic; 21,600 price rows + 300 news articles loaded immediately |

### Data volume in live mode

The first pipeline run fetches the last 1,000 candles per symbol from Binance
(5 symbols × 1,000 = **~5,000 rows**). Each subsequent hourly run adds only the
new candles since the last run (~60 per symbol × 5 = **~300 new rows/hour**).
Reaching 10,000 rows takes roughly **the first run plus about 17 more hours** of
operation. If you need 10,000+ rows right away, switch to sample mode — it loads
21,600 rows on the first run.

### Regenerating sample data

The repo ships with a pre-built sample dataset. If you want to regenerate it with
today's timestamps (needed if the anomaly detector's recency filter is cutting
off old data):

```bash
docker compose run --rm pipeline python -m src.ingestion.generate_sample
```

This creates 5 symbols × 3 days × 1,440 minutes = **21,600 synthetic price rows**
and **300 news articles** with injected anomaly spikes.

---

## Service URLs

| Service | URL | Default credentials |
|---|---|---|
| **Airflow** | http://localhost:8088 | `admin` / `admin` (see `AIRFLOW_ADMIN_*` in `.env`) |
| **Kibana** | http://localhost:5601 | No login (security disabled for local dev) |
| **pgAdmin** | http://localhost:5050 | `admin@cryptopulse.local` / `admin` (see `PGADMIN_*` in `.env`) |
| **Elasticsearch** | http://localhost:9200 | No login |
| **PostgreSQL** | `localhost:5433` | See `POSTGRES_*` in `.env` |

> Ports are all configurable in `.env`. The defaults above assume nothing else
> is already running on those ports.

---

## Manual Operation

All team-authored Python code runs inside the `pipeline` container.
Use `docker compose run --rm pipeline` to run one-shot commands:

```bash
# Run each step of the pipeline manually
docker compose run --rm pipeline python -m src.ingestion.ingest_prices
docker compose run --rm pipeline python -m src.ingestion.ingest_news
docker compose run --rm pipeline python -m src.analysis.detect_anomalies

# Regenerate the synthetic sample dataset
docker compose run --rm pipeline python -m src.ingestion.generate_sample

# Re-create Kibana data views if they got deleted
docker compose run --rm pipeline python -m src.common.kibana_bootstrap

# Run the unit test suite
docker compose run --rm pipeline python -m pytest tests/ -v
```

### Useful diagnostic commands

```bash
# Full health check — containers, HTTP endpoints, and row counts
chmod +x scripts/health_check.sh && ./scripts/health_check.sh

# Container status at a glance
docker compose ps

# Follow logs for a specific service
docker compose logs -f airflow-scheduler
docker compose logs -f pipeline

# Check row counts across all tables
docker exec cryptopulse-postgres \
  psql -U cryptopulse -d cryptopulse \
  -c "SELECT 'prices' AS tbl, COUNT(*) FROM prices
      UNION ALL SELECT 'news', COUNT(*) FROM news
      UNION ALL SELECT 'anomalies', COUNT(*) FROM anomalies
      UNION ALL SELECT 'anomaly_news_links', COUNT(*) FROM anomaly_news_links;"

# Check Elasticsearch document counts
curl -s "http://localhost:9200/_cat/indices?v&h=index,docs.count"

# Stop all containers, keep data volumes
docker compose down

# Stop all containers and wipe all data (full reset)
docker compose down -v
```

---

## Repository Layout

```
cryptopulse/
├── docker-compose.yml          # Full stack definition (9 services)
├── .env.example                # Template — copy to .env before first run
├── DOCUMENTATION.md            # Detailed technical reference for all components
├── docker/
│   ├── airflow/
│   │   ├── Dockerfile          # Airflow image with project dependencies
│   │   └── requirements.txt
│   └── pipeline/
│       ├── Dockerfile          # Lightweight Python image for team code
│       └── requirements.txt
├── dags/
│   └── cryptopulse_pipeline_dag.py   # Single Airflow DAG (3 tasks)
├── src/
│   ├── common/
│   │   ├── config.py           # Centralized env-var configuration
│   │   ├── db.py               # SQLAlchemy + Elasticsearch connection helpers
│   │   ├── es_index.py         # Index definitions, mappings, bulk-write helper
│   │   └── kibana_bootstrap.py # Automatic Kibana data view creation on startup
│   ├── ingestion/
│   │   ├── ingest_prices.py    # Binance API → Postgres + Elasticsearch
│   │   ├── ingest_news.py      # RSS → VADER sentiment → Postgres + Elasticsearch
│   │   └── generate_sample.py  # Synthetic data generator for offline demo
│   └── analysis/
│       └── detect_anomalies.py # Rolling z-score detection + SQL news linking
├── sql/
│   └── init.sql                # Postgres schema (auto-loaded on first startup)
├── kibana/
│   ├── dashboard.ndjson        # Exportable Kibana dashboard objects
│   └── data_views.ndjson       # Fallback data view import file
├── data/
│   └── sample/
│       ├── prices.csv          # Pre-built synthetic price data (21,600 rows)
│       └── news.json           # Pre-built synthetic news data (300 articles)
├── tests/
│   └── test_anomalies.py       # Unit tests for the anomaly detection logic
├── scripts/
│   └── health_check.sh         # Full-stack health check (containers + endpoints + counts)
└── docs/
    └── kibana_dashboard_guide.md  # Step-by-step Kibana dashboard setup guide
```

---

## Known Limitations

- **Single-asset anomaly detection.** Each trading pair is analyzed independently.
  Cross-asset correlations (e.g., a BTC spike pulling ETH along with it) are not
  modeled — that would be a natural extension.
- **English-only news sources.** We only parse two English RSS feeds. Events
  covered in other languages or on other platforms are missed.
- **VADER sentiment accuracy.** VADER is a general-purpose lexicon, not tuned
  for financial language. A domain-specific model like FinBERT would be more
  accurate but requires much more memory and was out of scope here.
- **Temporal co-occurrence only.** The news-anomaly link is based purely on
  time window and symbol mention — not semantic relevance or any causal logic.
- **Elasticsearch security disabled.** `xpack.security.enabled=false` is set
  for development convenience. Enable TLS and authentication before any
  deployment outside of localhost.
- **Live data accumulates slowly.** The first pipeline run fetches ~5,000 rows.
  After that, each hourly run adds only ~300 new rows. Reaching 10,000 rows
  takes roughly a full day of live operation. Sample mode loads 21,600 rows
  immediately and is fine for demos.

---

## Team

| Name | Student ID | Email | Main Contributions |
|---|---|---|---|
| Ömer Faruk San | 150220307 | san22@itu.edu.tr | Docker/Compose setup, Elasticsearch & Kibana integration, anomaly detection module |
| Sude Dilay Tunç | 150230716 | tuncs23@itu.edu.tr | Airflow DAG design and scheduling, price ingestion, PostgreSQL schema |
| Gonca Kaplan | 150220324 | kaplang22@itu.edu.tr | News ingestion, RSS parsing, VADER sentiment integration, sample data generator |

A full individual contribution table is in the technical report appendix.

---

## License

MIT — see [LICENSE](LICENSE).
