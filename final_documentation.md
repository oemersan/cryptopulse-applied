# CryptoPulse — Technical Documentation

> This document covers everything about how the project works: the architecture,
> the data flow, all the design decisions we made, and the commands you need
> to run things. It's meant to be a reference both during development and
> at demo time.

---

## Table of Contents

1. [Project Summary](#1-project-summary)
2. [System Architecture](#2-system-architecture)
3. [End-to-End Data Flow](#3-end-to-end-data-flow)
4. [Services and Containers](#4-services-and-containers)
5. [Database Schema](#5-database-schema)
6. [Python Source Code](#6-python-source-code)
7. [Airflow DAG](#7-airflow-dag)
8. [Elasticsearch Index Design](#8-elasticsearch-index-design)
9. [Kibana Setup](#9-kibana-setup)
10. [Environment Variables (.env)](#10-environment-variables-env)
11. [Running Commands](#11-running-commands)
12. [Health Check](#12-health-check)
13. [Test Suite](#13-test-suite)
14. [Sample Data Generation](#14-sample-data-generation)
15. [Design Decisions and Reasoning](#15-design-decisions-and-reasoning)

---

## 1. Project Summary

**CryptoPulse** is a fully containerized data engineering pipeline that automatically
detects sudden price movements (anomalies) in cryptocurrency markets and links
them to financial news articles published around the same time.

### The Problem We're Solving

Crypto prices can move sharply within minutes. When that happens, an analyst
usually wants to know: *"What was going on in the market at that moment?"*
Right now that means manually checking a price chart and then switching to
a news site to look for context. CryptoPulse automates that correlation step.

### Important Scope Note

The system detects **temporal co-occurrence**, not causation. We're not claiming
that news caused the price move or the other way around — we're just showing
which news articles appeared around the same time as an anomaly. The
interpretation is up to the analyst. This distinction is important and we
make it clear in the report to avoid the obvious "correlation isn't causation"
objection.

### Tools Used

| Tool | What We Use It For |
|---|---|
| **PostgreSQL** | Primary storage for prices, news, anomalies, and links |
| **pgAdmin** | Database admin UI — useful for verifying data during demo |
| **Apache Airflow** | Hourly pipeline scheduling + task dependency management |
| **Elasticsearch** | Full-text news search and the data source for Kibana |
| **Kibana** | Visual dashboards on top of Elasticsearch |

The assignment requires at least 2 of the course tools — we ended up using
all 5 of them.

---

## 2. System Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                       External Data Sources                       │
│      Binance REST API (prices)       RSS Feeds (news)             │
└────────────────┬──────────────────────────┬──────────────────────┘
                 │                          │
        ┌────────▼──────────┐    ┌──────────▼──────────┐
        │   ingest_prices   │    │    ingest_news       │  ← run in parallel
        │   (Python)        │    │    (Python + VADER)  │
        └────────┬──────────┘    └──────────┬───────────┘
                 │                          │
         ┌───────▼──────────────────────────▼──────────┐
         │                 Dual-Write                   │
         │   PostgreSQL  ←──────────────→  Elasticsearch │
         │   (structured queries)      (text search + Kibana) │
         └────────────────────┬────────────────────────┘
                              │
                 ┌────────────▼──────────────┐
                 │      detect_and_link       │
                 │   rolling z-score +        │
                 │   SQL-based news linking   │
                 └────────────┬──────────────┘
                              │
                 ┌────────────▼──────────────┐
                 │      Kibana Dashboards     │
                 │  · Price time series       │
                 │  · Anomaly counts          │
                 │  · News sentiment trend    │
                 │  · Anomaly-news context    │
                 └───────────────────────────┘
```

Airflow manages the whole flow hourly. `ingest_prices` and `ingest_news` run
in parallel, and `detect_and_link` only starts after both of them finish
successfully. That dependency is the main reason we're using Airflow instead
of just cron jobs.

---

## 3. End-to-End Data Flow

### Step 1 — Price Ingestion (`ingest_prices`)

- **Source:** Binance public REST API (`/api/v3/klines`, 1-minute OHLCV candles),
  or `data/sample/prices.csv` if `DATA_MODE=sample`.
- **What it does:** Fetches the last 1,000 candles per symbol and converts
  timestamps to UTC.
- **PostgreSQL:** Writes to the `prices` table using `ON CONFLICT DO NOTHING`.
  The `(symbol, open_time)` unique constraint means re-running the pipeline
  won't create duplicate rows.
- **Elasticsearch:** Bulk-indexes the same data into the `prices` index.
  Each document gets a stable `_id` in the format `symbol_timestamp`,
  so re-indexing is also idempotent.

### Step 2 — News Ingestion (`ingest_news`)

- **Source:** RSS feeds parsed with `feedparser`, or `data/sample/news.json`
  in sample mode.
- **Sentiment:** VADER scores each article's title + summary and produces
  a compound score. Thresholds: `≥ 0.05 → positive`, `≤ -0.05 → negative`,
  anything in between → `neutral`.
- **Symbol tagging:** Regex scans the article text to detect which crypto
  assets are mentioned (e.g. `\bbitcoin\b` maps to `BTC`). Word boundaries
  prevent false matches like "subtlety" matching "btc".
- **Idempotency:** Each article gets a 32-character `article_uid` derived from
  the SHA-256 hash of its URL. `ON CONFLICT (article_uid) DO NOTHING` prevents
  the same article from being saved twice.
- **Dual-write:** Structured metadata goes to Postgres; a text-searchable copy
  goes to Elasticsearch.

### Step 3 — Anomaly Detection and News Linking (`detect_and_link`)

- **Input:** Loads the last `max(zscore_window * 4, 240)` minutes of prices
  from Postgres for each symbol. The 4× multiplier gives the rolling window
  enough warm-up data.
- **Algorithm:**
  1. Compute minute-level log-returns: `return_t = ln(close_t / close_{t-1})`.
     We use log-returns instead of simple returns because they're closer to
     normally distributed, which makes the z-score more meaningful.
  2. Apply a rolling mean and standard deviation (window = `ZSCORE_WINDOW_MINUTES`).
  3. Replace any `rolling_std == 0` with `NaN` to avoid division by zero on
     flat price windows.
  4. Compute `z = (return - rolling_mean) / rolling_std`.
  5. Flag any row where `|z| ≥ ZSCORE_THRESHOLD` as an anomaly.
- **Saving anomalies:** Written to the `anomalies` table and mirrored to
  Elasticsearch.
- **News linking:** A single SQL query using CTEs finds all unlinked anomalies,
  then joins them to news articles that (a) mention the same asset and (b) were
  published within `NEWS_LINK_WINDOW_MINUTES` of the anomaly. Results go into
  `anomaly_news_links`.
- **Kibana context index:** The joined anomaly-news data is also written to
  the `anomaly_news_context` Elasticsearch index, which powers the context
  table in Kibana.

---

## 4. Services and Containers

The full stack is 9 containers defined in a single `docker-compose.yml`:

| Container | Image | Port | Purpose |
|---|---|---|---|
| `cryptopulse-postgres` | `postgres:15` | 5433→5432 | Main relational database |
| `cryptopulse-pgadmin` | `dpage/pgadmin4:8` | 5050→80 | Postgres web UI |
| `cryptopulse-airflow-init` | custom airflow | — | One-shot DB init and admin user creation |
| `cryptopulse-airflow-webserver` | custom airflow | 8088→8080 | Airflow web UI |
| `cryptopulse-airflow-scheduler` | custom airflow | — | DAG scheduling process |
| `cryptopulse-elasticsearch` | `elasticsearch:8.13.4` | 9200→9200 | Search engine and Kibana data source |
| `cryptopulse-kibana` | `kibana:8.13.4` | 5601→5601 | Dashboard UI |
| `cryptopulse-kibana-init` | custom pipeline | — | One-shot data view bootstrap |
| `cryptopulse-pipeline` | custom pipeline | — | Team Python code (idle by default; run manually) |

### Startup Order and Dependencies

```
postgres (healthy)
    │
    ├── pgadmin
    ├── airflow-init ──► airflow-webserver
    │                └── airflow-scheduler
    │
elasticsearch (healthy)
    │
    └── kibana (healthy)
            │
            └── kibana-init
```

All Airflow containers wait for `postgres: service_healthy` because Airflow
stores its own metadata in a separate Postgres database (`cryptopulse_airflow`).
Without this condition, Airflow tries to connect before Postgres is ready and
crashes on startup.

### Why Does the Pipeline Container Exist?

Airflow already imports the team's code via a shared volume mount, so a
separate `pipeline` container isn't technically required for the pipeline to
run. We added it specifically to satisfy the assignment requirement that
"team-authored code must run in its own container." In practice, you use it
to run one-off commands with `docker compose run --rm pipeline ...`.

---

## 5. Database Schema

`sql/init.sql` is automatically loaded by Postgres on first startup via
the `/docker-entrypoint-initdb.d/` mechanism.

### `prices` Table

```sql
CREATE TABLE prices (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(20)    NOT NULL,
    open_time   TIMESTAMPTZ    NOT NULL,
    open_price  NUMERIC(20,8)  NOT NULL,
    high_price  NUMERIC(20,8)  NOT NULL,
    low_price   NUMERIC(20,8)  NOT NULL,
    close_price NUMERIC(20,8)  NOT NULL,
    volume      NUMERIC(30,8)  NOT NULL,
    ingested_at TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT prices_symbol_time_unique UNIQUE (symbol, open_time)
);
CREATE INDEX idx_prices_symbol_time ON prices (symbol, open_time DESC);
```

We use `NUMERIC(20,8)` instead of `FLOAT` because floating-point precision
loss in financial data can cause subtle but real calculation errors. The
composite index on `(symbol, open_time DESC)` speeds up the "get the last
N minutes of prices for this symbol" query that the anomaly detector runs
every hour.

### `news` Table

```sql
CREATE TABLE news (
    id               BIGSERIAL PRIMARY KEY,
    article_uid      VARCHAR(255)  NOT NULL UNIQUE,
    source           VARCHAR(64)   NOT NULL,
    title            TEXT          NOT NULL,
    summary          TEXT,
    link             TEXT          NOT NULL,
    published_at     TIMESTAMPTZ   NOT NULL,
    sentiment_score  NUMERIC(6,4),
    sentiment_label  VARCHAR(16),
    mentioned_symbols TEXT[],
    ingested_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_news_symbols ON news USING GIN (mentioned_symbols);
```

`mentioned_symbols TEXT[]` is a PostgreSQL native array type. The `@>` operator
lets us query "all articles mentioning BTC" like this:
`WHERE mentioned_symbols @> ARRAY['BTC']`. We use a GIN (Generalized Inverted
Index) for this column because GIN is much faster than a B-tree for containment
queries on arrays. Without it, this query would do a full table scan every time
the anomaly linker runs.

### `anomalies` Table

```sql
CREATE TABLE anomalies (
    id           BIGSERIAL PRIMARY KEY,
    symbol       VARCHAR(20)   NOT NULL,
    detected_at  TIMESTAMPTZ   NOT NULL,
    z_score      NUMERIC(8,4)  NOT NULL,
    return_pct   NUMERIC(10,6) NOT NULL,
    rolling_mean NUMERIC(20,8) NOT NULL,
    rolling_std  NUMERIC(20,8) NOT NULL,
    direction    VARCHAR(8)    NOT NULL,  -- 'up' or 'down'
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT anomalies_symbol_time_unique UNIQUE (symbol, detected_at)
);
```

We store `rolling_mean` and `rolling_std` alongside the z-score because
if we ever change the threshold or window size in the future, we can
recalculate which anomalies would have been flagged without re-running
the full pipeline.

### `anomaly_news_links` Table (Many-to-Many)

```sql
CREATE TABLE anomaly_news_links (
    id              BIGSERIAL PRIMARY KEY,
    anomaly_id      BIGINT  NOT NULL REFERENCES anomalies(id) ON DELETE CASCADE,
    news_id         BIGINT  NOT NULL REFERENCES news(id)      ON DELETE CASCADE,
    time_offset_min INTEGER NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT anomaly_news_links_unique UNIQUE (anomaly_id, news_id)
);
```

`time_offset_min` stores how many minutes the article was published before or
after the anomaly (negative = before, positive = after). This is useful in
Kibana for filtering and for context during the demo. `ON DELETE CASCADE`
means if an anomaly row is deleted, its link rows are also cleaned up
automatically.

---

## 6. Python Source Code

### `src/common/config.py` — Central Configuration

All environment variable reading happens here. Other modules import
`from src.common.config import settings` instead of calling `os.environ`
directly. This way, if a variable is missing or mistyped, you get an error
in one place rather than scattered `KeyError`s across multiple files.

`POSTGRES_USER` and `POSTGRES_PASSWORD` use `os.environ[]` (raises `KeyError`
if missing) because the pipeline can't work without them. Optional variables
use `os.environ.get()` with sensible defaults.

The lazy singleton (`_settings = None`) means the module can be imported
without crashing even if the environment isn't fully set up yet — Airflow
does some things during import time where the env might not be ready.

### `src/common/db.py` — Connection Management

```python
# SQLAlchemy connection pool: 5 persistent + 10 overflow connections
_engine = create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10)

# Elasticsearch client: 30s timeout, 3 retries
_es_client = Elasticsearch(url, request_timeout=30, retry_on_timeout=True, max_retries=3)
```

`pool_pre_ping=True` checks whether a connection is still alive before using
it. This prevents `OperationalError` crashes that happen when a container
restarts and the SQLAlchemy pool has stale connections from before the restart.

### `src/common/es_index.py` — Elasticsearch Index Management

Contains the explicit field mappings for all 4 indices. `ensure_indices()` is
safe to call every DAG run — it's a no-op if the index already exists.

`bulk_index()` converts Python `datetime` objects to ISO 8601 strings before
sending them to Elasticsearch. This is necessary because Elasticsearch's
Python client doesn't automatically serialize `datetime` objects and will
throw a `SerializationError` if you try.

The `id_field` parameter lets callers specify which field should be used as
`_id`. If you re-index the same document, Elasticsearch updates it in place
instead of creating a duplicate.

### `src/ingestion/ingest_prices.py` — Price Ingestion

Two modes:
- **Live:** Calls `GET /api/v3/klines` with `limit=1000` (last ~16.7 hours
  of 1-minute candles). 15-second timeout is long enough for slow networks
  but won't block the pipeline indefinitely.
- **Sample:** Reads `data/sample/prices.csv` — no internet needed.

Each symbol is fetched inside its own `try/except` block so that if one
symbol fails (e.g. Binance API error), the other four still get processed.
The failure is logged as an error but doesn't abort the whole task.

### `src/ingestion/ingest_news.py` — News Ingestion

We use `feedparser` for RSS parsing. It handles HTTP, timeouts, and malformed
feeds gracefully — it returns an empty result instead of raising an exception,
which is exactly what we want.

The `_detect_symbols()` function uses word boundary (`\b`) regex to avoid
false positives. Without `\b`, searching for "btc" would match inside words
like "abstract" or "subtlety". With it, only standalone word matches are found.

VADER runs offline with no API key and produces deterministic results across
runs. It's not as accurate as FinBERT for financial text, but it's fast, tiny
(50KB), and works without GPU — a reasonable tradeoff for this project.

### `src/analysis/detect_anomalies.py` — Anomaly Detection

**Algorithm steps in order:**

1. Load the last `lookback_minutes` of prices from Postgres for each symbol.
2. Coerce `close_price` to float using `pd.to_numeric(..., errors='coerce')`.
   This step is important: PostgreSQL `NUMERIC` columns come back as Python
   `Decimal` objects, and `np.log()` doesn't accept `Decimal`. Any row that
   can't be converted to a number is dropped.
3. Compute minute-level log-returns.
4. Compute rolling mean and std with a 60-minute window.
5. Replace any zero std with `NaN` to avoid ±∞ z-scores on flat price windows.
6. Flag rows where `|z| ≥ threshold`.

**The news-linking SQL uses a CTE structure:**

```sql
WITH unlinked AS (
    -- Find anomalies that don't have any links yet
    SELECT a.id, a.symbol, a.detected_at,
           REPLACE(REPLACE(a.symbol,'USDT',''),'USD','') AS base
    FROM anomalies a
    LEFT JOIN anomaly_news_links l ON l.anomaly_id = a.id
    WHERE l.id IS NULL
),
candidates AS (
    -- Match them to news articles in the time window mentioning the same asset
    SELECT u.id AS anomaly_id, n.id AS news_id,
           EXTRACT(EPOCH FROM (n.published_at - u.detected_at)) / 60.0 AS time_offset_min
    FROM unlinked u
    JOIN news n ON n.mentioned_symbols @> ARRAY[u.base]
               AND n.published_at BETWEEN u.detected_at - (:win || ' minutes')::interval
                                      AND u.detected_at + (:win || ' minutes')::interval
)
INSERT INTO anomaly_news_links ... ON CONFLICT DO NOTHING
```

This runs as a single atomic SQL statement — it finds unlinked anomalies,
finds matching news, and inserts the links all in one go. The `WHERE l.id IS NULL`
filter in the `unlinked` CTE means we only process new anomalies on each run,
so re-running the pipeline doesn't re-insert links that already exist.

---

## 7. Airflow DAG

**File:** `dags/cryptopulse_pipeline_dag.py`

```
ingest_prices ──┐
                ├──► detect_and_link
ingest_news  ───┘
```

| Parameter | Value | Reason |
|---|---|---|
| `schedule` | `@hourly` | Binance provides 1-minute data; hourly runs are a reasonable granularity |
| `catchup=False` | False | We don't want Airflow to try filling in past hours when the system first starts |
| `max_active_runs=1` | 1 | Prevents two pipeline runs from writing to the database at the same time |
| `retries=2` | 2 | Handles temporary network issues automatically |
| `retry_delay` | 2 minutes | Gives APIs time to recover before retrying |

**Why Airflow?** The `[ingest_prices, ingest_news] >> detect_and_link` dependency
is the core justification. The analysis task is meaningless if it runs before
the ingestion tasks finish. You could try to replicate this with shell scripts
and cron, but Airflow gives you automatic retries, a monitoring UI, and clear
failure tracking basically for free.

**`PYTHONPATH=/opt/airflow`:** This is set in the Airflow container so that
DAG code can do `from src.analysis import ...` without any sys.path hacks.
The `dags/` and `src/` directories are volume-mounted, so code changes are
picked up by the scheduler without rebuilding the image.

---

## 8. Elasticsearch Index Design

| Index | Purpose | Time Field |
|---|---|---|
| `prices` | Price time series → Kibana line chart | `open_time` |
| `news` | Full-text news search → sentiment dashboard | `published_at` |
| `anomalies` | Anomaly counts and trends → Kibana bar chart | `detected_at` |
| `anomaly_news_context` | Pre-joined anomaly + news rows for the context table | `detected_at` |

We define explicit field mappings instead of relying on Elasticsearch's
auto-detection. Auto-detection can go wrong in annoying ways — for example,
if the first document it sees has a numeric field that looks like a string,
it'll map the field as `text` and then aggregations on it will fail silently.
Explicit mappings prevent this entirely.

Two important distinctions in the mappings:
- `keyword`: used for exact-match filtering, aggregation, and sorting
  (e.g. `symbol`, `direction`, `sentiment_label`)
- `text`: used for analyzed full-text search (e.g. `title`, `summary`)

---

## 9. Kibana Setup

### Automatic Bootstrap

The `kibana-init` container runs `src/common/kibana_bootstrap.py` once
Kibana is healthy. It creates 4 data views automatically:
`prices`, `anomalies`, `news`, `anomaly_news_context`.

The script polls Kibana's `/api/status` endpoint until it returns
`"level": "available"`, with a 120-second timeout and 3-second retry
interval. Without this wait, the API calls would fail because Kibana
needs time to connect to Elasticsearch after startup.

### Checking Data Views Manually

In the Kibana UI: **Stack Management → Data Views**

If a data view is missing:
```
Stack Management → Saved Objects → Import → kibana/data_views.ndjson
```

### Dashboard Panels

**Panel 1 — Price Time Series (`prices_timeseries`)**
- Data view: `prices`
- Lens: `open_time` on x-axis, average `close_price` on y-axis,
  broken down by `symbol` → Line chart

**Panel 2 — Anomaly Count (`anomalies_per_minute`)**
- Data view: `anomalies`
- Lens: `detected_at` on x-axis, count on y-axis, broken down by `direction`
  → Stacked bar chart

**Panel 3 — Sentiment Distribution (`news_sentiment_pie`)**
- Data view: `news`
- Lens: Pie chart on `sentiment_label`

**Panel 4 — Sentiment Trend (`news_sentiment_trend`)**
- Data view: `news`
- Lens: `published_at` on x-axis, average `sentiment_score` on y-axis,
  broken down by asset → Line chart

**Panel 5 — Anomaly-News Context Table (`anomaly_news_context_table`)**
- Data view: `anomaly_news_context`
- Discover table showing linked anomalies and articles side by side

---

## 10. Environment Variables (.env)

Copy `.env.example` to `.env` before first run. The `.env` file should not
be committed to git (it's in `.gitignore`).

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_USER` | `cryptopulse` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `cryptopulse_dev_password` | Change this in any non-local deployment |
| `POSTGRES_DB` | `cryptopulse` | Main database name |
| `POSTGRES_PORT` | `5433` | Host-side port (avoids conflicts with any local Postgres on 5432) |
| `AIRFLOW_UID` | `50000` | User ID for Airflow file ownership — set to `$(id -u)` on Linux |
| `AIRFLOW_WEBSERVER_PORT` | `8088` | Airflow UI host port |
| `AIRFLOW_ADMIN_USER` | `admin` | Airflow admin username |
| `AIRFLOW_ADMIN_PASSWORD` | `admin` | Airflow admin password |
| `AIRFLOW_FERNET_KEY` | (example value) | Used to encrypt stored connection credentials |
| `PGADMIN_DEFAULT_EMAIL` | `admin@cryptopulse.local` | pgAdmin login email |
| `PGADMIN_DEFAULT_PASSWORD` | `admin` | pgAdmin login password |
| `PGADMIN_PORT` | `5050` | pgAdmin host port |
| `ELASTICSEARCH_PORT` | `9200` | Elasticsearch host port |
| `ES_JAVA_OPTS` | `-Xms512m -Xmx512m` | Heap size cap — keeps the stack runnable on a 16GB laptop |
| `KIBANA_PORT` | `5601` | Kibana host port |
| `DATA_MODE` | `live` | `live` = Binance API + RSS feeds; `sample` = local CSV/JSON files |
| `CRYPTO_SYMBOLS` | `BTCUSDT,ETHUSDT,...` | Comma-separated list of Binance trading pairs |
| `NEWS_RSS_FEEDS` | CoinDesk, Cointelegraph | Comma-separated RSS feed URLs |
| `ZSCORE_THRESHOLD` | `2.5` | Anomaly detection threshold (|z| ≥ this value = anomaly) |
| `ZSCORE_WINDOW_MINUTES` | `60` | Rolling window size in minutes |
| `NEWS_LINK_WINDOW_MINUTES` | `30` | ±30 minutes around each anomaly to search for news |

**Generating a new Fernet key:**
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## 11. Running Commands

### First-Time Setup

```bash
# 1. Clone the repo
git clone <repo-url> cryptopulse
cd cryptopulse

# 2. Create your local .env file
cp .env.example .env

# 3. Set AIRFLOW_UID to your own user ID (important on Linux to avoid volume permission issues)
echo "AIRFLOW_UID=$(id -u)" >> .env

# 4. Build images and start everything
docker compose up --build -d

# 5. Follow the startup logs (Ctrl+C exits the log view, containers keep running)
docker compose logs -f
```

After about 2 minutes all services should be healthy and the UIs reachable.

### Everyday Usage

```bash
# Start all containers (if already built)
docker compose up -d

# Stop all containers, keep data
docker compose down

# Stop all containers and delete all data volumes (full reset)
docker compose down -v

# Restart a specific service
docker compose restart airflow-scheduler

# Follow logs for a specific service
docker compose logs -f airflow-webserver
docker compose logs -f pipeline
docker compose logs -f elasticsearch
```

### Rebuilding Images

```bash
# Rebuild all custom images
docker compose build

# Rebuild only the pipeline image
docker compose build pipeline

# Rebuild from scratch (no cache) — useful if dependencies are acting weird
docker compose build --no-cache pipeline
```

### Running the Pipeline Manually

```bash
# Ingest prices
docker compose run --rm pipeline python -m src.ingestion.ingest_prices

# Ingest news
docker compose run --rm pipeline python -m src.ingestion.ingest_news

# Run anomaly detection and news linking
docker compose run --rm pipeline python -m src.analysis.detect_anomalies

# Generate fresh sample data
docker compose run --rm pipeline python -m src.ingestion.generate_sample

# Re-bootstrap Kibana data views (if they got deleted)
docker compose run --rm pipeline python -m src.common.kibana_bootstrap
```

### Getting a Shell Inside a Container

```bash
# Enter the pipeline container
docker compose exec pipeline bash

# Enter the Airflow scheduler
docker compose exec airflow-scheduler bash

# Connect to Postgres with psql
docker exec -it cryptopulse-postgres psql -U cryptopulse -d cryptopulse

# Useful psql queries once inside:
# SELECT COUNT(*) FROM prices;
# SELECT COUNT(*) FROM news;
# SELECT symbol, COUNT(*) FROM anomalies GROUP BY symbol;
# \q   <- quit
```

### Database Queries

```bash
# Check row counts across all tables
docker exec cryptopulse-postgres \
  psql -U cryptopulse -d cryptopulse \
  -c "SELECT 'prices' AS tbl, COUNT(*) FROM prices
      UNION ALL SELECT 'news', COUNT(*) FROM news
      UNION ALL SELECT 'anomalies', COUNT(*) FROM anomalies
      UNION ALL SELECT 'anomaly_news_links', COUNT(*) FROM anomaly_news_links;"

# See the 10 most recent anomalies
docker exec cryptopulse-postgres \
  psql -U cryptopulse -d cryptopulse \
  -c "SELECT symbol, detected_at, z_score, direction FROM anomalies ORDER BY detected_at DESC LIMIT 10;"

# See anomaly-news matches
docker exec cryptopulse-postgres \
  psql -U cryptopulse -d cryptopulse \
  -c "SELECT a.symbol, a.detected_at, a.direction, n.title, l.time_offset_min
      FROM anomaly_news_links l
      JOIN anomalies a ON a.id = l.anomaly_id
      JOIN news n ON n.id = l.news_id
      ORDER BY a.detected_at DESC LIMIT 5;"
```

### Elasticsearch Queries

```bash
# Cluster health
curl http://localhost:9200/_cluster/health?pretty

# List all indices with document counts
curl http://localhost:9200/_cat/indices?v

# Document count in the prices index
curl http://localhost:9200/prices/_count

# Get the 5 most recent anomalies from Elasticsearch
curl -s http://localhost:9200/anomalies/_search \
  -H "Content-Type: application/json" \
  -d '{"size": 5, "sort": [{"detected_at": "desc"}]}' | python3 -m json.tool
```

### Airflow Commands

```bash
# Airflow web UI is at http://localhost:8088
# Credentials: AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD from .env

# List all DAGs
docker compose exec airflow-webserver airflow dags list

# Trigger the pipeline DAG manually (instead of waiting for the hourly schedule)
docker compose exec airflow-webserver airflow dags trigger cryptopulse_pipeline

# Test a single task without writing to Airflow's metadata database
docker compose exec airflow-scheduler \
  airflow tasks test cryptopulse_pipeline ingest_prices 2026-05-01
```

### Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8088 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env` |
| Kibana | http://localhost:5601 | No login (security disabled) |
| pgAdmin | http://localhost:5050 | `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD` from `.env` |
| Elasticsearch | http://localhost:9200 | No login (`xpack.security.enabled=false`) |
| PostgreSQL | `localhost:5433` | `POSTGRES_USER` / `POSTGRES_PASSWORD` from `.env` |

---

## 12. Health Check

### Automated Script

```bash
# Make the script executable the first time
chmod +x scripts/health_check.sh

# Run the full health check
./scripts/health_check.sh
```

The script checks:
1. **Container status:** Is each container running and passing its Docker healthcheck?
2. **HTTP endpoints:** Do Airflow, Elasticsearch, Kibana, and pgAdmin return 2xx/3xx?
3. **Postgres row counts:** How many rows are in `prices`, `news`, `anomalies`, and `anomaly_news_links`?
4. **Elasticsearch document counts:** Are there documents in all 4 indices?

Output is color-coded: green = OK, yellow = warning (0 rows), red = failure.

### Quick Manual Checks

```bash
# Status of all containers
docker compose ps

# Elasticsearch cluster health (one-liner)
curl -s http://localhost:9200/_cluster/health | python3 -m json.tool

# Airflow health endpoint
curl http://localhost:8088/health
```

---

## 13. Test Suite

**File:** `tests/test_anomalies.py`

Three unit tests, all targeting the pure-function parts of the anomaly
detector — no Postgres or Elasticsearch required:

| Test | What It Checks |
|---|---|
| `test_obvious_spike_is_flagged` | A synthetic flat series with a 5% injected jump is correctly flagged as `direction=up` |
| `test_calm_series_has_no_anomalies` | A noisy but normal series doesn't produce false positives at a high threshold |
| `test_too_short_history_returns_empty` | A series shorter than the rolling window returns an empty result, no crash |

```bash
# Run inside the pipeline container (recommended)
docker compose run --rm pipeline pytest tests/ -v

# Run a specific test
docker compose run --rm pipeline pytest tests/test_anomalies.py::test_obvious_spike_is_flagged -v
```

---

## 14. Sample Data Generation

If you want to run in `DATA_MODE=sample` mode (offline, no internet), you
need the sample files. The repo ships with pre-built ones, but you can
regenerate them with timestamps from today if needed (the anomaly detector
filters by recency, so old timestamps in the sample data won't show anomalies):

```bash
docker compose run --rm pipeline python -m src.ingestion.generate_sample
```

This generates:
- **`data/sample/prices.csv`:** 5 symbols × 3 days × 1,440 minutes = 21,600
  rows of OHLCV data, simulated using Geometric Brownian Motion. Each symbol
  has 6 injected price spikes so the anomaly detector has something to find.
- **`data/sample/news.json`:** 300 articles with synthetic but plausible
  headlines (positive, negative, neutral), distributed uniformly across the
  same time range as the price data.

Then set `DATA_MODE=sample` in `.env` and restart.

---

## 15. Design Decisions and Reasoning

### Dual-Write (PostgreSQL + Elasticsearch)

We write everything to both stores. This adds some complexity but each
store earns its place:

- **PostgreSQL** handles the things it's good at: JOIN queries (anomaly-news
  linking), ACID guarantees, NUMERIC precision for financial values, and
  `ON CONFLICT` for idempotency.
- **Elasticsearch** handles what Kibana needs: time-series aggregations and
  full-text search across article titles and summaries.

We could have avoided Elasticsearch by using PostgreSQL's built-in full-text
search and querying it from Grafana instead of Kibana. We chose the
Elasticsearch + Kibana pair because Kibana's native integration with its
own data source is much simpler to configure, and Elasticsearch was one of
the required course tools anyway.

### Idempotent Pipeline

Every `INSERT` statement uses `ON CONFLICT DO NOTHING`, and every
Elasticsearch document gets a deterministic `_id`. The result is that
running the pipeline multiple times on the same time window produces
exactly the same database state as running it once. This matters because:

- Airflow retries failed tasks — we don't want retries to create duplicate rows.
- You can reset the database with `docker compose down -v` and re-run
  without any special cleanup logic.

### Sample Mode

Being able to demo the project without live internet access is important.
The synthetic price data is generated using Geometric Brownian Motion with
injected spikes, so it looks realistic enough for a demo. Switching between
live and sample mode is just a `DATA_MODE=` change in `.env` with no code
changes required.

### LocalExecutor for Airflow

We use LocalExecutor instead of CeleryExecutor or KubernetesExecutor.
Our DAG has at most 2 parallel tasks running at the same time and everything
runs on a single machine, so there's no need for a distributed task queue.
LocalExecutor saves us from adding Redis or RabbitMQ to the stack, which
would add memory overhead and one more potential failure point.

### Elasticsearch Security Disabled

`xpack.security.enabled=false` is set for development convenience — it means
Kibana and our Python code can connect without authentication, which simplifies
setup significantly. In any real deployment you'd want to enable TLS and
set up proper credentials, but that's outside the scope of this project.

### `ES_JAVA_OPTS="-Xms512m -Xmx512m"`

By default, Elasticsearch claims half the available RAM for its JVM heap.
On a 16GB machine that would be 8GB, leaving barely enough for the rest
of the stack. We cap the heap at 512MB, which is plenty for a dataset of
this size and keeps the total memory usage around 4–5GB.

---
