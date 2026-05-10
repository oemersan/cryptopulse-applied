# CryptoPulse — Internal Technical Documentation

> This document is for the team. It explains every file in the repository,
> every design decision we made, and the reasoning behind it. The goal is
> that any team member should be able to defend any part of the system
> during the demo Q&A — even parts they didn't personally write.
>
> Keep this in `docs/internal_documentation.md`. It is referenced by the
> final report but written in a more conversational, in-depth style.

---

## Table of Contents

1. [What we built and why](#1-what-we-built-and-why)
2. [Architecture overview](#2-architecture-overview)
3. [The data flow, end to end](#3-the-data-flow-end-to-end)
4. [Repository layout](#4-repository-layout)
5. [Configuration files](#5-configuration-files)
6. [Docker setup](#6-docker-setup)
7. [Database schema (`sql/init.sql`)](#7-database-schema-sqlinitsql)
8. [Python source code (`src/`)](#8-python-source-code-src)
9. [Airflow DAG (`dags/`)](#9-airflow-dag-dags)
10. [Kibana setup](#10-kibana-setup)
11. [Tests](#11-tests)
12. [Scripts](#12-scripts)
13. [Design decisions, in detail](#13-design-decisions-in-detail)
14. [Anticipated demo questions and answers](#14-anticipated-demo-questions-and-answers)

---

## 1. What we built and why

### The problem

Cryptocurrency prices move sharply within minutes. When a sudden move
happens, an investor or analyst typically wants to ask: *"What was going
on around the market at this moment?"* Today this is done manually:
people open a chart, see a spike, then go to a news site and try to
correlate.

We built a system that automates that correlation: it ingests prices and
news in parallel, flags unusual price movements automatically, and shows
both side by side in a dashboard.

### The deliberate scope

We do **not** claim that news *causes* price moves. That would require
proper causal inference (counterfactuals, instrumental variables, etc.)
and is not what this course is about. Our system produces *temporal
co-occurrence*: "this anomaly happened, here are articles published in
the same window mentioning the same asset." The interpretation is left
to the analyst. This wording shows up in the abstract and the report,
and it's important because it pre-empts the obvious "but correlation
isn't causation" objection.

### What the rubric asks for

The course assignment requires:

- A **fully containerized** stack (Docker Compose, single command).
- Meaningful use of **at least 2** of the 6 course tools (Postgres,
  pgAdmin, NiFi, Elasticsearch, Kibana, Airflow). We use **5**:
  Postgres, pgAdmin, Airflow, Elasticsearch, Kibana.
- At least **10,000 records** in a real run.
- An **automated** end-to-end flow.
- Reasonable **failure recovery**.
- Runs on a typical student laptop (16 GB RAM).

We hit all of these. Concretely: a single `docker compose up --build`
brings up 8 containers; a hourly DAG runs three tasks; in a sample run
we ingest ~21,600 price rows and 300 news items.

---

## 2. Architecture overview

The system has four layers that map cleanly to four classic data-platform
concerns:

| Layer | Responsibility | Tools |
|---|---|---|
| **Ingestion** | Pull from external sources, normalize, write to storage | Python, `requests`, `feedparser`, VADER |
| **Storage** | Persist structured + unstructured data, allow queries | Postgres, Elasticsearch |
| **Orchestration** | Schedule, retry, manage dependencies | Airflow |
| **Presentation** | Visual exploration | pgAdmin, Kibana |

We keep these layers independent so that a change in one (say, swapping
Postgres for ClickHouse, or replacing VADER with FinBERT) does not force
changes in the others. This is one of the implicit asks under the
rubric's "well-separated layers" gold standard.

---

## 3. The data flow, end to end

```
Binance public API ─────┐
                        │
                        ▼
                ┌──────────────────┐
                │ ingest_prices    │── writes to Postgres `prices`
                │ task             │── writes to Elasticsearch `prices`
                └──────────────────┘
                                                          ─┐
RSS feeds (CoinDesk, ─────┐                                │
Cointelegraph)            │                                │
                          ▼                                │  parallel
                  ┌──────────────────┐                     │
                  │ ingest_news task │── VADER sentiment   │
                  │                  │── symbol detection  │
                  │                  │── writes to Postgres│
                  │                  │   `news`            │
                  │                  │── writes to ES      │
                  │                  │   `news`            │
                  └──────────────────┘                    ─┘
                          │
                          ▼
              ┌───────────────────────┐
              │ detect_and_link task  │── rolling z-score on
              │                       │   Postgres `prices`
              │                       │── writes anomalies
              │                       │   to Postgres + ES
              │                       │── joins anomalies with
              │                       │   nearby news (SQL)
              │                       │── writes
              │                       │   anomaly_news_links
              └───────────────────────┘
                          │
                          ▼
                  ┌────────────────────┐
                  │ Kibana dashboards  │── queries Elasticsearch
                  │ pgAdmin            │── queries Postgres
                  └────────────────────┘
```

The arrow that matters most for grading is the dependency between the
ingestion tasks and `detect_and_link`. The analysis is *meaningless*
until both upstream sources have produced rows for the current window —
that's exactly the kind of dependency Airflow exists to manage.

---

## 4. Repository layout

```
cryptopulse/
├── docker-compose.yml          # Brings up the whole stack
├── .env.example                # Template for secrets/ports/params
├── .env                        # Real values (gitignored)
├── .gitignore
├── .gitattributes              # Forces LF line endings cross-platform
├── README.md                   # Project landing page
├── LICENSE                     # MIT
│
├── docker/
│   ├── airflow/
│   │   ├── Dockerfile          # Adds our Python deps to Airflow base
│   │   └── requirements.txt
│   └── pipeline/
│       ├── Dockerfile          # Standalone container for our code
│       └── requirements.txt
│
├── dags/
│   └── cryptopulse_pipeline_dag.py  # 1 DAG, 3 tasks
│
├── src/
│   ├── __init__.py
│   ├── common/
│   │   ├── __init__.py
│   │   ├── config.py           # Reads .env, validates, exposes settings
│   │   ├── db.py               # Postgres + Elasticsearch connections
│   │   ├── es_index.py         # ES index mappings + bulk-write helper
│   │   └── kibana_bootstrap.py # Auto-creates Kibana data views
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── ingest_prices.py    # Binance API → Postgres + ES
│   │   ├── ingest_news.py      # RSS feeds → Postgres + ES (with sentiment)
│   │   └── generate_sample.py  # Synthesizes offline test data
│   └── analysis/
│       ├── __init__.py
│       └── detect_anomalies.py # Z-score + linking
│
├── sql/
│   └── init.sql                # Postgres schema, runs on first start
│
├── kibana/
│   └── data_views.ndjson       # Manual import backup for data views
│
├── data/
│   └── sample/                 # Generated synthetic data lives here
│
├── docs/
│   ├── walkthrough.md          # First-run guide
│   ├── kibana_dashboard_guide.md
│   └── internal_documentation.md   # this file
│
├── scripts/
│   └── health_check.sh         # One-command system status report
│
└── tests/
    └── test_anomalies.py       # Unit tests for z-score logic
```

---

## 5. Configuration files

### `.env.example` and `.env`

The `.env.example` file is committed to git and serves as a template;
`.env` is created by each developer locally (`cp .env.example .env`) and
is gitignored. This pattern is standard for a few reasons:

- **No secrets in git.** Even though our defaults are `admin / admin`
  and obviously not production-grade, getting in the habit is what
  matters.
- **Per-machine overrides.** A team member whose port 5432 is taken can
  set `POSTGRES_PORT=5434` without touching code.
- **Single source of truth.** Every container reads from the same `.env`,
  so we never hardcode the same value in multiple places.

Key variables and what they do:

| Variable | Purpose |
|---|---|
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | Postgres credentials |
| `POSTGRES_PORT` | Host-side port mapping (5433 to avoid clashing with a local Postgres) |
| `PGADMIN_*` | pgAdmin admin login |
| `AIRFLOW_FERNET_KEY` | Encrypts Airflow connections; pre-generated for reproducibility |
| `AIRFLOW_ADMIN_USER/PASSWORD` | Login for the Airflow web UI |
| `ES_JAVA_OPTS=-Xms512m -Xmx512m` | Caps Elasticsearch heap. Default is 1GB+, too much for a student laptop |
| `DATA_MODE` | `live` (Binance + RSS) or `sample` (offline files) |
| `CRYPTO_SYMBOLS` | Which Binance pairs to ingest |
| `NEWS_RSS_FEEDS` | Comma-separated RSS URLs |
| `ZSCORE_THRESHOLD` | Anomaly cutoff. 2.5 ≈ p99 in normal-ish data |
| `ZSCORE_WINDOW_MINUTES` | How long a rolling window we use for mean/std |
| `NEWS_LINK_WINDOW_MINUTES` | How far before/after an anomaly we look for related news |

### `.gitignore`

Standard Python project ignores (`__pycache__`, `.venv`, etc.) plus:

- `.env` (real secrets)
- `airflow.db`, `airflow.cfg`, `logs/` (Airflow runtime artifacts)
- `data/raw/`, `data/processed/` (we keep only `data/sample/` versioned)
- `postgres-data/`, `elasticsearch-data/` (any stray bind-mount dirs)

### `.gitattributes`

```
* text=auto eol=lf
*.sh   text eol=lf
```

This forces line endings to LF on checkout, regardless of OS. Without
this, a team member on Windows would commit `.sh` files with CRLF
endings, which then fail inside Linux containers with errors like
`/bin/bash^M: bad interpreter`. We have a Mac, a Windows, and a Linux
machine on the team, so this is non-optional.

---

## 6. Docker setup

### `docker-compose.yml`

This is the file the grader will spend the most time looking at. Eight
services, all defined in one file:

| Service | Image | Role |
|---|---|---|
| `postgres` | `postgres:15` | Relational store + Airflow metadata |
| `pgadmin` | `dpage/pgadmin4:8` | DB admin UI |
| `airflow-init` | (custom) | One-shot DB migration + admin user |
| `airflow-webserver` | (custom) | UI on port 8088 |
| `airflow-scheduler` | (custom) | Runs DAGs |
| `elasticsearch` | `8.13.4` | Search index |
| `kibana` | `8.13.4` | Dashboards |
| `kibana-init` | (custom) | One-shot data-view creation |
| `pipeline` | (custom) | Standalone container for our code |

A few patterns worth noting:

**1. YAML anchors for shared Airflow config.**

```yaml
x-airflow-common: &airflow-common
  build: ./docker/airflow
  environment: ...
  volumes: ...
```

This block is referenced from `airflow-init`, `airflow-webserver`,
`airflow-scheduler` via `<<: *airflow-common`. Keeps the three services
in lockstep without copy-paste.

**2. Healthchecks instead of fixed waits.**

`postgres` has a `pg_isready` healthcheck. `airflow-init` depends on
`postgres: condition: service_healthy`, so it waits properly. We do not
use `sleep 30`-style hacks anywhere.

**3. Two custom images, not one.**

`docker/airflow/Dockerfile` extends Apache's official Airflow image with
our Python dependencies. `docker/pipeline/Dockerfile` is a slim
`python:3.11-slim` image that hosts our team-authored code as its own
container. The latter exists specifically to satisfy the rubric's
"team-authored Python code must run in its own container" requirement.

The two `requirements.txt` files are intentionally identical — both
environments need the same libraries (pandas, requests, feedparser,
vaderSentiment, elasticsearch, etc.).

**4. Named volumes for persistence.**

`postgres-data`, `pgadmin-data`, `airflow-logs`, `elasticsearch-data`
are declared as named volumes. This means data survives `docker compose
down` (only `down -v` blows it away), which matches the rubric's
"persistent data must use named volumes" wording.

**5. Explicit bridge network.**

All services share `cryptopulse-net`. They reach each other by service
name (e.g. `postgres:5432`, `elasticsearch:9200`), never via host IP or
`localhost`. This is what the rubric calls out as "inter-service
communication must use the Docker network".

### Why Postgres also stores Airflow metadata

We point Airflow at the same Postgres instance we use for application
data. That's why pgAdmin shows ~46 tables instead of just our 4: the
Airflow metadata tables (`dag`, `task_instance`, `xcom`, `ab_user`,
etc.) are mixed in.

In production, Airflow's metadata DB is usually separated. We made the
trade-off because:

- It's one fewer container running, less memory pressure on the laptop.
- Both DBs are dev-grade with the same credentials anyway.
- A grader can immediately verify "yes, Airflow has its own state" by
  querying any `task_instance` row.

This is worth saying out loud in the demo if asked, because it shows we
made a *conscious* trade-off rather than missing the issue.

---

## 7. Database schema (`sql/init.sql`)

Postgres runs every `.sql` file under `/docker-entrypoint-initdb.d/` on
first startup. We mount our `sql/init.sql` there. The file creates four
tables:

### `prices`

```sql
CREATE TABLE prices (
    id           BIGSERIAL PRIMARY KEY,
    symbol       VARCHAR(20)    NOT NULL,
    open_time    TIMESTAMPTZ    NOT NULL,
    open_price   NUMERIC(20, 8) NOT NULL,
    high_price   NUMERIC(20, 8) NOT NULL,
    low_price    NUMERIC(20, 8) NOT NULL,
    close_price  NUMERIC(20, 8) NOT NULL,
    volume       NUMERIC(30, 8) NOT NULL,
    ingested_at  TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT prices_symbol_time_unique UNIQUE (symbol, open_time)
);
```

**Why these columns?** Binance returns OHLCV (open/high/low/close/volume)
candles. Standard format, we match it 1:1.

**Why `NUMERIC(20, 8)` instead of `FLOAT`?** Binary floats lose precision
at the 17th significant digit, which matters for prices like
`0.00045231` (XRP-style). NUMERIC keeps exact decimal representation.

**Why `UNIQUE (symbol, open_time)`?** Idempotency. Re-ingesting the same
candle twice is harmless because the constraint kicks in. The ingestion
SQL uses `ON CONFLICT (symbol, open_time) DO NOTHING`.

**Why an index on `(symbol, open_time DESC)`?** The analysis query
fetches "the last N minutes of prices for symbol X" — exactly this
shape.

### `news`

```sql
CREATE TABLE news (
    id                BIGSERIAL PRIMARY KEY,
    article_uid       VARCHAR(255) NOT NULL UNIQUE,
    source            VARCHAR(64)  NOT NULL,
    title             TEXT         NOT NULL,
    summary           TEXT,
    link              TEXT         NOT NULL,
    published_at      TIMESTAMPTZ  NOT NULL,
    sentiment_score   NUMERIC(6, 4),
    sentiment_label   VARCHAR(16),
    mentioned_symbols TEXT[],
    ingested_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

**`article_uid`** — sha256 hash of the article URL. Used as a stable
unique key for both Postgres (`UNIQUE`) and Elasticsearch (`_id`). This
is how the same article isn't ingested twice across runs.

**`mentioned_symbols TEXT[]`** — Postgres array. We chose this over a
separate join table because (a) it's a small set bounded by our
configured symbols, and (b) the GIN index makes containment queries
(`@> ARRAY['BTC']`) very fast. The trade-off is less normalized; that's
fine here.

**`sentiment_score NUMERIC(6, 4)`** — VADER returns a "compound" score in
[-1, 1] with 4 decimals of useful precision.

**`sentiment_label`** — derived from the score (>=0.05 positive, <=-0.05
negative, else neutral). We could compute this on read, but storing it
makes Kibana's pie chart trivial.

### `anomalies`

```sql
CREATE TABLE anomalies (
    id           BIGSERIAL PRIMARY KEY,
    symbol       VARCHAR(20)   NOT NULL,
    detected_at  TIMESTAMPTZ   NOT NULL,
    z_score      NUMERIC(8, 4) NOT NULL,
    return_pct   NUMERIC(10, 6) NOT NULL,
    rolling_mean NUMERIC(20, 8) NOT NULL,
    rolling_std  NUMERIC(20, 8) NOT NULL,
    direction    VARCHAR(8)    NOT NULL,
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT anomalies_symbol_time_unique UNIQUE (symbol, detected_at)
);
```

**Why store `rolling_mean` and `rolling_std`?** Two reasons:

1. *Reproducibility.* If we change the window or threshold later, having
   the historical context lets us understand whether old anomalies
   would still fire under the new params.
2. *Demo storytelling.* "This was 3.2 standard deviations above a mean
   that was nearly zero, so it was a real spike not just noise" is a
   stronger explanation than "z=3.2".

### `anomaly_news_links`

```sql
CREATE TABLE anomaly_news_links (
    id              BIGSERIAL PRIMARY KEY,
    anomaly_id      BIGINT NOT NULL REFERENCES anomalies(id) ON DELETE CASCADE,
    news_id         BIGINT NOT NULL REFERENCES news(id)      ON DELETE CASCADE,
    time_offset_min INTEGER NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT anomaly_news_links_unique UNIQUE (anomaly_id, news_id)
);
```

**This is the join table** that materializes the many-to-many between
anomalies and news. One anomaly can match multiple articles in its
window; one article can match multiple anomalies (e.g. a market-wide
event). `time_offset_min` is signed: negative means the news preceded
the anomaly, positive means it followed.

---

## 8. Python source code (`src/`)

### `src/common/config.py`

A `Settings` dataclass loaded once from environment variables. Everything
else imports from here:

```python
from src.common.config import settings
cfg = settings()
cfg.zscore_threshold  # -> 2.5
```

**Why a dataclass and not just `os.environ.get(...)` everywhere?**
Centralizing env access has two benefits: (1) we validate types once
(`int(...)`, `float(...)`) instead of letting bad types crash deep in
the code, and (2) we get a single grep target for "what configuration
does this project read?".

### `src/common/db.py`

Connection helpers. Two singletons:

- `get_engine()` — SQLAlchemy `Engine` connected to Postgres, with
  `pool_pre_ping=True` so dropped connections are detected automatically.
- `get_es()` — `Elasticsearch` client.

We also expose `get_session()` as a context manager that handles
`commit()` / `rollback()` correctly — but in practice most of our writes
go through the lower-level `engine.begin()` because we use raw SQL with
`ON CONFLICT` clauses.

### `src/common/es_index.py`

Index management for Elasticsearch:

```python
ensure_indices()       # idempotent: create indices if missing
bulk_index(name, docs) # bulk-write a list of dicts
```

Three constants — `INDEX_NEWS`, `INDEX_PRICES`, `INDEX_ANOMALIES` — keep
the index names in one place so ingestion and analysis can't drift.

The `_MAPPINGS` dict is **explicit on purpose**. ES can auto-detect
field types from the first document, but we've been bitten in the past
by numbers being indexed as strings (because the first batch happened
to come in stringly typed). Defining the mapping up front is dull but
correct.

`bulk_index` returns the number of docs that succeeded. We log this so
silent failures (e.g. mapping conflicts) are visible.

> **Bug we hit and fixed:** the original implementation copied `_id`
> into both the action header *and* the `_source` body. Some ES versions
> reject this. The fix: `_source = {k: v for k, v in doc.items() if k
> != id_field}`. A 1-line change that took 30 minutes to find because
> `bulk(...)` was called with `raise_on_error=False`, which silently
> swallowed the failures. We added a log line for the error count after
> that.

### `src/common/kibana_bootstrap.py`

A tiny script that calls Kibana's HTTP API to create three data views
(`prices`, `anomalies`, `news`) with the correct time field. It runs
once via the `kibana-init` container in compose.

Without this, every team member would manually click through Kibana's
UI on every fresh start. With it, dashboards work immediately after
`docker compose up`.

It polls `/api/status` until Kibana reports "available" before issuing
the create requests, because Kibana takes ~1-2 minutes to start.

### `src/ingestion/ingest_prices.py`

Pulls minute-level OHLCV candles for each configured symbol.

```python
def run() -> None:
    for symbol in cfg.crypto_symbols:
        df = _fetch_live(symbol) if cfg.data_mode == "live" else _read_sample(symbol)
        _upsert(df)        # → Postgres
        _index_es(df)      # → Elasticsearch
```

**Live mode** hits `https://api.binance.com/api/v3/klines` with `limit=1000`,
which gives us the most recent ~16 hours of 1-minute candles. The API
is public, no auth needed. We do not use authenticated endpoints to
keep the project free of secrets.

**Sample mode** reads `data/sample/prices.csv`, generated by
`generate_sample.py`. This is the demo fallback for the (rubric-required)
"works without internet" scenario.

**Postgres write** is `INSERT ... ON CONFLICT (symbol, open_time) DO
NOTHING`, so re-runs are safe.

**Elasticsearch write** uses a deterministic `_id` of
`{symbol}_{unix_timestamp}`, so re-runs overwrite the same document
instead of duplicating.

### `src/ingestion/ingest_news.py`

Most interesting ingestion module. It does four things in one pass:

1. **Parse** RSS feeds with `feedparser`.
2. **Score sentiment** with VADER (`SentimentIntensityAnalyzer`).
3. **Detect mentioned symbols** via simple keyword matching against a
   curated dictionary (e.g. `BTC` matches "bitcoin" or "btc" with word
   boundaries).
4. **Dual-write** to Postgres and Elasticsearch in one call.

We chose VADER over a transformer-based model (FinBERT, distilroberta-finance,
etc.) deliberately:

- **VADER is ~50KB, runs CPU-only, no warmup.** FinBERT pulls a 400+MB
  model and needs noticeable RAM. On a 16GB laptop where ES already
  takes 1GB, that's painful.
- **Container size matters.** Our pipeline image with VADER is well under
  500MB total. With FinBERT, multiply that by 2-3.
- **For headlines, VADER does the job.** It's a lexicon model
  specifically tuned for short, opinionated text. Crypto news headlines
  are short and opinionated.

The downside: VADER doesn't know finance-specific vocabulary ("bear",
"bull" land at the wrong meanings sometimes). We accept this trade-off
and call it out as a limitation in the report.

**Symbol detection** uses regex with `\b...\b` word boundaries:

```python
SYMBOL_KEYWORDS = {
    "BTC": ["bitcoin", "btc"],
    "ETH": ["ethereum", "eth", "ether"],
    ...
}
```

This is intentionally simple. A proper NER model would be better but
overkill for the scope.

### `src/ingestion/generate_sample.py`

Generates synthetic price and news data for offline runs. Three points
worth knowing:

1. **Geometric Brownian motion** for price walks. This is the standard
   stochastic process used to simulate stock prices in textbooks; it's
   not realistic at the microstructure level but it's correct on the
   macro shape.
2. **Injected spikes.** We deliberately add ~6 large jumps per symbol
   so the analysis DAG has anomalies to find. Without them the demo
   shows "0 anomalies", which would be a confusing result even though
   the system is working.
3. **`SEED = 42`.** Fixed RNG seed → the same sample data every time.
   Reproducibility for tests and screenshots.

### `src/analysis/detect_anomalies.py`

The most "data-engineering" module. Three sub-steps:

**Step 1: Load recent prices for each symbol.**

```python
SELECT open_time, close_price
FROM prices
WHERE symbol = :symbol
  AND open_time >= NOW() - make_interval(mins => :lookback)
ORDER BY open_time
```

We load `max(window * 4, 240)` minutes of history. The factor 4 ensures
the rolling window is fully populated even after a fresh start.

**Step 2: Compute the rolling z-score.**

```python
df["return"]       = log(close / close.shift(1))
df["rolling_mean"] = df["return"].rolling(window).mean()
df["rolling_std"]  = df["return"].rolling(window).std()
df["z_score"]      = (df["return"] - df["rolling_mean"]) / df["rolling_std"]
flagged = df[df["z_score"].abs() >= threshold]
```

**Why log returns instead of raw price changes?** Log returns are
additive over time and approximately normally distributed for small
moves, so a z-score on them is meaningful. Raw price differences are
not comparable across assets (a $1 move means everything for ADA, almost
nothing for BTC).

**Why a rolling window instead of a global mean/std?** Volatility
changes over time. A 60-minute window adapts to the current regime;
a global one would be dragged around by historical periods.

**Step 3: Insert anomalies and link to news.**

The link is a single SQL statement:

```sql
WITH unlinked AS (
    SELECT a.id, a.symbol, a.detected_at,
           REPLACE(REPLACE(a.symbol, 'USDT', ''), 'USD', '') AS base
    FROM anomalies a
    LEFT JOIN anomaly_news_links l ON l.anomaly_id = a.id
    WHERE l.id IS NULL
),
candidates AS (
    SELECT u.id AS anomaly_id, n.id AS news_id, ...
    FROM unlinked u
    JOIN news n ON n.mentioned_symbols @> ARRAY[u.base]
                AND n.published_at BETWEEN u.detected_at - interval
                                       AND u.detected_at + interval
)
INSERT INTO anomaly_news_links (...) SELECT ... FROM candidates;
```

**Why all in SQL?** Doing this in Python would mean pulling all
unlinked anomalies, fetching candidate news, computing the join in
pandas, and pushing it back. Postgres can do the whole thing in one
query, including the symbol-array containment check via the GIN index.

**Why the `WHERE l.id IS NULL` filter?** So we only link new anomalies
on each run; existing links are untouched. Idempotency.

**Why `REPLACE(REPLACE(symbol, 'USDT', ''), 'USD', '')`?** Because the
symbol in `prices`/`anomalies` is `BTCUSDT` (Binance trading pair) but
the `mentioned_symbols` column in `news` stores just `BTC` (the base
asset). We strip the quote currency on the fly.

### `src/analysis/__init__.py` and other `__init__.py`

Empty marker files so Python treats each directory as a package.
Required for the `from src.foo.bar import baz` imports to work.

---

## 9. Airflow DAG (`dags/`)

### `dags/cryptopulse_pipeline_dag.py`

One DAG, three tasks, one dependency:

```python
[ingest_prices, ingest_news] >> detect_and_link
```

**Why one DAG instead of three?** The earlier scaffold had three
separate DAGs. We collapsed them after realizing that `detect_and_link`
is *meaningless* until both ingestion tasks have produced rows for the
current window. A separate DAG would either (a) run on a worse signal
("just hope it's been long enough") or (b) need an `ExternalTaskSensor`,
which is more complex than a real dependency.

This is, in fact, the whole point of Airflow: **dependency-aware
scheduling**. Three independent DAGs would give us nothing that cron
+ shell scripts can't, which is a question we expect during the demo.

The DAG is `@hourly`. With `max_active_runs=1`, even if one run takes
unusually long, we never have two overlapping runs of the same DAG.

`retries=2` on each task with `retry_delay=timedelta(minutes=2)`. Two
retries is enough for transient API hiccups; more would mask real
problems.

`catchup=False` because we don't want Airflow to backfill all the hours
since `start_date`. We start fresh each time.

`doc_md` blocks on each task render in the Airflow UI when you click
the task — useful documentation that lives next to the code.

---

## 10. Kibana setup

### `kibana/data_views.ndjson`

A backup file. If `kibana-init` (the auto-bootstrap) ever fails, anyone
can manually go to **Stack Management → Saved Objects → Import** and
load this file.

### `docs/kibana_dashboard_guide.md`

A click-by-click guide to building the demo dashboards from scratch.
Why is this manual instead of automated?

We tried automating Kibana dashboards. Lens visualizations in 8.x serialize
to a complicated JSON structure with internal IDs that change between
versions. Hand-writing this JSON is fragile; importing somebody else's
JSON sometimes fails on subtle version mismatches.

The pragmatic call: ship the data views automatically (those are simple
and stable), then guide the team through the 10–15 minutes of clicking
that builds the dashboards. After they're built once, anyone can export
them to `kibana/dashboards.ndjson` and re-import them on another
machine.

This is also more honest: if a grader asks "is the dashboard
team-built?", the answer is yes, and we can show the construction
without claiming an AI generated visualization JSON we didn't audit.

---

## 11. Tests

### `tests/test_anomalies.py`

Three unit tests covering `_compute_anomalies`:

1. **An obvious 5% spike at the end of a flat series is flagged.**
   Sanity: the algorithm catches what it should.
2. **A purely calm series with a high threshold flags nothing.**
   Sanity in the other direction: no false positives on noise.
3. **A series shorter than the window returns empty without crashing.**
   Defensive: the function doesn't error on cold-start.

These are the testable parts because they don't need Postgres. Database
integration tests would need a test container, which is out of scope.

We don't aim for coverage; we aim for confidence on the one piece of
math that's actually domain-specific.

---

## 12. Scripts

### `scripts/health_check.sh`

A bash script that prints a one-screen health report:

- Are all expected containers running and healthy?
- Do Airflow / Postgres / Elasticsearch / Kibana / pgAdmin respond on
  their HTTP ports?
- How many rows are in each Postgres table?
- How many docs are in each Elasticsearch index?

Output is color-coded (green OK / yellow WARN / red FAIL), and the
script exits non-zero if anything failed — so it's CI-friendly too.

This is the first thing to run after `docker compose up --build -d`,
and the first thing to ask a teammate to run when they say "it doesn't
work for me".

---

## 13. Design decisions, in detail

This section reads like a list of "we considered X, chose Y, here's
why". The grading rubric specifically asks for tool choices to be
"justified through comparisons", and the demo Q&A will probe these.

### Postgres vs. another SQL database

**Considered:** MySQL, SQLite, ClickHouse.

**Chose:** Postgres.

**Why:** PG arrays + GIN indexes for the `mentioned_symbols` column;
strong ON CONFLICT support for idempotent upserts; first-class
TIMESTAMPTZ type for our time series. SQLite would have been simpler
but doesn't fit a containerized multi-process architecture. ClickHouse
would be faster for analytical queries but the operational overhead
isn't justified at our scale.

### Elasticsearch vs. just-Postgres for news

**Considered:** Just storing news in Postgres and using its full-text
search.

**Chose:** Postgres + Elasticsearch (dual write).

**Why:** Postgres FTS is good but (a) the rubric explicitly lists ES as
a course tool to use meaningfully, (b) Kibana's tight integration with
ES makes the dashboards much easier than wiring a third-party plotting
tool to Postgres, and (c) ES is what people actually reach for in
industry for this kind of layer.

The dual-write pattern adds complexity: now two systems can disagree.
We accept this because re-indexing is cheap (re-run the ingestion DAG)
and ES isn't the system of record — Postgres is.

### Airflow vs. cron + shell scripts

**Considered:** Three cron jobs running shell scripts that call our
Python.

**Chose:** Airflow.

**Why:** Once we have a real dependency between ingestion and analysis,
cron alone can't enforce it without ad-hoc lock files or sentinel
checks. Airflow gives us that for free, plus a UI showing run history,
logs, and retries. The course also lists Airflow as a tool to use
meaningfully.

### VADER vs. transformer model for sentiment

Already covered in the `ingest_news.py` section: we chose VADER for
size, speed, and CPU-only runtime. We disclose the trade-off (no
finance-specific vocabulary) as a limitation.

### Z-score vs. ML anomaly detection

**Considered:** Isolation forests, autoencoders, LSTM-based.

**Chose:** Rolling z-score on log returns.

**Why:** Three reasons.

1. **Defensibility.** Every step of a z-score is statistically
   transparent; there are no learned parameters to debug. In the demo,
   "this point is 3 standard deviations from the rolling mean" is a
   complete explanation.
2. **No training data.** The system is online; we don't have a
   pre-collected labeled dataset of "real" anomalies.
3. **Compute.** Runs in milliseconds on a laptop.

The cost is that we won't catch subtle anomalies (e.g. unusual *patterns*
that aren't large in magnitude). For a class project that's fine.

### Sample mode

**Why have one at all?** Because the rubric requires the system to "run
end-to-end on a typical student laptop" and a demo where we're at the
mercy of Binance's uptime is fragile. The sample mode also makes
testing deterministic: same seed → same data → same anomalies.

The cost is a bit of extra code and a `data/sample/` directory. Worth
it.

### Single-DAG vs. multi-DAG

Already covered in the DAG section. Short version: the dependency
between ingestion and analysis is real, so it should be in one DAG.

### Kibana dashboards manual vs. automated

Already covered in the Kibana section. Short version: data views are
automated, dashboards are documented manually because the JSON format
is fragile.

---

## 14. Anticipated demo questions and answers

These are the questions we expect the evaluation committee to ask.
Prepare a 30-second answer for each. Every team member should be able
to answer at least the first 5.

### Q1: "Walk me through the data flow."

**A:** "When a DAG run starts, two tasks run in parallel: `ingest_prices`
hits the Binance API for minute-level candles, and `ingest_news`
fetches RSS feeds, scores sentiment with VADER, and tags each article
with the crypto it mentions. Both tasks dual-write — Postgres for
structured queries, Elasticsearch for search and dashboards. Once both
finish, `detect_and_link` runs: it computes a rolling z-score on the
price series, flags points above threshold as anomalies, and joins
those anomalies with news published in the same time window for the
same asset. Kibana queries Elasticsearch for the dashboards; pgAdmin
queries Postgres for inspection."

### Q2: "Why two storage systems?"

**A:** "Postgres is our system of record — strong consistency, ACID
transactions, joins. It holds the structured time series, the anomalies,
and the link table. Elasticsearch sits beside it for what it's actually
designed for: full-text and faceted search over news, and as the
backing store for Kibana. Doing dashboards directly off Postgres would
work but would push us into building or buying a separate viz layer."

### Q3: "Why not just run three cron jobs?"

**A:** "Because there's a real dependency: the analysis is meaningless
until both ingestion tasks have produced rows for the current window.
With cron, we'd have to encode that dependency ourselves with lock
files or polling. Airflow gives us dependency-aware scheduling, retry
policies, and an audit log of every run for free. That's exactly the
feature set we needed."

### Q4: "How do you handle failure?"

**A:** "Three layers. (1) Each task has `retries=2` with a 2-minute
delay — covers transient API blips. (2) Idempotent writes everywhere:
Postgres uses `ON CONFLICT DO NOTHING` keyed on `(symbol, open_time)`;
Elasticsearch uses deterministic `_id`s. So a re-run can never produce
duplicates. (3) The system runs hourly. If one hour's run fails entirely,
the next hour's run pulls overlapping data from Binance and catches up
naturally."

### Q5: "Show me the system is robust to a service restart."

Run live: `docker compose restart elasticsearch`, wait ~30 seconds,
then trigger the DAG. The next ingestion succeeds because Postgres held
the data while ES was down, and the next analysis run repopulates ES
cleanly.

### Q6: "Why VADER and not a finance-tuned model?"

**A:** "Footprint and runtime. VADER is ~50KB and runs CPU-only with no
warmup. FinBERT is ~400MB. On a 16GB laptop where Elasticsearch already
takes 1GB, that matters. We accept that VADER doesn't know
finance-specific vocabulary; we call this out as a limitation in the
report and propose FinBERT as future work."

### Q7: "How are you sure your anomaly detector is working?"

**A:** "Two ways. First, a unit test: we feed in a flat series with one
synthetic 5% spike and assert the spike is flagged. Second, in sample
mode we deliberately inject ~6 large jumps per symbol, and the analysis
DAG finds them every time. The sample data is seeded so the result is
deterministic across runs."

### Q8: "Why are the tools the way they are — couldn't you have skipped pgAdmin?"

**A:** "We could, but the rubric specifically asks for meaningful use
of course tools. pgAdmin is the inspection layer that lets a non-coder
verify the database is doing what we say it is. During the demo, it's
how we show 'yes, the prices table has 5,000 rows', without dropping
into a `psql` shell."

### Q9: "What if the news API is down on demo day?"

**A:** "We have an offline mode (`DATA_MODE=sample`) that reads from
generated sample data. Same DAG, same dashboards, same dependencies —
just different inputs. Switching is one env-var change plus a service
restart. Sample data is also the deterministic test fixture."

### Q10: "How would you scale this 100x?"

**A:** "Three things would change. (1) Replace single-node Postgres
with a partitioned/sharded setup — at 100x we'd need TimescaleDB or
hypertables. (2) Replace the dual-write with CDC: write only to Postgres
and stream changes to Elasticsearch via Debezium. The current
dual-write doesn't scale because the writer is on the critical path of
both stores. (3) Move from a hourly DAG to a streaming pipeline — Kafka
+ Flink — because at 100x we'd care about sub-minute latency. None of
these would change our analytical logic; they're all infrastructure."

### Q11: "Make a small live change."

This is the rubric's "make a small live change" probe. Likely options:

- **Add a column.** In `sql/init.sql` add a column to `news`, then
  `docker exec -it cryptopulse-postgres psql -U cryptopulse -d cryptopulse
  -c "ALTER TABLE news ADD COLUMN test_col INT"`. (Don't actually edit
  the SQL file mid-demo; do the live `ALTER` in psql.)
- **Run a DAG.** From the Airflow UI, click the play button on
  `cryptopulse_pipeline`. Show that all three tasks succeed and that
  Postgres row counts increased.
- **Lower the anomaly threshold.** Edit `.env` to set `ZSCORE_THRESHOLD=1.5`
  (more sensitive), `docker compose restart airflow-scheduler`, trigger
  the DAG, point at the new (more) anomalies in pgAdmin or Kibana.

Practice this beforehand. The "small live change" is where the demo
either lands or face-plants.

---

## Appendix: useful commands

```bash
# Bring everything up from scratch
docker compose up --build -d

# Reset everything (nuclear)
docker compose down -v
docker compose up --build -d

# One-shot health report
bash scripts/health_check.sh

# Manual pipeline run
docker compose run --rm pipeline python -m src.ingestion.ingest_prices
docker compose run --rm pipeline python -m src.ingestion.ingest_news
docker compose run --rm pipeline python -m src.analysis.detect_anomalies

# Generate offline sample data
docker compose run --rm pipeline python -m src.ingestion.generate_sample

# Direct DB shell
docker exec -it cryptopulse-postgres psql -U cryptopulse -d cryptopulse

# Quick ES counts
curl 'http://localhost:9200/_cat/indices?v'

# Tail a service log
docker compose logs -f airflow-scheduler

# Run unit tests
docker compose run --rm pipeline pytest tests/
```
