# Kibana Dashboard Setup Guide

This guide walks through setting up the two demo dashboards after the
stack is up and data is flowing. It takes about 10–15 minutes total.

> The data views (`prices`, `anomalies`, `news`, `anomaly_news_context`) are normally created
> automatically by `src/common/kibana_bootstrap.py`. If for some reason
> they aren't there, manually import `kibana/data_views.ndjson` via
> **Stack Management → Saved Objects → Import**.

## Prerequisites

Before opening Kibana, make sure the pipeline has run at least once so
there is data to chart:

```bash
# Trigger the DAGs from the Airflow UI (http://localhost:8088)
# OR run them manually:
docker compose run --rm pipeline python -m src.ingestion.ingest_prices
docker compose run --rm pipeline python -m src.ingestion.ingest_news
docker compose run --rm pipeline python -m src.analysis.detect_anomalies
```

Then open Kibana at **http://localhost:5601**.

## Verify the data views

Go to **Stack Management → Data Views**. You should see three views:
`prices`, `anomalies`, `news`, `anomaly_news_context`. If one is missing,
click "Create data view" and add it (use `open_time`, `detected_at`,
`published_at`, `detected_at` as the timestamp field respectively).

## Dashboard 1 — Crypto Market Overview

1. **Visualize Library → Create visualization → Lens**.
2. Pick **prices** as the data view.
3. **Top right time range:** Last 24 hours.
4. Drag **`open_time`** onto the horizontal axis (auto-bin by minute).
5. Drag **`close_price`** onto the vertical axis, aggregation **Average**.
6. Drag **`symbol`** onto **Break down by**.
7. Chart type: **Line**. Save as `prices_timeseries`.

Then create a second viz:

1. **Lens** again, data view **anomalies**.
2. Same time range.
3. Horizontal: `detected_at` (auto-bin).
4. Vertical: **Count of records**.
5. Break down by `direction` (so up vs down anomalies are colored differently).
6. Chart type: **Bar — vertical stacked**. Save as `anomalies_per_minute`.

Now create a dashboard:

1. **Dashboard → Create dashboard**.
2. **Add panel → From library** → add `prices_timeseries` and `anomalies_per_minute`.
3. Resize so the price chart is on top, anomaly bars at the bottom.
4. Save as **Crypto Market Overview**.

## Dashboard 2 — News Sentiment & Anomaly Context

1. **Lens**, data view **news**, time range Last 24 hours.
2. Horizontal: `published_at` (auto-bin).
3. Vertical: Average of `sentiment_score`.
4. Break down by `mentioned_symbols`.
5. Chart type: **Line**. Save as `news_sentiment_trend`.

Second viz — sentiment label distribution:

1. **Lens**, data view **news**.
2. Slice by `sentiment_label`.
3. Chart type: **Pie**. Save as `news_sentiment_pie`.

Third viz — recent anomalies table:

1. **Discover** with data view **anomalies**.
2. Add columns: `detected_at`, `symbol`, `z_score`, `direction`.
3. Save as **search** named `recent_anomalies_table`.

Fourth viz — anomaly-news context table:

1. **Discover** with data view **anomaly_news_context**.
2. Add columns: `detected_at`, `symbol`, `direction`, `z_score`,
   `news_title`, `news_source`, `published_at`, `sentiment_label`,
   `time_offset_min`.
3. Save as **search** named `anomaly_news_context_table`.

Build the dashboard:

1. **Dashboard → Create dashboard**.
2. Add `news_sentiment_trend`, `news_sentiment_pie`,
   `recent_anomalies_table`, `anomaly_news_context_table`.
3. Save as **News Sentiment and Anomaly Context**.

## Exporting your dashboards (optional but recommended)

After you've built the dashboards, export them so they can be re-imported
on any team member's machine without re-doing the work:

1. **Stack Management → Saved Objects**.
2. Filter by type: `dashboard`, `lens`, `search`, `index-pattern`.
3. Select the ones you want, click **Export**, "Include related objects".
4. Save the resulting `.ndjson` file as `kibana/dashboards.ndjson`.
5. Commit it. Other team members can then **Import** it from the same
   menu and get the dashboards instantly.

## Demo screenshots

Once both dashboards look reasonable, take screenshots and put them in
`docs/screenshots/`. The report and slides require these.
