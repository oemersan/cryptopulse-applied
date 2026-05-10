"""Bootstrap Kibana: create data views and import dashboards.

Run after Kibana is up (e.g. as part of a one-shot init container or by hand):
    docker compose run --rm pipeline python -m src.common.kibana_bootstrap
"""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

KIBANA_HOST = os.environ.get("KIBANA_HOST", "kibana")
KIBANA_PORT = int(os.environ.get("KIBANA_PORT_INTERNAL", "5601"))
KIBANA_URL = f"http://{KIBANA_HOST}:{KIBANA_PORT}"

DATA_VIEWS = [
    {"id": "prices",               "title": "prices",               "timeFieldName": "open_time"},
    {"id": "anomalies",            "title": "anomalies",            "timeFieldName": "detected_at"},
    {"id": "news",                 "title": "news",                 "timeFieldName": "published_at"},
    {"id": "anomaly_news_context", "title": "anomaly_news_context", "timeFieldName": "detected_at"},
]

KIBANA_DIR = Path("/app/kibana")


def _wait_for_kibana(max_wait_seconds: int = 180) -> None:
    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        try:
            r = requests.get(f"{KIBANA_URL}/api/status", timeout=5)
            if r.ok and r.json().get("status", {}).get("overall", {}).get("level") == "available":
                log.info("Kibana is ready.")
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    raise RuntimeError(f"Kibana did not become ready within {max_wait_seconds}s")


def _create_data_view(view: dict) -> None:
    headers = {"kbn-xsrf": "true", "Content-Type": "application/json"}
    payload = {
        "data_view": {
            "id": view["id"],
            "name": view["title"],
            "title": view["title"],
            "timeFieldName": view["timeFieldName"],
        },
        "override": True,
    }
    r = requests.post(
        f"{KIBANA_URL}/api/data_views/data_view",
        json=payload,
        headers=headers,
        timeout=15,
    )
    if r.status_code in (200, 201):
        log.info("Created/updated data view: %s", view["title"])
    elif r.status_code == 409:
        log.info("Data view already exists: %s", view["title"])
    else:
        log.error("Failed to create %s: %d %s", view["title"], r.status_code, r.text)


def _import_ndjson(path: Path) -> None:
    headers = {"kbn-xsrf": "true"}
    with path.open("rb") as f:
        r = requests.post(
            f"{KIBANA_URL}/api/saved_objects/_import?overwrite=true",
            headers=headers,
            files={"file": (path.name, f, "application/ndjson")},
            timeout=30,
        )
    if r.ok:
        result = r.json()
        log.info(
            "Imported %s: %d success, %d errors",
            path.name,
            result.get("successCount", 0),
            len(result.get("errors", [])),
        )
        for err in result.get("errors", []):
            log.error("Import error in %s: %s", path.name, err)
    else:
        log.error("Failed to import %s: %d %s", path.name, r.status_code, r.text)


def run() -> None:
    log.info("Waiting for Kibana at %s ...", KIBANA_URL)
    _wait_for_kibana()
    for view in DATA_VIEWS:
        _create_data_view(view)
    ndjson_files = sorted(KIBANA_DIR.glob("*.ndjson")) if KIBANA_DIR.exists() else []
    if not ndjson_files:
        log.warning("No .ndjson files found in %s", KIBANA_DIR)
    for ndjson in ndjson_files:
        _import_ndjson(ndjson)
    log.info("Kibana bootstrap complete.")


if __name__ == "__main__":
    run()
