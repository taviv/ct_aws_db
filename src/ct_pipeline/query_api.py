"""
Read-only query API (Lambda Function URL behind CloudFront). Runs the shared dashboard SQL
with DuckDB over the current Parquet snapshot, which is cached in /tmp and refreshed when
curated/CURRENT.json changes.

GET /api/query?name=duration_summary&phase=PHASE2&year=2020&country=Germany&multicountry=true&healthy_volunteers=false
"""

import json
import logging
import shutil
import time
from pathlib import Path

from .build import attach_snapshot
from .config import CURRENT_KEY, Settings
from .db import connect
from .queries import ALL_QUERIES, run_query
from .storage import Store, open_store

logger = logging.getLogger(__name__)

API_TABLES = ("studies", "study_locations", "study_conditions", "study_interventions")
REFRESH_SECONDS = 300
HEADERS = {"Content-Type": "application/json", "Cache-Control": "public, max-age=900"}

_state = {"run_id": None, "con": None, "checked_at": 0.0}


def _connection(settings: Settings, store: Store):
    now = time.monotonic()
    if _state["con"] is not None and now - _state["checked_at"] < REFRESH_SECONDS:
        return _state["con"]
    _state["checked_at"] = now
    current = store.get_json(CURRENT_KEY)
    if current is None:
        raise RuntimeError("No snapshot published yet")
    if current["run_id"] == _state["run_id"]:
        return _state["con"]

    base = Path(settings.work_dir) / "snapshot"
    target = base / current["run_id"]
    for table in API_TABLES:
        store.download(current["tables"][table], target / table / "data.parquet")
    con = connect(Path(settings.work_dir) / "duckdb")
    attach_snapshot(con, target, API_TABLES)
    if _state["con"] is not None:
        _state["con"].close()
    for old in base.iterdir():
        if old != target:
            shutil.rmtree(old, ignore_errors=True)
    _state.update(run_id=current["run_id"], con=con)
    logger.info("Loaded snapshot %s", current["run_id"])
    return con


def _bool(value):
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("1", "true", "yes")


def parse_request(event: dict):
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET").upper()
    if method == "POST":
        body = json.loads(event.get("body") or "{}")
        params = {**(body.get("filters") or {}), "name": body.get("name") or body.get("query")}
    else:
        params = dict(event.get("queryStringParameters") or {})
        params["name"] = params.get("name") or params.get("query")
    filters = {
        "phase": params.get("phase") or None,
        "year": int(params["year"]) if params.get("year") else None,
        "country": params.get("country") or None,
        "multicountry": _bool(params.get("multicountry")),
        "healthy_volunteers": _bool(params.get("healthy_volunteers")),
    }
    return params.get("name") or "", {k: v for k, v in filters.items() if v is not None}


def _response(status: int, body, cache: bool = True) -> dict:
    headers = HEADERS if cache else {**HEADERS, "Cache-Control": "no-store"}
    return {"statusCode": status, "headers": headers, "body": json.dumps(body, default=str)}


def handle(event: dict, settings: Settings, store: Store) -> dict:
    try:
        name, filters = parse_request(event)
    except (ValueError, json.JSONDecodeError):
        return _response(400, {"error": "Invalid request parameters"}, cache=False)
    if name not in ALL_QUERIES:
        return _response(400, {"error": f"Unknown query {name!r}", "available": ALL_QUERIES}, cache=False)
    try:
        cur = _connection(settings, store).cursor()
        try:
            return _response(200, run_query(cur, name, filters))
        finally:
            cur.close()
    except Exception:
        logger.exception("Query %s failed (filters=%s)", name, filters)
        return _response(500, {"error": "Query failed"}, cache=False)


def lambda_handler(event, context):
    settings = Settings.from_env()
    return handle(event, settings, open_store(settings.data_uri))
