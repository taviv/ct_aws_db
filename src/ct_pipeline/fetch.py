"""
Fetch studies from the ClinicalTrials.gov v2 API into raw NDJSON pages.

Step Functions drives three actions so no invocation approaches the Lambda timeout:
  start      → decide full vs incremental, build the Essie query, return loop state
  fetch_page → fetch one page, write raw/<run_id>/page_NNNN.ndjson, return next state
  finalize   → write raw/<run_id>/manifest.json and return the list of page keys

Incremental mode adds ``AREA[LastUpdatePostDate]RANGE[<watermark - 1 day>,MAX]`` so a
weekly run only pulls studies changed since the last successful build.
"""

import json
import logging
import time
from datetime import date, datetime, timedelta, timezone

import urllib3
from urllib3.util.retry import Retry

from .config import CURRENT_KEY, RAW_PREFIX, WATERMARK_KEY, Settings
from .storage import Store

logger = logging.getLogger(__name__)

API_BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
MIN_REQUEST_INTERVAL = 0.5

_HTTP = urllib3.PoolManager(
    retries=Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    ),
    timeout=urllib3.Timeout(connect=10, read=60),
    headers={"Accept": "application/json"},
)


def build_query_term(base: str, start_year: str = "", updated_since: str = "") -> str:
    clauses = [f"({base})"]
    if start_year:
        year = int(start_year)
        if not 1900 <= year <= 2100:
            raise ValueError(f"start_year out of range: {start_year!r}")
        clauses.append(f"AREA[StartDate]RANGE[{year}-01-01,MAX]")
    if updated_since:
        clauses.append(f"AREA[LastUpdatePostDate]RANGE[{updated_since},MAX]")
    return " AND ".join(clauses)


def start(event: dict, settings: Settings, store: Store) -> dict:
    mode = event.get("mode") or "incremental"
    if mode not in ("incremental", "full"):
        raise ValueError(f"mode must be 'incremental' or 'full', got {mode!r}")

    watermark = store.get_json(WATERMARK_KEY)
    if mode == "incremental" and (not watermark or store.get_json(CURRENT_KEY) is None):
        logger.info("No previous snapshot/watermark — falling back to a full fetch.")
        mode = "full"

    updated_since = ""
    if mode == "incremental":
        last = date.fromisoformat(watermark["fetch_started_at"][:10])
        updated_since = (last - timedelta(days=1)).isoformat()

    start_year = str(event.get("start_year", settings.start_year) or "").strip()
    now = datetime.now(timezone.utc)
    state = {
        "run_id": now.strftime("%Y%m%dT%H%M%SZ"),
        "mode": mode,
        "fetch_started_at": now.isoformat(),
        "query_term": build_query_term(settings.query_term, start_year, updated_since),
        "page_token": None,
        "page_num": 1,
        "total_records": 0,
        "done": False,
    }
    logger.info("Run %s (%s): %s", state["run_id"], mode, state["query_term"])
    return state


def fetch_page(state: dict, settings: Settings, store: Store, http=None) -> dict:
    http = http or _HTTP
    params = {
        "format": "json",
        "query.term": state["query_term"],
        "pageSize": str(settings.page_size),
    }
    if settings.fields:
        params["fields"] = settings.fields
    if state.get("page_token"):
        params["pageToken"] = state["page_token"]

    resp = http.request("GET", API_BASE_URL, fields=params)
    if resp.status != 200:
        raise RuntimeError(f"ClinicalTrials.gov API returned {resp.status}: {resp.data[:300]!r}")
    data = json.loads(resp.data)

    studies = data.get("studies", [])
    next_token = data.get("nextPageToken")
    page_num = int(state["page_num"])

    if studies:
        key = f"{RAW_PREFIX}{state['run_id']}/page_{page_num:04d}.ndjson"
        body = "\n".join(json.dumps(s, ensure_ascii=False) for s in studies).encode("utf-8")
        store.put_bytes(key, body, "application/x-ndjson")
        logger.info("Page %d: %d studies → %s (%.1f MB)", page_num, len(studies), key, len(body) / 1e6)

    time.sleep(MIN_REQUEST_INTERVAL)
    return {
        **state,
        "page_token": next_token,
        "page_num": page_num + 1,
        "total_records": int(state["total_records"]) + len(studies),
        "done": not next_token,
    }


def finalize(state: dict, settings: Settings, store: Store) -> dict:
    run_prefix = f"{RAW_PREFIX}{state['run_id']}/"
    files = [k for k in store.list_keys(run_prefix) if k.endswith(".ndjson")]
    if not files and state["mode"] == "full":
        raise RuntimeError(f"Full run {state['run_id']} produced no data. Check the query term.")

    manifest = {
        "run_id": state["run_id"],
        "mode": state["mode"],
        "query_term": state["query_term"],
        "fields": settings.fields,
        "api_base_url": API_BASE_URL,
        "fetch_started_at": state["fetch_started_at"],
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "total_records": state["total_records"],
        "files": files,
    }
    store.put_json(f"{run_prefix}manifest.json", manifest)
    return {
        "run_id": state["run_id"],
        "mode": state["mode"],
        "fetch_started_at": state["fetch_started_at"],
        "total_records": state["total_records"],
        "file_count": len(files),
        "files": files,
    }
