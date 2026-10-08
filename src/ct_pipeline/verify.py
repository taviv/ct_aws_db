"""
Data integrity checks for a published snapshot: internal consistency, agreement with the
ClinicalTrials.gov API, a field-level spot check, and that the dashboard serves the same numbers.

Run against the live stack with ``make verify`` (scripts/verify.py).
"""

import json
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .build import attach_snapshot
from .config import CURRENT_KEY
from .db import connect
from .fetch import _HTTP, API_BASE_URL, MIN_REQUEST_INTERVAL
from .queries import run_query
from .storage import Store

PASS, WARN, FAIL, ERROR = "PASS", "WARN", "FAIL", "ERROR"
MAX_AGE_DAYS = 8
SOURCE_TOLERANCE = 0.01
SPOT_FIELDS = "NCTId,BriefTitle,OverallStatus,StartDate,CompletionDate,Phase,LastUpdatePostDate"
PAUSE = MIN_REQUEST_INTERVAL


@dataclass
class Check:
    name: str
    status: str
    detail: str


def _get_json(http, url, fields=None):
    resp = http.request("GET", url, fields=fields)
    if resp.status != 200:
        raise RuntimeError(f"GET {url} returned HTTP {resp.status}")
    return json.loads(resp.data)


def _date(s):
    if not s:
        return None
    return {10: s, 7: f"{s}-01", 4: f"{s}-01-01"}.get(len(s))


def _plain(obj):
    return json.loads(json.dumps(obj, default=str))


def _examples(ids, n=5):
    ids = sorted(ids)
    return ", ".join(ids[:n]) + (", ..." if len(ids) > n else "")


def source_ids(http, query_term: str, page_size: int = 1000) -> set:
    ids, token = set(), None
    while True:
        fields = {"format": "json", "query.term": query_term, "pageSize": str(page_size), "fields": "NCTId"}
        if token:
            fields["pageToken"] = token
        data = _get_json(http, API_BASE_URL, fields)
        ids.update(s["protocolSection"]["identificationModule"]["nctId"] for s in data.get("studies", []))
        token = data.get("nextPageToken")
        if not token:
            return ids
        time.sleep(PAUSE)


def check_latest_run(execution):
    status = execution["status"]
    detail = f"{execution['name']} started {execution['startDate']}: {status}"
    return {"SUCCEEDED": PASS, "RUNNING": WARN}.get(status, FAIL), detail


def check_freshness(current, now):
    age = now - datetime.fromisoformat(current["created_at"])
    detail = f"run {current['run_id']} ({current['mode']}) published {age.days} day(s) ago"
    return (PASS if age.days <= MAX_AGE_DAYS else WARN), detail


def check_row_counts(con, current):
    bad = []
    for table, expected in current["row_counts"].items():
        actual = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        if actual != expected:
            bad.append(f"{table}: CURRENT.json says {expected:,}, file has {actual:,}")
    return (FAIL, "; ".join(bad)) if bad else (PASS, f"{len(current['row_counts'])} tables match CURRENT.json")


def check_unique(con):
    total, distinct = con.execute("SELECT count(*), count(DISTINCT nct_id) FROM studies").fetchone()
    text = con.execute("SELECT count(*) FROM study_text").fetchone()[0]
    detail = f"{total:,} studies, {distinct:,} distinct IDs, {text:,} study_text rows"
    return (PASS if total == distinct == text else FAIL), detail


def check_source(con, http, query_term):
    ours = {r[0] for r in con.execute("SELECT nct_id FROM studies").fetchall()}
    src = source_ids(http, query_term)
    missing, extra = src - ours, ours - src
    allowed = int(len(src) * SOURCE_TOLERANCE)
    detail = f"source {len(src):,}, snapshot {len(ours):,}"
    if missing:
        detail += f"; {len(missing):,} missing from snapshot ({_examples(missing)})"
    if extra:
        detail += f"; {len(extra):,} not in source ({_examples(extra)})"
    return (PASS if len(missing) <= allowed and len(extra) <= allowed else FAIL), detail


def check_start_year(con, start_year):
    cutoff = f"{int(start_year)}-01-01"
    rows = con.execute(
        "SELECT nct_id FROM studies WHERE start_date IS NULL OR start_date < CAST(? AS DATE)", [cutoff]
    ).fetchall()
    ids = [r[0] for r in rows]
    detail = f"{len(ids):,} studies start before {cutoff} or have no start date"
    return (FAIL, f"{detail} ({_examples(ids)})") if ids else (PASS, detail)


def check_orphans(con, tables):
    bad = []
    for table in tables:
        if table == "studies":
            continue
        n = con.execute(f"SELECT count(*) FROM {table} ANTI JOIN studies USING (nct_id)").fetchone()[0]
        if n:
            bad.append(f"{table}: {n:,}")
    return (FAIL, "rows without a study: " + "; ".join(bad)) if bad else (PASS, "every child row has a study")


def check_durations(con):
    n = con.execute("SELECT count(*) FROM studies WHERE start_end < 0").fetchone()[0]
    detail = f"{n:,} studies complete before they start (as entered in the source; excluded from duration charts)"
    return (WARN if n else PASS), detail


def check_overview(con, http, dashboard_url, current):
    overview = _get_json(http, dashboard_url + "data/overview.json", {"_": str(int(time.time()))})
    if overview.get("run_id") != current["run_id"]:
        return FAIL, f"dashboard shows run {overview.get('run_id')}, latest is {current['run_id']}"
    bad = []
    for name in ("summary_stats", "status_breakdown", "phase_groups"):
        if overview["by_phase"]["ALL"].get(name) != _plain(run_query(con, name)):
            bad.append(name)
    for phase in overview["phases"]:
        served = overview["by_phase"].get(phase["value"], {}).get("summary_stats")
        if served != _plain(run_query(con, "summary_stats", {"phase": phase["value"]})):
            bad.append(f"summary_stats[{phase['value']}]")
    if bad:
        return FAIL, "dashboard numbers differ from the snapshot: " + ", ".join(bad)
    total = overview["by_phase"]["ALL"]["summary_stats"]["total_studies"]
    return PASS, f"overview.json is run {current['run_id']}, {total:,} studies, matches the snapshot"


def check_api(con, http, dashboard_url):
    served = _get_json(http, dashboard_url + "api/query", {"name": "summary_stats", "_": str(int(time.time()))})
    expected = _plain(run_query(con, "summary_stats"))
    if served != expected:
        diff = [k for k in expected if served.get(k) != expected[k]]
        return FAIL, f"api/query differs on {', '.join(diff)} (it caches for up to ~20 min after a run)"
    return PASS, f"api/query summary_stats matches the snapshot ({expected['total_studies']:,} studies)"


def check_spot(con, http, sample):
    rows = con.execute(
        "SELECT nct_id, brief_title, overall_status, start_date, completion_date, phases, last_update_post_date "
        f"FROM studies USING SAMPLE {int(sample)} ROWS"
    ).fetchall()
    mismatches, changed, gone = [], 0, 0
    for nct, title, status, start, completion, phases, updated in rows:
        resp = http.request("GET", f"{API_BASE_URL}/{nct}", fields={"fields": SPOT_FIELDS})
        time.sleep(PAUSE)
        if resp.status == 404:
            gone += 1
            continue
        if resp.status != 200:
            raise RuntimeError(f"GET {API_BASE_URL}/{nct} returned HTTP {resp.status}")
        ps = json.loads(resp.data)["protocolSection"]
        sm = ps.get("statusModule", {})
        api_updated = _date(sm.get("lastUpdatePostDateStruct", {}).get("date"))
        if api_updated and updated and api_updated > str(updated):
            changed += 1
            continue
        api = {
            "brief_title": ps.get("identificationModule", {}).get("briefTitle"),
            "overall_status": sm.get("overallStatus"),
            "start_date": _date(sm.get("startDateStruct", {}).get("date")),
            "completion_date": _date(sm.get("completionDateStruct", {}).get("date")),
            "phases": sorted(ps.get("designModule", {}).get("phases", [])),
        }
        ours = {
            "brief_title": title,
            "overall_status": status,
            "start_date": str(start) if start else None,
            "completion_date": str(completion) if completion else None,
            "phases": sorted(phases or []),
        }
        diff = [f"{k} {ours[k]!r} vs source {api[k]!r}" for k in api if api[k] != ours[k]]
        if diff:
            mismatches.append(f"{nct}: " + ", ".join(diff))
    detail = f"{len(rows)} random studies: {len(rows) - len(mismatches) - changed - gone} match"
    if changed:
        detail += f", {changed} updated in the source since the snapshot"
    if gone:
        detail += f", {gone} no longer in the source"
    if mismatches:
        return FAIL, detail + "; mismatches: " + "; ".join(mismatches[:5])
    return PASS, detail


def run_checks(
    store: Store,
    query_term: str,
    start_year: str = "",
    dashboard_url: str = "",
    latest_execution: dict = None,
    sample: int = 20,
    http=None,
    now: datetime = None,
) -> list:
    http = http or _HTTP
    now = now or datetime.now(timezone.utc)
    checks = []

    def run(name, fn, *args):
        try:
            status, detail = fn(*args)
        except Exception as e:
            status, detail = ERROR, f"{type(e).__name__}: {e}"
        checks.append(Check(name, status, detail))

    if latest_execution:
        run("Latest pipeline run", check_latest_run, latest_execution)
    current = store.get_json(CURRENT_KEY)
    if current is None:
        checks.append(Check("Published snapshot", FAIL, f"{CURRENT_KEY} not found - no run has finished"))
        return checks

    with tempfile.TemporaryDirectory(prefix="ct-verify-") as tmp:
        work = Path(tmp)
        for table, key in current["tables"].items():
            store.download(key, work / "snapshot" / table / "data.parquet")
        con = connect(work)
        attach_snapshot(con, work / "snapshot", list(current["tables"]))
        try:
            run("Snapshot freshness", check_freshness, current, now)
            run("Row counts", check_row_counts, con, current)
            run("Unique studies", check_unique, con)
            run("Matches ClinicalTrials.gov", check_source, con, http, query_term)
            if start_year:
                run("Start year filter", check_start_year, con, start_year)
            run("Orphan rows", check_orphans, con, list(current["tables"]))
            run("Durations", check_durations, con)
            if dashboard_url:
                run("Dashboard overview", check_overview, con, http, dashboard_url, current)
                run("Dashboard API", check_api, con, http, dashboard_url)
            if sample:
                run("Spot check vs source", check_spot, con, http, sample)
        finally:
            con.close()
    return checks


def report(checks) -> bool:
    for c in checks:
        print(f"{c.status:5}  {c.name}: {c.detail}")
    failed = [c for c in checks if c.status in (FAIL, ERROR)]
    warned = [c for c in checks if c.status == WARN]
    print(f"\n{len(checks) - len(failed) - len(warned)} passed, {len(warned)} warnings, {len(failed)} failed")
    return not failed
