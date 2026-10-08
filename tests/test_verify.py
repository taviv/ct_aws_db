import json
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from ct_pipeline import verify
from ct_pipeline.config import CURRENT_KEY, OVERVIEW_KEY
from ct_pipeline.fetch import API_BASE_URL
from ct_pipeline.verify import FAIL, PASS, WARN, run_checks

from .conftest import make_study
from .test_build import _run

DASH = "https://dash.example/"


@pytest.fixture(autouse=True)
def no_pause(monkeypatch):
    monkeypatch.setattr(verify, "PAUSE", 0)


class FakeHttp:
    def __init__(self, source, site_store, api_summary=None):
        self.source = {s["protocolSection"]["identificationModule"]["nctId"]: s for s in source}
        self.site_store = site_store
        self.api_summary = api_summary

    def request(self, method, url, fields=None):
        if url == API_BASE_URL:
            ids = sorted(self.source)
            start = int(fields.get("pageToken") or 0)
            size = 2
            page = {
                "studies": [
                    {"protocolSection": {"identificationModule": {"nctId": i}}} for i in ids[start : start + size]
                ]
            }
            if start + size < len(ids):
                page["nextPageToken"] = str(start + size)
            return self._ok(page)
        if url.startswith(API_BASE_URL + "/"):
            study = self.source.get(url.rsplit("/", 1)[1])
            return self._ok(study) if study else SimpleNamespace(status=404, data=b"")
        if url == DASH + "data/overview.json":
            return self._ok(self.site_store.get_json(OVERVIEW_KEY))
        if url == DASH + "api/query":
            summary = self.api_summary or self.site_store.get_json(OVERVIEW_KEY)["by_phase"]["ALL"]["summary_stats"]
            return self._ok(summary)
        raise AssertionError(url)

    @staticmethod
    def _ok(obj):
        return SimpleNamespace(status=200, data=json.dumps(obj).encode())


STUDIES = [make_study(f"NCT{i}", status="COMPLETED" if i % 2 else "RECRUITING") for i in range(1, 6)]


def _checks(settings, store, site_store, source, start_year="", **http_kw):
    _run(settings, store, site_store, "R1", [STUDIES], mode="full")
    created = datetime.fromisoformat(store.get_json(CURRENT_KEY)["created_at"])
    checks = run_checks(
        store,
        "AREA[Phase]PHASE1",
        start_year=start_year,
        dashboard_url=DASH,
        latest_execution={"name": "x", "startDate": "2024-06-01", "status": "SUCCEEDED"},
        sample=10,
        http=FakeHttp(source, site_store, **http_kw),
        now=created + timedelta(days=1),
    )
    return {c.name: c for c in checks}


def test_consistent_snapshot_passes_every_check(settings, store, site_store):
    changed = make_study("NCT2", status="COMPLETED", updated="2025-01-01")
    checks = _checks(settings, store, site_store, [changed if s is STUDIES[1] else s for s in STUDIES], "2020")
    assert {name: c.status for name, c in checks.items()} == dict.fromkeys(checks, PASS), checks
    assert "1 updated in the source since the snapshot" in checks["Spot check vs source"].detail


def test_detects_source_drift_field_mismatch_stale_api_and_start_year(settings, store, site_store):
    source = [make_study("NCT1", status="TERMINATED"), *STUDIES[1:4], make_study("NCT9")]
    stale = {"total_studies": 4}
    checks = _checks(settings, store, site_store, source, "2021", api_summary=stale)

    assert checks["Matches ClinicalTrials.gov"].status == FAIL
    assert "1 missing from snapshot (NCT9)" in checks["Matches ClinicalTrials.gov"].detail
    assert "1 not in source (NCT5)" in checks["Matches ClinicalTrials.gov"].detail
    assert checks["Spot check vs source"].status == FAIL
    assert "NCT1: overall_status 'COMPLETED' vs source 'TERMINATED'" in checks["Spot check vs source"].detail
    assert "1 no longer in the source" in checks["Spot check vs source"].detail
    assert checks["Dashboard API"].status == FAIL
    assert checks["Start year filter"].status == FAIL
    assert checks["Dashboard overview"].status == PASS
    assert checks["Row counts"].status == checks["Orphan rows"].status == PASS


def test_overview_from_an_older_run_fails(settings, store, site_store):
    _run(settings, store, site_store, "R1", [STUDIES], mode="full")
    overview = site_store.get_json(OVERVIEW_KEY)
    site_store.put_json(OVERVIEW_KEY, {**overview, "run_id": "R0"})
    checks = run_checks(store, "q", dashboard_url=DASH, sample=0, http=FakeHttp(STUDIES, site_store))
    by_name = {c.name: c for c in checks}
    assert by_name["Dashboard overview"].status == FAIL
    assert "dashboard shows run R0" in by_name["Dashboard overview"].detail


def test_missing_snapshot_and_failed_run(store):
    checks = run_checks(store, "q", latest_execution={"name": "x", "startDate": "d", "status": "FAILED"}, http=object())
    assert [(c.name, c.status) for c in checks] == [("Latest pipeline run", FAIL), ("Published snapshot", FAIL)]


def test_old_snapshot_and_negative_durations_warn(settings, store, site_store):
    studies = [*STUDIES, make_study("NCT7", start="2021-01-01", completion="2020-01-01")]
    _run(settings, store, site_store, "R1", [studies], mode="full")
    created = datetime.fromisoformat(store.get_json(CURRENT_KEY)["created_at"])
    checks = run_checks(store, "q", sample=0, http=FakeHttp(studies, site_store), now=created + timedelta(days=9))
    by_name = {c.name: c.status for c in checks}
    assert by_name["Snapshot freshness"] == WARN and by_name["Durations"] == WARN
