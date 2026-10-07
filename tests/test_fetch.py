import json

import pytest

from ct_pipeline import fetch
from ct_pipeline.config import CURRENT_KEY, WATERMARK_KEY


class FakeResponse:
    def __init__(self, status, payload):
        self.status = status
        self.data = json.dumps(payload).encode()


class FakeHttp:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def request(self, method, url, fields=None):
        self.calls.append(fields)
        return self.pages.pop(0)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(fetch, "MIN_REQUEST_INTERVAL", 0)


def test_build_query_term_combines_clauses():
    assert fetch.build_query_term("AREA[Phase]PHASE1") == "(AREA[Phase]PHASE1)"
    assert fetch.build_query_term("AREA[Phase]PHASE1", "2015", "2024-03-01") == (
        "(AREA[Phase]PHASE1) AND AREA[StartDate]RANGE[2015-01-01,MAX] AND AREA[LastUpdatePostDate]RANGE[2024-03-01,MAX]"
    )


def test_build_query_term_rejects_bad_year():
    with pytest.raises(ValueError):
        fetch.build_query_term("x", "15")


def test_start_falls_back_to_full_without_snapshot(settings, store):
    state = fetch.start({"mode": "incremental"}, settings, store)
    assert state["mode"] == "full"
    assert "LastUpdatePostDate" not in state["query_term"]
    assert state["page_num"] == 1 and not state["done"]


def test_start_incremental_uses_watermark_minus_one_day(settings, store):
    store.put_json(WATERMARK_KEY, {"fetch_started_at": "2024-05-10T06:00:00+00:00"})
    store.put_json(CURRENT_KEY, {"run_id": "prev", "tables": {}})
    state = fetch.start({}, settings, store)
    assert state["mode"] == "incremental"
    assert state["query_term"].endswith("AREA[LastUpdatePostDate]RANGE[2024-05-09,MAX]")


def test_start_full_ignores_watermark(settings, store):
    store.put_json(WATERMARK_KEY, {"fetch_started_at": "2024-05-10T06:00:00+00:00"})
    store.put_json(CURRENT_KEY, {"run_id": "prev", "tables": {}})
    state = fetch.start({"mode": "full", "start_year": "2010"}, settings, store)
    assert state["mode"] == "full"
    assert "LastUpdatePostDate" not in state["query_term"]
    assert "RANGE[2010-01-01,MAX]" in state["query_term"]


def test_start_rejects_unknown_mode(settings, store):
    with pytest.raises(ValueError):
        fetch.start({"mode": "weekly"}, settings, store)


def test_fetch_pages_and_finalize(settings, store):
    http = FakeHttp(
        [
            FakeResponse(200, {"studies": [{"a": 1}, {"a": 2}], "nextPageToken": "tok"}),
            FakeResponse(200, {"studies": [{"a": 3}]}),
        ]
    )
    state = fetch.start({"mode": "full"}, settings, store)
    state = fetch.fetch_page(state, settings, store, http=http)
    assert not state["done"] and state["page_token"] == "tok"
    state = fetch.fetch_page(state, settings, store, http=http)
    assert state["done"] and state["total_records"] == 3

    assert http.calls[0]["fields"] == settings.fields
    assert http.calls[0]["pageSize"] == "100"
    assert "pageToken" not in http.calls[0]
    assert http.calls[1]["pageToken"] == "tok"

    run = fetch.finalize(state, settings, store)
    assert run["file_count"] == 2
    assert run["files"][0].endswith("page_0001.ndjson")
    page1 = store.get_bytes(run["files"][0]).decode().splitlines()
    assert [json.loads(line) for line in page1] == [{"a": 1}, {"a": 2}]
    manifest = store.get_json(f"raw/{run['run_id']}/manifest.json")
    assert manifest["total_records"] == 3 and manifest["files"] == run["files"]


def test_fetch_page_raises_on_http_error(settings, store):
    state = fetch.start({"mode": "full"}, settings, store)
    with pytest.raises(RuntimeError, match="503"):
        fetch.fetch_page(state, settings, store, http=FakeHttp([FakeResponse(503, {})]))


def test_finalize_empty_full_run_fails(settings, store):
    state = fetch.start({"mode": "full"}, settings, store)
    with pytest.raises(RuntimeError):
        fetch.finalize({**state, "done": True}, settings, store)


def test_finalize_empty_incremental_run_is_ok(settings, store):
    state = {"run_id": "R", "mode": "incremental", "fetch_started_at": "t", "query_term": "q", "total_records": 0}
    assert fetch.finalize(state, settings, store)["file_count"] == 0
