import json

import pytest

from ct_pipeline import query_api
from ct_pipeline.build import build_snapshot
from ct_pipeline.queries import ALL_QUERIES, build_query
from ct_pipeline.transform import transform_page

from .conftest import make_study, ndjson


@pytest.fixture
def snapshot(settings, store, site_store):
    studies = [
        make_study("NCT1", phases=["PHASE1"], start="2019-01-01", completion="2020-01-01", countries=("France",)),
        make_study(
            "NCT2", phases=["PHASE1"], start="2019-01-01", completion="2019-07-01", countries=("France", "Germany")
        ),
        make_study(
            "NCT3", phases=["PHASE3"], start="2018-01-01", completion="2021-01-01", countries=("United States",)
        ),
        make_study("NCT4", phases=["PHASE3"], status="RECRUITING", completion="2027-01-01"),
    ]
    store.put_bytes("raw/R1/page_0001.ndjson", ndjson(studies))
    transform_page(settings, store, "R1", "raw/R1/page_0001.ndjson")
    build_snapshot(settings, store, site_store, "R1", "full", "2024-01-01T00:00:00+00:00")
    query_api._state.update(run_id=None, con=None, checked_at=0.0)
    yield
    query_api._state.update(run_id=None, con=None, checked_at=0.0)


def call(settings, store, **params):
    resp = query_api.handle({"queryStringParameters": params}, settings, store)
    return resp["statusCode"], json.loads(resp["body"]), resp["headers"]


def test_every_query_builds_with_all_filters():
    filters = {"phase": "PHASE1", "year": 2020, "country": "France", "healthy_volunteers": True}
    for name in ALL_QUERIES:
        sql, params = build_query(name, filters)
        assert sql.count("?") == len(params), name


def test_filter_values_are_bound_not_interpolated():
    sql, params = build_query("duration_summary", {"country": "x'; DROP TABLE studies; --"})
    assert "DROP" not in sql and params[-1].startswith("x'")


def test_duration_summary_by_phase(snapshot, settings, store):
    status, body, headers = call(settings, store, name="duration_summary")
    assert status == 200 and body["total_studies"] == 3
    assert headers["Cache-Control"].startswith("public")

    _, body, _ = call(settings, store, name="duration_summary", phase="PHASE3")
    assert body["total_studies"] == 1 and body["max_duration_days"] == 1096


def test_duration_filters(snapshot, settings, store):
    _, body, _ = call(settings, store, name="duration_studies", multicountry="true")
    assert [r[0] for r in body["rows"]] == ["NCT2"]
    _, body, _ = call(settings, store, name="duration_studies", country="France", phase="PHASE1", year="2020")
    assert [r[0] for r in body["rows"]] == ["NCT1"]
    _, body, _ = call(settings, store, name="duration_by_phase")
    assert {r[0]: r[1] for r in body["rows"]} == {"PHASE1": 2, "PHASE3": 1}


def test_post_body_is_accepted(snapshot, settings, store):
    event = {
        "requestContext": {"http": {"method": "POST"}},
        "body": json.dumps({"query": "duration_summary", "filters": {"phase": "PHASE1"}}),
    }
    body = json.loads(query_api.handle(event, settings, store)["body"])
    assert body["total_studies"] == 2


def test_bad_requests(snapshot, settings, store):
    status, body, headers = call(settings, store, name="drop_tables")
    assert status == 400 and "available" in body and headers["Cache-Control"] == "no-store"
    status, _, _ = call(settings, store, name="duration_summary", year="abc")
    assert status == 400


def test_no_snapshot_returns_500_without_details(settings, store):
    query_api._state.update(run_id=None, con=None, checked_at=0.0)
    status, body, _ = call(settings, store, name="duration_summary")
    assert status == 500 and body == {"error": "Query failed"}
