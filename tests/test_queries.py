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


def test_top_conditions_merges_spelling_variants(settings, store, site_store):
    names = {
        "NCT1": ["COVID-19", "Covid19"],
        "NCT2": ["Covid-19"],
        "NCT3": ["Diabetes Mellitus, Type 2"],
        "NCT4": ["Type 2 Diabetes"],
        "NCT5": ["type 2 diabetes melitus", "Type 1 Diabetes"],
        "NCT6": ["Diabetes Mellitus, Type 2"],
    }
    studies = []
    for nct_id, conditions in names.items():
        s = make_study(nct_id)
        s["protocolSection"]["conditionsModule"] = {"conditions": conditions}
        studies.append(s)
    store.put_bytes("raw/R1/page_0001.ndjson", ndjson(studies))
    transform_page(settings, store, "R1", "raw/R1/page_0001.ndjson")
    build_snapshot(settings, store, site_store, "R1", "full", "2024-01-01T00:00:00+00:00")
    query_api._state.update(run_id=None, con=None, checked_at=0.0)
    status, body, _ = call(settings, store, name="top_conditions")
    assert status == 200
    assert body["rows"] == [["Diabetes Mellitus, Type 2", 4], ["COVID-19", 2], ["Type 1 Diabetes", 1]]


def test_icd10_panel_queries():
    import duckdb

    from ct_pipeline.queries import run_query

    con = duckdb.connect()
    con.execute("CREATE TABLE studies AS FROM (VALUES ('NCT1', 'PHASE1'), ('NCT2', 'PHASE2')) t(nct_id, phase_group)")
    assert run_query(con, "icd10_chapters")["rows"] == []  # snapshot without study_condition_codes
    con.execute("DROP VIEW study_condition_codes")
    con.execute("""CREATE TABLE study_condition_codes AS FROM (VALUES
        ('NCT1', 'ICD10CM', 'C34.1', 'Malignant neoplasm of upper lobe', 2, 'C34'),
        ('NCT2', 'ICD10CM', 'C34', 'Malignant neoplasm of bronchus and lung', 2, 'C34'),
        ('NCT2', 'ICD10CM', 'E11', 'Type 2 diabetes mellitus', 4, 'E11'),
        ('NCT1', 'ICD10CM', 'C79.9', 'Secondary malignant neoplasm of unspecified site', 2, 'C79'),
        ('NCT2', 'ICD10CM', 'C85.9', 'Non-Hodgkin lymphoma, unspecified', 2, 'C85'),
        ('NCT2', 'ICD10CM', 'C85.1', 'Unspecified B-cell lymphoma', 2, 'C85'),
        ('NCT2', 'UMLS', 'x', 'x', NULL, NULL)
    ) t(nct_id, code_system, code, label, icd10_chapter, icd10_category)""")
    assert run_query(con, "icd10_chapters")["rows"] == [
        ["Neoplasms (C00-D49)", 2],
        ["Endocrine and metabolic (E00-E89)", 1],
    ]
    assert run_query(con, "top_icd10_categories", {"phase": "PHASE2"})["rows"] == [
        ["C34 Malignant neoplasm of bronchus and lung", 1],
        ["C85 Unspecified B-cell lymphoma", 1],
        ["E11 Type 2 diabetes mellitus", 1],
    ]
    assert "C79" not in [r[0][:3] for r in run_query(con, "top_icd10_categories")["rows"]]


def test_llm_tab_queries():
    import duckdb

    from ct_pipeline.queries import run_query

    con = duckdb.connect()
    con.execute("""CREATE TABLE studies AS FROM (VALUES
        ('NCT1', 'PHASE3'), ('NCT2', 'PHASE3'), ('NCT3', 'PHASE3'), ('NCT4', 'PHASE3'), ('NCT5', 'PHASE1')
    ) t(nct_id, phase_group)""")
    con.execute("""CREATE TABLE study_conditions AS FROM (VALUES
        ('NCT1', 'Hypertension'), ('NCT2', 'Solid Tumors'), ('NCT3', 'Advanced Cancer'), ('NCT4', 'Nausea'),
        ('NCT4', 'Cancer')
    ) t(nct_id, condition_name)""")
    con.execute("""CREATE TABLE study_condition_codes AS FROM (VALUES
        ('NCT1', 'ICD10CM', 'I10', 'Essential hypertension', 'mesh', 'exact', NULL, 'I10'),
        ('NCT2', 'ICD10CM', 'C80.1', 'Malignant neoplasm, unspecified', 'mesh', 'broad', NULL, 'C80'),
        ('NCT2', 'ICD10CM', 'C25', 'Malignant neoplasm of pancreas', 'llm', 'exact', 'pancreatic cancer', 'C25'),
        ('NCT2', 'ICD10CM', 'C18', 'Malignant neoplasm of colon', 'llm', 'exact', 'colon cancer', 'C18'),
        ('NCT3', 'ICD10CM', 'C80', 'Malignant neoplasm without specification', 'llm', 'broad', 'cancer', 'C80'),
        ('NCT4', 'ICD10CM', 'R11', 'Nausea and vomiting', 'llm', 'exact', 'chronic nausea', 'R11'),
        ('NCT5', 'ICD10CM', 'C34', 'Malignant neoplasm of bronchus and lung', 'llm', 'exact', 'NSCLC', 'C34')
    ) t(nct_id, code_system, code, label, via, match_type, evidence, icd10_category)""")
    assert run_query(con, "llm_coverage", {"phase": "PHASE3"}) == {
        "total_studies": 4,
        "exact_before": 1,
        "exact_after": 3,
        "broad_to_exact": 1,
        "uncoded_to_exact": 1,
        "llm_broad_only": 1,
    }
    assert run_query(con, "llm_top_categories", {"phase": "PHASE3"})["rows"] == [
        ["C18 Malignant neoplasm of colon", 1],
        ["C25 Malignant neoplasm of pancreas", 1],
        ["R11 Nausea and vomiting", 1],
    ]
    rows = {r[0]: r[1:] for r in run_query(con, "llm_examples", {"phase": "PHASE3"})["rows"]}
    assert rows == {
        "NCT2": [
            "Solid Tumors",
            "C80.1",
            "C18 Malignant neoplasm of colon; C25 Malignant neoplasm of pancreas",
            "colon cancer",
        ],
        "NCT4": ["Cancer; Nausea", None, "R11 Nausea and vomiting", "chronic nausea"],
    }


def test_top_conditions_merges_healthy_volunteer_names(settings, store, site_store):
    names = {
        "NCT1": ["Healthy"],
        "NCT2": ["Healthy Volunteers"],
        "NCT3": ["healthy adult subjects (HS)", "Healthy"],
        "NCT4": ["Healthy Aging"],
    }
    studies = []
    for nct_id, conditions in names.items():
        s = make_study(nct_id)
        s["protocolSection"]["conditionsModule"] = {"conditions": conditions}
        studies.append(s)
    store.put_bytes("raw/R1/page_0001.ndjson", ndjson(studies))
    transform_page(settings, store, "R1", "raw/R1/page_0001.ndjson")
    build_snapshot(settings, store, site_store, "R1", "full", "2024-01-01T00:00:00+00:00")
    query_api._state.update(run_id=None, con=None, checked_at=0.0)
    status, body, _ = call(settings, store, name="top_conditions")
    assert status == 200
    assert body["rows"] == [["Healthy volunteers", 3], ["Healthy Aging", 1]]
