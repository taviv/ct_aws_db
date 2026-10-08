import json
from datetime import date

import duckdb
import pytest

from ct_pipeline.transform import TABLES, transform_file

from .conftest import FIXTURE, load_studies, make_study, ndjson


@pytest.fixture
def con():
    c = duckdb.connect()
    yield c
    c.close()


def _read(con, out, table):
    cur = con.execute(f"SELECT * FROM read_parquet('{out / f'{table}.parquet'}')")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def test_transform_fixture_writes_every_table(con, tmp_path):
    counts = transform_file(con, FIXTURE, tmp_path, "page_0001")
    studies = load_studies()
    assert set(counts) == set(TABLES)
    assert counts["studies"] == len(studies)
    assert counts["study_text"] == len(studies)
    assert counts["study_locations"] > counts["studies"]
    for table in TABLES:
        assert (tmp_path / f"{table}.parquet").exists()


def test_transform_derived_columns(con, tmp_path):
    src = tmp_path / "in.ndjson"
    src.write_bytes(
        ndjson(
            [
                make_study("NCT00000001", phases=["PHASE2", "PHASE1"], start="2020-05", completion="2021-05-01"),
                make_study("NCT00000002", phases=[], start="2020-01-01", completion=None),
                make_study("NCT00000003", phases=["EARLY_PHASE1"], countries=("France", "Germany", "France")),
            ]
        )
    )
    transform_file(con, src, tmp_path / "out", "p1")
    rows = _read(con, tmp_path / "out", "studies")
    df = {r["nct_id"]: r for r in rows}

    assert df["NCT00000001"]["phase_group"] == "PHASE1/PHASE2"
    assert df["NCT00000002"]["phase_group"] == "NA"
    assert df["NCT00000003"]["phase_group"] == "EARLY_PHASE1"
    assert df["NCT00000001"]["start_date"] == date(2020, 5, 1)
    assert df["NCT00000001"]["start_end"] == (date(2021, 5, 1) - date(2020, 5, 1)).days
    assert df["NCT00000002"]["start_end"] is None
    assert {r["_src"] for r in rows} == {"p1"}

    phases = _read(con, tmp_path / "out", "study_phases")
    assert sorted(r["phase"] for r in phases if r["nct_id"] == "NCT00000001") == ["PHASE1", "PHASE2"]
    locs = _read(con, tmp_path / "out", "study_locations")
    assert {r["country"] for r in locs if r["nct_id"] == "NCT00000003"} == {"France", "Germany"}


def test_transform_full_page_fits_lambda_memory(tmp_path):
    base = load_studies()
    studies = []
    for i in range(1000):
        s = json.loads(json.dumps(base[i % len(base)]))
        s["protocolSection"]["identificationModule"]["nctId"] = f"NCT9{i:07d}"
        studies.append(s)
    src = tmp_path / "page.ndjson"
    src.write_bytes(ndjson(studies))

    c = duckdb.connect(config={"memory_limit": "512MB", "threads": 2})
    try:
        counts = transform_file(c, src, tmp_path / "out", "page_0001")
    finally:
        c.close()
    assert counts["studies"] == 1000
