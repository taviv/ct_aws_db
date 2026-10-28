import csv

import duckdb
import pytest

from ct_pipeline import xeval


def _con():
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE studies (nct_id VARCHAR, brief_title VARCHAR, phase_group VARCHAR, healthy_volunteers BOOLEAN)"
    )
    con.execute(
        "CREATE TABLE study_interventions (nct_id VARCHAR, intervention_type VARCHAR,"
        " name VARCHAR, description VARCHAR)"
    )
    con.execute(
        "CREATE TABLE study_condition_codes (nct_id VARCHAR, code_system VARCHAR, code VARCHAR,"
        " label VARCHAR, icd10_category VARCHAR, via VARCHAR, match_type VARCHAR, source VARCHAR)"
    )
    return con


def _study(nct, phase="PHASE3", title="Study", healthy=False):
    return nct, title, phase, healthy


def test_sample_excludes_healthy_and_vaccine():
    con = _con()
    con.executemany(
        "INSERT INTO studies VALUES (?, ?, ?, ?)",
        [
            _study("NCT1"),
            _study("NCT2", healthy=True),
            _study("NCT3", title="Flu vaccine trial", healthy=False),
            _study("NCT4", phase="PHASE2"),
            _study("NCT5"),
            _study("NCT6"),
        ],
    )
    con.execute("INSERT INTO study_interventions VALUES ('NCT6', 'BIOLOGICAL', 'HPV vaccine', NULL)")
    counts = xeval.pool_counts(con, ("PHASE3",))
    assert counts == {"total": 5, "healthy": 1, "vaccine": 2, "eligible": 2}
    assert set(xeval.sample_ids(con, ("PHASE3",), seed="1", limit=10)) == {"NCT1", "NCT5"}
    assert len(xeval.sample_ids(con, ("PHASE3",), seed="1", limit=1)) == 1


def test_sample_is_deterministic_per_seed():
    con = _con()
    con.executemany("INSERT INTO studies VALUES (?, ?, ?, ?)", [_study(f"NCT{i:03}") for i in range(50)])
    a = xeval.sample_ids(con, ("PHASE3",), seed="42", limit=10)
    assert a == xeval.sample_ids(con, ("PHASE3",), seed="42", limit=10)
    assert a != xeval.sample_ids(con, ("PHASE3",), seed="7", limit=10)


def test_classic_codes_excludes_llm_rows():
    con = _con()
    con.execute(
        """INSERT INTO study_condition_codes VALUES
        ('NCT1','ICD10CM','C34',NULL,'C34','mesh','exact','mondo'),
        ('NCT1','ICD10CM','C00-D49',NULL,NULL,'mesh_ancestor','broad','mondo'),
        ('NCT1','ICD10CM','Z00',NULL,'Z00','llm','exact','llm'),
        ('NCT1','ICD11','1D8',NULL,NULL,'mesh','exact','wikidata'),
        ('NCT2','ICD10CM','E11',NULL,'E11','text','exact','umls')"""
    )
    got = xeval.classic_codes(con, ["NCT1", "NCT2"])
    assert [r["code"] for r in got["NCT1"]] == ["C00-D49", "C34"]
    assert got["NCT1"][1]["cat"] == "C34" and got["NCT1"][0]["cat"] == "C00-D49"
    assert got["NCT2"][0]["via"] == "text"


def _doc(nct="NCT1", conds=("Lung cancer",)):
    return {"nct_id": nct, "brief_title": "t", "conditions": list(conds)}


def _classic(code, cat=None, via="mesh", mt="exact"):
    return [{"code": code, "cat": cat or code, "label": None, "via": via, "match_type": mt, "source": "x"}]


def _entry(*codes):
    return {
        "codes": [{"code": c, "confidence": conf, "evidence": "e", "evidence_found": found} for c, conf, found in codes]
    }


CHAPTERS = {"C34": 2, "C80": 2, "E11": 4, "J10": 10, "C00-D49": 2}


def test_compare_verdicts():
    ch = CHAPTERS
    agree = xeval.compare_study(_doc(), _classic("C34"), _entry(("C34", "high", True)), ch)
    assert agree["verdict"] == "agree"
    chap = xeval.compare_study(_doc(), _classic("C00-D49", "C00-D49"), _entry(("C34", "high", True)), ch)
    assert chap["verdict"] == "chapter_only"
    disagree = xeval.compare_study(_doc(), _classic("E11"), _entry(("J10", "high", True)), ch)
    assert disagree["verdict"] == "disagree"
    only_classic = xeval.compare_study(_doc(), _classic("E11"), _entry(), ch)
    assert only_classic["verdict"] == "classic_only"
    unverified = xeval.compare_study(_doc(), _classic("E11"), _entry(("J10", "high", False)), ch)
    assert unverified["verdict"] == "classic_only"
    only_llm = xeval.compare_study(_doc(), [], _entry(("C34", "high", True)), ch)
    assert only_llm["verdict"] == "llm_only"
    neither = xeval.compare_study(_doc(), [], None, ch)
    assert neither["verdict"] == "neither"


def test_compare_tracks_publishable_exact():
    # catch-all code + low confidence -> verified but not publishable as exact
    s = xeval.compare_study(_doc(), [], _entry(("C80", "low", True), ("E11", "medium", True)), CHAPTERS)
    assert s["_verified"] == {"C80", "E11"}
    assert s["_llm_exact"] == {"E11"}


def test_metrics_and_payload(tmp_path):
    docs = [_doc("NCT1"), _doc("NCT2"), _doc("NCT3"), _doc("NCT4")]
    classic = {
        "NCT1": _classic("C34"),
        "NCT2": _classic("E11"),
        "NCT3": _classic("C00-D49", "C00-D49", "mesh_ancestor", "broad"),
    }
    entries = {
        "NCT1": _entry(("C34", "high", True), ("E11", "medium", True)),
        "NCT2": _entry(("J10", "high", True)),
        "NCT3": _entry(("C80", "low", True)),
        "NCT4": _entry(("C34", "high", True)),
    }
    studies = xeval.compare(docs, classic, entries, CHAPTERS)
    h = xeval.metrics(studies)
    assert h["studies"] == 4
    assert h["verdicts"]["agree"] == 1 and h["verdicts"]["disagree"] == 1
    assert h["verdicts"]["chapter_only"] == 1 and h["verdicts"]["llm_only"] == 1
    assert h["llm_yield_where_not_exact"] == 1  # NCT4: exact LLM code, no classic code at all
    assert h["by_confidence"]["high"]["codes"] == 3
    assert h["by_confidence"]["high"]["match_exact"] == 1
    payload = xeval.eval_payload({"run_id": "r1", "reference_version": "v1"}, ("PHASE3",), "1", "m", studies, {})
    assert payload["headline"]["studies"] == 4 and payload["adjudicated"] is None
    assert not any(k.startswith("_") for s in payload["studies"] for k in s)
    n = xeval.write_review(tmp_path / "r.csv", studies)
    assert n == 3  # every non-'agree' study
    rows = list(csv.DictReader(open(tmp_path / "r.csv")))
    assert {r["nct_id"] for r in rows} == {"NCT2", "NCT3", "NCT4"}
    for r in rows:
        r["your_check"] = "llm"
    with open(tmp_path / "r2.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    merged = xeval.merge_adjudication(payload, tmp_path / "r2.csv")
    assert merged["adjudicated"]["reviewed"] == 3 and merged["adjudicated"]["llm"] == 3
    assert all(s["check"] == "llm" for s in merged["studies"] if s["verdict"] != "agree")


def test_merge_adjudication_rejects_bad_labels(tmp_path):
    p = tmp_path / "bad.csv"
    p.write_text("nct_id,your_check\nNCT9,maybe\n")
    payload = {"studies": [{"nct_id": "NCT9", "check": None}]}
    with pytest.raises(ValueError, match="your_check"):
        xeval.merge_adjudication(payload, p)
    p.write_text("nct_id,your_check\nNCT8,llm\n")
    with pytest.raises(ValueError, match="not in the sample"):
        xeval.merge_adjudication(payload, p)
