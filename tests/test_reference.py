import io
import json
import zipfile
from datetime import date, datetime, timezone

import duckdb
import pytest

from ct_pipeline import llm
from ct_pipeline import reference as R
from ct_pipeline.build import build_snapshot, recode_snapshot
from ct_pipeline.config import CURRENT_KEY, OVERVIEW_KEY, REFERENCE_CURRENT_KEY
from ct_pipeline.transform import transform_page

from .conftest import make_study, ndjson

TABULAR = b"""<?xml version="1.0"?>
<ICD10CM.tabular>
  <chapter><name>2</name><desc>Neoplasms (C00-D49)</desc>
    <sectionIndex>
      <sectionRef first="C30" last="C39" id="C30-C39">Malignant neoplasms of respiratory organs</sectionRef>
      <sectionRef first="C7A" last="C7A" id="C7A">Malignant neuroendocrine tumors</sectionRef>
      <sectionRef first="C76" last="C80" id="C76-C80">Malignant neoplasms of ill-defined sites</sectionRef>
    </sectionIndex>
  </chapter>
  <chapter><name>21</name><desc>Factors influencing health status (Z00-Z99)</desc>
    <sectionIndex><sectionRef first="Z00" last="Z13" id="Z00-Z13">Examinations</sectionRef></sectionIndex>
  </chapter>
</ICD10CM.tabular>
"""


def _order_line(n, code, billable, desc):
    return f"{n:05d} {code:<7} {billable} {desc[:60]:<60} {desc}"


ORDER = "\n".join(
    [
        _order_line(1, "C34", 0, "Malignant neoplasm of bronchus and lung"),
        _order_line(2, "C341", 0, "Malignant neoplasm of upper lobe, bronchus or lung"),
        _order_line(3, "C3411", 1, "Malignant neoplasm of upper lobe, right bronchus or lung"),
        _order_line(4, "C7A", 0, "Malignant neuroendocrine tumors"),
        _order_line(5, "C80", 0, "Malignant neoplasm without specification of site"),
        _order_line(6, "C801", 1, "Malignant (primary) neoplasm, unspecified"),
        _order_line(7, "Z00", 0, "Encounter for general examination"),
        _order_line(8, "Z006", 1, "Encounter for examination for normal comparison and control in clinical research"),
    ]
)

SSSOM = "\n".join(
    [
        "# curie_map:",
        "subject_id\tsubject_label\tpredicate_id\tobject_id\tobject_label\tmapping_justification",
        "MONDO:1\tlung cancer\tskos:exactMatch\tmesh:D008175\t\tx",
        "MONDO:1\tlung cancer\tskos:exactMatch\tICD10CM:C34\tMalignant neoplasm of bronchus and lung\tx",
        "MONDO:1\tlung cancer\tskos:exactMatch\tUMLS:C0242379\t\tx",
        "MONDO:2\tneoplasm\tskos:exactMatch\tmesh:D009369\t\tx",
        "MONDO:2\tneoplasm\tskos:broadMatch\tICD10CM:C80.1\t\tx",
        "MONDO:3\tbogus\tskos:exactMatch\tmesh:D000001\t\tx",
        "MONDO:3\tbogus\tskos:exactMatch\tICD10CM:Q99.99\t\tx",
        "MONDO:4\tno mesh\tskos:exactMatch\tICD10CM:C34\t\tx",
    ]
)


def _zip(name, data):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(name, data)
    return buf.getvalue()


def _wikidata(rows):
    return json.dumps(
        {"results": {"bindings": [{"mesh": {"value": m}, "code": {"value": c}} for m, c in rows]}}
    ).encode()


def fake_get(fy=2027, wikidata_icd10cm=(("D008175", "C34.11"), ("D002289", "C34"))):
    def get(url, headers=None):
        if url == R.CMS_ZIP + f"{fy}-code-tables-tabular-index.zip":
            return _zip(f"Table and Index/icd10cm_tabular_{fy}.xml", TABULAR)
        if url == R.CMS_ZIP + f"{fy}-code-descriptions-tabular-order.zip":
            return _zip(f"Code Descriptions/icd10cm_order_{fy}.txt", ORDER)
        if url.startswith(R.CMS_ZIP):
            return None
        if url == R.MONDO_SSSOM_URL:
            return SSSOM.encode()
        if url.startswith(R.WIKIDATA_SPARQL_URL):
            return _wikidata(wikidata_icd10cm if "P4229" in url else [("D008175", "1234")])
        raise AssertionError(url)

    return get


@pytest.fixture
def curated(monkeypatch):
    overrides = [
        {
            "mesh_id": "D001943",
            "code_system": "ICD10CM",
            "code": "C34",
            "label": "x",
            "match_type": "broad",
            "source": "curated",
        }
    ]
    rules = [
        {"pattern": "(normal )?healthy( volunteers?| subjects?)*", "code_system": "ICD10CM", "code": "Z00.6"}
        | {"match_type": "exact", "note": "healthy"}
    ]
    monkeypatch.setattr(R, "load_curated", lambda: (overrides, rules))


def _table(store, pointer, name):
    return duckdb.sql(f"SELECT * FROM read_parquet('{store.root / pointer['tables'][name]}')")


def test_normalize_and_fiscal_year():
    assert R.normalize_icd10cm("ICD10CM:c50911") == "C50.911"
    assert R.normalize_icd10cm("D55.D59") == "D55-D59"
    assert R.normalize_icd10cm("F99-F99") == "F99"
    assert R.normalize_icd10cm("M31.0+") == "M31.0"
    assert R.fiscal_year(date(2026, 9, 30)) == 2026 and R.fiscal_year(date(2026, 10, 1)) == 2027
    assert R.normalize_condition("Healthy  Volunteers (HV)") == "healthy volunteers hv"


def test_parse_icd10cm_hierarchy():
    rows = {r["code"]: r for r in R.parse_icd10cm(TABULAR, ORDER)}
    assert rows["C00-D49"]["level"] == "chapter" and rows["C00-D49"]["description"] == "Neoplasms"
    assert rows["C30-C39"]["level"] == "block"
    c = rows["C34.11"]
    assert (c["level"], c["is_billable"], c["category"], c["block"], c["chapter"]) == (
        "code",
        True,
        "C34",
        "C30-C39",
        2,
    )
    assert rows["C7A"]["level"] == "category" and rows["C7A"]["block"] == "C7A"
    assert rows["Z00.6"]["chapter"] == 21


def test_mondo_and_combine_prefer_curated_then_exact():
    mondo = R.parse_mondo_sssom(SSSOM)
    assert {(r["mesh_id"], r["code_system"], r["code"], r["match_type"]) for r in mondo} >= {
        ("D008175", "ICD10CM", "C34", "exact"),
        ("D008175", "UMLS", "C0242379", "exact"),
        ("D008175", "MONDO", "MONDO:1", "exact"),
        ("D009369", "ICD10CM", "C80.1", "broad"),
    }
    icd = {r["code"] for r in R.parse_icd10cm(TABULAR, ORDER)}
    wikidata = [{"mesh_id": "D008175", "code_system": "ICD10CM", "code": "C3411", "match_type": "exact"}]
    wikidata += [{"mesh_id": "D009369", "code_system": "ICD10CM", "code": "C80", "match_type": "exact"}]
    wikidata = [r | {"label": None, "source": "wikidata"} for r in wikidata]
    curated = [{"mesh_id": "D008175", "code_system": "ICD10CM", "code": "C34.1", "label": None}]
    curated = [r | {"match_type": "broad", "source": "curated"} for r in curated]
    rows, dropped = R.combine_maps(icd, {"curated": curated, "mondo": mondo, "wikidata": wikidata})
    icd_rows = {r["mesh_id"]: (r["code"], r["source"]) for r in rows if r["code_system"] == "ICD10CM"}
    assert icd_rows["D008175"] == ("C34.1", "curated")
    assert icd_rows["D009369"] == ("C80", "wikidata")  # exact beats MONDO's broad
    assert "D000001" not in icd_rows and dropped["mondo"] == 1


def test_build_reference_writes_versioned_tables(store, settings, curated):
    now = datetime(2026, 10, 12, tzinfo=timezone.utc)
    pointer = R.build_reference(store, settings.work_dir, get=fake_get(), now=now)
    assert store.get_json(REFERENCE_CURRENT_KEY) == pointer
    assert pointer["version"] == "20261012T000000Z" and pointer["sources"]["icd10cm"]["fiscal_year"] == 2027
    codes = _table(store, pointer, "condition_code_map").fetchall()
    assert ("D008175", "ICD11", "1234", None, "exact", "wikidata") in codes
    assert _table(store, pointer, "icd10cm_codes").count("*").fetchone()[0] == pointer["row_counts"]["icd10cm_codes"]


def test_build_reference_falls_back_to_previous_fiscal_year(store, settings, curated):
    pointer = R.build_reference(store, settings.work_dir, get=fake_get(fy=2026), today=date(2026, 10, 12))
    assert pointer["sources"]["icd10cm"]["fiscal_year"] == 2026


def test_build_reference_rejects_invalid_curated_codes(store, settings, monkeypatch):
    rules = [{"pattern": "x", "code_system": "ICD10CM", "code": "Q99.99", "match_type": "exact", "note": ""}]
    monkeypatch.setattr(R, "load_curated", lambda: ([], rules))
    with pytest.raises(ValueError, match="Q99.99"):
        R.build_reference(store, settings.work_dir, get=fake_get())


def test_shipped_curated_files_parse():
    overrides, rules = R.load_curated()
    assert len(overrides) > 50 and all(r["match_type"] in R.MATCH_TYPES for r in overrides + rules)
    healthy = rules[0]["pattern"]
    import re

    assert re.fullmatch(healthy, R.normalize_condition("Healthy Volunteers"))
    assert not re.fullmatch(healthy, R.normalize_condition("Healthy Aging"))


def _study(nct_id, conditions, meshes=(), ancestors=()):
    s = make_study(nct_id)
    s["protocolSection"]["conditionsModule"] = {"conditions": list(conditions)}
    s["derivedSection"] = {
        "conditionBrowseModule": {
            "meshes": [{"id": i, "term": t} for i, t in meshes],
            "ancestors": [{"id": i, "term": t} for i, t in ancestors],
        }
    }
    return s


def _build(settings, store, site_store, run_id, studies):
    key = f"raw/{run_id}/page_0001.ndjson"
    store.put_bytes(key, ndjson(studies))
    transform_page(settings, store, run_id, key)
    return build_snapshot(settings, store, site_store, run_id, "full", "2026-10-12T00:00:00+00:00")


def _codes(store):
    current = store.get_json(CURRENT_KEY)
    path = store.root / current["tables"]["study_condition_codes"]
    return duckdb.sql(
        f"SELECT nct_id, code_system, code, via, match_type, icd10_chapter FROM read_parquet('{path}')"
    ).fetchall()


def test_build_codes_conditions_by_mesh_text_and_ancestor(settings, store, site_store, curated):
    pointer = R.build_reference(store, settings.work_dir, get=fake_get())
    studies = [
        _study("NCT1", ["NSCLC"], meshes=[("D008175", "Lung Neoplasms")]),
        _study("NCT2", ["Healthy Volunteers"]),
        _study("NCT3", ["Advanced Solid Tumors"], meshes=[("D999999", "Unmapped")], ancestors=[("D009369", "Neo")]),
        _study("NCT4", ["Something rare"]),
    ]
    result = _build(settings, store, site_store, "R1", studies)
    current = store.get_json(CURRENT_KEY)
    assert current["reference_version"] == pointer["version"]
    assert result["row_counts"]["study_condition_codes"] == len(_codes(store))
    codes = {(n, s, c): (via, m, ch) for n, s, c, via, m, ch in _codes(store)}
    assert codes[("NCT1", "ICD10CM", "C34")] == ("mesh", "exact", 2)  # MONDO outranks Wikidata
    assert ("NCT1", "ICD10CM", "C34.11") not in codes
    assert codes[("NCT1", "UMLS", "C0242379")][0] == "mesh"
    assert codes[("NCT2", "ICD10CM", "Z00.6")] == ("text", "exact", 21)
    assert codes[("NCT3", "ICD10CM", "C80.1")] == ("mesh_ancestor", "broad", 2)
    assert not any(n == "NCT4" for n, _, _ in codes)


def test_condition_stats_script(settings, store, site_store, curated, monkeypatch, capsys):
    import importlib.util
    import sys
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "condition_stats", Path(__file__).parents[1] / "scripts" / "condition_stats.py"
    )
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)
    R.build_reference(store, settings.work_dir, get=fake_get())
    studies = [
        _study("NCT1", ["NSCLC"], meshes=[("D008175", "Lung Neoplasms")]),
        _study("NCT2", ["Healthy Volunteers"]),
        _study("NCT3", ["Advanced Solid Tumors"], meshes=[("D999999", "Unmapped")], ancestors=[("D009369", "Neo")]),
        _study("NCT4", ["Something rare"]),
    ]
    _build(settings, store, site_store, "R1", studies)
    monkeypatch.setattr(sys, "argv", ["condition_stats.py", "--data", str(store.root)])
    script.main()
    out = capsys.readouterr().out
    assert "| All     |       4 | 75.0% |      50.0% |" in out
    assert "| Healthy (Z00.6)       |" in out and "| something rare |       1 |" in out


def test_build_without_reference_skips_coding(settings, store, site_store):
    _build(settings, store, site_store, "R1", [_study("NCT1", ["Healthy"])])
    current = store.get_json(CURRENT_KEY)
    assert "study_condition_codes" not in current["tables"] and current["reference_version"] is None


def test_recode_applies_new_reference_without_refetch(settings, store, site_store, curated):
    _build(settings, store, site_store, "R1", [_study("NCT1", ["Healthy volunteers"])])
    assert "study_condition_codes" not in store.get_json(CURRENT_KEY)["tables"]
    R.build_reference(store, settings.work_dir, get=fake_get())
    result = recode_snapshot(settings, store, site_store, now=datetime(2026, 10, 13, tzinfo=timezone.utc))
    current = store.get_json(CURRENT_KEY)
    assert current["run_id"] == result["run_id"] == "20261013T000000Z-recode"
    assert current["previous_run_id"] == "R1" and current["row_counts"]["studies"] == 1
    assert [c[:3] for c in _codes(store)] == [("NCT1", "ICD10CM", "Z00.6")]
    overview = site_store.get_json(OVERVIEW_KEY)["by_phase"]["ALL"]
    assert overview["icd10_chapters"]["rows"] == [["Health status and services (Z00-Z99)", 1]]
    assert [r[0][:3] for r in overview["top_icd10_categories"]["rows"]] == ["Z00"]


def test_build_adds_llm_codes_only_for_studies_without_exact_codes(settings, store, site_store, curated):
    answer = {"code": "C34", "confidence": "high", "evidence": "lung", "evidence_found": True}
    entry = {"model": "m", "prompt": "v1", "at": "2026-10-14T00:00:00+00:00", "codes": [answer]}
    store.put_json(llm.CACHE_KEY, {"entries": {"NCT1": entry, "NCT4": entry}})
    pointer = R.build_reference(store, settings.work_dir, get=fake_get())
    assert pointer["row_counts"]["condition_llm_codes"] == 2
    studies = [_study("NCT1", ["NSCLC"], meshes=[("D008175", "Lung Neoplasms")]), _study("NCT4", ["Something rare"])]
    _build(settings, store, site_store, "R1", studies)
    codes = {(n, c, via): m for n, s, c, via, m, _ in _codes(store) if s == "ICD10CM"}
    assert codes[("NCT4", "C34", "llm")] == "exact"
    assert ("NCT1", "C34", "mesh") in codes and ("NCT1", "C34", "llm") not in codes


def test_llm_code_script_eval_and_code(settings, store, site_store, curated, monkeypatch, capsys, tmp_path):
    import importlib.util
    import sys
    from pathlib import Path

    R.build_reference(store, settings.work_dir, get=fake_get())
    studies = [_study("NCT1", ["NSCLC"], meshes=[("D008175", "Lung Neoplasms")]), _study("NCT4", ["Something rare"])]
    _build(settings, store, site_store, "R1", studies)
    spec = importlib.util.spec_from_file_location("llm_code", Path(__file__).parents[1] / "scripts" / "llm_code.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    class Client:
        def converse(self, **kw):
            answer = {"codes": [{"code": "C34", "evidence": "Study", "confidence": "high"}]}
            content = [{"toolUse": {"name": llm.TOOL_NAME, "input": answer}}]
            return {"output": {"message": {"content": content}}, "usage": {"inputTokens": 10, "outputTokens": 2}}

    monkeypatch.setattr(mod, "make_coder", lambda model, catalog: llm.BedrockCoder(model, catalog, client=Client()))
    review = tmp_path / "review.csv"
    argv = [
        "llm_code.py",
        "eval",
        "--model",
        "m",
        "--data",
        str(store.root),
        "--review",
        str(review),
        "--price-in",
        "1",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    mod.main()
    out = capsys.readouterr().out
    assert ">=1 category agrees with MeSH        100.0%" in out
    assert "estimated cost $0.00" in out
    assert review.read_text().splitlines()[1].startswith("NCT1,")

    monkeypatch.setattr(sys, "argv", ["llm_code.py", "code", "--model", "m", "--data", str(store.root)])
    mod.main()
    assert "1 studies have no exact ICD-10-CM code" in capsys.readouterr().out
    assert set(store.get_json(llm.CACHE_KEY)["entries"]) == {"NCT4"}

    code = ["llm_code.py", "code", "--model", "m", "--data", str(store.root), "--phase"]
    monkeypatch.setattr(sys, "argv", code + [" phase1,"])
    mod.main()
    assert "1 studies in PHASE1 have no exact ICD-10-CM code" in capsys.readouterr().out
    monkeypatch.setattr(sys, "argv", code + ["PHASE1,PHASE2/PHASE3"])
    with pytest.raises(SystemExit, match="Unknown --phase PHASE2/PHASE3; choose from PHASE1"):
        mod.main()


def test_llm_code_script_try_mode(settings, store, site_store, curated, monkeypatch, capsys, tmp_path):
    import importlib.util
    import sys
    from pathlib import Path

    R.build_reference(store, settings.work_dir, get=fake_get())
    studies = [
        _study("NCT1", ["NSCLC"], meshes=[("D008175", "Lung Neoplasms")]),
        _study("NCT4", ["Something rare"]),
        _study("NCT5", ["Advanced Solid Tumors"]),
    ]
    _build(settings, store, site_store, "R1", studies)
    spec = importlib.util.spec_from_file_location("llm_code", Path(__file__).parents[1] / "scripts" / "llm_code.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    sent = []

    class Client:
        def converse(self, **kw):
            sent.append(kw["messages"][0]["content"][0]["text"])
            answer = {"codes": [{"code": "C34.1", "evidence": "Study", "confidence": "high"}]}
            content = [{"toolUse": {"name": llm.TOOL_NAME, "input": answer}}]
            return {"output": {"message": {"content": content}}, "usage": {"inputTokens": 10, "outputTokens": 2}}

    monkeypatch.setattr(mod, "make_coder", lambda model, catalog: llm.BedrockCoder(model, catalog, client=Client()))
    base = ["llm_code.py", "try", "--model", "m", "--data", str(store.root)]
    monkeypatch.setattr(sys, "argv", base + ["--phase", "PHASE1"])
    mod.main()
    out = capsys.readouterr().out
    assert "NCT5 " in out and "NCT4 " not in out and "NCT1 " not in out
    assert "LLM:        C34" in out and "text fields sent: brief_summary" in out

    review = tmp_path / "try.csv"
    argv = base + ["--ids", "nct4,NCT9", "--fields", "brief_summary,eligibility_criteria", "--review", str(review)]
    monkeypatch.setattr(sys, "argv", argv)
    mod.main()
    assert "Not in the snapshot: NCT9" in capsys.readouterr().out
    rows = review.read_text().splitlines()
    assert rows[0].startswith("nct_id,phase,title,conditions,current_codes,llm_code") and rows[1].startswith("NCT4,")
    assert not store.list_keys("reference/llm")

    monkeypatch.setattr(sys, "argv", base + ["--fields", "summary"])
    with pytest.raises(SystemExit, match="Unknown --fields summary"):
        mod.main()
