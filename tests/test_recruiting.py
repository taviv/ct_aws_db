import duckdb

from ct_pipeline import llm, recruiting


def _code(code, conf="high", found=True):
    return {"code": code, "confidence": conf, "evidence": f"quote {code}", "evidence_found": found}


def test_targets_are_recruiting_phase3_with_a_us_or_canada_site():
    con = duckdb.connect()
    con.execute("CREATE TABLE studies (nct_id VARCHAR, overall_status VARCHAR, phase_group VARCHAR)")
    con.execute(
        "INSERT INTO studies VALUES ('NCT1','RECRUITING','PHASE3'), ('NCT2','RECRUITING','PHASE2/PHASE3'),"
        " ('NCT3','COMPLETED','PHASE3'), ('NCT4','RECRUITING','PHASE2'), ('NCT5','RECRUITING','PHASE3'),"
        " ('NCT6','NOT_YET_RECRUITING','PHASE3')"
    )
    con.execute("CREATE TABLE study_locations (nct_id VARCHAR, country VARCHAR, state VARCHAR)")
    con.execute(
        "INSERT INTO study_locations VALUES ('NCT1','United States','Ohio'), ('NCT1','Germany',NULL),"
        " ('NCT1','United States','Texas'), ('NCT2','Canada','Ontario'), ('NCT3','United States','Ohio'),"
        " ('NCT4','Canada','Quebec'), ('NCT5','France',NULL), ('NCT6','United States','Iowa')"
    )
    assert recruiting.targets(con) == ["NCT1", "NCT2"]
    loc = recruiting.locations(con, ["NCT1", "NCT2"])
    assert loc["NCT1"] == {"countries": ["United States"], "states": ["Ohio", "Texas"], "sites": 2}


def test_llm_first_codes_fall_back_to_rules_and_drop_supplementary():
    rules = [
        {"code": "E11", "via": "mesh", "match_type": "exact"},
        {"code": "E66", "via": "mesh", "match_type": "broad"},
        {"code": "Z79", "via": "mesh", "match_type": "exact"},
    ]
    llm_entry = {"codes": [_code("E10"), _code("Z79"), _code("C80"), _code("I10", "low"), _code("N18", found=False)]}
    assert recruiting.study_codes(llm_entry, rules) == [("E10", "llm", "high", "quote E10")]
    assert recruiting.study_codes({"codes": [_code("I10", "low")]}, rules) == [("E11", "rules", "", "")]
    assert recruiting.study_codes(None, []) == []


def test_code_rows_and_csv():
    doc = {"nct_id": "NCT1", "brief_title": "T", "phase_group": "PHASE3", "conditions": ["Diabetes", "Obesity"]}
    other = doc | {"nct_id": "NCT2", "conditions": []}
    rows = recruiting.code_rows(
        [doc, other],
        {"NCT1": {"codes": [_code("E11"), _code("E66", "medium")]}},
        {},
        {"E11": "Type 2 diabetes mellitus", "E66": "Overweight and obesity"},
        {"NCT1": {"countries": ["United States"], "states": ["Ohio"], "sites": 3}},
    )
    assert [(r["nct_id"], r["code"], r["source"]) for r in rows] == [
        ("NCT1", "E11", "llm"),
        ("NCT1", "E66", "llm"),
        ("NCT2", "", ""),
    ]
    assert rows[0]["label"] == "Type 2 diabetes mellitus" and rows[0]["states"] == "Ohio" and rows[2]["sites"] == 0
    text = recruiting.to_csv(rows)
    assert text.splitlines()[0] == ",".join(recruiting.COLUMNS) and len(text.splitlines()) == 4


def test_todo_counts_studies_without_a_current_answer():
    doc = {"nct_id": "NCT1", "brief_title": "T", "conditions": ["Asthma"]}
    h = llm.input_hash(llm.render_study(doc), "m")
    assert recruiting.todo([doc], {"NCT1": {"h": h}}, "m") == 0
    assert recruiting.todo([doc], {"NCT1": {"h": "old"}}, "m") == 1
    assert recruiting.cache_key("us.anthropic.claude-sonnet-4-6") == (
        "analysis/recruiting/llm_cache_us.anthropic.claude-sonnet-4-6.json"
    )
