import duckdb
import pytest

from ct_pipeline import llm, llm_run, trial_match


def _con():
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE studies (nct_id VARCHAR, brief_title VARCHAR, study_type VARCHAR,"
        " overall_status VARCHAR, phase_group VARCHAR)"
    )
    con.execute(
        "CREATE TABLE study_locations (nct_id VARCHAR, facility VARCHAR, status VARCHAR,"
        " city VARCHAR, state VARCHAR, country VARCHAR, zip VARCHAR, latitude DOUBLE, longitude DOUBLE)"
    )
    con.execute(
        "CREATE TABLE study_condition_codes (nct_id VARCHAR, code_system VARCHAR, code VARCHAR,"
        " label VARCHAR, icd10_category VARCHAR, via VARCHAR, match_type VARCHAR, source VARCHAR)"
    )
    return con


def _study(nct, status="RECRUITING", phase="PHASE3", stype="INTERVENTIONAL"):
    return nct, f"Trial {nct}", stype, status, phase


def _code(nct, code):
    return nct, "ICD10CM", code, code, code[:3], "mesh", "exact", "test"


def _loc(nct, country="United States", state="CA", city="LA"):
    return nct, "Site", "RECRUITING", city, state, country, "90001", 34.0, -118.0


CATS = {"C50": "Malignant neoplasm of breast", "C34": "Malignant neoplasm of bronchus and lung"}


def test_targets_filters():
    con = _con()
    con.executemany(
        "INSERT INTO studies VALUES (?, ?, ?, ?, ?)",
        [
            _study("NCT1"),  # keep
            _study("NCT2", status="COMPLETED"),  # not recruiting
            _study("NCT3", phase="PHASE1"),  # wrong phase
            _study("NCT4", stype="OBSERVATIONAL"),  # not interventional
            _study("NCT5"),  # site outside the country list
            _study("NCT6"),  # no cancer code
            _study("NCT7", phase="PHASE2/PHASE3"),  # keep
            _study("NCT8", status="ENROLLING_BY_INVITATION"),  # keep
            _study("NCT9"),  # keep — Canada site
        ],
    )
    for nct in ("NCT1", "NCT2", "NCT3", "NCT4", "NCT5", "NCT7", "NCT8", "NCT9"):
        con.execute("INSERT INTO study_condition_codes VALUES (?, ?, ?, ?, ?, ?, ?, ?)", _code(nct, "C50"))
    con.execute("INSERT INTO study_condition_codes VALUES (?, ?, ?, ?, ?, ?, ?, ?)", _code("NCT6", "E11"))
    for nct in ("NCT1", "NCT2", "NCT3", "NCT4", "NCT6", "NCT7", "NCT8"):
        con.execute("INSERT INTO study_locations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", _loc(nct))
    con.execute(
        "INSERT INTO study_locations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        _loc("NCT5", country="France", state="", city="Paris"),
    )
    con.execute(
        "INSERT INTO study_locations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        _loc("NCT9", country="Canada", state="ON", city="Toronto"),
    )
    ids = trial_match.targets(con, ("PHASE2", "PHASE2/PHASE3", "PHASE3"))
    assert ids == ["NCT1", "NCT7", "NCT8", "NCT9"]
    ids = trial_match.targets(con, ("PHASE2", "PHASE2/PHASE3", "PHASE3"), ("United States",))
    assert ids == ["NCT1", "NCT7", "NCT8"]


def test_parse_phases_accepts_shorthand():
    con = _con()
    con.execute("INSERT INTO studies VALUES ('NCT1', 't', 'INTERVENTIONAL', 'RECRUITING', 'PHASE2/PHASE3')")
    assert llm_run.parse_phases(con, "phase2/3") == ["PHASE2/PHASE3"]
    with pytest.raises(llm.LlmError):
        llm_run.parse_phases(con, "PHASE4")


def test_locations_groups_countries_states_and_cities():
    con = _con()
    con.execute("INSERT INTO study_locations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", _loc("NCT1", state="CA"))
    con.execute(
        "INSERT INTO study_locations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        _loc("NCT1", state="NY", city="NYC"),
    )
    con.execute(
        "INSERT INTO study_locations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        _loc("NCT1", country="Canada", state="ON", city="Toronto"),
    )
    locs = trial_match.locations(con, ["NCT1"])
    assert locs["NCT1"]["countries"] == ["Canada", "United States"]
    assert locs["NCT1"]["states"] == ["CA", "NY", "ON"]
    assert locs["NCT1"]["sites"] == 3
    assert "Toronto" in locs["NCT1"]["cities"]
    locs = trial_match.locations(con, ["NCT1"], ("United States",))
    assert locs["NCT1"]["sites"] == 2
    assert "Toronto" not in locs["NCT1"]["cities"]


def test_validate_match_cleans_fields():
    m = trial_match.validate_match(
        {
            "disease": "  metastatic NSCLC  ",
            "icd10": "C34.1 nonsense text",
            "setting": "FIRST_LINE",
            "prior_therapy": "after >=1 platinum line",
            "biomarkers": ["EGFR+", "EGFR+", "PD-L1 >= 50%"],
            "key_exclusions": ["active brain mets"],
            "patient_scope": "adults >=18",
            "confidence": "high",
        },
        CATS,
    )
    assert m["disease"] == "metastatic NSCLC"
    assert m["icd10"] == "C34"  # cut back to a real catalog category
    assert m["setting"] == "first_line"
    assert m["biomarkers"] == ["EGFR+", "PD-L1 >= 50%"]  # deduped
    assert m["confidence"] == "high"
    assert trial_match.validate_match({"setting": "mixed"}, CATS)["setting"] == "mixed"


def test_validate_match_defaults_and_bad_values():
    m = trial_match.validate_match({"setting": "bogus", "icd10": "ZZZ", "confidence": "very"}, CATS)
    assert m["setting"] == "other"
    assert m["icd10"] == ""
    assert m["confidence"] == "low"
    assert m["biomarkers"] == [] and m["key_exclusions"] == []
    m = trial_match.validate_match(None, CATS)
    assert m["disease"] == "" and m["setting"] == "other"


def test_build_entry_stores_match_and_prompt_version():
    entry = trial_match.build_entry(CATS, "m1")({"disease": "x", "setting": "adjuvant"}, "h123", "text")
    assert entry["h"] == "h123"
    assert entry["prompt"] == trial_match.PROMPT_VERSION
    assert entry["model"] == "m1"
    assert entry["match"]["setting"] == "adjuvant"


def test_input_hash_uses_passed_version():
    assert llm.input_hash("t", "m", "v1") != llm.input_hash("t", "m", "v6")
    assert llm.input_hash("t", "m") == llm.input_hash("t", "m", llm.PROMPT_VERSION)


def test_trials_payload_shape():
    docs = [
        {
            "nct_id": "NCT1",
            "brief_title": "Lung trial",
            "phase_group": "PHASE2",
            "overall_status": "RECRUITING",
            "conditions": ["NSCLC"],
            "interventions": ["DRUG: pembrolizumab"],
        }
    ]
    entries = {
        "NCT1": trial_match.build_entry(CATS, "m")(
            {
                "disease": "metastatic NSCLC",
                "icd10": "C34",
                "setting": "first_line",
                "biomarkers": ["PD-L1 >= 50%"],
                "key_exclusions": [],
                "confidence": "high",
            },
            "h",
            "text",
        )
    }
    locs = {"NCT1": {"countries": ["United States"], "states": ["CA"], "cities": ["LA"], "sites": 1}}
    payload = trial_match.trials_payload({"run_id": "r1"}, docs, entries, locs, CATS, "m")
    assert payload["count"] == 1 and payload["run_id"] == "r1"
    t = payload["trials"][0]
    assert t["nct_id"] == "NCT1" and t["phase"] == "PHASE2" and t["status"] == "RECRUITING"
    assert t["icd10"] == "C34" and t["icd10_label"] == CATS["C34"]
    assert t["states"] == ["CA"] and t["sites"] == 1 and t["countries"] == ["United States"]
    # unanswered studies are skipped
    payload = trial_match.trials_payload({"run_id": "r1"}, docs, {}, locs, CATS, "m")
    assert payload["count"] == 0


def test_cancer_codes_lists_c_categories():
    con = _con()
    for nct, code in (("NCT1", "C92.00"), ("NCT1", "C91.00"), ("NCT1", "C92.10"), ("NCT1", "E11"), ("NCT2", "C50")):
        con.execute("INSERT INTO study_condition_codes VALUES (?, ?, ?, ?, ?, ?, ?, ?)", _code(nct, code))
    assert trial_match.cancer_codes(con, ["NCT1"]) == {"NCT1": ["C91", "C92"]}


def test_trials_payload_multi_codes_and_fallback():
    cats = {**CATS, "C91": "Lymphoid leukemia", "C92": "Myeloid leukemia"}
    docs = [{"nct_id": n, "brief_title": n} for n in ("NCT1", "NCT2")]
    ans = {"disease": "x", "setting": "other", "biomarkers": [], "key_exclusions": [], "confidence": "low"}
    entries = {
        "NCT1": trial_match.build_entry(cats, "m")({**ans, "icd10": "C92"}, "h", "t"),
        "NCT2": trial_match.build_entry(cats, "m")({**ans, "icd10": ""}, "h", "t"),
    }
    codes = {"NCT1": ["C91", "C92"], "NCT2": ["C91", "C92"]}
    payload = trial_match.trials_payload({}, docs, entries, {}, cats, "m", codes)
    t1, t2 = payload["trials"]
    assert t1["icd10"] == "C92" and t1["icd10_codes"] == ["C92", "C91"] and t1["icd10_llm"] == "C92"
    assert t2["icd10"] == "C91" and t2["icd10_label"] == "Lymphoid leukemia" and t2["icd10_llm"] == ""
    assert payload["icd10_labels"] == {"C91": "Lymphoid leukemia", "C92": "Myeloid leukemia"}


def test_write_review_rows(tmp_path):
    docs = [
        {
            "nct_id": "NCT1",
            "brief_title": "T",
            "phase_group": "PHASE3",
            "overall_status": "RECRUITING",
            "conditions": ["C"],
            "interventions": [],
        }
    ]
    entries = {"NCT1": trial_match.build_entry(CATS, "m")({"disease": "d", "setting": "other"}, "h", "t")}
    n = trial_match.write_review(tmp_path / "r.csv", docs, entries)
    assert n == 1
    content = (tmp_path / "r.csv").read_text()
    assert "NCT1" in content and "your_check" in content
