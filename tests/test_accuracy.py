import csv

import duckdb

from ct_pipeline import accuracy, llm


def _con():
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE studies (nct_id VARCHAR, brief_title VARCHAR, phase_group VARCHAR, healthy_volunteers BOOLEAN)"
    )
    con.execute(
        "CREATE TABLE study_interventions (nct_id VARCHAR, intervention_type VARCHAR,"
        " name VARCHAR, description VARCHAR)"
    )
    con.execute("CREATE TABLE study_conditions (nct_id VARCHAR, condition_name VARCHAR)")
    con.execute(
        "CREATE TABLE study_condition_codes (nct_id VARCHAR, code_system VARCHAR, code VARCHAR, label VARCHAR,"
        " icd10_category VARCHAR, via VARCHAR, match_type VARCHAR, source VARCHAR)"
    )
    return con


def test_memberships_use_published_codes_and_keywords_over_the_eligible_pool():
    con = _con()
    con.executemany(
        "INSERT INTO studies VALUES (?, ?, 'PHASE3', ?)",
        [("NCT1", "Drug A", False), ("NCT2", "Drug B", False), ("NCT3", "Drug C", True), ("NCT4", "Drug D", False)],
    )
    con.executemany(
        "INSERT INTO study_conditions VALUES (?, ?)",
        [("NCT2", "Type 2 Diabetes"), ("NCT2", "Obesity"), ("NCT3", "Asthma"), ("NCT4", "Healthy")],
    )
    con.execute("INSERT INTO study_condition_codes VALUES ('NCT1','ICD10CM','C34.90','x','C34','mesh','broad','m')")
    members = accuracy.memberships(con, ["PHASE3"], {"C34": 2})
    assert members == {"NCT1": {2}, "NCT2": {4}, "NCT4": set()}  # NCT3 is a healthy-volunteer study


def test_draw_reuses_the_random_sample_first_and_caps_each_chapter():
    members = {f"NCT{i:03}": {2} if i % 2 else {4} for i in range(40)}
    members["NCT000"] = {2, 4}
    picks = accuracy.draw(members, "1", 5, reuse=["NCT039", "NCT999"], chapters={2: "N", 4: "E"})
    assert picks[2][0] == "NCT039" and len(picks[2]) == 5 and len(picks[4]) == 5
    assert all(2 in members[n] for n in picks[2])
    assert picks == accuracy.draw(members, "1", 5, reuse=["NCT039"], chapters={2: "N", 4: "E"})
    assert picks[4] != accuracy.draw(members, "2", 5, reuse=["NCT039"], chapters={2: "N", 4: "E"})[4]


def test_sample_payload_strata_and_review_sets():
    picks = {2: ["NCT1", "NCT2"], 4: ["NCT2", "NCT3"]}
    members = {"NCT1": {2}, "NCT2": {2, 4}, "NCT3": {4}, "NCT4": set()}
    s = accuracy.sample_payload({"run_id": "R"}, ["PHASE3"], "1", 2, members, picks, ["NCT4"], 0.25, 0.25)
    assert s["ids"] == ["NCT1", "NCT2", "NCT3", "NCT4"] and s["pool"] == {"2": 2, "4": 2}
    assert len(s["blind"]) == 1 and len(s["second"]) == 1 and not set(s["blind"]) & set(s["second"])
    assert accuracy.strata(s)["NCT2"] == ["Neoplasms", "Endocrine and metabolic"]
    assert accuracy.strata(s)["NCT4"] == ["Random sample"]


def test_seed_cache_copies_only_matching_model_and_prompt():
    doc = {"nct_id": "NCT1", "brief_title": "T", "conditions": ["Asthma"]}
    h = llm.input_hash(llm.render_study(doc), "haiku")
    arm = {}
    other = {"NCT1": {"h": h, "codes": []}}
    assert accuracy.seed_cache(arm, [other], [doc], "sonnet") == 0 and arm == {}
    assert accuracy.seed_cache(arm, [other], [doc], "haiku") == 1 and arm["NCT1"]["h"] == h
    assert accuracy.arm_key("us.anthropic.claude-sonnet-4-6") == (
        "analysis/coding_accuracy/llm_cache_us.anthropic.claude-sonnet-4-6.json"
    )


def _code(code, conf="high", found=True, quote="q"):
    return {"code": code, "confidence": conf, "evidence": quote, "evidence_found": found}


def _study(nct="NCT1", published=(), arms=None, blind=False, strata=("Neoplasms",)):
    return {
        "nct_id": nct,
        "title": "T",
        "conditions": ["c"],
        "strata": list(strata),
        "blind": blind,
        "second": False,
        "published": [dict(zip(("code", "via", "match_type"), p, strict=True)) for p in published],
        "arms": arms or {},
    }


def test_systems_combine_rules_and_llm_only_where_rules_have_no_exact_code():
    target = _study(
        published=[("C80", "mesh", "broad"), ("C34", "llm", "exact")],
        arms={"a": [_code("C34"), _code("C78", "high"), _code("J44", "low")], "b": [_code("C50", "medium")]},
    )
    s = accuracy.systems(target, ["a", "b"], cascade=("a", "b"))
    assert s["published"] == ({"C34"}, {"C80", "C34"})
    assert s["mesh_only"] == (set(), {"C80"}) and s["rules_only"] == (set(), {"C80"})
    assert s["combined:a"] == ({"C34"}, {"C80", "C34", "C78", "J44"})  # catch-all and low confidence aren't exact
    assert s["combined:b"][0] == {"C50"} and s["combined:cascade"][0] == {"C34"}
    coded = _study(published=[("E11", "mesh", "exact")], arms={"a": [_code("E66")]})
    assert accuracy.systems(coded, ["a"])["combined:a"] == ({"E11"}, {"E11"})


def test_cascade_falls_back_when_the_cheap_arm_is_unsure():
    s = _study(arms={"a": [_code("C34", "medium")], "b": [_code("C50")]})
    assert accuracy.systems(s, ["a", "b"], cascade=("a", "b"))["combined:cascade"][0] == {"C50"}


def test_sheet_pools_candidates_without_provenance_and_blinds(tmp_path):
    preds = {
        "studies": [
            _study("NCT1", [("C34", "mesh", "exact")], {"a": [_code("C34", quote="lung"), _code("C78", found=False)]}),
            _study("NCT2", [("E11", "mesh", "exact")], blind=True),
        ]
    }
    rows = accuracy.sheet_rows(preds, {"C34": "Lung"})
    assert [(r["nct_id"], r["code"]) for r in rows] == [("NCT1", "C34"), ("NCT1", ""), ("NCT2", "")]
    assert rows[0]["code_label"] == "Lung" and rows[0]["evidence"] == "lung" and rows[2]["blind"] == "1"
    path = tmp_path / "labels.csv"
    accuracy.write_sheet(path, rows)
    assert len(list(csv.DictReader(open(path)))) == 3
    assert len({r["nct_id"] for r in accuracy.sheet_rows(preds, {}, per_chapter_limit=1)}) == 1


def _labels(tmp_path, rows):
    path = tmp_path / "labels.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=accuracy.SHEET_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow({k: "" for k in accuracy.SHEET_COLUMNS} | r)
    return accuracy.read_labels(path)


def test_gold_needs_every_candidate_labeled_and_reads_missing_codes(tmp_path):
    s = _study(published=[("C34", "mesh", "exact"), ("C78", "mesh", "broad")])
    partial = _labels(tmp_path, [{"nct_id": "NCT1", "code": "C34", "correct": "y"}])
    assert accuracy.gold(s, partial["NCT1"]) is None
    full = _labels(
        tmp_path,
        [
            {"nct_id": "NCT1", "code": "C34", "correct": "y", "primary": "y"},
            {"nct_id": "NCT1", "code": "C78", "correct": "n", "error_type": "8"},
            {"nct_id": "NCT1", "missing_codes": "*j44.1, E11"},
        ],
    )
    assert accuracy.gold(s, full["NCT1"]) == ({"C34", "J44", "E11"}, {"C34", "J44"}, ["8"])
    blind = _labels(tmp_path, [{"nct_id": "NCT1", "missing_codes": "*C34"}])
    assert accuracy.gold({**s, "blind": True}, blind["NCT1"]) == ({"C34"}, {"C34"}, [])


def test_score_precision_recall_primary_and_decision(tmp_path):
    preds = {
        "models": ["a"],
        "studies": [
            _study("NCT1", [("C34", "mesh", "exact")], {"a": []}),
            _study("NCT2", [("C50", "llm", "exact")], {"a": [_code("C50"), _code("C18", "medium")]}),
            _study("NCT3", [], {"a": []}),
        ],
    }
    labels = _labels(
        tmp_path,
        [
            {"nct_id": "NCT1", "code": "C34", "correct": "y", "primary": "y"},
            {"nct_id": "NCT2", "code": "C50", "correct": "y", "primary": "y"},
            {"nct_id": "NCT2", "code": "C18", "correct": "n", "error_type": "4"},
            {"nct_id": "NCT2", "missing_codes": "C61"},
        ],
    )
    res = accuracy.score(preds, labels, reps=50)
    assert res["labeled"] == 2 and res["unlabeled"] == 1  # NCT3 has no label rows
    pub = res["groups"]["Neoplasms"]["systems"]["published"]["exact"]
    assert (pub["tp"], pub["fp"], pub["fn"]) == (2, 0, 1) and pub["precision"] == 1 and pub["primary_hit"] == 1
    arm = res["groups"]["Neoplasms"]["systems"]["combined:a"]["exact"]
    assert (arm["tp"], arm["fp"], arm["fn"]) == (2, 1, 1)
    assert res["groups"]["Neoplasms"]["errors"] == {"4": 1}
    assert pub["decision"] == "develop"  # recall 2/3 is under the bar
    assert res["paired_primary_vs_published"]["combined:a"] == {"better": 0, "worse": 0, "p_value": None}
    assert "## Neoplasms (2 studies)" in accuracy.report(res)


def test_decide_mcnemar_and_kappa():
    t = {"precision": 0.9, "recall": 0.85, "primary": 0.9}
    good = {"precision": 0.97, "recall": 0.9, "primary_hit": 0.95, "precision_ci": [0.92, 0.99]}
    assert accuracy.decide(good, t, []) == "go"
    assert accuracy.decide({**good, "precision_ci": [0.85, 0.99]}, t, []) == "fix"
    assert accuracy.decide({**good, "recall": 0.5}, t, ["1", "2", "5"]) == "fix"
    assert accuracy.decide({**good, "recall": 0.5}, t, ["5", "6"]) == "develop"
    assert accuracy.mcnemar(0, 0) is None and accuracy.mcnemar(5, 5) == 1.0
    assert round(accuracy.mcnemar(10, 0), 4) == 0.002
    assert accuracy.kappa([(True, True), (False, False)]) == 1.0
    assert accuracy.kappa([(True, False), (False, True)]) == -1.0
