import time

import duckdb
import pytest

from ct_pipeline import llm

DOC = {
    "nct_id": "NCT1",
    "brief_title": "Drug X in Advanced Solid Tumors",
    "official_title": None,
    "conditions": ["Advanced Solid Tumors"],
    "interventions": ["DRUG: X"],
    "brief_summary": "A phase 1 study of drug X.",
    "eligibility_criteria": "Inclusion: histologically confirmed non-small cell lung cancer\n  or  other solid tumor",
}


@pytest.fixture
def catalog(tmp_path):
    path = tmp_path / "icd.parquet"
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE icd (code VARCHAR, description VARCHAR, level VARCHAR, chapter INT, chapter_title VARCHAR)"
    )
    con.execute("""INSERT INTO icd VALUES
        ('C34', 'Malignant neoplasm of bronchus and lung', 'category', 2, 'Neoplasms'),
        ('C34.1', 'Malignant neoplasm of upper lobe, bronchus or lung', 'code', 2, 'Neoplasms'),
        ('C80', 'Malignant neoplasm without specification of site', 'category', 2, 'Neoplasms'),
        ('E11', 'Type 2 diabetes mellitus', 'category', 4, 'Endocrine'),
        ('Z00', 'Encounter for general examination', 'category', 21, 'Factors')""")
    con.execute(f"COPY icd TO '{path}' (FORMAT parquet)")
    return llm.category_catalog(con, path)


class FakeBedrock:
    def __init__(self, answer):
        self.answer = answer
        self.calls = []

    def converse(self, **kw):
        self.calls.append(kw)
        answer = self.answer(kw) if callable(self.answer) else self.answer
        if isinstance(answer, Exception):
            raise answer
        content = [{"text": "Coding."}, {"toolUse": {"toolUseId": "t1", "name": llm.TOOL_NAME, "input": answer}}]
        return {
            "output": {"message": {"role": "assistant", "content": content}},
            "usage": {"inputTokens": 100, "outputTokens": 20, "cacheReadInputTokens": 5000, "totalTokens": 120},
            "stopReason": "tool_use",
        }


LUNG = {"codes": [{"code": "C34.1", "evidence": "non-small cell lung cancer or other", "confidence": "high"}]}


def test_catalog_lists_categories_by_chapter(catalog):
    text, categories = catalog
    assert set(categories) == {"C34", "C80", "E11", "Z00"}
    assert text.startswith("# Chapter 2: Neoplasms\nC34 Malignant neoplasm of bronchus and lung\nC80 ")
    assert "# Chapter 21: Factors\nZ00 " in text


def test_validate_keeps_catalog_categories_and_checks_evidence(catalog):
    _, categories = catalog
    text = llm.render_study(DOC)
    answer = {
        "codes": [
            {"code": "C34.1", "evidence": "non-small cell lung cancer or other", "confidence": "high"},
            {"code": "c80", "evidence": "histologically confirmed ... other solid tumor", "confidence": "medium"},
            {"code": "E11", "evidence": "type 2 diabetes", "confidence": "sure"},
            {"code": "X99", "evidence": "Drug X", "confidence": "high"},
            {"code": "C34", "evidence": "lung", "confidence": "high"},
            "junk",
        ]
    }
    got = [(c["code"], c["confidence"], c["evidence_found"]) for c in llm.validate(answer, categories, text)]
    assert got == [("C34", "high", True), ("C80", "medium", True), ("E11", "low", False)]
    assert llm.validate({"codes": None}, categories, text) == []


def test_render_study_truncates_long_text():
    doc = DOC | {"eligibility_criteria": "x" * (llm.MAX_ELIGIBILITY_CHARS + 10), "official_title": "Official"}
    text = llm.render_study(doc)
    assert "Official title: Official" in text
    assert text.endswith("x [...]")


def test_bedrock_request_caches_catalog_and_forces_tool(catalog):
    client = FakeBedrock(LUNG)
    coder = llm.BedrockCoder("model-a", catalog[0], client=client)
    answer, usage = coder.code("study text")
    assert answer == LUNG
    assert usage["cacheReadInputTokens"] == 5000
    call = client.calls[0]
    assert call["modelId"] == "model-a"
    assert "C34 Malignant neoplasm of bronchus and lung" in call["system"][0]["text"]
    assert call["system"][1] == {"cachePoint": {"type": "default"}}
    assert call["toolConfig"]["toolChoice"] == {"tool": {"name": llm.TOOL_NAME}}
    assert call["messages"][0]["content"][0]["text"] == "study text"


def test_code_studies_caches_answers_and_recodes_changed_studies(catalog, store):
    _, categories = catalog
    client = FakeBedrock(LUNG)
    coder = llm.BedrockCoder("model-a", catalog[0], client=client)
    entries, info = llm.code_studies(coder, store, [DOC], categories)
    assert info["coded"] == 1 and info["complete"] and info["usage"]["inputTokens"] == 100
    assert entries["NCT1"]["codes"][0]["code"] == "C34"
    assert store.get_json(llm.CACHE_KEY)["entries"]["NCT1"]["prompt"] == llm.PROMPT_VERSION

    entries, info = llm.code_studies(coder, store, [DOC], categories)
    assert (info["cached"], info["coded"], len(client.calls)) == (1, 0, 1)

    changed = DOC | {"brief_summary": "Updated summary."}
    _, info = llm.code_studies(coder, store, [changed], categories)
    assert (info["cached"], info["coded"], len(client.calls)) == (0, 1, 2)


def test_code_studies_fails_fast_when_every_call_fails(catalog, store):
    coder = llm.BedrockCoder("model-a", catalog[0], client=FakeBedrock(RuntimeError("AccessDenied")))
    with pytest.raises(llm.LlmError, match="All of the first 1 Bedrock calls failed"):
        llm.code_studies(coder, store, [DOC], catalog[1])


def test_code_studies_stops_at_deadline(catalog, store):
    coder = llm.BedrockCoder("model-a", catalog[0], client=FakeBedrock(LUNG))
    entries, info = llm.code_studies(coder, store, [DOC], catalog[1], deadline=time.monotonic() - 1)
    assert entries == {} and not info["complete"]


def test_llm_code_rows_keep_verified_evidence_only(catalog):
    entries = {
        "NCT1": {
            "model": "m",
            "prompt": "v1",
            "at": "2026-10-14T00:00:00+00:00",
            "codes": [
                {"code": "C34", "confidence": "high", "evidence": "lung", "evidence_found": True},
                {"code": "C80", "confidence": "low", "evidence": "tumor", "evidence_found": True},
                {"code": "E11", "confidence": "high", "evidence": "made up", "evidence_found": False},
                {"code": "C99", "confidence": "high", "evidence": "gone", "evidence_found": True},
            ],
        }
    }
    rows = llm.llm_code_rows(entries, catalog[1])
    assert [(r["code"], r["match_type"]) for r in rows] == [("C34", "exact"), ("C80", "broad")]
    assert rows[0]["model"] == "m" and rows[0]["prompt_version"] == "v1"


def test_targets_and_documents():
    con = duckdb.connect()
    con.execute("CREATE TABLE studies (nct_id VARCHAR, brief_title VARCHAR, official_title VARCHAR)")
    con.execute("INSERT INTO studies VALUES ('NCT1','a',NULL), ('NCT2','b',NULL), ('NCT3','c',NULL), ('NCT4','d',NULL)")
    con.execute(
        "CREATE TABLE study_text (nct_id VARCHAR, brief_summary VARCHAR, detailed_description VARCHAR,"
        " eligibility_criteria VARCHAR)"
    )
    con.execute("INSERT INTO study_text VALUES ('NCT2', 'summary', NULL, NULL)")
    con.execute("CREATE TABLE study_conditions (nct_id VARCHAR, condition_name VARCHAR)")
    con.execute("INSERT INTO study_conditions VALUES ('NCT2','Tumor'), ('NCT2','Cancer'), ('NCT2','Cancer')")
    con.execute("CREATE TABLE study_interventions (nct_id VARCHAR, intervention_type VARCHAR, name VARCHAR)")
    con.execute("INSERT INTO study_interventions VALUES ('NCT2','DRUG','X'), ('NCT2',NULL,'Y')")
    con.execute(
        "CREATE TABLE study_condition_codes (nct_id VARCHAR, code_system VARCHAR, match_type VARCHAR, via VARCHAR)"
    )
    con.execute("""INSERT INTO study_condition_codes VALUES ('NCT1','ICD10CM','exact','mesh'),
        ('NCT2','ICD10CM','broad','mesh_ancestor'), ('NCT4','ICD10CM','exact','llm'), ('NCT3','UMLS','exact','mesh')""")
    assert [r[0] for r in con.execute(llm.TARGETS_SQL).fetchall()] == ["NCT2", "NCT3", "NCT4"]
    (doc,) = llm.study_documents(con, ["NCT2"])
    assert doc["conditions"] == ["Cancer", "Tumor"]
    assert doc["interventions"] == ["DRUG: X", "Y"]
    assert doc["brief_summary"] == "summary" and doc["eligibility_criteria"] is None
    assert [d["nct_id"] for d in llm.study_documents(con)] == ["NCT1", "NCT2", "NCT3", "NCT4"]


def test_render_study_sends_only_the_chosen_text_fields():
    assert "Summary:\nA phase 1" in llm.render_study(DOC) and "Eligibility criteria:\nInclusion" in llm.render_study(
        DOC
    )
    doc = {**DOC, "detailed_description": "Part B enrolls gastric cancer."}
    text = llm.render_study(doc, ("detailed_description",))
    assert "Detailed description:\nPart B enrolls gastric cancer." in text
    assert "Summary:" not in text and "Eligibility" not in text
