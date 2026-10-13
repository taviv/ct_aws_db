import io
import json
import logging
import time
import zipfile
from urllib.parse import parse_qs, urlsplit

import duckdb
import pytest

from ct_pipeline import reference as R
from ct_pipeline import umls as U
from ct_pipeline.handlers import reference_handler

from .test_reference import _build, _codes, _study, curated, fake_get  # noqa: F401

KEY = "secret-key-123"
MAP_HEADER = (
    "id\teffectiveTime\tactive\tmoduleId\trefsetId\treferencedComponentId\treferencedComponentName\t"
    "mapGroup\tmapPriority\tmapRule\tmapAdvice\tmapTarget\tmapTargetName\tcorrelationId\tmapCategoryId\tmapCategoryName"
)


def _map_line(concept, group, priority, rule, target, active="1"):
    return "\t".join(
        ["x", "20260901", active, "m", "r", concept, "name", group, priority, rule, "", target, "t", "", "", ""]
    )


SNOMED_MAP = "\n".join(
    [
        MAP_HEADER,
        _map_line("111", "1", "1", "IFA 248152002 | Female (finding) |", "C34.1"),
        _map_line("111", "1", "2", "OTHERWISE TRUE", "C34.11"),
        _map_line("222", "1", "1", "TRUE", "C50.919"),
        _map_line("222", "2", "1", "TRUE", "Z99.9"),
        _map_line("333", "1", "1", "TRUE", "C18.9", active="0"),
        _map_line("444", "1", "1", "TRUE", ""),
    ]
)


def _map_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("SNOMED_CT_to_ICD-10-CM_Resources/tls_Icd10cmHumanReadableMap_US1000124_20260901.tsv", SNOMED_MAP)
    return buf.getvalue()


class FakeUts:
    """UTS REST responses. crosswalk: {(mesh, target): [codes]}; search: {text: [cuis]}; atoms: {cui: [codes]}."""

    def __init__(self, crosswalk=None, search=None, atoms=None):
        self.crosswalk = crosswalk or {}
        self.search, self.atoms = search or {}, atoms or {}
        self.calls = []

    def __call__(self, url, headers=None):
        parts = urlsplit(url)
        q = {k: v[0] for k, v in parse_qs(parts.query).items()}
        self.calls.append(parts.path)
        if parts.path == "/releases":
            return json.dumps(
                [{"fileName": "m.zip", "releaseVersion": "2026-09-01", "downloadUrl": "https://dl/m.zip"}]
            )
        assert q.get("apiKey") == KEY, url
        if parts.path == "/download":
            return _map_zip()
        if parts.path.startswith("/rest/crosswalk/current/source/MSH/"):
            codes = self.crosswalk.get((parts.path.rsplit("/", 1)[-1], q["targetSource"]))
            return (
                json.dumps({"result": [{"ui": c, "name": f"n {c}", "obsolete": "false"} for c in codes]})
                if codes
                else None
            )
        if parts.path == "/rest/search/current":
            cuis = self.search.get(q["string"]) or ["NONE"]
            return json.dumps({"result": {"results": [{"ui": c, "name": c} for c in cuis]}})
        if parts.path.startswith("/rest/content/current/CUI/"):
            codes = self.atoms.get(parts.path.split("/")[5])
            base = "https://uts-ws.nlm.nih.gov/rest/content/2026AA/source/ICD10CM/"
            return json.dumps({"result": [{"code": base + c, "name": f"a {c}"} for c in codes]}) if codes else None
        raise AssertionError(url)


def test_parse_snomed_map_takes_default_group1_target():
    assert U.parse_snomed_icd_map(_map_zip()) == {"111": ["C34.11", "t"], "222": ["C50.919", "t"]}


def test_errors_never_contain_the_key(caplog):
    def failing(url, headers=None):
        raise RuntimeError(f"GET {url} → HTTP 401")

    client = U.UmlsClient(KEY, failing)
    with caplog.at_level(logging.DEBUG), pytest.raises(U.UmlsError) as e:
        client.crosswalk("D001943", "ICD10CM")
    assert "HTTP 401" in str(e.value) and KEY not in str(e.value) and e.value.__cause__ is None
    assert KEY not in caplog.text


def test_text_codes_skip_ambiguous_strings():
    client = U.UmlsClient(KEY, FakeUts(search={"nsclc": ["C1"], "aml": ["C2", "C3"]}, atoms={"C1": ["C34.90"]}))
    assert client.text_codes("nsclc") == [["C34.90", "a C34.90", "C1"]]
    assert client.text_codes("aml") == [] and client.text_codes("safety") == []


def test_lookup_caches_and_respects_deadline(store):
    uts = FakeUts(crosswalk={("D1", "ICD10CM"): ["C50"]})
    client = U.UmlsClient(KEY, uts)
    results, info = U.lookup(client, store, ["D1", "D2"], ["x"], deadline=time.monotonic() - 1)
    assert results == {} and not info["complete"] and uts.calls == []
    results, info = U.lookup(client, store, ["D1", "D2"], ["x"])
    assert info["complete"] and info["fetched"] == 5 and results["msh:ICD10CM:D1"] == [["C50", "n C50"]]
    calls = len(uts.calls)
    _, info = U.lookup(client, store, ["D1", "D2"], ["x"])
    assert info["cached"] == 5 and len(uts.calls) == calls
    assert KEY not in json.dumps(store.get_json(U.CACHE_KEY))


def _uts():
    # the fake ICD-10-CM set (test_reference.ORDER) has C34, C34.1, C34.11, C7A, C80, C80.1, Z00, Z00.6
    return FakeUts(
        crosswalk={
            ("D012345", "ICD10CM"): ["C80.1", "NOT-A-CODE"],
            ("D054321", "SNOMEDCT_US"): ["111"],
        },
        search={"nsclc": ["C9"]},
        atoms={"C9": ["C34.1"]},
    )


def test_build_reference_and_codes_with_umls(settings, store, site_store, curated):  # noqa: F811
    studies = [
        _study("NCT1", ["Breast thing"], meshes=[("D012345", "Breast Thing")]),
        _study("NCT2", ["Lung thing"], meshes=[("D054321", "Lung Thing")]),
        _study("NCT3", ["NSCLC"]),
        _study("NCT4", ["NSCLC"]),
    ]
    _build(settings, store, site_store, "R1", studies)
    pointer = R.build_reference(store, settings.work_dir, get=fake_get(), umls_client=U.UmlsClient(KEY, _uts()))
    info = pointer["sources"]["umls"]
    assert info["complete"] and info["snomed_icd10cm_map"] == "2026-09-01"
    assert info["dropped_invalid"]["umls"] == 1
    assert KEY not in json.dumps(pointer)
    path = store.root / pointer["tables"]["condition_code_map"]
    code_map = duckdb.sql(
        f"SELECT mesh_id, code, match_type, source FROM read_parquet('{path}') WHERE source LIKE 'umls%' ORDER BY 1"
    ).fetchall()
    assert code_map == [("D012345", "C80.1", "exact", "umls"), ("D054321", "C34.11", "broad", "umls_snomed")]

    from ct_pipeline.build import recode_snapshot

    recode_snapshot(settings, store, site_store)
    path = store.root / store.get_json(R.CURRENT_KEY)["tables"]["study_condition_codes"]
    codes = set(duckdb.sql(f"SELECT nct_id, code, via, source FROM read_parquet('{path}')").fetchall())
    assert ("NCT1", "C80.1", "mesh", "umls") in codes and ("NCT2", "C34.11", "mesh", "umls_snomed") in codes
    assert ("NCT3", "C34.1", "text", "umls") in codes and ("NCT4", "C34.1", "text", "umls") in codes


def test_reference_without_key_and_old_reference_without_text_map(settings, store, site_store, curated):  # noqa: F811
    pointer = R.build_reference(store, settings.work_dir, get=fake_get())
    assert pointer["sources"]["umls"] == {"skipped": "no UMLS API key"}
    assert pointer["row_counts"]["condition_text_map"] == 0
    del pointer["tables"]["condition_text_map"]
    store.put_json(R.REFERENCE_CURRENT_KEY, pointer)
    _build(settings, store, site_store, "R1", [_study("NCT1", ["Healthy Volunteers"])])
    assert {r[2] for r in _codes(store)} == {"Z00.6"}


def test_handler_skips_umls_when_secret_missing(monkeypatch):
    monkeypatch.setattr("ct_pipeline.handlers.umls_api_key", lambda name: None)
    seen = {}
    monkeypatch.setattr(
        "ct_pipeline.handlers.build_reference",
        lambda store, work, umls_client, deadline: (
            seen.update(client=umls_client) or {"version": "v", "row_counts": {}}
        ),
    )
    monkeypatch.setattr("ct_pipeline.handlers.open_store", lambda uri: type("S", (), {"get_json": lambda s, k: None})())
    assert reference_handler({}, None) == {"reference_version": "v", "row_counts": {}}
    assert seen["client"] is None


def test_umls_coverage_script(settings, store, site_store, curated, monkeypatch, capsys):  # noqa: F811
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "umls_coverage", Path(__file__).parents[1] / "scripts" / "umls_coverage.py"
    )
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)
    studies = [
        _study("NCT1", ["Breast thing"], meshes=[("D012345", "Breast Thing")]),
        _study("NCT3", ["NSCLC"]),
        _study("NCT4", ["NSCLC"]),
        _study("NCT5", ["Healthy Volunteers"]),
    ]
    R.build_reference(store, settings.work_dir, get=fake_get())
    _build(settings, store, site_store, "R1", studies)

    class Boto:
        def describe_stacks(self, StackName):
            return {"Stacks": [{"Outputs": [{"OutputKey": "DataBucketName", "OutputValue": "b"}]}]}

        def get_secret_value(self, SecretId):
            return {"SecretString": KEY + "\n"}

    monkeypatch.setattr(script.boto3, "client", lambda name: Boto())
    monkeypatch.setattr(script, "S3Store", lambda bucket: store)
    monkeypatch.setattr(script, "uts_get", _uts())
    monkeypatch.setattr("sys.argv", ["umls_coverage.py"])
    script.main()
    out = capsys.readouterr().out
    assert "1 → 4 of 4 (25.0% → 100.0%)" in out, out
    assert KEY not in out


class ForbiddenSnomed(FakeUts):
    def __call__(self, url, headers=None):
        if "targetSource=SNOMEDCT_US" in url:
            raise RuntimeError(f"GET {url} → HTTP 403")
        return super().__call__(url, headers)


def test_failed_lookups_are_counted_skipped_and_retried(store):
    client = U.UmlsClient(KEY, ForbiddenSnomed(crosswalk={("D1", "ICD10CM"): ["C50"]}))
    results, info = U.lookup(client, store, ["D1", "D2"], [])
    assert results == {"msh:ICD10CM:D1": [["C50", "n C50"]], "msh:ICD10CM:D2": []}
    assert info["errors"] == {"SNOMEDCT_US": {"403": 2}} and not info["complete"]
    assert KEY not in json.dumps(info)
    _, info = U.lookup(client, store, ["D1", "D2"], [])
    assert info["cached"] == 2 and info["fetched"] == 2  # only the failed ones are retried


def test_all_lookups_failing_aborts(store):
    def forbidden(url, headers=None):
        raise RuntimeError(f"GET {url} → HTTP 403")

    with pytest.raises(U.UmlsError, match="All of the first 2 UMLS lookups failed.*HTTP 403") as e:
        U.lookup(U.UmlsClient(KEY, forbidden), store, ["D1"], [])
    assert KEY not in str(e.value)


def test_client_rate_limit():
    client = U.UmlsClient(KEY, FakeUts(), rate=20)
    t0 = time.monotonic()
    for _ in range(5):
        client.crosswalk("D1", "ICD10CM")
    assert time.monotonic() - t0 >= 4 / 20
