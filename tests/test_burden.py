from ct_pipeline import burden


def test_category_and_chapter():
    assert burden.category("C50.9") == "C50"
    assert burden.category("B88-") == "B88"
    assert burden.category("-9") is None
    assert burden.category("ZZZ1") is None
    assert burden.category(None) is None
    assert [burden.chapter(c) for c in ["A04", "D48", "D50", "H59", "H60", "O9A", "U07", "W19", "Z00"]] == [
        1, 2, 3, 7, 8, 15, 22, 20, 21,
    ]  # fmt: skip
    assert burden.chapter(None) is None


def test_parse_wonder_sums_to_categories_and_skips_suppressed():
    xml = """<data-table>
    <r><c l="2016"/><c l="Breast" cd="C50.9" cf="t"/><c v="41,000"/><c v="323,127,513"/></r>
    <r><c l="2016"/><c l="Breast nos" cd="C50.1"/><c v="10"/><c v="1"/></r>
    <r><c l="2016"/><c l="Rare" cd="A02.1"/><c v="Suppressed"/><c v="1"/></r>
    <r><c l="2016"/><c l="Hypertension" cd="I10"/><c v="35,000"/><c v="1"/></r>
    </data-table>"""
    rows = burden.parse_wonder(xml)
    assert [(r["icd10_category"], r["value"], r["icd10_chapter"]) for r in rows] == [
        ("C50", 41010.0, 2),
        ("I10", 35000.0, 9),
    ]
    assert rows[0]["source"] == "cdc_wonder_ucd" and rows[0]["year"] == 2016


def test_wonder_request_xml_has_year_and_codes():
    xml = burden.wonder_request_xml(2018)
    assert "<name>F_D76.V1</name><value>2018</value>" in xml
    assert "<name>B_2</name><value>D76.V2-level3</value>" in xml
    assert "accept_datause_restrictions" in xml


def test_parse_wonder_export():
    text = (
        '"Notes"\t"Year"\t"Year Code"\t"Cause of death"\t"Cause of death Code"\t"Deaths"\t"Population"\n'
        '\t"2021"\t2021\t"Breast"\t"C50.9"\t42000\t1\n'
        '\t"2021"\t2021\t"x"\t"A02.1"\tSuppressed\t1\n'
        '"Total"\t\t\t\t\t3000000\t1\n'
        '"---"\n'
    )
    rows = burden.parse_wonder_export(text)
    assert [(r["year"], r["icd10_category"], r["value"]) for r in rows] == [(2021, "C50", 42000.0)]


def test_visit_rows_counts_a_visit_once_per_category():
    diags = [
        ["E119", "E118", "I10-", "-9", "ZZZ1"],
        ["J069", "E11-", "-9", "-9", "-9"],
        ["-9", "-9", "-9", "-9", "-9"],
    ]
    rows = {
        (r["measure"], r["icd10_category"]): r for r in burden.visit_rows("namcs", 2019, [100.0, 50.0, 25.0], diags)
    }
    assert rows[("visits_any_dx", "E11")]["value"] == 150.0
    assert rows[("visits_any_dx", "E11")]["n_records"] == 2
    assert rows[("visits_first_dx", "E11")]["value"] == 100.0
    assert rows[("visits_first_dx", "J06")]["value"] == 50.0
    assert ("visits_any_dx", "ZZZ") not in rows
    assert rows[("visits_total", None)]["value"] == 175.0


def test_parse_gbd_filters_and_maps_chapters():
    text = (
        "measure_name,location_name,sex_name,age_name,cause_name,metric_name,year,val,upper,lower\n"
        "DALYs (Disability-Adjusted Life Years),United States of America,Both,All ages,Neoplasms,Number,2019,"
        "21000000,1,1\n"
        "DALYs (Disability-Adjusted Life Years),United States of America,Both,All ages,"
        "Diabetes and kidney diseases,Number,2019,9000000,1,1\n"
        "DALYs (Disability-Adjusted Life Years),United States of America,Both,All ages,Neoplasms,Rate,2019,6000,1,1\n"
        "Prevalence,Canada,Both,All ages,Neoplasms,Number,2019,1,1,1\n"
    )
    rows = burden.parse_gbd(text)
    assert [(r["measure"], r["gbd_cause"], r["icd10_chapter"], r["value"]) for r in rows] == [
        ("dalys", "Neoplasms", 2, 21000000.0),
        ("dalys", "Diabetes and kidney diseases", None, 9000000.0),
    ]


def test_dashboard_payload_and_validation_drop_non_disease_rows():
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "us_burden", Path(__file__).resolve().parents[1] / "scripts" / "us_burden.py"
    )
    us = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(us)
    rows = [
        burden.row("cdc_wonder_ucd", "deaths", 2019, "C34", 100.0),
        burden.row("cdc_wonder_ucd", "deaths", 2019, "W19", 30.0),
        burden.row("cdc_wonder_ucd", "deaths", 2019, "S72", 0.0),
        burden.row("namcs", "visits_any_dx", 2019, "C34", 2e6),
    ]
    cats = [("Z23", 21, 50), ("C79", 2, 40), ("C34", 2, 20), ("U07", 22, 10), ("S72", 19, 5)]
    chapters = {2: 45, 21: 50, 22: 10, 19: 5}
    p = us.dashboard_payload(rows, "2026-01-01", 100, cats, chapters, "2016-01-01", {"U07": "Emergency use"})
    assert [c["code"] for c in p["categories"]] == ["C34", "U07", "S72"]
    assert p["categories"][1]["label"] == "COVID-19"
    by_ch = {c["chapter"]: c for c in p["chapters"]}
    assert 21 not in by_ch and by_ch[19]["deaths"] == 30  # external-cause deaths shown on injury
    assert by_ch[2]["deaths"] == 100 and by_ch[2]["office_visits"] == 2_000_000
    md = us.validation_md(rows, "2026-01-01", 100, cats, chapters, "2016-01-01", 10)
    assert "| C34" in md and "| Z23" not in md and "| C79" not in md
    assert "| 20,000.0 | 10.0 |" in md  # trials per 100k deaths, per 1M office visits
