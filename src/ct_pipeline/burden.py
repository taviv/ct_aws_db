"""US population burden by ICD-10 category, to compare against trial counts.

Sources (all free; the API/FTP ones need no login):
- CDC WONDER Underlying Cause of Death (deaths), via the WONDER XML API.
- NCHS NAMCS (office visits) and NHAMCS-ED (emergency visits) public-use files, survey-weighted.
- IHME GBD results-tool CSV exports (prevalence, deaths, DALYs), mapped to ICD-10 chapters.

Everything is reduced to one long table: source, measure, year, icd10_category, icd10_chapter, gbd_cause,
value, n_records (unweighted survey records, for the NCHS "<30 records is unreliable" rule).
"""

import csv
import io
import re
import time
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

COLUMNS = ["source", "measure", "year", "icd10_category", "icd10_chapter", "gbd_cause", "value", "n_records"]

# ICD-10 chapter (1-22, same numbering as study_condition_codes.icd10_chapter) by first code in the range
_CHAPTER_STARTS = [
    ("A00", 1), ("C00", 2), ("D50", 3), ("E00", 4), ("F01", 5), ("G00", 6), ("H00", 7), ("H60", 8), ("I00", 9),
    ("J00", 10), ("K00", 11), ("L00", 12), ("M00", 13), ("N00", 14), ("O00", 15), ("P00", 16), ("Q00", 17),
    ("R00", 18), ("S00", 19), ("V00", 20), ("Z00", 21), ("U00", 22),
]  # fmt: skip
_CATEGORY = re.compile(r"^[A-Z][0-9][0-9A-Z]")


def category(code) -> str | None:
    """3-character ICD-10 category from 'C50.9', 'C509', 'B88-' etc.; None for blanks/NCHS fillers."""
    code = str(code or "").strip().upper()
    if not _CATEGORY.match(code) or code.startswith("ZZZ"):
        return None
    return code[:3]


def chapter(cat: str | None) -> int | None:
    if not cat:
        return None
    if cat[0] == "U":
        return 22
    if cat < "A00" or cat >= "U00" and cat < "V00":
        return None
    best = None
    for start, ch in _CHAPTER_STARTS:
        if start[0] != "U" and cat >= start:
            best = ch
    return best


def row(source, measure, year, cat=None, value=0.0, n_records=None, gbd_cause=None, chapter_no=None) -> dict:
    return {
        "source": source,
        "measure": measure,
        "year": int(year),
        "icd10_category": cat,
        "icd10_chapter": chapter_no if chapter_no is not None else chapter(cat),
        "gbd_cause": gbd_cause,
        "value": float(value),
        "n_records": n_records,
    }


# --- CDC WONDER --------------------------------------------------------------------------------------------

WONDER_URL = "https://wonder.cdc.gov/controller/datarequest/{db}"
WONDER_DB = "D76"  # Underlying Cause of Death 1999-2020; the newer D158 (2018+) is not served by the API
WONDER_YEARS = range(1999, 2021)
WONDER_WAIT = 16  # the API rejects requests less than 15 s apart


def wonder_request_xml(year: int, db: str = WONDER_DB) -> str:
    """Deaths for one year, grouped by year and ICD-10 code (4 characters), all US."""
    v = lambda n: f"{db}.V{n}"  # noqa: E731
    params = {
        "B_1": f"{v(1)}-level1", "B_2": f"{v(2)}-level3", "B_3": "*None*", "B_4": "*None*", "B_5": "*None*",
        "M_1": f"{db}.M1", "M_2": f"{db}.M2", "M_3": f"{db}.M3",
        f"F_{v(1)}": [str(year)], f"F_{v(2)}": ["*All*"], f"F_{v(9)}": ["*All*"], f"F_{v(10)}": ["*All*"],
        f"F_{v(27)}": ["*All*"],
        f"I_{v(1)}": "", f"I_{v(2)}": "*All* (All Causes of Death)",
        "O_V1_fmode": "freg", "O_V2_fmode": "freg", "O_V9_fmode": "freg", "O_V10_fmode": "freg", "O_V27_fmode": "freg",
        "O_age": v(5), "O_ucd": v(2), "O_location": v(9), "O_urban": v(19),
        "O_javascript": "on", "O_precision": "1", "O_rate_per": "100000", "O_show_totals": "false",
        "O_timeout": "600", "O_title": "", "O_oc-sect1-request": "close", "O_aar": "aar_none", "O_aar_pop": "0000",
        f"VM_{db}.M6_{v(10)}": "", "action-Send": "Send",
        f"V_{v(1)}": "", f"V_{v(2)}": "", f"V_{v(5)}": ["*All*"], f"V_{v(4)}": "*All*", f"V_{v(6)}": "00",
        f"V_{v(9)}": "", f"V_{v(10)}": "", f"V_{v(25)}": "", f"V_{v(27)}": "",
        **{f"V_{v(n)}": "*All*" for n in (7, 8, 11, 12, 17, 19, 20, 21, 22, 23, 24, 51, 52)},
    }  # fmt: skip
    parts = []
    for name, values in {**params, "accept_datause_restrictions": "true"}.items():
        values = values if isinstance(values, list) else [values]
        parts.append(f"<parameter><name>{name}</name>{''.join(f'<value>{x}</value>' for x in values)}</parameter>")
    return "<request-parameters>" + "".join(parts) + "</request-parameters>"


def parse_wonder(xml: str) -> list[dict]:
    """Deaths per 3-character category from a WONDER response (suppressed/blank cells skipped)."""
    totals: dict[tuple[int, str], float] = {}
    for r in re.findall(r"<r>(.*?)</r>", xml, re.S):
        cells = re.findall(r"<c ([^>]*?)/?>", r)
        if len(cells) < 3:
            continue
        year = re.search(r'l="(\d{4})"', cells[0])
        code = re.search(r'cd="([^"]+)"', cells[1])
        deaths = re.search(r'v="([\d,]+)"', cells[2])
        cat = category(code.group(1)) if code else None
        if year and cat and deaths:
            key = (int(year.group(1)), cat)
            totals[key] = totals.get(key, 0) + float(deaths.group(1).replace(",", ""))
    return [row("cdc_wonder_ucd", "deaths", y, c, n) for (y, c), n in sorted(totals.items())]


def fetch_wonder(years, cache: Path, log=print) -> list[dict]:
    out, last = [], 0.0
    for year in years:
        path = cache / f"wonder_{WONDER_DB}_{year}.xml"
        if not path.exists():
            time.sleep(max(0.0, WONDER_WAIT - (time.time() - last)))
            data = urllib.parse.urlencode(
                {"request_xml": wonder_request_xml(year), "accept_datause_restrictions": "true"}
            ).encode()
            req = urllib.request.Request(WONDER_URL.format(db=WONDER_DB), data)
            body = urllib.request.urlopen(req, timeout=600).read().decode("latin-1")
            last = time.time()
            path.write_text(body, encoding="latin-1")
        rows = parse_wonder(path.read_text(encoding="latin-1"))
        if not rows:
            raise RuntimeError(f"CDC WONDER returned no rows for {year} (see {path})")
        log(f"  WONDER {year}: {sum(r['value'] for r in rows):,.0f} deaths in {len(rows)} categories")
        out += rows
    return out


def parse_wonder_export(text: str) -> list[dict]:
    """A tab-delimited 'Export Results' file from the WONDER web form (Year + ICD-10 Code(s) + Deaths),
    for years the API doesn't serve (e.g. 2021+ from 'Underlying Cause of Death, 2018-2023, Single Race')."""
    totals: dict[tuple[int, str], float] = {}
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.startswith('"---')]
    reader = csv.DictReader(lines, delimiter="\t")
    for r in reader:
        if r.get("Notes", "").strip() == "Total" or not r.get("Year Code"):
            continue
        code = r.get("Cause of death Code") or r.get("ICD-10 Code") or r.get("UCD - ICD-10 Code") or ""
        cat = category(code)
        deaths = (r.get("Deaths") or "").replace(",", "")
        if cat and deaths.isdigit():
            key = (int(float(r["Year Code"])), cat)
            totals[key] = totals.get(key, 0) + float(deaths)
    return [row("cdc_wonder_ucd", "deaths", y, c, n) for (y, c), n in sorted(totals.items())]


# --- NAMCS / NHAMCS-ED ---------------------------------------------------------------------------------------

NCHS_DOCS = "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation"
# Stata public-use files. NAMCS has no 2017 PUF, and from 2020 it became the health-center component (different
# design), so office visits stop at 2019.
NCHS_FILES = {
    ("namcs", 2016): "NAMCS/2016/namcs2016-stata.zip",
    ("namcs", 2018): "NAMCS/2018/NAMCS2018-stata.zip",
    ("namcs", 2019): "NAMCS/2019/NAMCS2019-stata.zip",
    ("nhamcs_ed", 2016): "NHAMCS/stata/ED2016-stata.zip",
    ("nhamcs_ed", 2017): "NHAMCS/stata/ed2017-stata.zip",
    ("nhamcs_ed", 2018): "NHAMCS/stata/ED2018-stata.zip",
    ("nhamcs_ed", 2019): "NHAMCS/stata/ED2019-stata.zip",
    ("nhamcs_ed", 2020): "NHAMCS/stata/ed2020-stata.zip",
    ("nhamcs_ed", 2021): "NHAMCS/stata/ed2021-stata.zip",
    ("nhamcs_ed", 2022): "NHAMCS/stata/ed2022-stata.zip",
}
DIAG_COLUMNS = [f"DIAG{i}" for i in range(1, 6)]


def visit_rows(source: str, year: int, weights: list, diags: list) -> list[dict]:
    """Survey-weighted visits per category: a visit counts once per category among DIAG1-5 (any-listed), and
    separately for its first-listed diagnosis."""
    any_w: dict[str, float] = {}
    any_n: dict[str, int] = {}
    first_w: dict[str, float] = {}
    first_n: dict[str, int] = {}
    for w, codes in zip(weights, diags, strict=True):
        cats = [category(c) for c in codes]
        for c in {c for c in cats if c}:
            any_w[c] = any_w.get(c, 0) + w
            any_n[c] = any_n.get(c, 0) + 1
        if cats[0]:
            first_w[cats[0]] = first_w.get(cats[0], 0) + w
            first_n[cats[0]] = first_n.get(cats[0], 0) + 1
    out = [row(source, "visits_any_dx", year, c, any_w[c], any_n[c]) for c in sorted(any_w)]
    out += [row(source, "visits_first_dx", year, c, first_w[c], first_n[c]) for c in sorted(first_w)]
    out.append(row(source, "visits_total", year, None, sum(weights), len(weights)))
    return out


def fetch_nchs(keys, cache: Path, log=print) -> list[dict]:
    import pandas as pd

    out = []
    for source, year in keys:
        rel = NCHS_FILES[(source, year)]
        path = cache / Path(rel).name
        if not path.exists():
            urllib.request.urlretrieve(f"{NCHS_DOCS}/{rel}", path)
        with zipfile.ZipFile(path) as z:
            name = next(n for n in z.namelist() if n.lower().endswith(".dta"))
            df = pd.read_stata(io.BytesIO(z.read(name)), convert_categoricals=False, columns=[*DIAG_COLUMNS, "PATWT"])
        rows = visit_rows(
            source, year, df["PATWT"].astype(float).tolist(), df[DIAG_COLUMNS].astype(str).values.tolist()
        )
        log(f"  {source} {year}: {len(df):,} records, {df['PATWT'].sum() / 1e6:,.1f}M weighted visits")
        out += rows
    return out


# --- IHME GBD ------------------------------------------------------------------------------------------------

# GBD 2021 level-2 causes -> the ICD-10 chapters their ICD codes mostly fall in. Approximate: GBD assigns
# deaths/burden by its own cause list, so chapter totals are only comparable at this coarse level.
GBD_CAUSE_CHAPTERS = {
    "HIV/AIDS and sexually transmitted infections": (1,),
    "Respiratory infections and tuberculosis": (1, 10),
    "Enteric infections": (1,),
    "Neglected tropical diseases and malaria": (1,),
    "Other infectious diseases": (1,),
    "COVID-19": (22,),
    "Maternal and neonatal disorders": (15, 16),
    "Nutritional deficiencies": (3, 4),
    "Neoplasms": (2,),
    "Cardiovascular diseases": (9,),
    "Chronic respiratory diseases": (10,),
    "Digestive diseases": (11,),
    "Neurological disorders": (6,),
    "Mental disorders": (5,),
    "Substance use disorders": (5,),
    "Diabetes and kidney diseases": (4, 14),
    "Skin and subcutaneous diseases": (12,),
    "Sense organ diseases": (7, 8),
    "Musculoskeletal disorders": (13,),
    "Other non-communicable diseases": (17, 14, 3),
    "Transport injuries": (19,),
    "Unintentional injuries": (19,),
    "Self-harm and interpersonal violence": (19,),
}
GBD_MEASURES = {"Prevalence": "prevalence", "Incidence": "incidence", "Deaths": "deaths", "DALYs": "dalys"}


def _pick(r: dict, *names) -> str:
    for n in names:
        if r.get(n) not in (None, ""):
            return r[n]
    return ""


def parse_gbd(text: str) -> list[dict]:
    """Rows of a GBD results-tool CSV for the USA, both sexes, all ages, metric Number. Causes not in
    GBD_CAUSE_CHAPTERS are kept with icd10_chapter empty; single-chapter causes get that chapter."""
    out = []
    for r in csv.DictReader(io.StringIO(text.lstrip("\ufeff"))):
        loc = _pick(r, "location_name", "location")
        if loc not in ("United States of America", "United States"):
            continue
        if _pick(r, "metric_name", "metric") != "Number":
            continue
        if _pick(r, "sex_name", "sex") not in ("Both", "") or _pick(r, "age_name", "age") not in ("All ages", ""):
            continue
        measure = GBD_MEASURES.get(_pick(r, "measure_name", "measure").split(" (")[0])
        if not measure:
            continue
        cause = _pick(r, "cause_name", "cause")
        chapters = GBD_CAUSE_CHAPTERS.get(cause, ())
        out.append(
            row(
                "ihme_gbd", measure, _pick(r, "year", "year_id"), value=float(_pick(r, "val")), gbd_cause=cause,
                chapter_no=chapters[0] if len(chapters) == 1 else None,
            )
        )  # fmt: skip
    return out
