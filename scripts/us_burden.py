#!/usr/bin/env python3
"""
US population burden by ICD-10 category vs US phase 3 trial counts: `make us-burden`
(or `python scripts/us_burden.py --data ~/local/data`).

Downloads (free, cached in --cache) and normalises:
  - CDC WONDER Underlying Cause of Death: deaths by 3-character ICD-10 category, 2016-2020 (API);
    later years via --wonder-export files saved from the WONDER web form.
  - NAMCS office visits (2016, 2018, 2019) and NHAMCS-ED emergency visits (2016-2022), survey-weighted.
  - IHME GBD results-tool CSV exports passed with --gbd (prevalence/deaths/DALYs), mapped to ICD-10 chapters.
Then counts phase 3 (incl. phase 2/3) trials started since --since with at least one US site per
ICD-10-CM category, and writes:
  <out>/burden.csv      long table: source, measure, year, icd10_category, icd10_chapter, gbd_cause, value, ...
  <out>/trials.csv      US phase 3 trials per category
  <out>/validation.md   top disease categories by trial count next to mean annual deaths and visits
  <out>/us_burden.json  per-chapter and per-category payload for the Trials vs Burden tab
uploads the first three to the data bucket under analysis/us_burden/ and the JSON to the SiteBucket as
data/us_burden.json (--no-publish to skip).

Read-only against the stack; no LLM calls.
"""

import argparse
import csv
import json
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ct_pipeline import burden  # noqa: E402
from ct_pipeline.build import attach_snapshot  # noqa: E402
from ct_pipeline.config import CURRENT_KEY, REFERENCE_CURRENT_KEY  # noqa: E402
from ct_pipeline.db import connect  # noqa: E402
from ct_pipeline.storage import LocalStore, S3Store, open_store  # noqa: E402

TABLES = ("studies", "study_condition_codes", "study_locations")
# own prefix: reference/ is pruned as versioned snapshots, curated/ likewise
PUBLISH_PREFIX = "analysis/us_burden/"
OUTPUTS = {"burden.csv": "text/csv", "trials.csv": "text/csv", "validation.md": "text/markdown"}
DASHBOARD_KEY = "data/us_burden.json"
# Not diseases: symptoms (R), external causes (V-Y), health status/encounters (Z); and neoplasm catch-alls
# (ill-defined, secondary and unspecified sites) that say nothing about the cancer type.
NON_DISEASE_CHAPTERS = {18, 20, 21}
CATCH_ALL_CATEGORIES = {"C76", "C77", "C78", "C79", "C80"}
LABEL_OVERRIDES = {"U07": "COVID-19"}  # ICD-10-CM titles U07 "Emergency use of U07"
INJURY_CHAPTER, EXTERNAL_CHAPTER = 19, 20  # WONDER codes injury deaths to external causes, not S/T
TRIALS_SQL = """
WITH us AS (SELECT DISTINCT nct_id FROM study_locations WHERE country = 'United States'),
p3 AS (
    SELECT nct_id FROM studies
    WHERE phase_group IN ('PHASE3', 'PHASE2/PHASE3') AND start_date >= ?::DATE AND start_date <= ?::DATE
      AND overall_status <> 'WITHDRAWN' AND nct_id IN (SELECT nct_id FROM us)
)
SELECT icd10_category, any_value(icd10_chapter) AS chapter, count(DISTINCT nct_id) AS trials
FROM study_condition_codes
WHERE code_system = 'ICD10CM' AND icd10_category IS NOT NULL AND nct_id IN (SELECT nct_id FROM p3)
GROUP BY 1 ORDER BY 3 DESC, 1
"""
TOTAL_SQL = TRIALS_SQL.split("SELECT icd10_category")[0] + "SELECT count(*) FROM p3"
CHAPTER_SQL = (
    TRIALS_SQL.split("SELECT icd10_category")[0]
    + """SELECT icd10_chapter, count(DISTINCT nct_id) FROM study_condition_codes
WHERE code_system = 'ICD10CM' AND icd10_chapter IS NOT NULL AND nct_id IN (SELECT nct_id FROM p3) GROUP BY 1"""
)
CHAPTER_NAMES = {
    1: "Infectious", 2: "Neoplasms", 3: "Blood/immune", 4: "Endocrine/metabolic", 5: "Mental", 6: "Nervous system",
    7: "Eye", 8: "Ear", 9: "Circulatory", 10: "Respiratory", 11: "Digestive", 12: "Skin", 13: "Musculoskeletal",
    14: "Genitourinary", 15: "Pregnancy", 16: "Perinatal", 17: "Congenital & genetic", 18: "Symptoms/signs",
    19: "Injury/poisoning", 20: "External causes", 21: "Health status/Z codes", 22: "Special purposes (COVID)",
}  # fmt: skip


def stack_output(stack: str, key: str) -> str:
    import boto3

    outputs = boto3.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]["Outputs"]
    return next(o["OutputValue"] for o in outputs if o["OutputKey"] == key)


def open_data(args):
    return open_store(args.data) if args.data else S3Store(stack_output(args.stack, "DataBucketName"))


def open_site(args):
    """Site store for the dashboard payload: --site, else <data>/../site locally, else the stack's SiteBucket."""
    if args.site:
        return open_store(args.site)
    if args.data:
        site = Path(args.data).parent / "site"
        return LocalStore(site) if not str(args.data).startswith("s3") and site.exists() else None
    return S3Store(stack_output(args.stack, "SiteBucketName"))


def trial_counts(store, args):
    current = store.get_json(CURRENT_KEY)
    if current is None:
        sys.exit("No published snapshot yet (curated/CURRENT.json)")
    as_of = current["created_at"][:10]
    with tempfile.TemporaryDirectory(prefix="ct-burden-") as tmp:
        work = Path(tmp)
        for table in TABLES:
            store.download(current["tables"][table], work / "snapshot" / table / "data.parquet")
        con = connect(work)
        attach_snapshot(con, work / "snapshot", list(TABLES))
        params = [args.since, as_of]
        cats = con.execute(TRIALS_SQL, params).fetchall()
        chapters = dict(con.execute(CHAPTER_SQL, params).fetchall())
        total = con.execute(TOTAL_SQL, params).fetchone()[0]
        labels = {}
        ref = store.get_json(REFERENCE_CURRENT_KEY)
        if ref and "icd10cm_codes" in ref["tables"]:
            path = work / "icd10cm_codes.parquet"
            store.download(ref["tables"]["icd10cm_codes"], path)
            labels = dict(
                con.execute(f"SELECT code, description FROM read_parquet('{path}') WHERE level = 'category'").fetchall()
            )
    return as_of, total, cats, chapters, labels


def mean_by(rows, source, measure, key):
    """Mean annual value per key over the years the source has."""
    sums, years = defaultdict(float), set()
    for r in rows:
        if r["source"] == source and r["measure"] == measure:
            years.add(r["year"])
            if r[key] is not None:
                sums[r[key]] += r["value"]
    return {k: v / len(years) for k, v in sums.items()}, sorted(years)


def is_disease(cat, ch) -> bool:
    return ch not in NON_DISEASE_CHAPTERS and cat not in CATCH_ALL_CATEGORIES


def label_of(cat, labels) -> str:
    return LABEL_OVERRIDES.get(cat) or labels.get(cat, "")


def per(n, denom, scale):
    return n / denom * scale if denom else None


def fmt(x):
    return "" if x is None else f"{x:,.0f}"


def fmt1(x):
    return "" if x is None else f"{x:,.1f}"


def span(years):
    return f"{years[0]}-{years[-1]}" if years else "n/a"


def validation_md(rows, as_of, total, cats, chapters, since, top, labels=None):
    labels = labels or {}
    deaths, dy = mean_by(rows, "cdc_wonder_ucd", "deaths", "icd10_category")
    office, oy = mean_by(rows, "namcs", "visits_any_dx", "icd10_category")
    ed, ey = mean_by(rows, "nhamcs_ed", "visits_any_dx", "icd10_category")
    out = [
        "# US phase 3 trials vs US burden by ICD-10 category",
        "",
        f"Trials: phase 3 or 2/3, started {since} to {as_of}, at least one US site; {total:,} trials. A trial with "
        "several categories counts in each. Deaths: CDC WONDER underlying cause, mean per year "
        f"({span(dy)}). Office / ED visits: NAMCS ({', '.join(map(str, oy)) or 'n/a'}) / NHAMCS-ED ({span(ey)}), "
        "survey-weighted mean per year, any of up to 5 listed diagnoses. Deaths are WHO ICD-10, the rest ICD-10-CM; "
        "injury deaths are coded to external causes (V01-Y89), so S/T categories show no deaths.",
        "",
        "",
        "Symptom (R), external-cause (V-Y) and Z categories and neoplasm catch-alls (C76-C80: ill-defined, "
        "secondary and unspecified sites) are left out of this table; chapter totals below include them.",
        "",
        "| Category | Ch. | US ph3 trials | Deaths/yr | Office visits/yr | ED visits/yr | "
        "Trials per 100k deaths | Trials per 1M office visits |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for cat, ch, n in [c for c in cats if is_disease(c[0], c[1])][:top]:
        d, o = deaths.get(cat), office.get(cat)
        out.append(
            f"| {cat} {label_of(cat, labels)} | {ch} | {n:,} | {fmt(d)} | {fmt(o)} | {fmt(ed.get(cat))} | "
            f"{fmt1(per(n, d, 1e5))} | {fmt1(per(n, o, 1e6))} |"
        )
    deaths_ch, _ = mean_by(rows, "cdc_wonder_ucd", "deaths", "icd10_chapter")
    office_ch, _ = mean_by(rows, "namcs", "visits_any_dx", "icd10_chapter")
    dalys_ch, gy = mean_by(rows, "ihme_gbd", "dalys", "icd10_chapter")
    out += [
        "",
        "## By chapter",
        "",
        "Visits by chapter add up category visits, so a visit with diagnoses in two categories of one chapter "
        "counts twice." + (f" DALYs: GBD ({span(gy)}), single-chapter causes only." if gy else ""),
        "",
        "| Chapter | US ph3 trials | Deaths/yr | Office visits/yr (sum of categories) | DALYs/yr |",
        "|---|---|---|---|---|",
    ]
    for ch in sorted(chapters, key=lambda c: -chapters[c]):
        out.append(
            f"| {ch} {CHAPTER_NAMES.get(ch, '')} | {chapters[ch]:,} | {fmt(deaths_ch.get(ch))} | "
            f"{fmt(office_ch.get(ch))} | {fmt(dalys_ch.get(ch))} |"
        )
    return "\n".join(out) + "\n"


def dashboard_payload(rows, as_of, total, cats, chapters, since, labels=None, top=60) -> dict:
    """Chapter- and category-level trials vs burden for the Trials vs Burden tab."""
    labels = labels or {}
    deaths, dy = mean_by(rows, "cdc_wonder_ucd", "deaths", "icd10_category")
    office, oy = mean_by(rows, "namcs", "visits_any_dx", "icd10_category")
    ed, ey = mean_by(rows, "nhamcs_ed", "visits_any_dx", "icd10_category")
    deaths_ch, _ = mean_by(rows, "cdc_wonder_ucd", "deaths", "icd10_chapter")
    office_ch, _ = mean_by(rows, "namcs", "visits_any_dx", "icd10_chapter")
    ed_ch, _ = mean_by(rows, "nhamcs_ed", "visits_any_dx", "icd10_chapter")
    dalys_ch, gy = mean_by(rows, "ihme_gbd", "dalys", "icd10_chapter")
    prev_ch, _ = mean_by(rows, "ihme_gbd", "prevalence", "icd10_chapter")
    # injury deaths sit under external causes in WONDER; show them on the injury chapter
    deaths_ch[INJURY_CHAPTER] = deaths_ch.get(INJURY_CHAPTER, 0) + deaths_ch.pop(EXTERNAL_CHAPTER, 0) or None

    def rnd(x):
        return None if x is None else round(x)

    chapter_rows = [
        {
            "chapter": ch,
            "label": CHAPTER_NAMES.get(ch, str(ch)),
            "trials": n,
            "deaths": rnd(deaths_ch.get(ch)),
            "office_visits": rnd(office_ch.get(ch)),
            "ed_visits": rnd(ed_ch.get(ch)),
            "dalys": rnd(dalys_ch.get(ch)),
            "prevalence": rnd(prev_ch.get(ch)),
        }
        for ch, n in sorted(chapters.items(), key=lambda kv: -kv[1])
        if ch not in NON_DISEASE_CHAPTERS
    ]
    category_rows = [
        {
            "code": cat,
            "label": label_of(cat, labels),
            "chapter": ch,
            "trials": n,
            "deaths": rnd(deaths.get(cat)),
            "office_visits": rnd(office.get(cat)),
            "ed_visits": rnd(ed.get(cat)),
        }
        for cat, ch, n in cats
        if is_disease(cat, ch)
    ][:top]
    return {
        "as_of": as_of,
        "since": since,
        "trials": total,
        "years": {"deaths": dy, "office_visits": oy, "ed_visits": ey, "gbd": gy},
        "chapters": chapter_rows,
        "categories": category_rows,
    }


def write_csv(path, rows, columns):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--stack", default="ct-pipeline")
    ap.add_argument("--data", help="local data dir instead of the stack's bucket")
    ap.add_argument("--since", default="2016-01-01", help="first trial start date and first data year")
    ap.add_argument("--cache", default="/tmp/ct-burden-cache", help="download cache")
    ap.add_argument("--out", default="burden_report")
    ap.add_argument("--gbd", nargs="*", default=[], help="GBD results-tool CSV export(s)")
    ap.add_argument("--wonder-export", nargs="*", default=[], help="WONDER web-form export(s) for later years")
    ap.add_argument("--skip", nargs="*", default=[], choices=["wonder", "namcs", "nhamcs_ed", "trials"])
    ap.add_argument("--top", type=int, default=10, help="rows in the validation table")
    ap.add_argument("--site", help=f"site store for {DASHBOARD_KEY} (default: the stack's SiteBucket)")
    ap.add_argument("--no-publish", action="store_true", help=f"don't upload to {PUBLISH_PREFIX} or {DASHBOARD_KEY}")
    args = ap.parse_args()

    first = int(args.since[:4])
    cache, out = Path(args.cache), Path(args.out)
    cache.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)
    rows = []
    if "wonder" not in args.skip:
        print("CDC WONDER deaths (16 s between API calls)")
        rows += burden.fetch_wonder([y for y in burden.WONDER_YEARS if y >= first], cache)
    for path in args.wonder_export:
        extra = burden.parse_wonder_export(Path(path).read_text(encoding="latin-1"))
        have = {r["year"] for r in rows if r["source"] == "cdc_wonder_ucd"}
        rows += [r for r in extra if r["year"] not in have and r["year"] >= first]
        print(f"  WONDER export {path}: years {sorted({r['year'] for r in extra})}")
    for source in ("namcs", "nhamcs_ed"):
        if source not in args.skip:
            print(f"{source} visits")
            keys = sorted(k for k in burden.NCHS_FILES if k[0] == source and k[1] >= first)
            rows += burden.fetch_nchs(keys, cache)
    for path in args.gbd:
        g = burden.parse_gbd(Path(path).read_text(encoding="utf-8-sig"))
        rows += [r for r in g if r["year"] >= first]
        print(f"  GBD {path}: {len(g)} rows")
    write_csv(out / "burden.csv", rows, burden.COLUMNS)
    print(f"Wrote {out / 'burden.csv'} ({len(rows):,} rows)")

    store = open_data(args)
    if "trials" not in args.skip:
        as_of, total, cats, chapters, labels = trial_counts(store, args)
        write_csv(
            out / "trials.csv",
            [{"icd10_category": c, "icd10_chapter": ch, "us_phase3_trials": n} for c, ch, n in cats],
            ["icd10_category", "icd10_chapter", "us_phase3_trials"],
        )
        (out / "validation.md").write_text(
            validation_md(rows, as_of, total, cats, chapters, args.since, args.top, labels)
        )
        payload = dashboard_payload(rows, as_of, total, cats, chapters, args.since, labels)
        (out / "us_burden.json").write_text(json.dumps(payload, indent=1))
        print(f"Wrote {out / 'trials.csv'}, {out / 'validation.md'} and {out / 'us_burden.json'}")

    if args.no_publish:
        return
    for name, content_type in OUTPUTS.items():
        if (out / name).exists():
            store.upload(out / name, PUBLISH_PREFIX + name, content_type)
    print(f"Uploaded {', '.join(OUTPUTS)} to {store.location(PUBLISH_PREFIX)}")
    site = open_site(args)
    if site and (out / "us_burden.json").exists():
        site.put_json(DASHBOARD_KEY, json.loads((out / "us_burden.json").read_text()))
        print(f"Wrote {DASHBOARD_KEY} ({site.location(DASHBOARD_KEY)})")


if __name__ == "__main__":
    main()
