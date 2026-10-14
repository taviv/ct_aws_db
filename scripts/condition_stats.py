#!/usr/bin/env python3
"""
Condition-coding coverage per phase for the published snapshot: `make condition-stats`
(or `python scripts/condition_stats.py --stack ct-pipeline`, `--data local/data` for a local run).

Read-only. Each study is counted once, under the strongest evidence behind its ICD-10-CM codes.
"""

import argparse
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ct_pipeline.build import attach_snapshot  # noqa: E402
from ct_pipeline.config import CURRENT_KEY  # noqa: E402
from ct_pipeline.db import connect  # noqa: E402
from ct_pipeline.queries import PHASE_GROUP_LABELS  # noqa: E402
from ct_pipeline.storage import S3Store, open_store  # noqa: E402

CHAPTERS = {
    1: "Infectious (A00-B99)",
    2: "Neoplasms (C00-D49)",
    3: "Blood/immune (D50-D89)",
    4: "Endocrine/metabolic (E00-E89)",
    5: "Mental (F01-F99)",
    6: "Nervous system (G00-G99)",
    7: "Eye (H00-H59)",
    8: "Ear (H60-H95)",
    9: "Circulatory (I00-I99)",
    10: "Respiratory (J00-J99)",
    11: "Digestive (K00-K95)",
    12: "Skin (L00-L99)",
    13: "Musculoskeletal (M00-M99)",
    14: "Genitourinary (N00-N99)",
    15: "Pregnancy (O00-O9A)",
    16: "Perinatal (P00-P96)",
    17: "Congenital (Q00-Q99)",
    18: "Symptoms/signs (R00-R99)",
    19: "Injury/poisoning (S00-T88)",
    20: "External causes (V00-Y99)",
    21: "Health status/Z codes (Z00-Z99)",
    22: "Special purposes (U00-U85)",
}

TIERS = [
    ("mesh_exact", "MeSH, exact"),
    ("mesh_broad", "MeSH, broad"),
    ("healthy", "Healthy (Z00.6)"),
    ("text_rule", "Curated text rule"),
    ("umls_text", "UMLS text match"),
    ("ancestor", "MeSH ancestor (broad)"),
    ("none_mesh", "Uncoded, has MeSH"),
    ("none", "Uncoded, no MeSH"),
]

STUDY_TIERS = """
CREATE TEMP TABLE study_tier AS
WITH c AS (SELECT * FROM study_condition_codes WHERE code_system = 'ICD10CM'),
ev AS (
    SELECT nct_id,
           bool_or(via = 'mesh' AND match_type = 'exact')                   AS mesh_exact,
           bool_or(via = 'mesh')                                             AS mesh_any,
           bool_or(via = 'text' AND code = 'Z00.6')                          AS healthy,
           bool_or(via = 'text' AND source = 'curated' AND code <> 'Z00.6')  AS text_rule,
           bool_or(via = 'text' AND source LIKE 'umls%')                     AS umls_text,
           bool_or(via = 'mesh_ancestor')                                    AS ancestor,
           count(DISTINCT code)                                              AS n_codes,
           bool_or(match_type = 'exact')                                     AS any_exact,
           list(DISTINCT source)                                             AS sources
    FROM c GROUP BY nct_id
),
has_mesh AS (SELECT DISTINCT nct_id FROM condition_mesh_terms WHERE NOT is_ancestor)
SELECT s.nct_id, s.phase_group, coalesce(ev.n_codes, 0) AS n_codes, coalesce(ev.any_exact, false) AS any_exact,
       ev.sources,
       CASE WHEN ev.mesh_exact THEN 'mesh_exact'
            WHEN ev.mesh_any THEN 'mesh_broad'
            WHEN ev.healthy THEN 'healthy'
            WHEN ev.text_rule THEN 'text_rule'
            WHEN ev.umls_text THEN 'umls_text'
            WHEN ev.ancestor THEN 'ancestor'
            WHEN h.nct_id IS NOT NULL THEN 'none_mesh'
            ELSE 'none' END AS tier
FROM studies s LEFT JOIN ev USING (nct_id) LEFT JOIN has_mesh h USING (nct_id)
"""


def pct(n, d):
    return f"{n / d:.1%}" if d else "-"


def table(headers, rows):
    widths = [max(len(str(x)) for x in col) for col in zip(headers, *rows, strict=True)]

    def line(r):
        cells = [str(x).rjust(w) if i else str(x).ljust(w) for i, (x, w) in enumerate(zip(r, widths, strict=True))]
        return "| " + " | ".join(cells) + " |"

    print(line(headers))
    print("|" + "|".join("-" * (w + 2) for w in widths) + "|")
    for r in rows:
        print(line(r))
    print()


def report(con, current):
    con.execute(STUDY_TIERS)
    groups = [
        g for (g,) in con.execute("SELECT phase_group FROM study_tier GROUP BY 1 ORDER BY min(phase_group)").fetchall()
    ]
    cols = groups + [None]
    label = lambda g: PHASE_GROUP_LABELS.get(g, g) if g else "All"  # noqa: E731

    def where(g):
        return ("WHERE phase_group = ?", [g]) if g else ("", [])

    print(f"Snapshot {current['run_id']}, reference {current.get('reference_version')}\n")

    summary = []
    for g in cols:
        w, a = where(g)
        n, coded, exact, codes = con.execute(
            f"""
            SELECT count(*), count(*) FILTER (WHERE n_codes > 0), count(*) FILTER (WHERE any_exact),
                   avg(n_codes) FILTER (WHERE n_codes > 0)
            FROM study_tier {w}""",
            a,
        ).fetchone()
        nh = con.execute(f"SELECT count(*) FROM study_tier {w} {'AND' if w else 'WHERE'} tier <> 'healthy'", a)
        nh = nh.fetchone()[0]
        coded_nh = con.execute(
            f"SELECT count(*) FROM study_tier {w} {'AND' if w else 'WHERE'} tier NOT IN ('healthy','none','none_mesh')",
            a,
        ).fetchone()[0]
        summary.append(
            [label(g), f"{n:,}", pct(coded, n), pct(exact, n), pct(coded_nh, nh), f"{codes:.2f}" if codes else "-"]
        )
    print("Studies with an ICD-10-CM code")
    table(["Phase", "Studies", "Coded", "Exact code", "Coded excl. healthy", "Codes/coded study"], summary)

    rows = []
    for key, name in TIERS:
        row = [name]
        for g in cols:
            w, a = where(g)
            n, k = con.execute(
                f"SELECT count(*), count(*) FILTER (WHERE tier = ?) FROM study_tier {w}", [key] + a
            ).fetchone()
            row.append(pct(k, n))
        rows.append(row)
    print("Strongest evidence per study (share of studies)")
    table(["Evidence"] + [label(g) for g in cols], rows)

    rows = []
    for (src,) in con.execute("SELECT DISTINCT unnest(sources) FROM study_tier ORDER BY 1").fetchall():
        row = [src]
        for g in cols:
            w, a = where(g)
            n, k = con.execute(
                f"SELECT count(*), count(*) FILTER (WHERE list_contains(sources, ?)) FROM study_tier {w}", [src] + a
            ).fetchone()
            row.append(pct(k, n))
        rows.append(row)
    print("Studies with at least one code from each source")
    table(["Source"] + [label(g) for g in cols], rows)

    chapters = con.execute("""
        SELECT icd10_chapter, count(DISTINCT nct_id) AS n FROM study_condition_codes
        WHERE code_system = 'ICD10CM' GROUP BY 1 ORDER BY n DESC LIMIT 12""").fetchall()
    rows = []
    for ch, _ in chapters:
        row = [CHAPTERS.get(ch, str(ch))]
        for g in cols:
            w, a = where(g)
            n, k = con.execute(
                f"""
                SELECT count(*), count(*) FILTER (WHERE nct_id IN (
                    SELECT nct_id FROM study_condition_codes WHERE code_system = 'ICD10CM' AND icd10_chapter = ?))
                FROM study_tier {w}""",
                [ch] + a,
            ).fetchone()
            row.append(pct(k, n))
        rows.append(row)
    print("Top ICD-10-CM chapters (share of studies; a study can be in several)")
    table(["Chapter"] + [label(g) for g in cols], rows)

    rows = con.execute("""
        SELECT lower(c.condition_name) AS name, count(DISTINCT c.nct_id) AS n
        FROM study_conditions c JOIN study_tier t USING (nct_id)
        WHERE t.n_codes = 0 GROUP BY 1 ORDER BY n DESC LIMIT 15""").fetchall()
    print("Most common condition names on uncoded studies")
    table(["Condition", "Studies"], [[name, f"{n:,}"] for name, n in rows])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--stack", default="ct-pipeline", help="read the stack's data bucket (needs AWS credentials)")
    src.add_argument("--data", help="data store URI instead of --stack (local dir or s3://bucket)")
    args = ap.parse_args()

    if args.data:
        store = open_store(args.data)
    else:
        import boto3

        stack = boto3.client("cloudformation").describe_stacks(StackName=args.stack)["Stacks"][0]
        store = S3Store({o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["DataBucketName"])

    current = store.get_json(CURRENT_KEY)
    if current is None:
        sys.exit(f"{CURRENT_KEY} not found - no run has finished")
    if "study_condition_codes" not in current["tables"]:
        sys.exit("no study_condition_codes in the snapshot yet - run `make reference`")
    needed = ["studies", "study_conditions", "condition_mesh_terms", "study_condition_codes"]
    with tempfile.TemporaryDirectory(prefix="ct-cstats-") as tmp:
        work = Path(tmp)
        for t in needed:
            store.download(current["tables"][t], work / "snapshot" / t / "data.parquet")
        con = connect(work)
        attach_snapshot(con, work / "snapshot", needed)
        try:
            report(con, current)
        finally:
            con.close()


if __name__ == "__main__":
    main()
