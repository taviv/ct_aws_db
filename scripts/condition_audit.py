#!/usr/bin/env python3
"""
Suspicion-ranked audit of deterministic (non-LLM) ICD-10-CM condition codes.

  make condition-audit [ARGS="--phase PHASE3 --out condition_audit.csv"]
  (or `python scripts/condition_audit.py --stack ct-pipeline | --data local/data`)

Read-only, no Bedrock calls. Ranks Phase 3 (or --phase) studies whose classic codes look shaky,
from the signals the cross-evaluation showed actually predict wrongness:

  llm_contradicts  cached LLM answer (production or eval cache) has verified high-confidence
                   categories sharing no category AND no chapter with the classic codes
  ancestor_via     code arrived via the mesh_ancestor broad fallback
  umls_broad       umls_* text-map source at broad match_type (crosswalk noise)
  catch_all        catch-all code (C76-C80, C96)
  broad_only       the study has no exact code at all

Output CSV is sorted by score (sum of flag weights) for hand review; fill your_check
(ok|wrong|partial) per row.
"""

import argparse
import csv
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ct_pipeline import llm, llm_run  # noqa: E402
from ct_pipeline.storage import S3Store, open_store  # noqa: E402

EVAL_CACHE_KEY = "reference/llm_eval_cache.json"

WEIGHTS = {"llm_contradicts": 6, "ancestor_via": 3, "umls_broad": 3, "catch_all": 2, "broad_only": 1}

ROWS_SQL = """
    SELECT c.nct_id, c.code, c.icd10_category, c.icd10_chapter, c.via, c.match_type, c.source,
           s.brief_title, s.phase_group,
           (SELECT string_agg(sc.condition_name, '; ' ORDER BY sc.condition_name)
              FROM (SELECT DISTINCT condition_name FROM study_conditions d WHERE d.nct_id = c.nct_id) sc
           ) AS conditions
    FROM study_condition_codes c JOIN studies s USING (nct_id)
    WHERE c.code_system = 'ICD10CM' AND c.via <> 'llm' {phase}
    ORDER BY c.nct_id, c.code
"""


def code_flags(code: str, via: str, match_type: str, source: str) -> list:
    flags = []
    if via == "mesh_ancestor":
        flags.append("ancestor_via")
    if (source or "").startswith("umls") and match_type == "broad":
        flags.append("umls_broad")
    if code[:3] in llm.CATCH_ALL:
        flags.append("catch_all")
    return flags


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--stack", default="ct-pipeline", help="read the stack's data bucket (needs AWS credentials)")
    src.add_argument("--data", help="data store URI instead of --stack (local dir or s3://bucket)")
    ap.add_argument("--phase", default="PHASE3", help="comma-separated phase_group values (default PHASE3)")
    ap.add_argument("--out", default="condition_audit.csv", help="output CSV (default condition_audit.csv)")
    args = ap.parse_args()

    if args.data:
        store = open_store(args.data)
    else:
        import boto3

        stack = boto3.client("cloudformation").describe_stacks(StackName=args.stack)["Stacks"][0]
        store = S3Store({o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["DataBucketName"])

    with tempfile.TemporaryDirectory(prefix="ct-audit-") as tmp:
        work = Path(tmp)
        current, con, icd = llm_run.open_snapshot(store, work)
        try:
            phases = llm_run.parse_phases(con, args.phase)
            where, params = llm_run.phase_filter(phases)
            chapters = dict(con.execute(f"SELECT code, chapter FROM read_parquet('{icd}')").fetchall())
            rows = con.execute(ROWS_SQL.format(phase=where), params).fetchall()
        finally:
            con.close()

    # cached LLM answers for the contradiction flag: production cache + eval cache
    llm_answers = {}
    for key in (llm.CACHE_KEY, EVAL_CACHE_KEY):
        for nct, e in ((store.get_json(key) or {}).get("entries") or {}).items():
            strong = llm.strong_codes(e)
            if strong:
                llm_answers.setdefault(nct, set()).update(strong)

    studies = {}
    for nct, code, cat, chapter, via, mtype, source, title, phase_group, conds in rows:
        st = studies.setdefault(
            nct,
            {
                "title": title,
                "phase": phase_group,
                "conditions": conds,
                "codes": [],
                "cats": set(),
                "chapters": set(),
                "flags": set(),
                "exact": False,
            },
        )
        st["codes"].append(f"{code}:{via}:{mtype}")
        if cat:
            st["cats"].add(cat)
        if chapter:
            st["chapters"].add(chapter)
        if mtype == "exact":
            st["exact"] = True
        st["flags"].update(code_flags(code, via, mtype, source or ""))

    out_rows = []
    for nct, st in studies.items():
        if not st["exact"]:
            st["flags"].add("broad_only")
        strong = llm_answers.get(nct, set())
        if strong and not (strong & st["cats"]) and not ({chapters.get(c) for c in strong} & st["chapters"]):
            st["flags"].add("llm_contradicts")
        if not st["flags"]:
            continue
        score = sum(WEIGHTS[f] for f in st["flags"])
        out_rows.append(
            {
                "nct_id": nct,
                "phase": st["phase"],
                "score": score,
                "flags": " ".join(sorted(st["flags"])),
                "conditions": st["conditions"] or "",
                "classic_codes": " ".join(st["codes"]),
                "llm_codes": " ".join(sorted(strong)),
                "title": st["title"],
                "your_check": "",
            }
        )
    out_rows.sort(key=lambda r: (-r["score"], r["nct_id"]))

    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()) if out_rows else ["nct_id"])
        w.writeheader()
        w.writerows(out_rows)

    counts = {}
    for r in out_rows:
        for f in r["flags"].split():
            counts[f] = counts.get(f, 0) + 1
    print(
        f"Snapshot {current['run_id']}: {len(studies):,} classic-coded studies audited, "
        f"{len(out_rows):,} flagged -> {args.out}"
    )
    for f, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {f:16} {n:5,}")
    if llm_answers:
        print(f"  (LLM answers available for {len(llm_answers):,} studies)")


if __name__ == "__main__":
    main()
