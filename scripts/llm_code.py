#!/usr/bin/env python3
"""
LLM tier for condition coding (ROADMAP.md phase 1, steps 4-5), run from AWS CloudShell.

  make llm-eval MODEL=<id> [ARGS="--sample 300"]
      Codes a sample of studies that already have exact MeSH-derived ICD-10-CM codes (never shown to the model)
      and prints a scorecard. Writes only its own cache, reference/llm_eval_cache.json.
  make llm-code MODEL=<id> [ARGS="--limit 1000"]
      Codes studies with no exact ICD-10-CM code into reference/llm_cache.json (resumable: rerun to continue).
      `make reference` then publishes them in study_condition_codes as via='llm'.

MODEL is a Bedrock model or inference-profile id. --review FILE also writes every answer to a CSV for spot checks.
--price-* (USD per million tokens) turn the token counts into a cost estimate.
"""

import argparse
import csv
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ct_pipeline import llm  # noqa: E402
from ct_pipeline.build import attach_snapshot  # noqa: E402
from ct_pipeline.config import CURRENT_KEY  # noqa: E402
from ct_pipeline.db import connect  # noqa: E402
from ct_pipeline.reference import download_reference  # noqa: E402
from ct_pipeline.storage import LocalStore, S3Store  # noqa: E402

EVAL_CACHE_KEY = "reference/llm_eval_cache.json"
TABLES = ("studies", "study_text", "study_conditions", "study_interventions", "study_condition_codes")
GOLD_SQL = """
    SELECT nct_id, list_sort(list(DISTINCT icd10_category)) AS gold FROM study_condition_codes
    WHERE code_system = 'ICD10CM' AND via = 'mesh' AND match_type = 'exact'
      AND icd10_category IS NOT NULL AND icd10_category <> 'Z00'
    GROUP BY 1 ORDER BY hash(nct_id || ?) LIMIT ?
"""


def make_coder(model: str, catalog: str):
    return llm.BedrockCoder(model, catalog)


def open_data(args):
    if args.data:
        return LocalStore(args.data)
    import boto3

    stack = boto3.client("cloudformation").describe_stacks(StackName=args.stack)["Stacks"][0]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    return S3Store(outputs["DataBucketName"])


def usage_line(usage: dict, args) -> str:
    keys = ("inputTokens", "outputTokens", "cacheReadInputTokens", "cacheWriteInputTokens")
    prices = (args.price_in, args.price_out, args.price_cache_read, args.price_cache_write)
    line = ", ".join(f"{k} {usage.get(k, 0):,}" for k in keys)
    if any(prices):
        cost = sum(usage.get(k, 0) * p for k, p in zip(keys, prices, strict=True)) / 1e6
        line += f"; estimated cost ${cost:,.2f}"
    return line


def verified(entry: dict) -> list:
    return [c["code"] for c in entry.get("codes") or [] if c["evidence_found"]]


def write_review(path, docs, entries, gold=None) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["nct_id", "title", "conditions", "gold", "codes", "evidence", "no_code_reason"])
        for d in docs:
            e = entries.get(d["nct_id"])
            if e is None:
                continue
            codes = e.get("codes") or []
            w.writerow(
                [
                    d["nct_id"],
                    d["brief_title"],
                    "; ".join(d["conditions"]),
                    " ".join((gold or {}).get(d["nct_id"], [])),
                    " ".join(
                        f"{c['code']}:{c['confidence']}{'' if c['evidence_found'] else ':unverified'}" for c in codes
                    ),
                    " | ".join(c["evidence"] for c in codes),
                    e.get("no_code_reason", ""),
                ]
            )
    print(f"\nWrote {path}")


def scorecard(docs, entries, gold: dict, chapters: dict) -> None:
    n = len(entries)
    if not n:
        print("No answers.")
        return
    codes = [c for e in entries.values() for c in e.get("codes") or []]
    preds = {k: set(verified(e)) for k, e in entries.items()}
    with_pred = [k for k in preds if preds[k]]
    hit = sum(1 for k in preds if preds[k] & set(gold[k]))
    recall = sum(len(preds[k] & set(gold[k])) / len(gold[k]) for k in preds) / n
    precision = sum(len(preds[k] & set(gold[k])) / len(preds[k]) for k in with_pred) / max(len(with_pred), 1)
    chap = sum(1 for k in preds if {chapters.get(c) for c in preds[k]} & {chapters.get(c) for c in gold[k]})
    print(f"\nScorecard ({n:,} studies with exact MeSH-derived categories, hidden from the model)")
    print(f"  answered with >=1 verified category  {len(with_pred) / n:6.1%}")
    print(f"  no code                              {sum(1 for e in entries.values() if not e.get('codes')) / n:6.1%}")
    print(f"  quoted evidence found in protocol    {sum(c['evidence_found'] for c in codes) / max(len(codes), 1):6.1%}")
    print(f"  >=1 category agrees with MeSH        {hit / n:6.1%}")
    print(f"  same chapter as MeSH                 {chap / n:6.1%}")
    print(f"  category recall / precision          {recall:6.1%} / {precision:.1%}")
    print("\nDisagreements (MeSH-derived codes can be wrong too):")
    titles = {d["nct_id"]: d["brief_title"] for d in docs}
    shown = 0
    for k in sorted(preds):
        if not preds[k] & set(gold[k]) and shown < 20:
            shown += 1
            print(f"  {k}  MeSH {' '.join(gold[k]):12} LLM {' '.join(sorted(preds[k])) or '-':12} {titles[k][:60]}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=("eval", "code"))
    ap.add_argument("--model", required=True, help="Bedrock model or inference-profile id")
    ap.add_argument("--stack", default="ct-pipeline")
    ap.add_argument("--data", help="local data directory instead of the stack's data bucket")
    ap.add_argument("--sample", type=int, default=300, help="eval: number of studies")
    ap.add_argument("--seed", default="1")
    ap.add_argument("--limit", type=int, help="code: at most this many studies (in NCT order)")
    ap.add_argument("--workers", type=int, default=llm.WORKERS)
    ap.add_argument("--review", help="write answers to this CSV")
    for name in ("in", "out", "cache-read", "cache-write"):
        ap.add_argument(f"--price-{name}", type=float, default=0.0)
    args = ap.parse_args()
    if not args.model:
        sys.exit("Set MODEL to a Bedrock model or inference-profile id (see README: LLM tier)")

    store = open_data(args)
    current = store.get_json(CURRENT_KEY)
    if current is None:
        sys.exit("No published snapshot yet (curated/CURRENT.json)")
    if "study_condition_codes" not in current["tables"]:
        sys.exit("The snapshot has no study_condition_codes yet: run `make reference` first")
    with tempfile.TemporaryDirectory(prefix="ct-llm-") as tmp:
        work = Path(tmp)
        for table in TABLES:
            store.download(current["tables"][table], work / "snapshot" / table / "data.parquet")
        if download_reference(store, work / "reference") is None:
            sys.exit("No reference yet: run `make reference` first")
        con = connect(work)
        attach_snapshot(con, work / "snapshot", list(TABLES))
        icd = work / "reference" / "icd10cm_codes" / "data.parquet"
        catalog, categories = llm.category_catalog(con, icd)
        coder = make_coder(args.model, catalog)
        progress = lambda d, n: print(f"  {d:,}/{n:,}", flush=True)  # noqa: E731

        if args.mode == "eval":
            gold = dict(con.execute(GOLD_SQL, [args.seed, args.sample]).fetchall())
            docs = llm.study_documents(con, sorted(gold))
            print(f"Snapshot {current['run_id']}: coding {len(docs):,} studies with {args.model}", flush=True)
            entries, info = llm.code_studies(
                coder, store, docs, categories, workers=args.workers, progress=progress, cache_key=EVAL_CACHE_KEY
            )
            chapters = dict(
                con.execute(f"SELECT code, chapter FROM read_parquet('{icd}') WHERE level = 'category'").fetchall()
            )
            scorecard(docs, entries, gold, chapters)
        else:
            ids = [r[0] for r in con.execute(llm.TARGETS_SQL).fetchall()]
            print(f"Snapshot {current['run_id']}: {len(ids):,} studies have no exact ICD-10-CM code", flush=True)
            ids = ids[: args.limit] if args.limit else ids
            docs = llm.study_documents(con, ids)
            print(f"Coding {len(docs):,} with {args.model} (cached answers are reused)", flush=True)
            entries, info = llm.code_studies(coder, store, docs, categories, workers=args.workers, progress=progress)
            coded = [e for e in entries.values() if verified(e)]
            print(f"\n{len(coded):,} of {len(entries):,} answered studies got >=1 verified category")
            counts = {}
            for e in coded:
                for c in verified(e):
                    counts[c] = counts.get(c, 0) + 1
            for code, n in sorted(counts.items(), key=lambda kv: -kv[1])[:15]:
                print(f"  {n:6,}  {code:5} {categories[code][:60]}")
            print("\nRun `make reference` to publish these codes (via='llm') to the dashboard data.")
            gold = None
        print(
            f"\nCalls: {info['coded']:,} new, {info['cached']:,} cached, {info['failed']:,} failed",
            info["errors"] or "",
        )
        print(f"Tokens: {usage_line(info['usage'], args)}")
        if args.review:
            write_review(args.review, docs, entries, gold)


if __name__ == "__main__":
    main()
