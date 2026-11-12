#!/usr/bin/env python3
"""
Coding-accuracy experiment: `make coding-accuracy ARGS="<mode> ..."` (see src/ct_pipeline/accuracy.py).

  sample  draw 50 Phase 3 studies per top-8 ICD-10-CM chapter (+ the xeval random 300); free, read-only
  code    run each LLM arm (--models a,b,c) on the sample; prints the plan and cost unless --go
  sheet   freeze predictions.json and write labels.csv for hand labels (--pilot N: N studies per chapter)
  score   score a filled labels.csv (--labels FILE [--second FILE] [--cascade cheap,strong])

Outputs go to analysis/coding_accuracy/ in the data bucket (never reference/ or curated/) and to --out.
"""

import argparse
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ct_pipeline import accuracy, llm, llm_run, xeval  # noqa: E402
from ct_pipeline.storage import LocalStore, S3Store  # noqa: E402

EVAL_CACHE_KEY = "reference/llm_eval_cache.json"


def stack_outputs(stack: str) -> dict:
    import boto3

    desc = boto3.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in desc.get("Outputs", [])}


def open_data(args):
    if args.data:
        return LocalStore(args.data)
    return S3Store(stack_outputs(args.stack)["DataBucketName"])


def make_coder(model: str, catalog: str):
    return llm.BedrockCoder(model, catalog)


def cost(n: int, prices) -> float | None:
    usage = dict(zip(llm_run.USAGE_KEYS, (n * t for t in accuracy.TOKENS_PER_STUDY), strict=True))
    return llm_run.estimated_cost(usage, prices)


def save(store, out: Path, key: str, data) -> None:
    out.mkdir(parents=True, exist_ok=True)
    name = key.rsplit("/", 1)[-1]
    if isinstance(data, str):
        (out / name).write_text(data)
        store.put_bytes(key, data.encode(), "text/markdown")
    else:
        (out / name).write_text(json.dumps(data, indent=1, default=str))
        store.put_json(key, data)
    print(f"Wrote {out / name} and {store.location(key)}")


def need(store, key: str, hint: str):
    obj = store.get_json(key)
    if obj is None:
        sys.exit(f"No {key} yet: run `{hint}` first")
    return obj


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=("sample", "code", "sheet", "score"))
    ap.add_argument("--stack", default="ct-pipeline")
    ap.add_argument("--data", help="local data directory instead of the stack's data bucket")
    ap.add_argument("--out", default="coding_accuracy", help="local output directory")
    ap.add_argument("--phase", default="PHASE3")
    ap.add_argument("--seed", default="1")
    ap.add_argument("--per-chapter", type=int, default=50)
    ap.add_argument("--reuse-sample", type=int, default=300, help="sample: xeval random sample size reused (seed 1)")
    ap.add_argument("--blind-share", type=float, default=0.10)
    ap.add_argument("--second-share", type=float, default=0.10)
    ap.add_argument("--models", default="", help="comma-separated Bedrock model/inference-profile ids (code, sheet)")
    ap.add_argument("--go", action="store_true", help="code: actually call Bedrock (otherwise print the plan)")
    ap.add_argument("--workers", type=int, default=llm.WORKERS)
    ap.add_argument("--pilot", type=int, help="sheet: only this many studies per chapter")
    ap.add_argument("--labels", help="score: filled labels.csv")
    ap.add_argument("--second", help="score: second reviewer's labels.csv (Cohen's kappa)")
    ap.add_argument("--cascade", help="score: 'cheap,strong' model ids: strong only where cheap has no verified high")
    ap.add_argument("--min-precision", type=float, default=0.90)
    ap.add_argument("--min-recall", type=float, default=0.85)
    ap.add_argument("--min-primary", type=float, default=0.90)
    for name in ("in", "out", "cache-read", "cache-write"):
        ap.add_argument(f"--price-{name}", type=float, default=0.0, help="USD per million tokens")
    args = ap.parse_args()
    out = Path(args.out)
    models = [m.strip() for m in args.models.split(",") if m.strip()]
    store = open_data(args)

    if args.mode == "score":
        if not args.labels:
            sys.exit("score needs --labels FILE")
        preds = need(store, accuracy.PREDICTIONS_KEY, "make coding-accuracy ARGS=sheet")
        cascade = tuple(m.strip() for m in args.cascade.split(",")) if args.cascade else None
        if cascade and (len(cascade) != 2 or not set(cascade) <= set(preds["models"])):
            sys.exit(f"--cascade needs two of the sheet's models: {', '.join(preds['models'])}")
        res = accuracy.score(
            preds,
            accuracy.read_labels(args.labels),
            cascade=cascade,
            seed=int(args.seed) if args.seed.isdigit() else 1,
            thresholds={"precision": args.min_precision, "recall": args.min_recall, "primary": args.min_primary},
            second=accuracy.read_labels(args.second) if args.second else None,
        )
        save(store, out, accuracy.RESULTS_KEY, res)
        text = accuracy.report(res)
        save(store, out, accuracy.REPORT_KEY, text)
        print("\n" + text)
        return

    with tempfile.TemporaryDirectory(prefix="ct-acc-") as tmp:
        try:
            current, con, icd = llm_run.open_snapshot(store, Path(tmp))
            phases = llm_run.parse_phases(con, args.phase)
        except llm.LlmError as e:
            sys.exit(str(e))

        if args.mode == "sample":
            members = accuracy.memberships(con, phases, accuracy.chapter_of(con, icd))
            reuse = xeval.sample_ids(con, phases, "1", args.reuse_sample) if args.reuse_sample else []
            picks = accuracy.draw(members, args.seed, args.per_chapter, reuse)
            payload = accuracy.sample_payload(
                current, phases, args.seed, args.per_chapter, members, picks, reuse, args.blind_share, args.second_share
            )
            save(store, out, accuracy.SAMPLE_KEY, payload)
            reused = set(reuse)
            print(f"\nSnapshot {current['run_id']}: {payload['pool_total']:,} eligible {'/'.join(phases)} studies")
            print(f"{'Chapter':28} {'pool':>6} {'drawn':>6} {'reused':>7}  examples")
            for key, ch in payload["chapters"].items():
                ids = ch["ids"]
                print(
                    f"{ch['name']:28} {payload['pool'][key]:6,} {len(ids):6} {sum(n in reused for n in ids):7}"
                    f"  {', '.join(ids[:3])}"
                )
            print(
                f"Unique studies: {len(payload['ids'])} ({len(reuse)} random reused); "
                f"blind {len(payload['blind'])}, second review {len(payload['second'])}"
            )
            return

        sample = need(store, accuracy.SAMPLE_KEY, "make coding-accuracy ARGS=sample")
        if sample["snapshot_run_id"] != current["run_id"]:
            print(f"Note: sample drawn on snapshot {sample['snapshot_run_id']}, current is {current['run_id']}")
        if not models:
            sys.exit("Pass --models <id>[,<id>...]")
        docs = llm.study_documents(con, sample["ids"])
        catalog, categories = llm.category_catalog(con, icd)

        if args.mode == "code":
            others = [(store.get_json(k) or {}).get("entries", {}) for k in (EVAL_CACHE_KEY, llm.CACHE_KEY)]
            for m in models:
                key = accuracy.arm_key(m)
                cache = store.get_json(key) or {"entries": {}}
                copied = accuracy.seed_cache(cache["entries"], others, docs, m)
                if copied:
                    store.put_json(key, cache)
                todo = sum(
                    1
                    for d in docs
                    if cache["entries"].get(d["nct_id"], {}).get("h") != llm.input_hash(llm.render_study(d), m)
                )
                prices = [args.price_in, args.price_out, args.price_cache_read, args.price_cache_write]
                est = cost(todo, prices)
                print(
                    f"{m}: {len(docs)} studies, {len(docs) - todo} already answered ({copied} copied from existing "
                    f"caches), {todo} to code" + (f", estimated ${est:,.2f}" if est is not None else "")
                )
                if not args.go or not todo:
                    continue
                entries, info = llm.code_studies(
                    make_coder(m, catalog),
                    store,
                    docs,
                    categories,
                    workers=args.workers,
                    cache_key=key,
                    progress=lambda d, n: print(f"  {d:,}/{n:,}", flush=True),
                )
                spent = llm_run.estimated_cost(info["usage"], prices)
                print(f"  coded {info['coded']:,}, failed {info['failed']:,}", info["errors"] or "")
                print(f"  tokens: {info['usage']}" + (f"; estimated cost ${spent:,.2f}" if spent is not None else ""))
            if not args.go:
                print("\nNothing sent to Bedrock. Add --go (and --price-* for a cost line) to run.")
            return

        # sheet
        arms = {m: (store.get_json(accuracy.arm_key(m)) or {}).get("entries", {}) for m in models}
        for m, entries in arms.items():
            missing = len(sample["ids"]) - len(entries)
            if missing > 0:
                print(f"Warning: {m} has no answer for {missing} studies (run `code --go` first)")
        by_id = {d["nct_id"]: d for d in docs}
        changed = sum(
            llm.revalidate_entry(e, llm.render_study(by_id[n]), categories)
            for entries in arms.values()
            for n, e in entries.items()
            if n in by_id
        )
        if changed:
            print(f"Re-checked cached evidence under the current rules: {changed} answers changed")
        preds = accuracy.predictions(sample, docs, accuracy.published_rows(con, sample["ids"]), arms)
        save(store, out, accuracy.PREDICTIONS_KEY, preds)
        rows = accuracy.sheet_rows(preds, categories, args.pilot)
        name = "labels_pilot.csv" if args.pilot else "labels.csv"
        out.mkdir(parents=True, exist_ok=True)
        accuracy.write_sheet(out / name, rows)
        print(f"Wrote {out / name}: {len({r['nct_id'] for r in rows})} studies, {len(rows)} rows")


if __name__ == "__main__":
    main()
