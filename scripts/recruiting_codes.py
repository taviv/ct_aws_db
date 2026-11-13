#!/usr/bin/env python3
"""
LLM-first ICD-10-CM codes for recruiting Phase 3 (and Phase 2/3) trials with a US or Canadian site, from CloudShell:

  make recruiting-codes MODEL=<id> [ARGS="--price-in 3.3 --price-out 16.5 --price-cache-read 0.33"]
      Prints how many studies match, how many are already answered and the estimated cost; sends nothing.
  make recruiting-codes MODEL=<id> ARGS="--go ..."
      Codes them (resumable: rerun to continue) and writes recruiting/codes_<model>.csv here and in
      analysis/recruiting/ of the data bucket. Nothing is published to the dashboard or Athena tables.
"""

import argparse
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ct_pipeline import accuracy, llm, llm_run, recruiting  # noqa: E402
from ct_pipeline.storage import LocalStore, S3Store  # noqa: E402

EVAL_CACHE_KEY = "reference/llm_eval_cache.json"
TABLES = tuple(llm_run.TABLES) + ("study_locations",)


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


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--model", required=True, help="Bedrock model or inference-profile id")
    ap.add_argument("--stack", default="ct-pipeline")
    ap.add_argument("--data", help="local data directory instead of the stack's data bucket")
    ap.add_argument("--out", default="recruiting", help="local output directory")
    ap.add_argument("--limit", type=int, help="code at most this many studies (in NCT order)")
    ap.add_argument("--workers", type=int, default=llm.WORKERS)
    ap.add_argument("--go", action="store_true", help="send the studies to Bedrock")
    for name in ("in", "out", "cache-read", "cache-write"):
        ap.add_argument(f"--price-{name}", type=float, default=0.0, help="USD per million tokens")
    args = ap.parse_args()
    prices = [args.price_in, args.price_out, args.price_cache_read, args.price_cache_write]

    store = open_data(args)
    with tempfile.TemporaryDirectory(prefix="ct-recruiting-") as tmp:
        try:
            current, con, icd = llm_run.open_snapshot(store, Path(tmp), TABLES)
        except llm.LlmError as e:
            sys.exit(str(e))
        ids = recruiting.targets(con)
        ids = ids[: args.limit] if args.limit else ids
        docs = llm.study_documents(con, ids)
        catalog, categories = llm.category_catalog(con, icd)

        key = recruiting.cache_key(args.model)
        cache = store.get_json(key) or {"entries": {}}
        others = [
            (store.get_json(k) or {}).get("entries", {})
            for k in (llm.CACHE_KEY, EVAL_CACHE_KEY, accuracy.arm_key(args.model))
        ]
        copied = accuracy.seed_cache(cache["entries"], others, docs, args.model)
        if copied:
            store.put_json(key, cache)
        todo = recruiting.todo(docs, cache["entries"], args.model)
        est = cost(todo, prices)
        print(
            f"Snapshot {current['run_id']}: {len(docs):,} recruiting Phase 3 or Phase 2/3 studies with a site "
            f"in {' or '.join(recruiting.COUNTRIES)}; {len(docs) - todo:,} already answered by {args.model} "
            f"({copied:,} copied from existing caches), {todo:,} to code"
            + (f", estimated ${est:,.2f}" if est is not None else "")
        )
        entries = {d["nct_id"]: cache["entries"][d["nct_id"]] for d in docs if d["nct_id"] in cache["entries"]}
        if todo and not args.go:
            print("\nNothing sent to Bedrock. Add --go (and --price-* for a cost line) to run.")
            return
        if todo:
            entries, info = llm.code_studies(
                make_coder(args.model, catalog),
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
            if info["failed"]:
                print("  Rerun the same command to retry the failed studies (answers so far are cached).")

        rows = recruiting.code_rows(
            docs, entries, accuracy.published_rows(con, ids), categories, recruiting.locations(con, ids)
        )
        text = recruiting.to_csv(rows)
        out_key = recruiting.codes_key(args.model)
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        (out / out_key.rsplit("/", 1)[-1]).write_text(text)
        store.put_bytes(out_key, text.encode(), "text/csv")
        by_source = {}
        for r in rows:
            by_source.setdefault(r["source"] or "none", set()).add(r["nct_id"])
        print(
            f"\nWrote {out / out_key.rsplit('/', 1)[-1]} and {store.location(out_key)}: "
            + ", ".join(f"{len(v):,} studies coded by {k}" for k, v in sorted(by_source.items()))
        )


if __name__ == "__main__":
    main()
