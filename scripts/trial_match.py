#!/usr/bin/env python3
"""
Cancer trial finder PoV: extract patient-matching fields for recruiting cancer trials (US/CA/AU/UK).

  make trial-match MODEL=<id> [ARGS="--limit 10 --review trials_review.csv"]
      For recruiting interventional trials with a malignant-neoplasm code and a site in the US,
      Canada, Australia or the UK (phase 2+),
      one Bedrock call per study extracts disease/setting/prior-therapy/biomarkers/exclusions/
      patient-scope into reference/trial_match_cache.json (resumable: rerun to continue), then
      publishes data/trials.json to the site bucket for the dashboard "Cancer Trials" tab.

  Options: --phase PHASE2,PHASE2/PHASE3,PHASE3 (default); --countries "United States,Canada";
  --review FILE (hand-check CSV, fill your_check); --site DIR (site store for data/trials.json);
  --no-publish; --price-* (USD per million tokens) for a cost estimate.
"""

import argparse
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ct_pipeline import llm, llm_run, trial_match  # noqa: E402
from ct_pipeline.storage import LocalStore, S3Store, open_store  # noqa: E402

TABLES = tuple(llm_run.TABLES) + ("study_locations",)


def stack_outputs(stack: str) -> dict:
    import boto3

    desc = boto3.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in desc.get("Outputs", [])}


def open_data(args):
    if args.data:
        return open_store(args.data)
    return S3Store(stack_outputs(args.stack)["DataBucketName"])


def open_site(args):
    """Site store for data/trials.json: --site, else <data>/../site locally, else the stack's SiteBucket."""
    if args.site:
        return open_store(args.site)
    if args.data and not str(args.data).startswith("s3"):
        site = Path(args.data).parent / "site"
        return LocalStore(site) if site.exists() else None
    if args.data:
        return None
    return S3Store(stack_outputs(args.stack)["SiteBucketName"])


def prices(args) -> list:
    return [args.price_in, args.price_out, args.price_cache_read, args.price_cache_write]


def usage_line(usage: dict, price_list) -> str:
    line = ", ".join(f"{k} {usage.get(k, 0):,}" for k in llm_run.USAGE_KEYS)
    cost = llm_run.estimated_cost(usage, price_list)
    if cost is not None:
        line += f"; estimated cost ${cost:,.2f}"
    return line


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--model", required=True, help="Bedrock model or inference-profile id")
    ap.add_argument("--stack", default="ct-pipeline")
    ap.add_argument("--data", help="local data directory instead of the stack's data bucket")
    ap.add_argument("--site", help="site bucket/dir for data/trials.json (default: the stack's)")
    ap.add_argument(
        "--phase",
        default=",".join(trial_match.DEFAULT_PHASES),
        help="comma-separated phase_group values (default phase 2 and up)",
    )
    ap.add_argument("--ids", help="comma-separated NCT IDs instead of the target query")
    ap.add_argument(
        "--countries",
        default=",".join(trial_match.COUNTRIES),
        help="comma-separated site countries (default US, Canada, Australia, UK)",
    )
    ap.add_argument("--limit", type=int, help="at most this many studies (in NCT order)")
    ap.add_argument("--workers", type=int, default=llm.WORKERS)
    ap.add_argument("--review", help="write extracted fields to this CSV for hand-checking")
    ap.add_argument("--no-publish", action="store_true", help="skip writing data/trials.json")
    for name in ("in", "out", "cache-read", "cache-write"):
        ap.add_argument(f"--price-{name}", type=float, default=0.0)
    args = ap.parse_args()

    store = open_data(args)
    with tempfile.TemporaryDirectory(prefix="ct-trials-") as tmp:
        work = Path(tmp)
        try:
            current, con, icd = llm_run.open_snapshot(store, work, TABLES)
            phases = llm_run.parse_phases(con, args.phase)
        except llm.LlmError as e:
            sys.exit(str(e))
        catalog, categories = llm.category_catalog(con, icd)
        coder = trial_match.make_coder(args.model, catalog)

        countries = [c.strip() for c in args.countries.split(",") if c.strip()]
        if args.ids:
            ids = sorted({i.strip().upper() for i in args.ids.split(",") if i.strip()})
        else:
            ids = trial_match.targets(con, phases, countries)
        if args.limit:
            ids = ids[: args.limit]
        print(
            f"Snapshot {current['run_id']}: {len(ids):,} recruiting cancer trials "
            f"({', '.join(phases)}; sites in {', '.join(countries)}) to extract",
            flush=True,
        )
        docs = llm.study_documents(con, ids)
        missing = sorted(set(ids) - {d["nct_id"] for d in docs})
        if missing:
            print("Not in the snapshot:", ", ".join(missing))
        locs = trial_match.locations(con, ids, countries)
        codes = trial_match.cancer_codes(con, ids)
        con.close()

        progress = lambda d, n: print(f"  {d:,}/{n:,}", flush=True)  # noqa: E731
        entries, info = llm.code_studies(
            coder,
            store,
            docs,
            categories,
            workers=args.workers,
            progress=progress,
            cache_key=trial_match.CACHE_KEY,
            version=trial_match.PROMPT_VERSION,
            build_entry=trial_match.build_entry(categories, args.model),
        )
        print(
            f"\nCalls: {info['coded']:,} new, {info['cached']:,} cached, {info['failed']:,} failed",
            info["errors"] or "",
        )
        print(f"Tokens: {usage_line(info['usage'], prices(args))}")

        payload = trial_match.trials_payload(current, docs, entries, locs, categories, args.model, codes)
        if not args.no_publish:
            site = open_site(args)
            if site is not None:
                site.put_json(trial_match.TRIALS_KEY, payload)
                print(f"Wrote {trial_match.TRIALS_KEY} ({site.location(trial_match.TRIALS_KEY)})")
            else:
                print("Pass --site to publish data/trials.json; nothing uploaded.")
        if args.review:
            n = trial_match.write_review(args.review, docs, entries)
            print(f"Wrote {args.review}: {n} trials to hand-check (fill your_check: ok|wrong|partial)")


if __name__ == "__main__":
    main()
