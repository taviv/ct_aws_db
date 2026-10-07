#!/usr/bin/env python3
"""
Run the whole pipeline locally (no AWS): fetch → transform → build, writing to a local
"bucket" directory. Same code paths as the Lambdas.

  python scripts/local_pipeline.py --data ./local/data --site ./local/site --max-pages 3
  python scripts/local_pipeline.py --data s3://my-bucket --site s3://my-site-bucket   # against S3
"""

import argparse
import logging
import os
import sys
import time
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ct_pipeline import fetch  # noqa: E402
from ct_pipeline.build import build_snapshot  # noqa: E402
from ct_pipeline.config import Settings  # noqa: E402
from ct_pipeline.storage import open_store  # noqa: E402
from ct_pipeline.transform import transform_page  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=os.environ.get("CT_DATA_URI", "local/data"))
    ap.add_argument("--site", default=os.environ.get("CT_SITE_URI", "local/site"))
    ap.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    ap.add_argument("--query-term", help="override CT_QUERY_TERM")
    ap.add_argument("--start-year", help="override CT_START_YEAR")
    ap.add_argument("--page-size", type=int)
    ap.add_argument("--max-pages", type=int, default=0, help="stop after N pages (0 = all)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()
    settings = replace(
        settings, data_uri=args.data, site_uri=args.site, work_dir=os.environ.get("CT_WORK_DIR", "local/work")
    )
    if args.query_term:
        settings = replace(settings, query_term=args.query_term)
    if args.start_year is not None:
        settings = replace(settings, start_year=args.start_year)
    if args.page_size:
        settings = replace(settings, page_size=args.page_size)
    store, site = open_store(settings.data_uri), open_store(settings.site_uri)

    t0 = time.time()
    state = fetch.start({"mode": args.mode}, settings, store)
    while not state["done"]:
        state = fetch.fetch_page(state, settings, store)
        if args.max_pages and state["page_num"] > args.max_pages:
            state["done"] = True
    run = fetch.finalize(state, settings, store)
    logging.info("Fetched %d files (%s) in %.0fs", run["file_count"], run["mode"], time.time() - t0)
    if not run["file_count"]:
        logging.info("Nothing changed since the last run.")
        return

    for key in run["files"]:
        transform_page(settings, store, run["run_id"], key)
    result = build_snapshot(settings, store, site, run["run_id"], run["mode"], run["fetch_started_at"])
    logging.info("Done in %.0fs: %s", time.time() - t0, result["row_counts"])


if __name__ == "__main__":
    main()
