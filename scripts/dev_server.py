#!/usr/bin/env python3
"""
Serve the dashboards locally the same way CloudFront does in AWS:
  /            → dashboard/*.html
  /data/*      → <site>/data/*  (overview JSON written by the build)
  /api/query   → ct_pipeline.query_api (DuckDB over the current snapshot)

  python scripts/dev_server.py --data local/data --site local/site --port 8000
"""

import argparse
import os
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dataclasses import replace  # noqa: E402

from ct_pipeline import query_api  # noqa: E402
from ct_pipeline.config import Settings  # noqa: E402
from ct_pipeline.storage import open_store  # noqa: E402


def make_handler(settings, store, site_dir: Path):
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=str(ROOT / "dashboard"), **kw)

        def translate_path(self, path):
            p = urlsplit(path).path
            if p.startswith("/data/"):
                return str(site_dir / p.lstrip("/"))
            return super().translate_path(path)

        def do_GET(self):
            url = urlsplit(self.path)
            if url.path == "/":
                self.send_response(302)
                self.send_header("Location", "/dashboard.html")
                self.end_headers()
                return
            if url.path == "/api/query":
                event = {
                    "requestContext": {"http": {"method": "GET"}},
                    "queryStringParameters": dict(parse_qsl(url.query)),
                }
                resp = query_api.handle(event, settings, store)
                body = resp["body"].encode()
                self.send_response(resp["statusCode"])
                for k, v in resp["headers"].items():
                    if k != "Cache-Control":
                        self.send_header(k, v)
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            super().do_GET()

    return Handler


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.environ.get("CT_DATA_URI", "local/data"))
    ap.add_argument("--site", default=os.environ.get("CT_SITE_URI", "local/site"))
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()
    settings = replace(
        Settings.from_env(),
        data_uri=args.data,
        site_uri=args.site,
        work_dir=os.environ.get("CT_WORK_DIR", "local/work"),
    )
    handler = make_handler(settings, open_store(args.data), Path(args.site).resolve())
    print(f"Serving on http://localhost:{args.port}/")
    ThreadingHTTPServer(("0.0.0.0", args.port), handler).serve_forever()


if __name__ == "__main__":
    main()
