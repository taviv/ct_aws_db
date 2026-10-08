"""DuckDB connections that write only under a scratch directory (Lambda's working dir is read-only)."""

from pathlib import Path

import duckdb


def connect(work_dir) -> duckdb.DuckDBPyConnection:
    work = Path(work_dir)
    tmp = work / "duckdb_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(config={"temp_directory": str(tmp), "home_directory": str(work)})
