"""
Build a new versioned snapshot from a run's staging Parquet:

  curated/<run_id>/<table>/data.parquet   = current snapshot − changed studies + staged rows
  curated/CURRENT.json                    → points at the new snapshot (atomic switch)
  <site>/data/overview.json               → every overview query, for every phase group
  Glue tables                             → repointed at the new snapshot (optional)
  state/watermark.json                    → next incremental fetch starts here
"""

import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .config import CURATED_PREFIX, CURRENT_KEY, OVERVIEW_KEY, STAGING_PREFIX, WATERMARK_KEY, Settings
from .queries import OVERVIEW_QUERIES, PHASE_GROUP_LABELS, run_query
from .storage import Store
from .transform import TABLES

logger = logging.getLogger(__name__)


def _q(path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def merge_tables(con, staging_dir: Path, current_dir, out_dir: Path) -> dict:
    """Merge staged pages into the current snapshot; returns row counts per table."""
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE winners AS
        SELECT nct_id, arg_max(_src, coalesce(last_update_post_date, DATE '1900-01-01')) AS _src
        FROM read_parquet({_q(staging_dir / "studies" / "*.parquet")})
        GROUP BY nct_id
    """)
    counts = {}
    for table in TABLES:
        staged = f"""
            SELECT * EXCLUDE (_src) FROM read_parquet({_q(staging_dir / table / "*.parquet")})
            SEMI JOIN winners USING (nct_id, _src)
        """
        current = current_dir / f"{table}.parquet" if current_dir else None
        if current is not None and current.exists():
            body = f"""
                {staged}
                UNION ALL BY NAME
                SELECT * FROM read_parquet({_q(current)}) ANTI JOIN winners USING (nct_id)
            """
        else:
            body = staged
        out = out_dir / table / "data.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY ({body} ORDER BY nct_id) TO {_q(out)} (FORMAT parquet, COMPRESSION zstd)")
        counts[table] = con.execute(f"SELECT count(*) FROM read_parquet({_q(out)})").fetchone()[0]
    return counts


def attach_snapshot(con, snapshot_dir: Path, tables=None) -> None:
    for table in tables or TABLES:
        con.execute(
            f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet({_q(snapshot_dir / table / 'data.parquet')})"
        )


def overview_payload(con, run_id: str) -> dict:
    groups = run_query(con, "phase_groups")["rows"]
    phases = [{"value": g, "label": PHASE_GROUP_LABELS.get(g, g), "studies": n} for g, n in groups]
    by_phase = {}
    for phase in [None] + [p["value"] for p in phases]:
        filters = {"phase": phase} if phase else {}
        by_phase[phase or "ALL"] = {name: run_query(con, name, filters) for name in OVERVIEW_QUERIES}
    return {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "phases": phases,
        "by_phase": by_phase,
    }


def table_columns(con, path: Path) -> list:
    return [(r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_q(path)})").fetchall()]


def build_snapshot(
    settings: Settings, store: Store, site_store: Store, run_id: str, mode: str, fetch_started_at: str, glue_client=None
) -> dict:
    work = Path(settings.work_dir) / "build" / run_id
    shutil.rmtree(work, ignore_errors=True)
    staging_dir, current_dir, out_dir = work / "staging", work / "current", work / "out"

    staged_keys = store.list_keys(f"{STAGING_PREFIX}{run_id}/")
    if not staged_keys:
        raise RuntimeError(f"No staging files for run {run_id}")
    for key in staged_keys:
        store.download(key, staging_dir / key[len(f"{STAGING_PREFIX}{run_id}/") :])

    previous = store.get_json(CURRENT_KEY)
    use_current = previous is not None and mode != "full"
    if use_current:
        for table, key in previous["tables"].items():
            if table in TABLES:
                store.download(key, current_dir / f"{table}.parquet")

    (work / "tmp").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(config={"temp_directory": str(work / "tmp")})
    try:
        counts = merge_tables(con, staging_dir, current_dir if use_current else None, out_dir)

        tables = {}
        for table in TABLES:
            key = f"{CURATED_PREFIX}{run_id}/{table}/data.parquet"
            store.upload(out_dir / table / "data.parquet", key, "application/vnd.apache.parquet")
            tables[table] = key

        current = {
            "run_id": run_id,
            "mode": mode,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "previous_run_id": previous["run_id"] if previous else None,
            "tables": tables,
            "row_counts": counts,
        }
        store.put_json(CURRENT_KEY, current)

        attach_snapshot(con, out_dir)
        if settings.site_uri:
            site_store.put_json(OVERVIEW_KEY, overview_payload(con, run_id))

        if settings.glue_database and glue_client is not None:
            from .glue import register_tables

            register_tables(
                glue_client,
                settings.glue_database,
                {
                    t: (
                        store.location(f"{CURATED_PREFIX}{run_id}/{t}/"),
                        table_columns(con, out_dir / t / "data.parquet"),
                    )
                    for t in TABLES
                },
            )
    finally:
        con.close()

    store.put_json(WATERMARK_KEY, {"fetch_started_at": fetch_started_at, "run_id": run_id})
    store.delete_prefix(f"{STAGING_PREFIX}{run_id}/")
    _prune_snapshots(store, settings.keep_snapshots)
    shutil.rmtree(work, ignore_errors=True)

    logger.info("Snapshot %s built: %s", run_id, counts)
    return {"run_id": run_id, "mode": mode, "row_counts": counts}


def _prune_snapshots(store: Store, keep: int) -> None:
    keys = [k for k in store.list_keys(CURATED_PREFIX) if k != CURRENT_KEY]
    runs = sorted({k[len(CURATED_PREFIX) :].split("/", 1)[0] for k in keys})
    for old in runs[:-keep] if keep > 0 else []:
        store.delete_prefix(f"{CURATED_PREFIX}{old}/")
