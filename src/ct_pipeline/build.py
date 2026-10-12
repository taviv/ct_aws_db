"""
Build a new versioned snapshot from a run's staging Parquet:

  curated/<run_id>/<table>/data.parquet   = current snapshot − changed studies + staged rows
  curated/CURRENT.json                    → points at the new snapshot (atomic switch)
  <site>/data/overview.json               → every overview query, for every phase group
  curated/<run_id>/study_condition_codes  = conditions coded against reference/CURRENT.json (if present)
  Glue tables                             → repointed at the new snapshot (optional)
  state/watermark.json                    → next incremental fetch starts here
"""

import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

from .config import CURATED_PREFIX, CURRENT_KEY, OVERVIEW_KEY, STAGING_PREFIX, WATERMARK_KEY, Settings
from .db import connect
from .queries import OVERVIEW_QUERIES, PHASE_GROUP_LABELS, run_query
from .reference import REFERENCE_TABLES, download_reference
from .storage import Store
from .transform import TABLES

DERIVED_TABLES = ("study_condition_codes",)

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


def code_conditions(con, ref_dir: Path, out_dir: Path, ref_version: str) -> int:
    """
    study_condition_codes, one row per (study, code system, code, evidence):
      via='mesh'          the study's own condition MeSH term maps to the code
      via='text'          a curated rule matches a free-text condition name (e.g. healthy volunteers → Z00.6)
      via='mesh_ancestor' fallback for studies with no ICD-10-CM code yet: the most specific code reachable
                          from a MeSH ancestor; always match_type='broad'
    Expects study_conditions and condition_mesh_terms views (attach_snapshot).
    """
    ref = {t: f"read_parquet({_q(ref_dir / t / 'data.parquet')})" for t in REFERENCE_TABLES}
    out = out_dir / "study_condition_codes" / "data.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
        WITH icd AS (SELECT * FROM {ref["icd10cm_codes"]}),
        code_map AS (SELECT * FROM {ref["condition_code_map"]}),
        direct AS (
            SELECT DISTINCT t.nct_id, m.code_system, m.code, m.label, 'mesh' AS via, t.mesh_id, t.term AS mesh_term,
                   NULL::VARCHAR AS condition_name, m.match_type, m.source
            FROM condition_mesh_terms t JOIN code_map m USING (mesh_id)
            WHERE NOT t.is_ancestor
        ),
        text AS (
            SELECT DISTINCT c.nct_id, r.code_system, r.code, NULL::VARCHAR AS label, 'text' AS via,
                   NULL::VARCHAR AS mesh_id, NULL::VARCHAR AS mesh_term, c.condition_name, r.match_type,
                   'curated' AS source
            FROM study_conditions c
            JOIN {ref["condition_text_rules"]} r
              ON regexp_full_match(trim(regexp_replace(lower(c.condition_name), '[^a-z0-9]+', ' ', 'g')), r.pattern)
        ),
        coded AS (
            SELECT nct_id FROM direct WHERE code_system = 'ICD10CM'
            UNION SELECT nct_id FROM text WHERE code_system = 'ICD10CM'
        ),
        ancestor AS (
            SELECT DISTINCT t.nct_id, m.code_system, m.code, m.label, 'mesh_ancestor' AS via, t.mesh_id,
                   t.term AS mesh_term, NULL::VARCHAR AS condition_name, 'broad' AS match_type, m.source,
                   CASE i.level WHEN 'code' THEN 4 WHEN 'category' THEN 3 WHEN 'block' THEN 2 ELSE 1 END AS specificity
            FROM condition_mesh_terms t
            JOIN code_map m USING (mesh_id)
            JOIN icd i ON i.code = m.code
            WHERE t.is_ancestor AND m.code_system = 'ICD10CM' AND t.nct_id NOT IN (SELECT nct_id FROM coded)
        ),
        best_ancestor AS (
            SELECT * EXCLUDE (specificity) FROM ancestor
            QUALIFY specificity = max(specificity) OVER (PARTITION BY nct_id)
        ),
        coded_all AS (
            SELECT * FROM direct UNION ALL SELECT * FROM text UNION ALL SELECT * FROM best_ancestor
        )
        SELECT a.nct_id, a.code_system, a.code, coalesce(i.description, a.label) AS label,
               i.chapter AS icd10_chapter, i.block AS icd10_block, i.category AS icd10_category,
               a.via, a.mesh_id, a.mesh_term, a.condition_name, a.match_type, a.source,
               {_q(ref_version)} AS ref_version
        FROM coded_all a LEFT JOIN icd i ON a.code_system = 'ICD10CM' AND i.code = a.code
        ORDER BY a.nct_id, a.code_system, a.code, a.via, a.mesh_id, a.condition_name
        ) TO {_q(out)} (FORMAT parquet, COMPRESSION zstd)
    """)
    return con.execute(f"SELECT count(*) FROM read_parquet({_q(out)})").fetchone()[0]


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

    con = connect(work)
    try:
        counts = merge_tables(con, staging_dir, current_dir if use_current else None, out_dir)
        _publish(settings, store, site_store, con, work, run_id, mode, previous, counts, glue_client)
    finally:
        con.close()

    store.put_json(WATERMARK_KEY, {"fetch_started_at": fetch_started_at, "run_id": run_id})
    store.delete_prefix(f"{STAGING_PREFIX}{run_id}/")
    _prune_snapshots(store, settings.keep_snapshots)
    shutil.rmtree(work, ignore_errors=True)

    logger.info("Snapshot %s built: %s", run_id, counts)
    return {"run_id": run_id, "mode": mode, "row_counts": counts}


def recode_snapshot(settings: Settings, store: Store, site_store: Store, glue_client=None, now=None) -> dict:
    """Republish the current snapshot as a new run with derived tables recomputed (e.g. after a reference update)."""
    previous = store.get_json(CURRENT_KEY)
    if previous is None:
        raise RuntimeError("No current snapshot to recode")
    run_id = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ") + "-recode"
    work = Path(settings.work_dir) / "build" / run_id
    shutil.rmtree(work, ignore_errors=True)
    for table in TABLES:
        store.download(previous["tables"][table], work / "out" / table / "data.parquet")
    counts = {t: previous["row_counts"][t] for t in TABLES}
    con = connect(work)
    try:
        _publish(settings, store, site_store, con, work, run_id, "recode", previous, counts, glue_client)
    finally:
        con.close()
    _prune_snapshots(store, settings.keep_snapshots)
    shutil.rmtree(work, ignore_errors=True)
    logger.info("Snapshot %s recoded from %s: %s", run_id, previous["run_id"], counts)
    return {"run_id": run_id, "mode": "recode", "row_counts": counts}


def _publish(settings, store, site_store, con, work: Path, run_id, mode, previous, counts, glue_client) -> None:
    """Derive coded tables, upload the snapshot, switch CURRENT.json, publish overview, update Glue."""
    out_dir = work / "out"
    attach_snapshot(con, out_dir)
    reference = download_reference(store, work / "reference")
    derived = ()
    if reference is None:
        logger.warning("No reference/CURRENT.json yet; skipping study_condition_codes (run the Reference function)")
    else:
        counts["study_condition_codes"] = code_conditions(con, work / "reference", out_dir, reference["version"])
        derived = DERIVED_TABLES

    tables = {}
    for table in (*TABLES, *derived):
        key = f"{CURATED_PREFIX}{run_id}/{table}/data.parquet"
        store.upload(out_dir / table / "data.parquet", key, "application/vnd.apache.parquet")
        tables[table] = key

    store.put_json(
        CURRENT_KEY,
        {
            "run_id": run_id,
            "mode": mode,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "previous_run_id": previous["run_id"] if previous else None,
            "reference_version": reference["version"] if reference else None,
            "tables": tables,
            "row_counts": counts,
        },
    )

    if settings.site_uri:
        site_store.put_json(OVERVIEW_KEY, overview_payload(con, run_id))

    if settings.glue_database and glue_client is not None:
        from .glue import register_tables

        glue_tables = {
            t: (store.location(f"{CURATED_PREFIX}{run_id}/{t}/"), table_columns(con, out_dir / t / "data.parquet"))
            for t in tables
        }
        if reference is not None:
            for t, key in reference["tables"].items():
                glue_tables[t] = (
                    store.location(key.rsplit("/", 1)[0] + "/"),
                    table_columns(con, work / "reference" / t / "data.parquet"),
                )
        register_tables(glue_client, settings.glue_database, glue_tables)


def _prune_snapshots(store: Store, keep: int) -> None:
    keys = [k for k in store.list_keys(CURATED_PREFIX) if k != CURRENT_KEY]
    runs = sorted({k[len(CURATED_PREFIX) :].split("/", 1)[0] for k in keys})
    for old in runs[:-keep] if keep > 0 else []:
        store.delete_prefix(f"{CURATED_PREFIX}{old}/")
