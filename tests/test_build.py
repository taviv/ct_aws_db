import duckdb

from ct_pipeline.build import build_snapshot
from ct_pipeline.config import CURRENT_KEY, OVERVIEW_KEY, WATERMARK_KEY
from ct_pipeline.transform import TABLES, transform_page

from .conftest import FIXTURE, load_studies, make_study, ndjson


def _run(settings, store, site_store, run_id, pages, mode="incremental", fetched="2024-06-01T00:00:00+00:00"):
    for i, studies in enumerate(pages, 1):
        key = f"raw/{run_id}/page_{i:04d}.ndjson"
        store.put_bytes(key, studies if isinstance(studies, bytes) else ndjson(studies))
        transform_page(settings, store, run_id, key)
    return build_snapshot(settings, store, site_store, run_id, mode, fetched)


def _query(store, sql):
    current = store.get_json(CURRENT_KEY)
    con = duckdb.connect()
    for t, key in current["tables"].items():
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{store.root / key}')")
    return con.execute(sql).fetchall()


def test_full_build_publishes_snapshot_overview_and_watermark(settings, store, site_store):
    result = _run(settings, store, site_store, "R1", [FIXTURE.read_bytes()], mode="full")
    assert result["row_counts"]["studies"] == len(load_studies())

    current = store.get_json(CURRENT_KEY)
    assert current["run_id"] == "R1" and set(current["tables"]) == set(TABLES)
    assert store.get_json(WATERMARK_KEY)["fetch_started_at"] == "2024-06-01T00:00:00+00:00"
    assert store.list_keys("staging/") == []

    overview = site_store.get_json(OVERVIEW_KEY)
    assert overview["by_phase"]["ALL"]["summary_stats"]["total_studies"] == len(load_studies())
    phases = {p["value"]: p["studies"] for p in overview["phases"]}
    assert {"PHASE1", "PHASE3"} <= set(phases)
    for value, n in phases.items():
        assert overview["by_phase"][value]["summary_stats"]["total_studies"] == n


def test_duplicate_within_run_keeps_latest_update(settings, store, site_store):
    old = make_study("NCT1", title="old", updated="2024-01-01", countries=("France",))
    new = make_study("NCT1", title="new", updated="2024-02-01", countries=("Japan", "Chile"))
    _run(settings, store, site_store, "R1", [[new, make_study("NCT2")], [old]], mode="full")
    assert _query(store, "SELECT brief_title FROM studies WHERE nct_id = 'NCT1'") == [("new",)]
    assert sorted(_query(store, "SELECT country FROM study_locations WHERE nct_id = 'NCT1'")) == [
        ("Chile",),
        ("Japan",),
    ]


def test_incremental_build_upserts_and_replaces_child_rows(settings, store, site_store):
    _run(
        settings,
        store,
        site_store,
        "R1",
        [
            [
                make_study("NCT1", phases=["PHASE1"], countries=("France", "Spain")),
                make_study("NCT2", phases=["PHASE3"]),
            ]
        ],
        mode="full",
    )
    _run(
        settings,
        store,
        site_store,
        "R2",
        [
            [
                make_study("NCT1", phases=["PHASE2"], countries=("Italy",), updated="2024-07-01"),
                make_study("NCT3", phases=["PHASE4"]),
            ]
        ],
    )

    assert _query(store, "SELECT nct_id, phase_group FROM studies ORDER BY 1") == [
        ("NCT1", "PHASE2"),
        ("NCT2", "PHASE3"),
        ("NCT3", "PHASE4"),
    ]
    assert _query(store, "SELECT country FROM study_locations WHERE nct_id = 'NCT1'") == [("Italy",)]
    assert _query(store, "SELECT phase FROM study_phases WHERE nct_id = 'NCT1'") == [("PHASE2",)]
    current = store.get_json(CURRENT_KEY)
    assert current["run_id"] == "R2" and current["previous_run_id"] == "R1"


def test_full_mode_replaces_snapshot_and_prunes_old_runs(settings, store, site_store):
    _run(settings, store, site_store, "R1", [[make_study("NCT1"), make_study("NCT2")]], mode="full")
    _run(settings, store, site_store, "R2", [[make_study("NCT3")]])
    _run(settings, store, site_store, "R3", [[make_study("NCT9")]], mode="full")
    assert _query(store, "SELECT nct_id FROM studies") == [("NCT9",)]
    runs = {k.split("/")[1] for k in store.list_keys("curated/") if k != CURRENT_KEY}
    assert runs == {"R2", "R3"}  # keep_snapshots=2
