#!/usr/bin/env python3
"""
Measure what the UMLS layer adds before enabling it: `make umls-coverage` (AWS CloudShell).

Reads the UMLS API key from Secrets Manager and the current snapshot and reference from the data bucket,
looks up the snapshot's condition MeSH terms and strings in UMLS, then codes the snapshot with and
without the UMLS rows and prints the difference. The only thing it writes is the UMLS lookup cache
(reference/umls_cache.json), which the Reference function reuses; it is resumable if interrupted.
"""

import argparse
import sys
import tempfile
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ct_pipeline import umls  # noqa: E402
from ct_pipeline.build import attach_snapshot, code_conditions  # noqa: E402
from ct_pipeline.config import CURRENT_KEY  # noqa: E402
from ct_pipeline.db import connect  # noqa: E402
from ct_pipeline.reference import (  # noqa: E402
    SCHEMAS,
    combine_maps,
    download_reference,
    prepare_umls,
    uts_get,
    valid_text_map,
    write_table,
)
from ct_pipeline.storage import S3Store  # noqa: E402


def rows(con, path: Path) -> list:
    rel = con.sql(f"SELECT * FROM read_parquet('{path}')")
    return [dict(zip(rel.columns, r, strict=True)) for r in rel.fetchall()]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stack", default="ct-pipeline")
    ap.add_argument("--secret-name", default="ct-pipeline/umls-api-key")
    args = ap.parse_args()

    stack = boto3.client("cloudformation").describe_stacks(StackName=args.stack)["Stacks"][0]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    store = S3Store(outputs["DataBucketName"])
    key = boto3.client("secretsmanager").get_secret_value(SecretId=args.secret_name)["SecretString"].strip()
    client = umls.UmlsClient(key, uts_get)

    current = store.get_json(CURRENT_KEY)
    if current is None:
        sys.exit("No published snapshot yet (curated/CURRENT.json)")
    with tempfile.TemporaryDirectory(prefix="ct-umls-") as tmp:
        work = Path(tmp)
        for table in ("studies", "study_conditions", "condition_mesh_terms"):
            store.download(current["tables"][table], work / "snapshot" / table / "data.parquet")
        before = work / "before"
        reference = download_reference(store, before)
        if reference is None:
            sys.exit("No reference yet: run `make reference` first")
        con = connect(work)
        attach_snapshot(con, work / "snapshot", ["studies", "study_conditions", "condition_mesh_terms"])

        mesh_ids, texts = umls.lookup_inputs(con)
        print(f"Snapshot {current['run_id']}, reference {reference['version']}")
        print(f"Looking up {len(mesh_ids):,} MeSH ids and {len(texts):,} condition strings in UMLS…", flush=True)
        icd_codes = {r["code"] for r in rows(con, before / "icd10cm_codes" / "data.parquet")}
        maps, text_rows, info = umls.fetch_umls(
            client, store, mesh_ids, texts, progress=lambda d, n: print(f"  {d:,}/{n:,}", flush=True)
        )
        print(f"SNOMED CT → ICD-10-CM map {info['snomed_icd10cm_map']}: {info['snomed_map_concepts']:,} concepts")
        maps, text_rows, prep = prepare_umls(icd_codes, maps, text_rows)

        base = [r for r in rows(con, before / "condition_code_map" / "data.parquet") if r["source"] not in umls.SOURCES]
        after = work / "after"
        for name in ("icd10cm_codes", "condition_text_rules", "condition_llm_codes"):
            (after / name).mkdir(parents=True)
            (after / name / "data.parquet").write_bytes((before / name / "data.parquet").read_bytes())
        code_map, dropped = combine_maps(icd_codes, {"base": base, **maps})
        text_map, _ = valid_text_map(icd_codes, text_rows)
        base_map, _ = combine_maps(icd_codes, {"base": base})
        write_table(con, base_map, SCHEMAS["condition_code_map"], before / "condition_code_map" / "data.parquet")
        write_table(con, [], SCHEMAS["condition_text_map"], before / "condition_text_map" / "data.parquet")
        write_table(con, code_map, SCHEMAS["condition_code_map"], after / "condition_code_map" / "data.parquet")
        write_table(con, text_map, SCHEMAS["condition_text_map"], after / "condition_text_map" / "data.parquet")
        print(
            f"UMLS rows: {len(maps['umls']):,} MeSH→ICD-10-CM, {len(maps['umls_snomed']):,} via SNOMED CT, "
            f"{len(text_map):,} text; dropped as not in ICD-10-CM: {prep['dropped_invalid']}, "
            f"as ambiguous: {prep['dropped_ambiguous']}"
        )

        code_conditions(con, before, work / "out_before", "before")
        code_conditions(con, after, work / "out_after", "after")
        for name in ("before", "after"):
            path = work / f"out_{name}" / "study_condition_codes" / "data.parquet"
            con.execute(f"CREATE VIEW codes_{name} AS SELECT * FROM read_parquet('{path}') WHERE code_system='ICD10CM'")
        q = con.sql
        total = q("SELECT count(*) FROM studies").fetchone()[0]
        n_before = q("SELECT count(DISTINCT nct_id) FROM codes_before").fetchone()[0]
        n_after = q("SELECT count(DISTINCT nct_id) FROM codes_after").fetchone()[0]
        print(
            f"\nStudies with an ICD-10-CM code: {n_before:,} → {n_after:,} of {total:,} "
            f"({n_before / total:.1%} → {n_after / total:.1%})"
        )
        print("\nNewly coded studies by route (a study can count in several):")
        new = "SELECT * FROM codes_after WHERE nct_id NOT IN (SELECT nct_id FROM codes_before)"
        for via, source, n in q(
            f"SELECT via, source, count(DISTINCT nct_id) FROM ({new}) GROUP BY ALL ORDER BY 3 DESC"
        ).fetchall():
            print(f"  {via:14} {source:12} {n:6,}")
        changed = q("""
            SELECT count(*) FROM (SELECT nct_id, list_sort(list(DISTINCT code)) c FROM codes_before GROUP BY 1) b
            JOIN (SELECT nct_id, list_sort(list(DISTINCT code)) c FROM codes_after GROUP BY 1) a USING (nct_id)
            WHERE a.c <> b.c
        """).fetchone()[0]
        print(f"\nAlready-coded studies whose code set changed: {changed:,}")
        print("\nMeSH terms whose codes changed, by studies (before → after):")
        before_codes, after_codes = _codes_by_mesh(base_map), _codes_by_mesh(code_map)
        changed_mesh = {m for m in before_codes if after_codes.get(m, set()) != before_codes[m]}
        terms = q(
            "SELECT mesh_id, any_value(term), count(DISTINCT nct_id) FROM condition_mesh_terms "
            "WHERE NOT is_ancestor GROUP BY 1"
        ).fetchall()
        top = sorted((t for t in terms if t[0] in changed_mesh), key=lambda t: -t[2])[:20]
        print(f"  ({len(changed_mesh):,} MeSH terms changed)")
        for mesh_id, term, n in top:
            b = ", ".join(sorted(before_codes[mesh_id]))
            a = ", ".join(sorted(after_codes.get(mesh_id, ()))) or "-"
            print(f"  {n:5,}  {term[:40]:40} {b[:30]} → {a[:30]}")
        print("\nMost common new mappings (check these look right):")
        for term, code, label, source, n in q(f"""
            SELECT coalesce(mesh_term, condition_name), code, label, source, count(DISTINCT nct_id)
            FROM ({new}) WHERE source IN ('umls', 'umls_snomed') GROUP BY ALL ORDER BY 5 DESC LIMIT 25
        """).fetchall():
            print(f"  {n:5,}  {term[:45]:45} → {code:8} {(label or '')[:40]}  [{source}]")
        print("\nMost common conditions still uncoded:")
        for name, n in q("""
            SELECT lower(condition_name), count(*) FROM study_conditions
            WHERE nct_id NOT IN (SELECT nct_id FROM codes_after) GROUP BY 1 ORDER BY 2 DESC LIMIT 15
        """).fetchall():
            print(f"  {n:5,}  {name}")
        if info["errors"]:
            print(f"\nFailed lookups by target and HTTP status (retried next run): {info['errors']}")
        if not info["complete"]:
            print("\nLookups incomplete; run again to continue.")
        con.close()


def _codes_by_mesh(rows: list) -> dict:
    out = {}
    for r in rows:
        if r["code_system"] == "ICD10CM":
            out.setdefault(r["mesh_id"], set()).add(r["code"])
    return out


if __name__ == "__main__":
    main()
