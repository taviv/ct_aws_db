"""
Reference vocabularies for condition coding, versioned under reference/<version>/:

  icd10cm_codes          ICD-10-CM chapters, blocks, categories and codes (CMS tabular + order files)
  condition_code_map     condition MeSH ID → code (ICD10CM, ICD11, MONDO, UMLS), with source and match type
  condition_text_map     normalised free-text condition name → code (UMLS concept search; empty without a key)
  condition_text_rules   regex on normalised free-text condition names → code (curated, data/)
  reference/CURRENT.json → the active version

Crosswalk sources, best first: curated overrides (data/mesh_overrides.csv), UMLS (CUI synonymy; needs an
API key, see umls.py), MONDO SSSOM, Wikidata, UMLS via SNOMED CT. Exact matches beat broad ones.
Every ICD-10-CM code is validated against the CMS code list; invalid codes are dropped and counted.
"""

import csv
import io
import json
import logging
import re
import shutil
import xml.etree.ElementTree as ET
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import urllib3
from urllib3.util.retry import Retry

from . import umls
from .config import CURRENT_KEY, REFERENCE_CURRENT_KEY, REFERENCE_PREFIX
from .db import connect

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
KEEP_VERSIONS = 3

CMS_ZIP = "https://www.cms.gov/files/zip/"
TABULAR_ZIPS = ("{fy}-code-tables-tabular-and-index.zip", "{fy}-code-tables-tabular-index.zip")
ORDER_ZIPS = ("{fy}-code-descriptions-tabular-order.zip",)
MONDO_SSSOM_URL = (
    "https://raw.githubusercontent.com/monarch-initiative/mondo/master/src/ontology/mappings/mondo.sssom.tsv"
)
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
WIKIDATA_PROPERTIES = {"ICD10CM": "P4229", "ICD11": "P7807"}

SOURCE_PRIORITY = ("curated", "umls", "mondo", "wikidata", "umls_snomed")
MATCH_TYPES = ("exact", "broad")

SCHEMAS = {
    "icd10cm_codes": {
        "code": "VARCHAR",
        "description": "VARCHAR",
        "level": "VARCHAR",
        "is_billable": "BOOLEAN",
        "category": "VARCHAR",
        "block": "VARCHAR",
        "block_title": "VARCHAR",
        "chapter": "INTEGER",
        "chapter_title": "VARCHAR",
    },
    "condition_code_map": {
        "mesh_id": "VARCHAR",
        "code_system": "VARCHAR",
        "code": "VARCHAR",
        "label": "VARCHAR",
        "match_type": "VARCHAR",
        "source": "VARCHAR",
    },
    "condition_text_map": {
        "condition_norm": "VARCHAR",
        "code_system": "VARCHAR",
        "code": "VARCHAR",
        "label": "VARCHAR",
        "match_type": "VARCHAR",
        "source": "VARCHAR",
    },
    "condition_text_rules": {
        "pattern": "VARCHAR",
        "code_system": "VARCHAR",
        "code": "VARCHAR",
        "match_type": "VARCHAR",
        "note": "VARCHAR",
    },
}
REFERENCE_TABLES = tuple(SCHEMAS)

_HTTP = urllib3.PoolManager(
    retries=Retry(total=4, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504)),
    timeout=urllib3.Timeout(connect=10, read=180),
)
USER_AGENT = "ct-aws-db-reference/1.0 (https://github.com/taviv/ct_aws_db)"


def http_get(url: str, headers=None, **request_kw) -> bytes | None:
    """Body for 200, None for 404; raises otherwise."""
    r = _HTTP.request(
        "GET", url, headers={"User-Agent": USER_AGENT, **(headers or {})}, preload_content=True, **request_kw
    )
    if r.status == 404:
        return None
    if r.status != 200:
        raise RuntimeError(f"GET {url} → HTTP {r.status}")
    return r.data


def uts_get(url: str, headers=None) -> bytes | None:
    """http_get for the UMLS API: fail fast on a stalled request (it is retried on the next run)."""
    return http_get(
        url,
        headers,
        timeout=urllib3.Timeout(connect=10, read=30),
        retries=Retry(total=2, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504)),
    )


def normalize_condition(text: str) -> str:
    """Lower-case, non-alphanumerics → single spaces. Mirrored in SQL by build.code_conditions."""
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()


def fiscal_year(today: date) -> int:
    """ICD-10-CM fiscal year N runs from 1 October N-1."""
    return today.year + (1 if today.month >= 10 else 0)


# ---------------------------------------------------------------- ICD-10-CM


def dotted(code: str) -> str:
    code = code.strip().upper().replace(".", "")
    return code if len(code) <= 3 else f"{code[:3]}.{code[3:]}"


def normalize_icd10cm(raw: str) -> str:
    """'ICD10CM:C50.911' / 'c50911' → 'C50.911'; ranges 'E08-E13' / 'E08.E13' → 'E08-E13'; 'F99-F99' → 'F99'."""
    raw = (raw or "").strip().upper().removeprefix("ICD10CM:").strip().rstrip("+*")
    m = re.fullmatch(r"([A-Z][0-9][0-9A-Z])[-.]([A-Z][0-9][0-9A-Z])", raw)
    if m:
        first, last = m.groups()
        return first if first == last else f"{first}-{last}"
    return dotted(raw)


def _read_text(data: bytes) -> str:
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("latin-1")


def parse_icd10cm(tabular_xml: bytes, order_txt: str) -> list:
    """Rows for icd10cm_codes: chapters and blocks from the tabular XML, categories/codes from the order file."""
    root = ET.fromstring(tabular_xml)
    rows, blocks = {}, []
    for ch in root.iter("chapter"):
        num, title = int(ch.findtext("name").strip()), ch.findtext("desc").strip()
        m = re.search(r"\(([A-Z0-9]{3})-([A-Z0-9]{3})\)\s*$", title)
        if m:
            code = f"{m.group(1)}-{m.group(2)}"
            title = title[: m.start()].strip()
            rows[code] = _icd_row(code, title, "chapter", None, None, None, num, title)
        index = ch.find("sectionIndex")
        for ref in index if index is not None else []:
            first, _, last = ref.get("id").partition("-")
            desc = (ref.text or "").strip()
            blocks.append((first, last or first, ref.get("id"), desc, num, title))
            rows.setdefault(
                ref.get("id"), _icd_row(ref.get("id"), desc, "block", None, ref.get("id"), desc, num, title)
            )

    def block_of(category):
        for first, last, bid, desc, num, title in blocks:
            if first <= category <= last:
                return bid, desc, num, title
        return None, None, None, None

    previous = (None, None, None, None)
    for line in order_txt.splitlines():
        if len(line) < 16 or not line[:5].isdigit():
            continue
        compact, billable = line[6:13].strip(), line[14] == "1"
        desc = line[77:].strip() or line[16:76].strip()
        # Categories such as C4A/C7A/D3A sort outside their block's range; inherit the previous category's block
        found = block_of(compact[:3])
        bid, bdesc, num, title = found = found if found[0] else previous
        previous = found
        code = dotted(compact)
        level = "category" if len(compact) == 3 else "code"
        rows[code] = _icd_row(code, desc, level, compact[:3], bid, bdesc, num, title, billable)
    return list(rows.values())


def _icd_row(code, desc, level, category, block, block_title, chapter, chapter_title, billable=False):
    return {
        "code": code,
        "description": desc,
        "level": level,
        "is_billable": billable,
        "category": category,
        "block": block,
        "block_title": block_title,
        "chapter": chapter,
        "chapter_title": chapter_title,
    }


def _zip_member(data: bytes, pattern: str) -> bytes:
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for name in z.namelist():
            if re.search(pattern, name, re.I):
                return z.read(name)
    raise RuntimeError(f"No file matching {pattern!r} in ICD-10-CM zip")


def fetch_icd10cm(get, today: date) -> tuple:
    """(rows, source info) for the current fiscal year, falling back to the previous one."""
    fy = fiscal_year(today)
    for year in (fy, fy - 1):
        order_url = CMS_ZIP + ORDER_ZIPS[0].format(fy=year)
        order = get(order_url)
        if order is None:
            continue
        for name in TABULAR_ZIPS:
            tab_url = CMS_ZIP + name.format(fy=year)
            tabular = get(tab_url)
            if tabular is not None:
                rows = parse_icd10cm(
                    _zip_member(tabular, r"icd10cm[-_]tabular[-_]\d{4}\.xml$"),
                    _read_text(_zip_member(order, r"icd10cm[-_]order[-_]\d{4}\.txt$")),
                )
                return rows, {"fiscal_year": year, "urls": [tab_url, order_url]}
    raise RuntimeError(f"ICD-10-CM files for FY{fy} / FY{fy - 1} not found under {CMS_ZIP}")


# ---------------------------------------------------------------- crosswalks


def parse_mondo_sssom(text: str) -> list:
    """MeSH → ICD-10-CM / ICD-11 / UMLS / MONDO via MONDO concepts that exactly match a MeSH descriptor."""
    lines = [ln for ln in text.splitlines() if ln and not ln.startswith("#")]
    reader = csv.DictReader(lines, delimiter="\t")
    mesh_of, targets = {}, []
    for r in reader:
        subj, pred, obj = r["subject_id"], r["predicate_id"], r["object_id"]
        if not subj.startswith("MONDO:"):
            continue
        if obj.lower().startswith("mesh:") and pred == "skos:exactMatch":
            mesh_of.setdefault(subj, set()).add(obj[5:])
        elif pred in ("skos:exactMatch", "skos:broadMatch"):
            targets.append((subj, r.get("subject_label") or "", pred, obj, r.get("object_label") or ""))
    rows = []
    for mondo, label in {(t[0], t[1]) for t in targets}:
        targets.append((mondo, label, "skos:exactMatch", mondo, label))
    for subj, subj_label, pred, obj, obj_label in targets:
        prefix, _, code = obj.partition(":")
        system = {"ICD10CM": "ICD10CM", "icd11.foundation": "ICD11", "UMLS": "UMLS", "MONDO": "MONDO"}.get(prefix)
        if system is None or subj not in mesh_of:
            continue
        if system == "MONDO":
            code = obj
        if system in ("UMLS", "MONDO") and pred != "skos:exactMatch":
            continue
        for mesh in mesh_of[subj]:
            rows.append(
                {
                    "mesh_id": mesh,
                    "code_system": system,
                    "code": code,
                    "label": obj_label or subj_label,
                    "match_type": "exact" if pred == "skos:exactMatch" else "broad",
                    "source": "mondo",
                }
            )
    return rows


def fetch_wikidata(get) -> list:
    rows = []
    for system, prop in WIKIDATA_PROPERTIES.items():
        query = f"SELECT ?mesh ?code WHERE {{ ?item wdt:P486 ?mesh ; wdt:{prop} ?code . }}"
        url = WIKIDATA_SPARQL_URL + "?" + urlencode({"query": query, "format": "json"})
        data = get(url, headers={"Accept": "application/sparql-results+json"})
        for b in json.loads(data)["results"]["bindings"]:
            rows.append(
                {
                    "mesh_id": b["mesh"]["value"],
                    "code_system": system,
                    "code": b["code"]["value"],
                    "label": None,
                    "match_type": "exact",
                    "source": "wikidata",
                }
            )
    return rows


def load_curated() -> tuple:
    """(mesh override rows, text rule rows) from the CSVs shipped in data/."""
    with open(DATA_DIR / "mesh_overrides.csv", newline="") as f:
        overrides = [
            {
                "mesh_id": r["mesh_id"],
                "code_system": "ICD10CM",
                "code": r["icd10cm"],
                "label": r["mesh_term"],
                "match_type": r["match_type"],
                "source": "curated",
            }
            for r in csv.DictReader(ln for ln in f if not ln.startswith("#"))
        ]
    with open(DATA_DIR / "condition_text_rules.csv", newline="") as f:
        rules = [
            {
                "pattern": r["pattern"],
                "code_system": "ICD10CM",
                "code": r["icd10cm"],
                "match_type": r["match_type"],
                "note": r["note"],
            }
            for r in csv.DictReader(ln for ln in f if not ln.startswith("#"))
        ]
    for rule in rules:
        re.compile(rule["pattern"])
    return overrides, rules


def combine_maps(icd_codes: set, sources: dict) -> tuple:
    """
    Validate ICD-10-CM codes, then keep, per (mesh_id, code_system), only the rows of the best
    (match type, source) — curated always wins. Returns (rows, {source: dropped invalid count}).
    """
    dropped, candidates = {}, []
    for source, rows in sources.items():
        dropped[source] = 0
        for r in rows:
            if r["code_system"] == "ICD10CM":
                r = {**r, "code": normalize_icd10cm(r["code"])}
                if r["code"] not in icd_codes:
                    dropped[source] += 1
                    continue
            if r["match_type"] not in MATCH_TYPES:
                raise ValueError(f"Unknown match_type {r['match_type']!r} from {source}")
            candidates.append(r)

    def rank(r):
        curated = r["source"] != "curated"
        return (curated, MATCH_TYPES.index(r["match_type"]), SOURCE_PRIORITY.index(r["source"]))

    best = {}
    for r in candidates:
        key = (r["mesh_id"], r["code_system"])
        best[key] = min(best.get(key, rank(r)), rank(r))
    seen, out = set(), []
    for r in candidates:
        k = (r["mesh_id"], r["code_system"], r["code"])
        if rank(r) == best[(r["mesh_id"], r["code_system"])] and k not in seen:
            seen.add(k)
            out.append(r)
    return sorted(out, key=lambda r: (r["mesh_id"], r["code_system"], r["code"])), dropped


# ---------------------------------------------------------------- build / load


def write_table(con, rows: list, schema: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    src = path.with_suffix(".ndjson")
    src.write_text("".join(json.dumps({c: r.get(c) for c in schema}) + "\n" for r in rows))
    cols = "{" + ", ".join(f"'{c}': '{t}'" for c, t in schema.items()) + "}"
    con.execute(
        f"COPY (SELECT * FROM read_json('{src}', format='newline_delimited', columns={cols})) "
        f"TO '{path}' (FORMAT parquet, COMPRESSION zstd)"
    )
    src.unlink()


def snapshot_lookup_inputs(store, work: Path) -> tuple:
    """UMLS lookup inputs (MeSH ids, condition strings) from the current snapshot; ([], []) if there is none."""
    current = store.get_json(CURRENT_KEY)
    if current is None:
        return [], []
    con = connect(work)
    try:
        for table in ("study_conditions", "condition_mesh_terms"):
            path = work / "snapshot" / table / "data.parquet"
            store.download(current["tables"][table], path)
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')")
        return umls.lookup_inputs(con)
    finally:
        con.close()


def valid_text_map(icd_codes: set, rows: list) -> tuple:
    """(condition_text_map rows with valid, normalized ICD-10-CM codes, deduplicated; dropped invalid count)."""
    seen, out, dropped = set(), [], 0
    for r in rows:
        r = {**r, "code": normalize_icd10cm(r["code"])}
        if r["code"] not in icd_codes:
            dropped += 1
        elif (r["condition_norm"], r["code"]) not in seen:
            seen.add((r["condition_norm"], r["code"]))
            out.append(r)
    return sorted(out, key=lambda r: (r["condition_norm"], r["code"])), dropped


def build_reference(
    store, work_dir: str, get=http_get, today: date = None, now: datetime = None, umls_client=None, deadline=None
) -> dict:
    """umls_client: an umls.UmlsClient to add the UMLS layer; deadline (time.monotonic()) caps its lookups."""
    now = now or datetime.now(timezone.utc)
    version = now.strftime("%Y%m%dT%H%M%SZ")
    work = Path(work_dir) / "reference" / version
    shutil.rmtree(work, ignore_errors=True)
    icd_rows, icd_info = fetch_icd10cm(get, today or now.date())
    icd_codes = {r["code"] for r in icd_rows}
    overrides, rules = load_curated()
    bad_rules = [r["code"] for r in rules if normalize_icd10cm(r["code"]) not in icd_codes]
    if bad_rules:
        raise ValueError(
            f"condition_text_rules.csv has codes not in ICD-10-CM FY{icd_info['fiscal_year']}: {bad_rules}"
        )
    rules = [{**r, "code": normalize_icd10cm(r["code"])} for r in rules]
    mondo = parse_mondo_sssom(_read_text(get(MONDO_SSSOM_URL)))
    wikidata = fetch_wikidata(get)
    sources = {"curated": overrides, "mondo": mondo, "wikidata": wikidata}
    text_map, umls_info = [], {"skipped": "no UMLS API key"}
    if umls_client is not None:
        mesh_ids, texts = snapshot_lookup_inputs(store, work / "inputs")
        umls_maps, text_map, umls_info = umls.fetch_umls(umls_client, store, mesh_ids, texts, deadline, today)
        sources |= umls_maps
    code_map, dropped = combine_maps(icd_codes, sources)
    text_map, dropped["umls_text"] = valid_text_map(icd_codes, text_map)
    if umls_client is not None:
        umls_info |= {
            "rows": {s: len(sources[s]) for s in umls.SOURCES} | {"text": len(text_map)},
            "dropped_invalid": {s: dropped[s] for s in (*umls.SOURCES, "umls_text")},
        }
    if dropped["curated"]:
        raise ValueError(f"mesh_overrides.csv has {dropped['curated']} codes not in ICD-10-CM")

    tables = {
        "icd10cm_codes": icd_rows,
        "condition_code_map": code_map,
        "condition_text_map": text_map,
        "condition_text_rules": rules,
    }
    con = connect(work)
    keys = {}
    try:
        for name, rows in tables.items():
            path = work / name / "data.parquet"
            write_table(con, rows, SCHEMAS[name], path)
            keys[name] = f"{REFERENCE_PREFIX}{version}/{name}/data.parquet"
            store.upload(path, keys[name], "application/vnd.apache.parquet")
    finally:
        con.close()
    pointer = {
        "version": version,
        "created_at": now.isoformat(),
        "tables": keys,
        "row_counts": {n: len(r) for n, r in tables.items()},
        "sources": {
            "icd10cm": icd_info,
            "mondo": {"url": MONDO_SSSOM_URL, "rows": len(mondo), "dropped_invalid": dropped["mondo"]},
            "wikidata": {"url": WIKIDATA_SPARQL_URL, "rows": len(wikidata), "dropped_invalid": dropped["wikidata"]},
            "curated": {"mesh_overrides": len(overrides), "text_rules": len(rules)},
            "umls": umls_info,
        },
    }
    store.put_json(REFERENCE_CURRENT_KEY, pointer)
    _prune_versions(store, KEEP_VERSIONS)
    shutil.rmtree(work, ignore_errors=True)
    logger.info("Reference %s built: %s", version, pointer["row_counts"])
    return pointer


def download_reference(store, dest: Path) -> dict | None:
    """Download the current reference tables to dest/<table>/data.parquet; None if no reference exists yet."""
    pointer = store.get_json(REFERENCE_CURRENT_KEY)
    if pointer is None:
        return None
    for name, key in pointer["tables"].items():
        store.download(key, dest / name / "data.parquet")
    missing = [n for n in REFERENCE_TABLES if n not in pointer["tables"]]  # references built by older code
    if missing:
        con = connect(dest)
        try:
            for name in missing:
                write_table(con, [], SCHEMAS[name], dest / name / "data.parquet")
        finally:
            con.close()
    return pointer


def _prune_versions(store, keep: int) -> None:
    keys = [k for k in store.list_keys(REFERENCE_PREFIX) if k != REFERENCE_CURRENT_KEY]
    versions = sorted({k[len(REFERENCE_PREFIX) :].split("/", 1)[0] for k in keys})
    for old in versions[:-keep]:
        store.delete_prefix(f"{REFERENCE_PREFIX}{old}/")
