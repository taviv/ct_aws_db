"""
UMLS layer for condition coding (needs a UMLS licence and API key; ROADMAP.md phase 1, step 3).

Adds to the reference build:
  source='umls'         MeSH descriptor → ICD-10-CM codes sharing its UMLS CUI (UTS crosswalk); exact
  source='umls_snomed'  MeSH → SNOMED CT (shared CUI) → NLM's SNOMED CT to ICD-10-CM map default target; broad
  condition_text_map    normalized free-text condition → the single UMLS concept it names → its ICD-10-CM atoms
                        (umls, exact) or, if it has none, its SNOMED CT atoms → NLM map (umls_snomed, broad)
Each key's codes are then collapsed to one code (reference.prepare_umls).

UMLS and SNOMED CT content is licensed: these rows and the lookup cache stay in the private data bucket.
UTS only accepts the API key as a query parameter, so request URLs are never logged or put in errors.
"""

import csv
import io
import json
import logging
import re
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote, urlencode

UTS = "https://uts-ws.nlm.nih.gov"
SNOMED_MAP_RELEASES = f"{UTS}/releases?releaseType=snomed-ct-to-icd-10-cm-mapping-resources&current=true"
CACHE_KEY = "reference/umls_cache.json"
CACHE_TTL_DAYS = 180
WORKERS = 5
RATE = 10  # requests/second across all workers; NLM allows at most 20 per IP and stops serving above that
CHUNK = 200
MIN_TEXT_COUNT = 2
DEFAULT_MAP_RULES = ("TRUE", "OTHERWISE TRUE")
SOURCES = ("umls", "umls_snomed")
ATOM_SOURCES = ("ICD10CM", "SNOMEDCT_US")

logger = logging.getLogger(__name__)
logging.getLogger("urllib3").setLevel(logging.ERROR)  # urllib3 retry warnings include the URL (and key)


class UmlsError(RuntimeError):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


def _obsolete(r: dict) -> bool:
    return str(r.get("obsolete", "false")).lower() == "true"


class UmlsClient:
    def __init__(self, api_key: str, get, rate: float = RATE):
        if not api_key:
            raise ValueError("UMLS API key is empty")
        self._key = api_key
        self._get = get
        self._rate = rate
        self._lock = threading.Lock()
        self._next = 0.0

    def _throttle(self):
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            self._next = start + 1 / self._rate
        if start > now:
            time.sleep(start - now)

    def _fetch(self, url: str, params: dict):
        self._throttle()
        try:
            return self._get(f"{url}?{urlencode({**params, 'apiKey': self._key})}")
        except Exception as e:
            m = re.search(r"HTTP (\d+)", str(e))
            status = int(m.group(1)) if m else None
            detail = f"HTTP {status}" if status else type(e).__name__
            target = f" targetSource={params['targetSource']}" if "targetSource" in params else ""
            raise UmlsError(f"UMLS request to {url.removeprefix(UTS)}{target} failed ({detail})", status) from None

    def _json(self, path: str, **params):
        data = self._fetch(UTS + path, params)
        return json.loads(data) if data else None

    def crosswalk(self, mesh_id: str, target: str) -> list:
        """[[code, name]] in `target` sharing a CUI with the MeSH descriptor."""
        d = self._json(f"/rest/crosswalk/current/source/MSH/{quote(mesh_id)}", targetSource=target, pageSize=200)
        return [[r["ui"], r.get("name")] for r in (d or {}).get("result") or [] if not _obsolete(r)]

    def search(self, text: str) -> list:
        """CUIs of the concepts whose normalized name is `text` (at most 5)."""
        d = self._json(
            "/rest/search/current", string=text, searchType="normalizedString", returnIdType="concept", pageSize=5
        )
        results = ((d or {}).get("result") or {}).get("results") or []
        return [r["ui"] for r in results if r.get("ui") not in (None, "NONE")]

    def atoms(self, cui: str) -> list:
        """[[source, code, name]] for the concept's ICD-10-CM and SNOMED CT atoms (name only for ICD-10-CM)."""
        d = self._json(f"/rest/content/current/CUI/{cui}/atoms", sabs=",".join(ATOM_SOURCES), pageSize=200)
        out = []
        for a in (d or {}).get("result") or []:
            *_, sab, code = a["code"].rstrip("/").split("/")
            atom = [sab, code, a.get("name") if sab == "ICD10CM" else None]
            if sab in ATOM_SOURCES and not _obsolete(a) and atom not in out:
                out.append(atom)
        return out

    def snomed_icd_map(self) -> tuple:
        """({snomed concept id: [icd10cm code, name]}, release version) from NLM's current map release."""
        release = json.loads(self._get(SNOMED_MAP_RELEASES))[0]
        data = self._fetch(f"{UTS}/download", {"url": release["downloadUrl"]})
        if not data:
            raise UmlsError(f"UMLS download of {release['fileName']} returned nothing")
        return parse_snomed_icd_map(data), release["releaseVersion"]


def parse_snomed_icd_map(zip_bytes: bytes) -> dict:
    """Default (unconditional) group-1 target per SNOMED CT concept from the SNOMED CT to ICD-10-CM map."""
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    names = [n for n in zf.namelist() if not n.endswith("/")]
    member = next((n for n in names if "humanreadable" in n.lower() and n.lower().endswith((".tsv", ".txt"))), None)
    member = member or next((n for n in names if "extendedmap" in n.lower() and "snapshot" in n.lower()), None)
    if member is None:
        raise ValueError(f"No SNOMED CT to ICD-10-CM map file in the release zip: {names[:20]}")
    reader = csv.DictReader(io.StringIO(zf.read(member).decode("utf-8-sig")), delimiter="\t", quoting=csv.QUOTE_NONE)
    missing = {"referencedComponentId", "mapGroup", "mapPriority", "mapRule", "mapTarget"} - set(
        reader.fieldnames or []
    )
    if missing:
        raise ValueError(f"{member} lacks columns {sorted(missing)}")
    best = {}
    for r in reader:
        if r.get("active", "1") != "1" or r["mapGroup"] != "1" or not r["mapTarget"].strip():
            continue
        if r["mapRule"].strip().upper() not in DEFAULT_MAP_RULES:
            continue
        priority, concept = int(r["mapPriority"]), r["referencedComponentId"]
        if concept not in best or priority < best[concept][0]:
            best[concept] = (priority, r["mapTarget"].strip(), r.get("mapTargetName"))
    return {k: [code, name] for k, (_, code, name) in best.items()}


def lookup_inputs(con) -> tuple:
    """(condition MeSH ids incl. ancestors, normalized condition strings used ≥ MIN_TEXT_COUNT times)."""
    mesh = con.execute(
        "SELECT DISTINCT mesh_id FROM condition_mesh_terms WHERE mesh_id IS NOT NULL ORDER BY 1"
    ).fetchall()
    texts = con.execute(f"""
        SELECT n FROM (SELECT trim(regexp_replace(lower(condition_name), '[^a-z0-9]+', ' ', 'g')) AS n
                       FROM study_conditions)
        WHERE n <> '' GROUP BY n HAVING count(*) >= {MIN_TEXT_COUNT} ORDER BY n
    """).fetchall()
    return [r[0] for r in mesh], [r[0] for r in texts]


def lookup(client, store, mesh_ids, texts, deadline=None, today=None, progress=None) -> tuple:
    """
    Crosswalk, search and atom results per lookup key, cached in CACHE_KEY for CACHE_TTL_DAYS. Stops starting
    new lookups after `deadline` (time.monotonic()); the next run continues from the cache.
    """
    today = today or date.today()
    stale = (today - timedelta(days=CACHE_TTL_DAYS)).isoformat()
    cache = (store.get_json(CACHE_KEY) or {}).get("entries", {})
    fresh = {k for k, v in cache.items() if v["at"] >= stale}
    keep, errors, stats = set(), {}, {"fetched": 0, "cached": 0, "complete": True}

    def run(job):
        key, (fn, *args) = job
        try:
            return key, fn(*args), None
        except UmlsError as e:
            return key, None, e

    def save(prune=False):
        entries = {k: v for k, v in cache.items() if k in keep} if prune else cache
        store.put_json(CACHE_KEY, {"updated_at": datetime.now(timezone.utc).isoformat(), "entries": entries})

    def run_jobs(pool, jobs):
        keep.update(jobs)
        todo = [k for k in jobs if k not in fresh]
        stats["cached"] += len(jobs) - len(todo)
        had_results = len(todo) < len(jobs)
        for i in range(0, len(todo), CHUNK):
            if deadline is not None and time.monotonic() > deadline:
                stats["complete"] = False
                return
            chunk = todo[i : i + CHUNK]
            failed = []
            for key, value, err in pool.map(run, [(k, jobs[k]) for k in chunk]):
                if err is None:
                    cache[key] = {"at": today.isoformat(), "v": value}
                    had_results = True
                else:  # not cached, so retried next run
                    kind = key.split(":")[1] if key.startswith("msh:") else key.split(":")[0]
                    errors.setdefault(kind, {}).setdefault(str(err.status or "error"), 0)
                    errors[kind][str(err.status or "error")] += 1
                    failed.append(err)
            if not had_results:  # key/licence/access problem
                raise UmlsError(f"All of the first {len(chunk)} UMLS lookups failed, e.g. {failed[0]}")
            if failed:
                logger.warning("%d UMLS lookups failed in this batch, e.g. %s", len(failed), failed[0])
            stats["fetched"] += len(chunk)
            if progress:
                progress(i + len(chunk), len(todo))
            if (i // CHUNK) % 5 == 4:
                save()

    try:
        with ThreadPoolExecutor(WORKERS) as pool:
            jobs = {f"msh:{t}:{m}": (client.crosswalk, m, t) for m in mesh_ids for t in ("ICD10CM", "SNOMEDCT_US")}
            run_jobs(pool, jobs | {f"search:{t}": (client.search, t) for t in texts})
            cuis = {c for t in texts for c in _single(cache.get(f"search:{t}"))}
            run_jobs(pool, {f"cui:{c}": (client.atoms, c) for c in sorted(cuis)})
    finally:
        save(prune=True)
    info = {"lookups": len(keep), **stats, "errors": errors}
    info["complete"] = stats["complete"] and not errors
    return {k: cache[k]["v"] for k in keep if k in cache}, info


def _single(entry) -> list:
    """[cui] if a cached search found exactly one concept, else []."""
    return entry["v"] if entry and len(entry["v"]) == 1 else []


def umls_rows(results: dict, snomed_map: dict, mesh_ids, texts) -> tuple:
    """(condition_code_map rows, condition_text_map rows) from lookup results; codes not yet validated."""
    rows = []
    for m in mesh_ids:
        for code, name in results.get(f"msh:ICD10CM:{m}") or []:
            rows.append(_row(mesh_id=m, code=code, label=name, match_type="exact", source="umls"))
        for concept, _ in results.get(f"msh:SNOMEDCT_US:{m}") or []:
            if concept in snomed_map:
                code, name = snomed_map[concept]
                rows.append(_row(mesh_id=m, code=code, label=name, match_type="broad", source="umls_snomed"))
    text_rows = []
    for t in texts:
        cuis = results.get(f"search:{t}") or []
        atoms = results.get(f"cui:{cuis[0]}") or [] if len(cuis) == 1 else []
        icd = [(code, name) for sab, code, name in atoms if sab == "ICD10CM"]
        for code, name in icd:
            text_rows.append(_row(condition_norm=t, code=code, label=name, match_type="exact", source="umls"))
        for concept in [] if icd else sorted({code for sab, code, _ in atoms if sab == "SNOMEDCT_US"}):
            if concept in snomed_map:
                code, name = snomed_map[concept]
                text_rows.append(
                    _row(condition_norm=t, code=code, label=name, match_type="broad", source="umls_snomed")
                )
    return rows, text_rows


def _row(**kw) -> dict:
    return {"code_system": "ICD10CM", **kw}


def fetch_umls(client, store, mesh_ids, texts, deadline=None, today=None, progress=None) -> tuple:
    """(condition_code_map rows by source, condition_text_map rows, info) for the given lookup inputs."""
    snomed_map, release = client.snomed_icd_map()
    results, info = lookup(client, store, mesh_ids, texts, deadline, today, progress)
    rows, text_rows = umls_rows(results, snomed_map, mesh_ids, texts)
    by_source = {s: [r for r in rows if r["source"] == s] for s in SOURCES}
    info |= {"snomed_icd10cm_map": release, "snomed_map_concepts": len(snomed_map)}
    if not info["complete"]:
        logger.warning("UMLS lookups incomplete (%s of %s fetched); the next run continues", info["fetched"], info)
    return by_source, text_rows, info
