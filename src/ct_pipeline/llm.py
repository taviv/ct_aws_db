"""
LLM tier for condition coding (ROADMAP.md phase 1, step 5): protocol-aware ICD-10-CM coding via Amazon Bedrock.

For studies with no exact ICD-10-CM code after the curated, MeSH and UMLS steps, the model reads the protocol
(titles, conditions, interventions, summary, eligibility criteria) and selects ICD-10-CM categories from the
full category catalog, which is sent as a cached system prompt so it cannot invent codes. Each answer is
validated (the category must exist; the quoted evidence must appear in the protocol text) and cached per
study in the private data bucket under CACHE_KEY, keyed by a hash of the model input, so only new or changed
studies are sent again.
"""

import hashlib
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

PROMPT_VERSION = "v5"
CACHE_KEY = "reference/llm_cache.json"
MAX_SUMMARY_CHARS = 2000
MAX_ELIGIBILITY_CHARS = 8000
MAX_CODES = 12
CONFIDENCE = ("high", "medium", "low")
# categories that name no specific site; always low confidence, so published as broad
CATCH_ALL = ("C76", "C77", "C78", "C79", "C80", "C96")
WORKERS = 4
CHUNK = 50
TOOL_NAME = "record_icd10cm"

TARGETS_SQL = """
    SELECT nct_id FROM studies
    WHERE nct_id NOT IN (SELECT nct_id FROM study_condition_codes
                         WHERE code_system = 'ICD10CM' AND match_type = 'exact' AND via <> 'llm')
    ORDER BY nct_id
"""

INSTRUCTIONS = """You assign ICD-10-CM categories to clinical trial protocols.

Code the disease or condition of the population the study enrolls. Do not code interventions, outcomes,
adverse events, or conditions that are only exclusion criteria or comorbidities.
If the study treats a symptom or complication (e.g. nausea, pain, anxiety, cachexia, thrombocytopenia) in
patients with a disease, code that symptom or complication first; add the disease only if its site or type is named.
Use the protocol text to resolve vague condition names: for "advanced solid tumors", code the specific cancers
the eligibility criteria name; use C80 only if any solid tumor is eligible or no primary site is named, never
next to site categories just because a generic term like "Advanced Cancer" is listed. Use the most specific
category the text supports, at most {max_codes} categories, the main condition first.
Gastroesophageal (esophagogastric) junction cancer is C16 (cardia), not C15.
Use the secondary-site categories C77-C79 only when the study targets the metastases themselves (e.g. brain
metastases); "metastatic pancreatic cancer" is C25 alone.
For every category, quote verbatim the protocol text that supports it (copy it exactly; use "..." between
non-adjacent fragments).
Confidence: high = the condition is named explicitly in the text; medium = inferred from indirect wording
(e.g. histology, biomarker or the drug's indication); low = a generic fallback such as C80 or C96 from terms like
"advanced malignancies".
If the study enrolls only healthy volunteers, use Z00. If no category fits, return no codes and give the reason.
Choose only categories from this catalog and call {tool} exactly once.

ICD-10-CM categories:
{catalog}"""

TOOL = {
    "toolSpec": {
        "name": TOOL_NAME,
        "description": "Record the ICD-10-CM categories for the study's condition(s).",
        "inputSchema": {
            "json": {
                "type": "object",
                "properties": {
                    "codes": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "code": {"type": "string", "description": "ICD-10-CM category from the catalog"},
                                "evidence": {"type": "string", "description": "verbatim quote from the protocol"},
                                "confidence": {
                                    "type": "string",
                                    "enum": list(CONFIDENCE),
                                    "description": "high: named explicitly; medium: inferred; low: generic fallback",
                                },
                            },
                            "required": ["code", "evidence", "confidence"],
                        },
                    },
                    "no_code_reason": {"type": "string", "description": "why no category fits, if none"},
                },
                "required": ["codes"],
            }
        },
    }
}


class LlmError(RuntimeError):
    pass


def category_catalog(con, icd_path) -> tuple:
    """(catalog text grouped by chapter, {category: description}) from an icd10cm_codes parquet file."""
    rows = con.execute(f"""
        SELECT chapter, chapter_title, code, description FROM read_parquet('{icd_path}')
        WHERE level = 'category' ORDER BY chapter, code
    """).fetchall()
    lines, chapter, categories = [], None, {}
    for ch, ch_title, code, desc in rows:
        if ch != chapter:
            lines.append(f"# Chapter {ch}: {ch_title}")
            chapter = ch
        lines.append(f"{code} {desc}")
        categories[code] = desc
    return "\n".join(lines), categories


def study_documents(con, nct_ids=None) -> list:
    """Model input per study; needs studies, study_text, study_conditions, study_interventions views."""
    where = ""
    if nct_ids is not None:
        con.execute("CREATE OR REPLACE TEMP TABLE _llm_ids AS SELECT unnest(?::VARCHAR[]) AS nct_id", [list(nct_ids)])
        where = "WHERE s.nct_id IN (SELECT nct_id FROM _llm_ids)"
    rel = con.sql(f"""
        SELECT s.nct_id, s.brief_title, s.official_title,
               coalesce((SELECT list_sort(list_distinct(list(c.condition_name)))
                         FROM study_conditions c WHERE c.nct_id = s.nct_id), []) AS conditions,
               coalesce((SELECT list_sort(list_distinct(list(coalesce(i.intervention_type || ': ', '') || i.name)))
                         FROM study_interventions i WHERE i.nct_id = s.nct_id), []) AS interventions,
               t.brief_summary, t.detailed_description, t.eligibility_criteria
        FROM studies s LEFT JOIN study_text t USING (nct_id)
        {where}
        ORDER BY s.nct_id
    """)
    return [dict(zip(rel.columns, r, strict=True)) for r in rel.fetchall()]


def _truncate(text, limit: int) -> str:
    text = (text or "").strip()
    return text if len(text) <= limit else text[:limit].rstrip() + " [...]"


TEXT_FIELDS = {
    "brief_summary": ("Summary", MAX_SUMMARY_CHARS),
    "detailed_description": ("Detailed description", MAX_SUMMARY_CHARS),
    "eligibility_criteria": ("Eligibility criteria", MAX_ELIGIBILITY_CHARS),
}
DEFAULT_FIELDS = ("brief_summary", "detailed_description", "eligibility_criteria")


def render_study(doc: dict, fields=DEFAULT_FIELDS) -> str:
    parts = [f"Title: {doc.get('brief_title') or ''}"]
    if doc.get("official_title") and doc["official_title"] != doc.get("brief_title"):
        parts.append(f"Official title: {doc['official_title']}")
    parts.append("Conditions: " + "; ".join(doc.get("conditions") or []))
    if doc.get("interventions"):
        parts.append("Interventions: " + "; ".join(doc["interventions"]))
    for field in fields:
        heading, limit = TEXT_FIELDS[field]
        if doc.get(field):
            parts.append(f"{heading}:\n" + _truncate(doc[field], limit))
    return "\n\n".join(parts)


def input_hash(text: str, model_id: str) -> str:
    return hashlib.sha256(f"{PROMPT_VERSION}\n{model_id}\n{text}".encode()).hexdigest()[:16]


def _norm(text: str) -> str:
    """Letters and digits only: quotes survive re-spacing, dropped list numbers/bullets and punctuation changes."""
    text = re.sub(r"(?m)^\s*(?:[-*•]|\(?\d{1,2}[.)])\s+", " ", text)
    return re.sub(r"[\W_]+", "", text).lower()


def evidence_found(evidence: str, text: str) -> bool:
    haystack = _norm(text)
    fragments = [_norm(f) for f in re.split(r"\.\.\.|…|;", evidence or "")]
    fragments = [f for f in fragments if f]
    return bool(fragments) and all(f in haystack for f in fragments)


def validate(answer: dict, categories: dict, text: str) -> list:
    """Keep catalog categories (a full code like C34.1 is cut back to C34), deduplicated, at most MAX_CODES."""
    out, seen = [], set()
    for item in (answer or {}).get("codes") or []:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code") or "").strip().upper().split(" ")[0]
        if code not in categories:
            code = code.split(".")[0]
        if code not in categories or code in seen:
            continue
        seen.add(code)
        confidence = item.get("confidence") if item.get("confidence") in CONFIDENCE else "low"
        if code in CATCH_ALL:
            confidence = "low"
        evidence = str(item.get("evidence") or "")
        out.append(
            {
                "code": code,
                "confidence": confidence,
                "evidence": evidence,
                "evidence_found": evidence_found(evidence, text),
            }
        )
    return out[:MAX_CODES]


class BedrockCoder:
    """One Converse call per study, with the instructions + catalog as a cached system prompt and a forced tool call."""

    def __init__(self, model_id: str, catalog: str, client=None, max_tokens: int = 1024):
        if not model_id:
            raise ValueError("Bedrock model id is empty")
        self.model_id = model_id
        self.system = INSTRUCTIONS.format(max_codes=MAX_CODES, tool=TOOL_NAME, catalog=catalog)
        self.max_tokens = max_tokens
        self._client = client

    @property
    def client(self):
        if self._client is None:
            import boto3
            from botocore.config import Config

            self._client = boto3.client(
                "bedrock-runtime", config=Config(retries={"mode": "adaptive", "max_attempts": 10}, read_timeout=120)
            )
        return self._client

    def code(self, text: str) -> tuple:
        resp = self.client.converse(
            modelId=self.model_id,
            system=[{"text": self.system}, {"cachePoint": {"type": "default"}}],
            messages=[{"role": "user", "content": [{"text": text}]}],
            toolConfig={"tools": [TOOL], "toolChoice": {"tool": {"name": TOOL_NAME}}},
            inferenceConfig={"maxTokens": self.max_tokens, "temperature": 0},
        )
        usage = {k: v for k, v in (resp.get("usage") or {}).items() if isinstance(v, int)}
        for block in resp["output"]["message"]["content"]:
            if "toolUse" in block:
                return block["toolUse"].get("input") or {}, usage
        raise LlmError(f"No {TOOL_NAME} call in the response (stopReason={resp.get('stopReason')})")


def code_studies(coder, store, docs, categories, deadline=None, workers=WORKERS, progress=None, cache_key=CACHE_KEY):
    """
    Code `docs` (study_documents rows), reusing cached answers whose input hash matches. Stops starting new
    calls after `deadline` (time.monotonic()); the next run continues from the cache. Returns (entries, info).
    """
    cache = (store.get_json(cache_key) or {}).get("entries", {}) if cache_key else {}
    usage, errors = {}, {}
    stats = {"studies": len(docs), "cached": 0, "coded": 0, "failed": 0, "complete": True}
    jobs = []
    for doc in docs:
        text = render_study(doc)
        h = input_hash(text, coder.model_id)
        if cache.get(doc["nct_id"], {}).get("h") == h:
            stats["cached"] += 1
        else:
            jobs.append((doc["nct_id"], text, h))

    def run(job):
        nct_id, text, h = job
        try:
            answer, used = coder.code(text)
        except Exception as e:  # noqa: BLE001 - one bad study must not stop the batch
            return nct_id, None, None, e
        entry = {
            "h": h,
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "model": coder.model_id,
            "prompt": PROMPT_VERSION,
            "codes": validate(answer, categories, text),
            "no_code_reason": str(answer.get("no_code_reason") or "")[:500],
        }
        return nct_id, entry, used, None

    def save():
        if cache_key:
            store.put_json(cache_key, {"updated_at": datetime.now(timezone.utc).isoformat(), "entries": cache})

    try:
        with ThreadPoolExecutor(workers) as pool:
            for i in range(0, len(jobs), CHUNK):
                if deadline is not None and time.monotonic() > deadline:
                    stats["complete"] = False
                    break
                chunk_ok = 0
                for nct_id, entry, used, err in pool.map(run, jobs[i : i + CHUNK]):
                    if err is not None:
                        stats["failed"] += 1
                        kind = type(err).__name__
                        if hasattr(err, "response"):
                            kind = err.response.get("Error", {}).get("Code", kind)
                        errors[kind] = errors.get(kind, 0) + 1
                        last_error = err
                        continue
                    cache[nct_id] = entry
                    chunk_ok += 1
                    stats["coded"] += 1
                    for k, v in used.items():
                        usage[k] = usage.get(k, 0) + v
                if i == 0 and chunk_ok == 0:  # model access, quota or request problem
                    raise LlmError(f"All of the first {min(CHUNK, len(jobs))} Bedrock calls failed, e.g. {last_error}")
                save()
                if progress:
                    progress(min(i + CHUNK, len(jobs)), len(jobs))
    finally:
        if jobs:
            save()
    stats["complete"] = stats["complete"] and not errors
    entries = {d["nct_id"]: cache[d["nct_id"]] for d in docs if d["nct_id"] in cache}
    return entries, {**stats, "usage": usage, "errors": errors}


def llm_code_rows(cache_entries: dict, categories: dict) -> list:
    """condition_llm_codes rows: answers with verified evidence for categories still in the catalog."""
    rows = []
    for nct_id, e in sorted(cache_entries.items()):
        for c in e.get("codes") or []:
            if c.get("evidence_found") and c["code"] in categories:
                rows.append(
                    {
                        "nct_id": nct_id,
                        "code_system": "ICD10CM",
                        "code": c["code"],
                        "match_type": "broad" if c["confidence"] == "low" or c["code"] in CATCH_ALL else "exact",
                        "confidence": c["confidence"],
                        "evidence": c["evidence"][:1000],
                        "model": e.get("model"),
                        "prompt_version": e.get("prompt"),
                        "coded_at": e.get("at"),
                    }
                )
    return rows


def answer_json(entry: dict) -> str:
    return json.dumps({"codes": entry.get("codes"), "no_code_reason": entry.get("no_code_reason")}, ensure_ascii=False)
