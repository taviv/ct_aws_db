"""
Trial-match pass (cancer trial finder PoV): structured patient-matching fields for recruiting US cancer trials.

A second LLM pass next to condition coding (`llm.py`): for recruiting US-site interventional trials with a
malignant-neoplasm code (phase 2 and up by default), the model reads the protocol (titles, conditions,
interventions, summary, eligibility criteria) and reports the fields a doctor needs to tell whether a
patient might be eligible — disease, treatment setting, prior therapy, biomarkers, key exclusions and
patient scope — plus the ICD-10-CM category it would assign, as a cross-check against the coding tier.
Answers are cached per study in the private data bucket under CACHE_KEY, keyed by a hash of the model
input, so only new or changed studies are sent again. The run publishes TRIALS_KEY (data/trials.json)
to the site bucket for the dashboard "Cancer Trials" tab.
"""

import json
import logging
from datetime import datetime, timezone

from . import llm

logger = logging.getLogger(__name__)

PROMPT_VERSION = "v1"
CACHE_KEY = "reference/trial_match_cache.json"
TRIALS_KEY = "data/trials.json"
TOOL_NAME = "record_trial_match"
DEFAULT_PHASES = ("PHASE2", "PHASE2/3", "PHASE3")
SETTINGS = (
    "neoadjuvant",
    "adjuvant",
    "first_line",
    "relapsed_refractory",
    "maintenance",
    "supportive_care",
    "prevention",
    "other",
)
MAX_BIOMARKERS = 8
MAX_EXCLUSIONS = 6
MAX_STR = 240
MAX_FIELD_STR = 120
STATUSES = ("RECRUITING", "ENROLLING_BY_INVITATION")

# recruiting interventional trials with a US site and a malignant-neoplasm (C*) ICD-10-CM code
TARGETS_SQL = """
    SELECT s.nct_id FROM studies s
    WHERE s.study_type = 'INTERVENTIONAL'
      AND s.overall_status IN ('RECRUITING', 'ENROLLING_BY_INVITATION')
      AND list_contains(?::VARCHAR[], s.phase_group)
      AND EXISTS (SELECT 1 FROM study_locations l
                  WHERE l.nct_id = s.nct_id AND l.country = 'United States')
      AND EXISTS (SELECT 1 FROM study_condition_codes c
                  WHERE c.nct_id = s.nct_id AND c.code_system = 'ICD10CM' AND c.code LIKE 'C%')
    ORDER BY s.nct_id
"""

LOCATIONS_SQL = """
    SELECT nct_id,
           list_sort(list_distinct(list(state)))   AS states,
           list_sort(list_distinct(list(city)))    AS cities,
           count(*)                                AS sites
    FROM study_locations
    WHERE country = 'United States' AND nct_id IN (SELECT unnest(?::VARCHAR[]))
    GROUP BY 1
"""

INSTRUCTIONS = """You extract matching fields from a cancer clinical trial protocol so that a doctor can
tell whether a patient might be eligible.

Report these fields from the protocol text:
- disease: the cancer being studied — one short phrase with site, histology and stage when stated
  (e.g. "metastatic NSCLC", "relapsed/refractory DLBCL", "high-risk early breast cancer").
- icd10: the ICD-10-CM category code from the catalog that best names the disease; "" if none fits.
- setting: exactly one of: neoadjuvant, adjuvant, first_line (untreated advanced/metastatic disease),
  relapsed_refractory, maintenance, supportive_care, prevention, other.
- prior_therapy: prior treatment patients must or must not have had, in a few words
  (e.g. ">=1 prior platinum line; no prior anti-PD-1", "treatment-naive"). "" if the text says nothing.
- biomarkers: biomarkers, mutations or expression levels that are required or used for
  stratification, each a short phrase (e.g. "EGFR exon 19 deletion", "HER2+", "PD-L1 TPS >= 50%",
  "MRD positive"); [] when none.
- key_exclusions: up to {max_exclusions} short phrases for the exclusions most likely to rule a
  patient out (e.g. "active brain metastases", "ECOG >= 2", "prior checkpoint inhibitor");
  [] if none are notable.
- patient_scope: sex and age range in a few words (e.g. "adults >=18, all sexes", "females only");
  "" if standard adults.
- confidence: high = fields come straight from the text; medium = partly inferred; low = sparse text.

Be terse — these fields feed a search table, not prose. Call {tool} exactly once.

ICD-10-CM categories:
{catalog}"""

TOOL = {
    "toolSpec": {
        "name": TOOL_NAME,
        "description": "Record the patient-matching fields for a recruiting cancer trial.",
        "inputSchema": {
            "json": {
                "type": "object",
                "properties": {
                    "disease": {"type": "string", "description": "cancer type, site/histology/stage"},
                    "icd10": {"type": "string", "description": "best ICD-10-CM category from the catalog, or ''"},
                    "setting": {"type": "string", "enum": list(SETTINGS)},
                    "prior_therapy": {"type": "string", "description": "required/excluded prior treatment"},
                    "biomarkers": {"type": "array", "items": {"type": "string"}},
                    "key_exclusions": {"type": "array", "items": {"type": "string"}},
                    "patient_scope": {"type": "string", "description": "sex/age limits"},
                    "confidence": {
                        "type": "string",
                        "enum": list(llm.CONFIDENCE),
                        "description": "high: explicit; medium: partly inferred; low: sparse text",
                    },
                },
                "required": ["disease", "setting", "biomarkers", "key_exclusions", "confidence"],
            }
        },
    }
}


def make_coder(model: str, catalog: str) -> llm.BedrockCoder:
    """BedrockCoder with the trial-match instructions + tool (same cached-system-prompt pattern)."""
    system = INSTRUCTIONS.format(max_exclusions=MAX_EXCLUSIONS, tool=TOOL_NAME, catalog=catalog)
    return llm.BedrockCoder(model, catalog, system=system, tool=TOOL, tool_name=TOOL_NAME, max_tokens=2048)


def _short(value, limit: int) -> str:
    return str(value or "").strip()[:limit]


def _str_list(value, limit: int) -> list:
    out = []
    for item in value or []:
        item = _short(item, MAX_FIELD_STR)
        if item and item not in out:
            out.append(item)
    return out[:limit]


def validate_match(answer: dict, categories: dict) -> dict:
    """Clean a record_trial_match answer into the stored shape; icd10 is checked against the catalog."""
    a = answer or {}
    setting = str(a.get("setting") or "").strip().lower()
    code = str(a.get("icd10") or "").strip().upper().split(" ")[0]
    if code not in categories:
        code = code.split(".")[0]
    return {
        "disease": _short(a.get("disease"), MAX_STR),
        "icd10": code if code in categories else "",
        "setting": setting if setting in SETTINGS else "other",
        "prior_therapy": _short(a.get("prior_therapy"), MAX_STR),
        "biomarkers": _str_list(a.get("biomarkers"), MAX_BIOMARKERS),
        "key_exclusions": _str_list(a.get("key_exclusions"), MAX_EXCLUSIONS),
        "patient_scope": _short(a.get("patient_scope"), MAX_STR),
        "confidence": a.get("confidence") if a.get("confidence") in llm.CONFIDENCE else "low",
    }


def build_entry(categories: dict, model: str):
    """code_studies build_entry callback: validate the match answer and store it under 'match'."""

    def _entry(answer: dict, h: str, _text: str) -> dict:
        return {
            "h": h,
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "model": model,
            "prompt": PROMPT_VERSION,
            "match": validate_match(answer, categories),
        }

    return _entry


def targets(con, phases) -> list:
    """NCT IDs of recruiting US-site interventional trials with a malignant-neoplasm code, in `phases`."""
    return [r[0] for r in con.execute(TARGETS_SQL, [list(phases)]).fetchall()]


def us_locations(con, nct_ids) -> dict:
    """{nct_id: {states, cities, sites}} for US locations (the deterministic part of the finder)."""
    out = {}
    for nct_id, states, cities, sites in con.execute(LOCATIONS_SQL, [list(nct_ids)]).fetchall():
        out[nct_id] = {
            "states": [s for s in states if s],
            "cities": [c for c in cities if c],
            "sites": sites,
        }
    return out


def trials_payload(current: dict, docs: list, entries: dict, locs: dict, categories: dict, model: str) -> dict:
    """data/trials.json: one row per answered trial, deterministic fields joined to the match output."""
    trials = []
    for d in docs:
        entry = entries.get(d["nct_id"])
        if entry is None:
            continue
        m = entry.get("match") or {}
        code = m.get("icd10") or ""
        loc = locs.get(d["nct_id"], {})
        trials.append(
            {
                "nct_id": d["nct_id"],
                "title": d["brief_title"],
                "phase": d.get("phase_group") or "",
                "status": d.get("overall_status") or "",
                "disease": m.get("disease", ""),
                "icd10": code,
                "icd10_label": categories.get(code, ""),
                "setting": m.get("setting", ""),
                "prior_therapy": m.get("prior_therapy", ""),
                "biomarkers": m.get("biomarkers", []),
                "key_exclusions": m.get("key_exclusions", []),
                "patient_scope": m.get("patient_scope", ""),
                "confidence": m.get("confidence", ""),
                "conditions": d.get("conditions") or [],
                "interventions": d.get("interventions") or [],
                "states": loc.get("states", []),
                "cities": loc.get("cities", []),
                "sites": loc.get("sites", 0),
            }
        )
    return {
        "run_id": (current or {}).get("run_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model": model,
        "prompt": PROMPT_VERSION,
        "count": len(trials),
        "trials": trials,
    }


def write_review(path, docs: list, entries: dict) -> int:
    """One row per trial for hand-checking the extraction; fill your_check (ok|wrong|partial)."""
    import csv

    n = 0
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "nct_id",
                "phase",
                "title",
                "conditions",
                "disease",
                "icd10",
                "setting",
                "prior_therapy",
                "biomarkers",
                "key_exclusions",
                "patient_scope",
                "confidence",
                "your_check",
            ]
        )
        for d in docs:
            e = entries.get(d["nct_id"])
            if e is None:
                continue
            m = e.get("match") or {}
            w.writerow(
                [
                    d["nct_id"],
                    d.get("phase_group") or "",
                    d["brief_title"],
                    "; ".join(d.get("conditions") or []),
                    m.get("disease", ""),
                    m.get("icd10", ""),
                    m.get("setting", ""),
                    m.get("prior_therapy", ""),
                    "; ".join(m.get("biomarkers") or []),
                    "; ".join(m.get("key_exclusions") or []),
                    m.get("patient_scope", ""),
                    m.get("confidence", ""),
                    "",
                ]
            )
            n += 1
    return n


def match_json(entry: dict) -> str:
    return json.dumps(entry.get("match") or {}, ensure_ascii=False)
