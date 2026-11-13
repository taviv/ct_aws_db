"""
LLM-first ICD-10-CM codes for recruiting Phase 3 trials with a US or Canadian site (rough draft for patient trial
search). Every target study is coded by the model, whether or not the rules tier already has a code; a study keeps
the model's publishable codes and falls back to the rules' exact codes only when the model has none, as the
`llm_first:<model>` arm of the coding-accuracy experiment. Supplementary categories are left out, as in scoring.
Everything goes to analysis/recruiting/ in the data bucket, never to reference/ or curated/.
"""

import csv
import io
import re

from . import accuracy, llm

PREFIX = "analysis/recruiting/"
COUNTRIES = ("United States", "Canada")
PHASES = ("PHASE3", "PHASE2/PHASE3")
STATUSES = ("RECRUITING",)

TARGETS_SQL = """
    SELECT s.nct_id FROM studies s
    WHERE list_contains(?::VARCHAR[], s.overall_status) AND list_contains(?::VARCHAR[], s.phase_group)
      AND EXISTS (SELECT 1 FROM study_locations l
                  WHERE l.nct_id = s.nct_id AND list_contains(?::VARCHAR[], l.country))
    ORDER BY s.nct_id
"""
LOCATIONS_SQL = """
    SELECT nct_id, list_sort(list_distinct(list(country))) AS countries,
           list_sort(list_distinct(list(state))) AS states, count(*) AS sites
    FROM study_locations
    WHERE list_contains(?::VARCHAR[], country) AND nct_id IN (SELECT unnest(?::VARCHAR[]))
    GROUP BY 1
"""
COLUMNS = [
    "nct_id",
    "title",
    "phase",
    "conditions",
    "countries",
    "states",
    "sites",
    "code",
    "label",
    "source",
    "confidence",
    "evidence",
    "url",
]


def targets(con, statuses=STATUSES, phases=PHASES, countries=COUNTRIES) -> list:
    return [r[0] for r in con.execute(TARGETS_SQL, [list(statuses), list(phases), list(countries)]).fetchall()]


def locations(con, nct_ids, countries=COUNTRIES) -> dict:
    rows = con.execute(LOCATIONS_SQL, [list(countries), list(nct_ids)]).fetchall()
    return {n: {"countries": c, "states": [s for s in st if s], "sites": k} for n, c, st, k in rows}


def cache_key(model: str) -> str:
    return PREFIX + "llm_cache_" + re.sub(r"[^A-Za-z0-9.-]+", "_", model) + ".json"


def codes_key(model: str) -> str:
    return PREFIX + "codes_" + re.sub(r"[^A-Za-z0-9.-]+", "_", model) + ".csv"


def study_codes(entry: dict | None, published: list) -> list:
    """[(code, source, confidence, evidence)]: the model's publishable codes, else the rules' exact codes."""
    llm_codes = [c for c in (entry or {}).get("codes") or [] if accuracy._publishable(c)]
    if llm_codes:
        picked = [(c["code"], "llm", c["confidence"], c["evidence"]) for c in llm_codes]
    else:
        rules = {r["code"] for r in published if r["via"] != "llm" and r["match_type"] == "exact"}
        picked = [(code, "rules", "", "") for code in sorted(rules)]
    return [p for p in picked if accuracy.scored(p[0])]


def code_rows(docs, entries: dict, published: dict, categories: dict, sites: dict) -> list:
    """One CSV row per (study, code); a study with no code gets one row with an empty code."""
    rows = []
    for d in docs:
        n = d["nct_id"]
        loc = sites.get(n, {})
        base = {
            "nct_id": n,
            "title": d["brief_title"],
            "phase": d["phase_group"],
            "conditions": "; ".join(d["conditions"]),
            "countries": "; ".join(loc.get("countries") or []),
            "states": "; ".join(loc.get("states") or []),
            "sites": loc.get("sites", 0),
            "url": f"https://clinicaltrials.gov/study/{n}",
        }
        picked = study_codes(entries.get(n), published.get(n, []))
        for code, source, confidence, evidence in picked or [("", "", "", "")]:
            rows.append(
                base
                | {
                    "code": code,
                    "label": categories.get(code, ""),
                    "source": source,
                    "confidence": confidence,
                    "evidence": evidence,
                }
            )
    return rows


def to_csv(rows) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, COLUMNS)
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def todo(docs, cache: dict, model: str) -> int:
    return sum(1 for d in docs if cache.get(d["nct_id"], {}).get("h") != llm.input_hash(llm.render_study(d), model))
