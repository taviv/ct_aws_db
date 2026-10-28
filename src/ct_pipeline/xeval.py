"""
Cross-evaluation of deterministic vs LLM condition coding (dashboard "Coding eval" tab).

A random sample of Phase-3 studies — healthy-volunteer and vaccine trials excluded — is coded by
the protocol-text LLM and compared to the deterministic codes already in study_condition_codes
(via <> 'llm': MeSH/UMLS crosswalks, curated text rules, UMLS text matches, MeSH-ancestor fallback).
The result is written to the site bucket as data/eval.json; hand labels from the review CSV merge
back into it (--adjudicate).
"""

import csv
from datetime import datetime, timezone

from . import llm

EVAL_KEY = "data/eval.json"

# The population the experiment excludes, mirroring the build's healthy/vaccine handling:
# healthy_volunteers=true covers Z00 candidates; the '%vaccin%' pattern mirrors the via='intervention' Z23 rule.
_POOL = """
    SELECT s.nct_id,
           coalesce(s.healthy_volunteers, false) AS healthy,
           lower(s.brief_title) LIKE '%vaccin%'
           OR EXISTS (SELECT 1 FROM study_interventions i
                      WHERE i.nct_id = s.nct_id
                        AND lower(concat_ws(' ', i.name, i.description)) LIKE '%vaccin%') AS vaccine
    FROM studies s
    WHERE list_contains(?::VARCHAR[], s.phase_group)
"""

POOL_SQL = f"""
    WITH pool AS ({_POOL})
    SELECT COUNT(*) AS total,
           COUNT(*) FILTER (WHERE healthy) AS healthy,
           COUNT(*) FILTER (WHERE vaccine AND NOT healthy) AS vaccine,
           COUNT(*) FILTER (WHERE NOT healthy AND NOT vaccine) AS eligible
    FROM pool
"""

SAMPLE_SQL = f"""
    WITH pool AS ({_POOL})
    SELECT nct_id FROM pool
    WHERE NOT healthy AND NOT vaccine
    ORDER BY hash(nct_id || ?) LIMIT ?
"""

CLASSIC_SQL = """
    SELECT nct_id, code, label, icd10_category, via, match_type, source
    FROM study_condition_codes
    WHERE code_system = 'ICD10CM' AND via <> 'llm'
      AND nct_id IN (SELECT unnest(?::VARCHAR[]))
    ORDER BY nct_id, code, via
"""

VERDICTS = ("agree", "chapter_only", "disagree", "classic_only", "llm_only", "neither")
CHECKS = ("llm", "classic", "both", "neither")


def pool_counts(con, phases) -> dict:
    row = con.execute(POOL_SQL, [list(phases)]).fetchone()
    return dict(zip(("total", "healthy", "vaccine", "eligible"), row, strict=True))


def sample_ids(con, phases, seed, limit) -> list:
    """Random Phase-3 sample without healthy-volunteer or vaccine trials; deterministic per seed."""
    return [r[0] for r in con.execute(SAMPLE_SQL, [list(phases), str(seed), limit]).fetchall()]


def classic_codes(con, ids) -> dict:
    """nct_id -> deterministic code rows (via <> 'llm'), as returned by CLASSIC_SQL."""
    rows = con.execute(CLASSIC_SQL, [list(ids)]).fetchall()
    out = {}
    for nct_id, code, label, category, via, match_type, source in rows:
        out.setdefault(nct_id, []).append(
            {
                "code": code,
                "cat": category or code,
                "label": label,
                "via": via,
                "match_type": match_type,
                "source": source,
            }
        )
    return out


def _chapter(code: str, chapters: dict):
    return chapters.get(code)


def compare_study(doc: dict, classic: list, entry: dict | None, chapters: dict) -> dict:
    """One eval row: deterministic codes vs the LLM's verified answers, plus a verdict."""
    classic = classic or []
    classic_any = {r["cat"] for r in classic}
    classic_exact = {r["cat"] for r in classic if r["match_type"] == "exact"}
    llm_rows = []
    verified, publishable = set(), set()
    for c in (entry or {}).get("codes") or []:
        found = bool(c.get("evidence_found"))
        llm_rows.append(
            {
                "code": c["code"],
                "confidence": c.get("confidence"),
                "evidence": c.get("evidence"),
                "found": found,
            }
        )
        if found:
            verified.add(c["code"])
            if c["code"] not in llm.CATCH_ALL and c.get("confidence") != "low":
                publishable.add(c["code"])

    if classic_any and verified:
        if verified & classic_any:
            verdict = "agree"
        elif {_chapter(c, chapters) for c in verified} & {_chapter(c, chapters) for c in classic_any}:
            verdict = "chapter_only"
        else:
            verdict = "disagree"
    elif classic_any:
        verdict = "classic_only"
    elif verified:
        verdict = "llm_only"
    else:
        verdict = "neither"

    return {
        "nct_id": doc["nct_id"],
        "title": doc.get("brief_title"),
        "conditions": doc.get("conditions") or [],
        "classic": classic,
        "llm": llm_rows,
        "no_code_reason": (entry or {}).get("no_code_reason") or "",
        "verdict": verdict,
        "check": None,
        # derived sets for metrics (not shipped to the dashboard)
        "_classic_any": classic_any,
        "_classic_exact": classic_exact,
        "_verified": verified,
        "_llm_exact": publishable,
    }


def compare(docs: list, classic: dict, entries: dict, chapters: dict) -> list:
    return [compare_study(d, classic.get(d["nct_id"]), entries.get(d["nct_id"]), chapters) for d in docs]


def _macro(pairs) -> float | None:
    """Mean of |intersection| / |reference| over (predicted, reference) sets; None if no pairs."""
    vals = [len(p & r) / len(r) for p, r in pairs if r]
    return sum(vals) / len(vals) if vals else None


def metrics(studies: list) -> dict:
    n = len(studies)
    verdicts = {v: sum(1 for s in studies if s["verdict"] == v) for v in VERDICTS}
    coded_classic = [s for s in studies if s["_classic_any"]]
    exact_classic = [s for s in studies if s["_classic_exact"]]
    coded_llm = [s for s in studies if s["_verified"]]
    exact_llm = [s for s in studies if s["_llm_exact"]]
    both = [s for s in studies if s["_classic_any"] and s["_verified"]]
    pairs = [(s["_verified"], s["_classic_exact"]) for s in studies]
    llm_where_not_exact = [s for s in studies if s["_llm_exact"] and not s["_classic_exact"]]
    # calibration: share of verified LLM codes that hit any / the exact deterministic code of the same study
    by_conf = {}
    for s in studies:
        for c in s["llm"]:
            if not c["found"]:
                continue
            b = by_conf.setdefault(c["confidence"], {"codes": 0, "match_any": 0, "match_exact": 0})
            b["codes"] += 1
            b["match_any"] += c["code"] in s["_classic_any"]
            b["match_exact"] += c["code"] in s["_classic_exact"]
    return {
        "studies": n,
        "classic_coded": len(coded_classic),
        "classic_exact": len(exact_classic),
        "llm_verified": len(coded_llm),
        "llm_exact": len(exact_llm),
        "both_coded": len(both),
        "verdicts": verdicts,
        "agree_exact": sum(1 for s in both if s["_verified"] & s["_classic_exact"]),
        "recall_vs_exact": _macro(pairs),
        "precision_vs_exact": _macro([(r, p) for p, r in pairs]),
        "llm_yield_where_not_exact": len(llm_where_not_exact),
        "llm_yield_broad_only": sum(1 for s in llm_where_not_exact if s["_classic_any"]),
        "llm_yield_uncoded": sum(1 for s in llm_where_not_exact if not s["_classic_any"]),
        "avg_codes_classic_exact": sum(len(s["_classic_exact"]) for s in studies) / max(n, 1),
        "avg_codes_llm": sum(len(s["_verified"]) for s in studies) / max(n, 1),
        "by_confidence": by_conf,
    }


def _public(s: dict) -> dict:
    return {k: v for k, v in s.items() if not k.startswith("_")}


def eval_payload(current: dict, phases, seed, model: str, studies: list, pool: dict) -> dict:
    return {
        "kind": "coding_eval",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_run_id": current.get("run_id"),
        "reference_version": current.get("reference_version"),
        "phase": list(phases),
        "seed": str(seed),
        "model": model,
        "prompt_version": llm.PROMPT_VERSION,
        "pool": pool,
        "headline": metrics(studies),
        "adjudicated": None,
        "studies": [_public(s) for s in studies],
    }


def write_review(path, studies: list) -> int:
    """Review CSV for hand-labeling disagreements: one row per study that is not a plain 'agree'."""
    rows = [s for s in studies if s["verdict"] != "agree"]
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["nct_id", "title", "conditions", "classic_codes", "llm_codes", "evidence", "verdict", "your_check"])
        for s in rows:
            w.writerow(
                [
                    s["nct_id"],
                    s["title"],
                    "; ".join(s["conditions"]),
                    " ".join(f"{r['code']}:{r['via']}" for r in s["classic"]) or "-",
                    " ".join(f"{c['code']}:{c['confidence']}{'' if c['found'] else ':unverified'}" for c in s["llm"])
                    or "-",
                    " | ".join(c["evidence"] or "" for c in s["llm"]),
                    s["verdict"],
                    "",
                ]
            )
    return len(rows)


def merge_adjudication(payload: dict, path) -> dict:
    """Fold hand labels ('llm'|'classic'|'both'|'neither' in your_check) into an eval payload."""
    checks = {}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            label = (r.get("your_check") or "").strip().lower()
            if label:
                if label not in CHECKS:
                    raise ValueError(f"{path}: {r.get('nct_id')}: your_check={label!r}; use one of {', '.join(CHECKS)}")
                checks[r["nct_id"]] = label
    by_id = {s["nct_id"]: s for s in payload["studies"]}
    unknown = sorted(set(checks) - set(by_id))
    if unknown:
        raise ValueError(f"{path}: not in the sample: {', '.join(unknown)}")
    for nct_id, label in checks.items():
        by_id[nct_id]["check"] = label
    counts = {c: sum(1 for v in checks.values() if v == c) for c in CHECKS}
    payload["adjudicated"] = {
        "reviewed": len(checks),
        **counts,
        "llm_wins_share": counts["llm"] / len(checks) if checks else None,
        "classic_wins_share": counts["classic"] / len(checks) if checks else None,
    }
    return payload
