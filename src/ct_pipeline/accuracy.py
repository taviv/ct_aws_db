"""
Coding-accuracy experiment (`make coding-accuracy`): precision and recall of the published ICD-10-CM codes
against hand labels, per ICD-10-CM chapter, for Phase 3 studies (healthy-volunteer and vaccine trials excluded,
as in xeval).

1. sample:  per chapter, `per_chapter` studies drawn uniformly from the studies that belong to it. A study
            belongs to a chapter when any published code (any via, exact or broad) or a condition/title keyword
            points to it, so studies the pipeline missed can still be drawn. The xeval random sample is reused
            first and kept as an unstratified "random" stratum for overall estimates.
2. code:    each LLM arm (a Bedrock model) codes every sampled study into its own cache; answers already cached
            for the same model and prompt (eval or production cache) are copied, not re-asked.
3. sheet:   predictions.json freezes what every system predicts; labels.csv lists, per study, the union of the
            candidate categories (published codes + every arm) for hand labels, without saying who proposed them.
4. score:   per chapter and system: code-level precision/recall/F1 with study-clustered bootstrap intervals,
            primary-diagnosis hit rate, coverage, error types and a go / fix / develop call.
"""

import csv
import hashlib
import math
import random
import re
from datetime import datetime, timezone

from . import llm, xeval

PREFIX = "analysis/coding_accuracy/"
SAMPLE_KEY = PREFIX + "sample.json"
PREDICTIONS_KEY = PREFIX + "predictions.json"
RESULTS_KEY = PREFIX + "results.json"
REPORT_KEY = PREFIX + "report.md"

# the 8 largest ICD-10-CM chapters by Phase 3 volume (overview "ICD-10-CM chapters" chart)
CHAPTERS = {
    2: "Neoplasms",
    4: "Endocrine and metabolic",
    6: "Nervous system",
    9: "Circulatory",
    1: "Infectious and parasitic",
    10: "Respiratory",
    11: "Digestive",
    14: "Genitourinary",
}
# condition/title keywords that also put a study in a chapter (recall safety net, not a coder)
KEYWORDS = {
    1: r"\bhiv\b|hepatitis [bc]|infect|covid|sars-cov|tubercul|malaria|sepsis|bacter|viral|fungal|influenza",
    2: r"cancer|carcinom|tumou?r|neoplas|lymphoma|leuka?emia|myeloma|sarcoma|melanoma|glioma|blastoma|malignan",
    4: r"diabet|obes|thyroid|lipid|cholesterol|metabolic|hypoglyc|growth hormone",
    6: r"alzheimer|parkinson|epilep|seizure|migraine|multiple sclerosis|neuropath|dementia|myasthenia",
    9: r"heart|cardi|hypertens|atrial|coronary|stroke|thrombo|embolism|aneurysm|vascular",
    10: r"asthma|copd|pulmonary|pneumon|bronch|respiratory|cystic fibrosis",
    11: r"crohn|colitis|cirrhosis|liver|gastr|bowel|pancreatitis|esophag|hepatic|steatohepatitis",
    14: r"kidney|renal|bladder|urinary|nephr|endometriosis|prostatic hyperplasia|infertility",
}
YES = {"y", "yes", "1", "true", "x"}
# supplementary categories (infectious agent B95-B97, drug poisoning/adverse effect T36-T50, external causes V-Y,
# status/history Z codes) are left out of the sheet and of scoring; Z00/Z23 stay as the healthy-population codes
SUPPLEMENTARY = re.compile(r"B9[5-7]|T(3[6-9]|4[0-9]|50)|[V-Y]..|Z(?!00|23)..")


def scored(code: str) -> bool:
    return not SUPPLEMENTARY.fullmatch(code[:3])


ERROR_TYPES = {
    "1": "wrong MeSH term at CT.gov",
    "2": "wrong crosswalk row",
    "3": "LLM: right chapter, wrong category",
    "4": "LLM over-coded (comorbidity/exclusion)",
    "5": "LLM: wrong chapter",
    "6": "basket-trial type missed",
    "7": "quote rejected (correct code dropped)",
    "8": "too broad (catch-all/ancestor only)",
}
FIXABLE = {"1", "2", "7"}
# measured tokens per study (Sonnet 4.6 smoke test, 2 studies): input, output, cache read, cache write
TOKENS_PER_STUDY = (1900, 165, 27400, 0)
SHEET_COLUMNS = [
    "nct_id",
    "chapters",
    "blind",
    "second_review",
    "title",
    "conditions",
    "url",
    "code",
    "code_label",
    "evidence",
    "correct",
    "primary",
    "error_type",
    "missing_codes",
    "notes",
]


def _rank(seed, key: str) -> str:
    return hashlib.sha256(f"{seed}:{key}".encode()).hexdigest()


def chapter_of(con, icd_path) -> dict:
    """ICD-10-CM category -> chapter number."""
    return dict(
        con.execute(f"SELECT code, chapter FROM read_parquet('{icd_path}') WHERE level = 'category'").fetchall()
    )


def memberships(con, phases, chapters_by_code: dict, chapters=CHAPTERS) -> dict:
    """nct_id -> chapters it belongs to (published code in the chapter or keyword hit), over the eligible pool."""
    pool = [
        r[0]
        for r in con.execute(
            f"WITH pool AS ({xeval._POOL}) SELECT nct_id FROM pool WHERE NOT healthy AND NOT vaccine", [list(phases)]
        ).fetchall()
    ]
    out = {n: set() for n in pool}
    for nct_id, cat in con.execute(
        "SELECT DISTINCT nct_id, icd10_category FROM study_condition_codes"
        " WHERE code_system = 'ICD10CM' AND icd10_category IS NOT NULL"
    ).fetchall():
        ch = chapters_by_code.get(cat)
        if nct_id in out and ch in chapters:
            out[nct_id].add(ch)
    patterns = {ch: re.compile(KEYWORDS[ch]) for ch in chapters if ch in KEYWORDS}
    for nct_id, text in con.execute("""
        SELECT s.nct_id, lower(concat_ws(' | ', s.brief_title, string_agg(c.condition_name, ' | ')))
        FROM studies s LEFT JOIN study_conditions c USING (nct_id) GROUP BY s.nct_id, s.brief_title
    """).fetchall():
        if nct_id in out:
            out[nct_id] |= {ch for ch, p in patterns.items() if p.search(text or "")}
    return out


def draw(members: dict, seed, per_chapter: int, reuse=(), chapters=CHAPTERS) -> dict:
    """chapter -> sampled nct_ids: reused (already random) studies first, then a seeded random order."""
    reuse = [n for n in reuse if n in members]
    reused = set(reuse)
    order = reuse + sorted((n for n in members if n not in reused), key=lambda n: _rank(seed, n))
    return {ch: [n for n in order if ch in members[n]][:per_chapter] for ch in chapters}


def sample_payload(current, phases, seed, per_chapter, members, picks, random_ids, blind_share, second_share):
    ids = sorted({n for ids in picks.values() for n in ids} | set(random_ids))
    ranked = sorted(ids, key=lambda n: _rank(f"{seed}:review", n))
    n_blind = round(len(ids) * blind_share)
    n_second = round(len(ids) * second_share)
    return {
        "kind": "coding_accuracy_sample",
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "snapshot_run_id": current.get("run_id"),
        "reference_version": current.get("reference_version"),
        "phases": list(phases),
        "seed": str(seed),
        "per_chapter": per_chapter,
        "chapters": {str(ch): {"name": CHAPTERS.get(ch, str(ch)), "ids": picks[ch]} for ch in picks},
        "pool": {str(ch): sum(1 for m in members.values() if ch in m) for ch in picks},
        "pool_total": len(members),
        "random": list(random_ids),
        "blind": sorted(ranked[:n_blind]),
        "second": sorted(ranked[n_blind : n_blind + n_second]),
        "ids": ids,
    }


def strata(sample: dict) -> dict:
    """nct_id -> stratum names (chapter names, plus 'Random sample')."""
    out = {}
    for ch in sample["chapters"].values():
        for n in ch["ids"]:
            out.setdefault(n, []).append(ch["name"])
    for n in sample["random"]:
        out.setdefault(n, []).append("Random sample")
    return out


def seed_cache(arm_cache: dict, other_caches, docs, model: str) -> int:
    """Copy answers for `model` from other caches when their input hash matches; returns how many were copied."""
    copied = 0
    for d in docs:
        h = llm.input_hash(llm.render_study(d), model)
        if arm_cache.get(d["nct_id"], {}).get("h") == h:
            continue
        for cache in other_caches:
            e = cache.get(d["nct_id"])
            if e and e.get("h") == h:
                arm_cache[d["nct_id"]] = e
                copied += 1
                break
    return copied


def arm_key(model: str) -> str:
    return PREFIX + "llm_cache_" + re.sub(r"[^A-Za-z0-9.-]+", "_", model) + ".json"


def published_rows(con, ids) -> dict:
    out = {}
    for nct_id, cat, via, match_type in con.execute(
        """
        SELECT DISTINCT nct_id, icd10_category, via, match_type FROM study_condition_codes
        WHERE code_system = 'ICD10CM' AND icd10_category IS NOT NULL AND nct_id IN (SELECT unnest(?::VARCHAR[]))
        ORDER BY ALL
        """,
        [list(ids)],
    ).fetchall():
        out.setdefault(nct_id, []).append({"code": cat, "via": via, "match_type": match_type})
    return out


def predictions(sample: dict, docs, published: dict, arms: dict) -> dict:
    """Frozen per-study predictions: published rows and every arm's validated answer."""
    groups = strata(sample)
    blind, second = set(sample["blind"]), set(sample["second"])
    studies = []
    for d in docs:
        n = d["nct_id"]
        studies.append(
            {
                "nct_id": n,
                "title": d.get("brief_title"),
                "conditions": list(d.get("conditions") or []),
                "strata": groups.get(n, []),
                "blind": n in blind,
                "second": n in second,
                "published": published.get(n, []),
                "arms": {
                    m: [
                        {k: c.get(k) for k in ("code", "confidence", "evidence", "evidence_found")}
                        for c in (entries.get(n) or {}).get("codes") or []
                    ]
                    for m, entries in arms.items()
                },
            }
        )
    return {
        "kind": "coding_accuracy_predictions",
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "snapshot_run_id": sample.get("snapshot_run_id"),
        "models": list(arms),
        "prompt_version": llm.PROMPT_VERSION,
        "studies": studies,
    }


def candidates(study: dict) -> dict:
    """category -> LLM quotes, for every category any system predicts for the study."""
    out = {r["code"]: [] for r in study["published"] if scored(r["code"])}
    for codes in study["arms"].values():
        for c in codes:
            if c.get("evidence_found") and scored(c["code"]):
                out.setdefault(c["code"], [])
                if c.get("evidence") and c["evidence"] not in out[c["code"]]:
                    out[c["code"]].append(c["evidence"])
    return out


def sheet_rows(preds: dict, categories: dict, per_chapter_limit=None) -> list:
    """labels.csv rows: one per candidate (none for blind studies) plus one 'add missing' row per study."""
    studies = preds["studies"]
    if per_chapter_limit:
        keep, seen = set(), {}
        for s in studies:
            for g in s["strata"]:
                if seen.get(g, 0) < per_chapter_limit and s["nct_id"] not in keep:
                    seen[g] = seen.get(g, 0) + 1
                    keep.add(s["nct_id"])
        studies = [s for s in studies if s["nct_id"] in keep]
    rows = []
    for s in studies:
        base = {
            "nct_id": s["nct_id"],
            "chapters": "; ".join(s["strata"]),
            "blind": "1" if s["blind"] else "",
            "second_review": "1" if s["second"] else "",
            "title": s["title"],
            "conditions": "; ".join(s["conditions"]),
            "url": f"https://clinicaltrials.gov/study/{s['nct_id']}",
        }
        if not s["blind"]:
            for code, quotes in sorted(candidates(s).items()):
                rows.append(
                    base | {"code": code, "code_label": categories.get(code, ""), "evidence": " | ".join(quotes)}
                )
        rows.append(base | {"code": "", "code_label": "(add missing codes; prefix the main diagnosis with *)"})
    return [{k: r.get(k, "") for k in SHEET_COLUMNS} for r in rows]


def write_sheet(path, rows) -> None:
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SHEET_COLUMNS)
        w.writeheader()
        w.writerows(rows)


def _cats(text: str) -> list:
    """'*C34.1, e11' -> [('C34', True), ('E11', False)]."""
    out = []
    for tok in re.split(r"[\s,;]+", text or ""):
        primary = tok.startswith("*")
        code = tok.lstrip("*").strip().upper()[:3]
        if re.fullmatch(r"[A-Z][0-9][0-9A-Z]", code):
            out.append((code, primary))
    return out


def read_labels(path) -> dict:
    """nct_id -> {'rows': {code: row}, 'missing': [(code, primary)], 'notes'}."""
    out = {}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            s = out.setdefault(r["nct_id"].strip(), {"rows": {}, "missing": []})
            code = (r.get("code") or "").strip().upper()
            if code:
                s["rows"][code] = r
            s["missing"] += _cats(r.get("missing_codes"))
    return out


def gold(study: dict, labels: dict | None):
    """(gold categories, primary categories, error types of false positives) or None if not fully labeled."""
    if labels is None:
        return None
    cands = set() if study["blind"] else set(candidates(study))
    rows = labels["rows"]
    if any((rows.get(c, {}).get("correct") or "").strip() == "" for c in cands):
        return None
    g = {c for c in cands if rows[c]["correct"].strip().lower() in YES}
    p = {c for c in cands if (rows[c].get("primary") or "").strip().lower() in YES}
    for code, primary in labels["missing"]:
        if not scored(code):
            continue
        g.add(code)
        if primary:
            p.add(code)
    errors = [
        (rows[c].get("error_type") or "").strip()
        for c in cands
        if c not in g and (rows[c].get("error_type") or "").strip()
    ]
    return g, p, errors


def _publishable(c: dict) -> bool:
    return bool(c.get("evidence_found")) and c["code"] not in llm.CATCH_ALL and c.get("confidence") != "low"


def systems(study: dict, models, cascade=None) -> dict:
    """system -> (exact categories, exact+broad categories) for one study."""
    pub = study["published"]
    rules = [r for r in pub if r["via"] != "llm"]
    rules_exact = {r["code"] for r in rules if r["match_type"] == "exact"}
    rules_all = {r["code"] for r in rules}
    out = {
        "published": ({r["code"] for r in pub if r["match_type"] == "exact"}, {r["code"] for r in pub}),
        "mesh_only": (
            {r["code"] for r in pub if r["via"] == "mesh" and r["match_type"] == "exact"},
            {r["code"] for r in pub if r["via"] == "mesh"},
        ),
        "rules_only": (rules_exact, rules_all),
    }
    answers = dict(study["arms"])
    if cascade:
        first, second = cascade
        strong = any(c.get("evidence_found") and c.get("confidence") == "high" for c in answers.get(first) or [])
        answers["cascade"] = answers.get(first) if strong else answers.get(second)
        models = [*models, "cascade"]
    for m in models:
        codes = answers.get(m) or []
        exact = {c["code"] for c in codes if _publishable(c)}
        verified = {c["code"] for c in codes if c.get("evidence_found")}
        out[f"llm_only:{m}"] = (exact, verified)
        # the LLM tier only codes studies without an exact rules code, and never replaces one
        target = not rules_exact
        out[f"combined:{m}"] = (rules_exact | (exact if target else set()), rules_all | (verified if target else set()))
    return {k: tuple({c for c in v if scored(c)} for v in pair) for k, pair in out.items()}


def _ratio(a, b):
    return a / b if b else None


def _stats(units) -> dict:
    tp = sum(u[0] for u in units)
    fp = sum(u[1] for u in units)
    fn = sum(u[2] for u in units)
    p, r = _ratio(tp, tp + fp), _ratio(tp, tp + fn)
    f1 = 2 * p * r / (p + r) if p and r else None
    return {"tp": tp, "fp": fp, "fn": fn, "precision": p, "recall": r, "f1": f1}


def _bootstrap(units, key, reps, rng):
    vals = []
    for _ in range(reps):
        v = _stats([units[rng.randrange(len(units))] for _ in units])[key]
        if v is not None:
            vals.append(v)
    if not vals:
        return None
    vals.sort()
    return [vals[int(0.025 * (len(vals) - 1))], vals[int(0.975 * (len(vals) - 1))]]


def mcnemar(b: int, c: int) -> float | None:
    """Exact two-sided McNemar p-value from the discordant counts."""
    n = b + c
    if n == 0:
        return None
    k = min(b, c)
    return min(1.0, 2 * sum(math.comb(n, i) for i in range(k + 1)) / 2**n)


def kappa(pairs) -> float | None:
    """Cohen's kappa for (label_a, label_b) boolean pairs."""
    pairs = list(pairs)
    n = len(pairs)
    if not n:
        return None
    po = sum(a == b for a, b in pairs) / n
    pa, pb = sum(a for a, _ in pairs) / n, sum(b for _, b in pairs) / n
    pe = pa * pb + (1 - pa) * (1 - pb)
    return None if pe == 1 else (po - pe) / (1 - pe)


def decide(m: dict, thresholds: dict, errors: list) -> str:
    """'go' / 'fix' / 'develop' for one chapter's metrics (exact view)."""
    p, r, hit = m["precision"], m["recall"], m["primary_hit"]
    lo = (m.get("precision_ci") or [None])[0]
    meets = all(v is not None for v in (p, r, hit))
    meets = meets and p >= thresholds["precision"] and r >= thresholds["recall"] and hit >= thresholds["primary"]
    if meets and lo is not None and lo >= thresholds["precision"]:
        return "go"
    fixable = errors and sum(e in FIXABLE for e in errors) > len(errors) / 2
    return "fix" if meets or fixable else "develop"


def score(preds: dict, labels: dict, cascade=None, reps=1000, seed=1, thresholds=None, second=None) -> dict:
    thresholds = thresholds or {"precision": 0.90, "recall": 0.85, "primary": 0.90}
    models = preds["models"]
    rng = random.Random(seed)
    scored, skipped = [], []
    for s in preds["studies"]:
        g = gold(s, labels.get(s["nct_id"]))
        (scored.append((s, *g)) if g else skipped.append(s["nct_id"]))
    names = list(systems(preds["studies"][0], models, cascade)) if preds["studies"] else []
    groups = {}
    for s, *_ in scored:
        for st in s["strata"]:
            groups.setdefault(st, []).append(s["nct_id"])
    order = [*CHAPTERS.values(), "Random sample"]
    groups = {g: groups[g] for g in sorted(groups, key=lambda g: order.index(g) if g in order else len(order))}
    groups["All sampled"] = [s["nct_id"] for s, *_ in scored]
    by_id = {s["nct_id"]: (s, g, p, e) for s, g, p, e in scored}
    preds_by = {n: systems(s, models, cascade) for n, (s, *_) in by_id.items()}

    results = {}
    for group, ids in groups.items():
        errors = [e for n in ids for e in by_id[n][3]]
        results[group] = {
            "studies": len(ids),
            "errors": {t: errors.count(t) for t in sorted(set(errors))},
            "systems": {},
        }
        for name in names:
            for view, idx in (("exact", 0), ("all", 1)):
                units, hits, prim, covered, full = [], 0, 0, 0, 0
                for n in ids:
                    _, g, p, _ = by_id[n]
                    pred = preds_by[n][name][idx]
                    units.append((len(pred & g), len(pred - g), len(g - pred)))
                    covered += bool(pred)
                    full += pred == g
                    if p:
                        prim += 1
                        hits += bool(pred & p)
                m = _stats(units)
                m |= {
                    "precision_ci": _bootstrap(units, "precision", reps, rng) if units else None,
                    "recall_ci": _bootstrap(units, "recall", reps, rng) if units else None,
                    "primary_hit": _ratio(hits, prim),
                    "primary_studies": prim,
                    "coverage": _ratio(covered, len(ids)),
                    "fully_correct": _ratio(full, len(ids)),
                }
                if view == "exact":
                    m["decision"] = decide(m, thresholds, errors)
                results[group]["systems"].setdefault(name, {})[view] = m

    # paired comparison of primary-diagnosis hits, each combined arm vs the published codes
    paired = {}
    for name in names:
        if not name.startswith("combined:"):
            continue
        b = c = 0
        for n, (_, _, p, _) in by_id.items():
            if not p:
                continue
            x, y = bool(preds_by[n][name][0] & p), bool(preds_by[n]["published"][0] & p)
            b += x and not y
            c += y and not x
        paired[name] = {"better": b, "worse": c, "p_value": mcnemar(b, c)}

    blind = [n for n, (s, *_) in by_id.items() if s["blind"]]
    open_ = [n for n, (s, *_) in by_id.items() if not s["blind"]]

    def recall(ids):
        return _stats(
            [
                (len(preds_by[n]["published"][0] & by_id[n][1]), 0, len(by_id[n][1] - preds_by[n]["published"][0]))
                for n in ids
            ]
        )["recall"]

    agreement = None
    if second:
        pairs = []
        for n, lab in second.items():
            first = labels.get(n)
            if not first:
                continue
            for code, row in lab["rows"].items():
                a, b2 = (first["rows"].get(code) or {}).get("correct", "").strip(), (row.get("correct") or "").strip()
                if a and b2:
                    pairs.append((a.lower() in YES, b2.lower() in YES))
        agreement = {"pairs": len(pairs), "kappa": kappa(pairs)}

    return {
        "kind": "coding_accuracy_results",
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "snapshot_run_id": preds.get("snapshot_run_id"),
        "models": models,
        "cascade": list(cascade) if cascade else None,
        "thresholds": thresholds,
        "labeled": len(scored),
        "unlabeled": len(skipped),
        "groups": results,
        "paired_primary_vs_published": paired,
        "anchoring": {
            "blind_studies": len(blind),
            "published_recall_blind": recall(blind) if blind else None,
            "published_recall_open": recall(open_) if open_ else None,
        },
        "agreement": agreement,
    }


def _pct(v):
    return "–" if v is None else f"{100 * v:.0f}%"


def _ci(ci):
    return "" if not ci else f" ({100 * ci[0]:.0f}–{100 * ci[1]:.0f})"


def report(res: dict) -> str:
    lines = [
        "# Coding accuracy by ICD-10-CM chapter",
        "",
        f"Snapshot {res['snapshot_run_id']}; models {', '.join(res['models'])}"
        + (f"; cascade {' → '.join(res['cascade'])}" if res["cascade"] else ""),
        f"Labeled studies scored: {res['labeled']} ({res['unlabeled']} not fully labeled, skipped).",
        "Precision/recall are code level (ICD-10-CM categories), exact codes only, with 95% study-clustered "
        "bootstrap intervals. Thresholds for 'go': precision lower bound ≥ "
        f"{_pct(res['thresholds']['precision'])}, recall ≥ {_pct(res['thresholds']['recall'])}, primary hit ≥ "
        f"{_pct(res['thresholds']['primary'])}.",
        "",
    ]
    for group, g in res["groups"].items():
        lines += [
            f"## {group} ({g['studies']} studies)",
            "",
            "| System | Precision | Recall | F1 | Primary hit | Coverage | Call |",
            "|---|---|---|---|---|---|---|",
        ]
        for name, v in g["systems"].items():
            m = v["exact"]
            lines.append(
                f"| {name} | {_pct(m['precision'])}{_ci(m['precision_ci'])} | {_pct(m['recall'])}{_ci(m['recall_ci'])}"
                f" | {_pct(m['f1'])} | {_pct(m['primary_hit'])} | {_pct(m['coverage'])} | {m['decision']} |"
            )
        if g["errors"]:
            lines += ["", "Error types: " + ", ".join(f"{ERROR_TYPES.get(t, t)} {n}" for t, n in g["errors"].items())]
        lines.append("")
    if res["paired_primary_vs_published"]:
        lines += ["## Primary-diagnosis hits vs published (paired, McNemar)", ""]
        for name, v in res["paired_primary_vs_published"].items():
            p = "–" if v["p_value"] is None else f"{v['p_value']:.3f}"
            lines.append(f"- {name}: better on {v['better']}, worse on {v['worse']} studies, p = {p}")
        lines.append("")
    a = res["anchoring"]
    lines.append(
        f"Anchoring check: published recall {_pct(a['published_recall_blind'])} on {a['blind_studies']} blind-labeled "
        f"studies vs {_pct(a['published_recall_open'])} on the rest."
    )
    if res["agreement"]:
        k = res["agreement"]["kappa"]
        lines.append(
            f"Second reviewer: {res['agreement']['pairs']} candidate labels, Cohen's kappa "
            + ("–" if k is None else f"{k:.2f}")
        )
    return "\n".join(lines) + "\n"
