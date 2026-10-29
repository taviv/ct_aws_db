#!/usr/bin/env python3
"""
Trial duration vs condition (ICD-10-CM chapter): `make duration-experiments`
(or `python scripts/duration_experiments.py --data ~/local/data --out duration_report`).

Read-only. Needs lifelines (installed into .analysis-venv by the Makefile target).

1. Censoring-aware durations: Kaplan-Meier median start->completion per chapter, where ongoing trials
   are censored at the snapshot date (UNKNOWN-status trials at their last verification), next to the
   naive "completed trials only" median, which is biased short for a cohort that starts in 2016.
2. Adjusted effect: Weibull AFT (time ratios) and Cox (hazard ratios) of chapter membership, adjusted for
   phase, enrollment, sites, countries, sponsor class, randomisation, masking, purpose, start year.
3. Decomposition for completed trials: start->primary completion, primary->final completion,
   enrollment per site-month, and the longest primary-outcome time frame.

A study with codes in several chapters counts in each one (multi-hot in the models).
"""

import argparse
import json
import math
import sys
import tempfile
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ct_pipeline.build import attach_snapshot  # noqa: E402
from ct_pipeline.config import CURRENT_KEY  # noqa: E402
from ct_pipeline.db import connect  # noqa: E402
from ct_pipeline.queries import PHASE_GROUP_LABELS  # noqa: E402
from ct_pipeline.storage import LocalStore, S3Store, open_store  # noqa: E402

TABLES = ("studies", "study_condition_codes", "study_locations", "study_outcomes")
CHAPTERS = {
    1: "Infectious",
    2: "Neoplasms",
    3: "Blood/immune",
    4: "Endocrine/metabolic",
    5: "Mental",
    6: "Nervous system",
    7: "Eye",
    8: "Ear",
    9: "Circulatory",
    10: "Respiratory",
    11: "Digestive",
    12: "Skin",
    13: "Musculoskeletal",
    14: "Genitourinary",
    15: "Pregnancy",
    16: "Perinatal",
    17: "Congenital & genetic",
    18: "Symptoms/signs",
    19: "Injury/poisoning",
    20: "External causes",
    21: "Health status/Z codes",
    22: "Special purposes (COVID)",
}
ENDED = ("COMPLETED", "TERMINATED")
ONGOING = ("RECRUITING", "ACTIVE_NOT_RECRUITING", "ENROLLING_BY_INVITATION", "NOT_YET_RECRUITING", "SUSPENDED")
UNIT_DAYS = {"day": 1, "week": 7, "month": 30.44, "year": 365.25}

# one row per started interventional trial: observed time (days) and whether it ended
COHORT_SQL = """
CREATE TEMP TABLE cohort AS
WITH s AS (
    SELECT *, try_strptime(status_verified_date::VARCHAR, ['%Y-%m-%d', '%Y-%m'])::DATE AS verified
    FROM studies
    WHERE study_type = 'INTERVENTIONAL' AND start_date_type = 'ACTUAL' AND start_date <= ?::DATE
      AND overall_status NOT IN ('WITHDRAWN')
),
sites AS (
    SELECT nct_id, count(*) AS sites, count(DISTINCT country) AS countries FROM study_locations GROUP BY 1
)
SELECT s.nct_id, s.phase_group, s.overall_status, s.start_date, year(s.start_date) AS start_year,
       s.enrollment_count, s.lead_sponsor_class, s.allocation, s.masking, s.primary_purpose,
       s.healthy_volunteers, coalesce(sites.sites, 0) AS sites, coalesce(sites.countries, 0) AS countries,
       s.primary_completion_date, s.primary_completion_date_type, s.completion_date,
       CASE WHEN s.overall_status IN {ended} AND s.completion_date_type = 'ACTUAL' THEN 1 ELSE 0 END AS event,
       CASE
         WHEN s.overall_status IN {ended} AND s.completion_date_type = 'ACTUAL' THEN s.start_end
         WHEN s.overall_status IN {ongoing} THEN date_diff('day', s.start_date, ?::DATE)
         ELSE date_diff('day', s.start_date, least(coalesce(s.verified, ?::DATE), ?::DATE))
       END AS t
FROM s LEFT JOIN sites USING (nct_id)
""".format(ended=str(ENDED), ongoing=str(ONGOING))

CHAPTER_SQL = """
CREATE TEMP TABLE study_chapter AS
SELECT DISTINCT nct_id, icd10_chapter AS chapter FROM study_condition_codes
WHERE code_system = 'ICD10CM' AND icd10_chapter IS NOT NULL
"""


DASHBOARD_KEY = "data/duration_by_condition.json"


def stack_output(stack: str, key: str) -> str:
    import boto3

    outputs = boto3.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]["Outputs"]
    return next(o["OutputValue"] for o in outputs if o["OutputKey"] == key)


def open_data(args):
    if args.data:
        return open_store(args.data)
    return S3Store(stack_output(args.stack, "DataBucketName"))


def open_site(args):
    """Site store for the dashboard payload: --site, else <data>/../site locally, else the stack's SiteBucket."""
    if args.site:
        return open_store(args.site)
    if args.data:
        site = Path(args.data).parent / "site"
        return LocalStore(site) if not str(args.data).startswith("s3") and site.exists() else None
    return S3Store(stack_output(args.stack, "SiteBucketName"))


def dashboard_payload(results, chapters) -> dict:
    """Per-chapter KM medians and adjusted time ratios for the Study Duration tab."""
    km = {r["group"]: r for r in results["km"]}
    rows = []
    for c in chapters:
        label = CHAPTERS.get(c, str(c))
        if label in km:
            m = results["models"].get(f"ch_{c}", {})
            rows.append(
                {
                    "chapter": c,
                    "label": label,
                    **km[label],
                    **{k: m.get(k) for k in ("time_ratio", "tr_low", "tr_high")},
                }
            )
    keep = ("run_id", "as_of", "generated_at", "trials", "phases", "fit")
    return {**{k: results[k] for k in keep}, "overall": km.get("All trials"), "chapters": rows}


def frame_days(text: str) -> float | None:
    """Longest time span named in an outcome time frame ('Baseline up to Week 24' -> 168)."""
    import re

    best = None
    t = (text or "").lower()
    for num, unit in re.findall(r"(\d+(?:\.\d+)?)\s*-?\s*(day|week|month|year)s?\b", t) + [
        (n, u) for u, n in re.findall(r"\b(day|week|month|year)s?\s*(\d+(?:\.\d+)?)", t)
    ]:
        d = float(num) * UNIT_DAYS[unit]
        if d < 365.25 * 30:
            best = d if best is None else max(best, d)
    return best


def km_table(df, chapters, min_n):
    from lifelines import KaplanMeierFitter
    from lifelines.utils import median_survival_times

    rows = []
    groups = [("All trials", df)] + [(CHAPTERS.get(c, str(c)), df[df[f"ch_{c}"] == 1]) for c in chapters]
    for name, g in groups:
        if len(g) < min_n:
            continue
        km = KaplanMeierFitter().fit(g["t"], g["event"])
        ci = median_survival_times(km.confidence_interval_)
        done = g[(g["event"] == 1) & (g["overall_status"] == "COMPLETED")]
        rows.append(
            {
                "group": name,
                "trials": len(g),
                "ended": int(g["event"].sum()),
                "km_median_days": _num(km.median_survival_time_),
                "km_ci_low": _num(ci.iloc[0, 0]),
                "km_ci_high": _num(ci.iloc[0, 1]),
                "completed_only_median_days": _num(done["t"].median()),
                "pct_ended_by_3y": round(100 * (1 - float(km.predict(1095))), 1),
            }
        )
    rows.sort(key=lambda r: -(r["km_median_days"] or 1e9) if r["group"] != "All trials" else -1e12)
    return rows


def _num(x):
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return None
    return round(float(x))


def design(df, chapters):
    import numpy as np
    import pandas as pd

    X = pd.DataFrame(index=df.index)
    for c in chapters:
        X[f"ch_{c}"] = df[f"ch_{c}"]
    for p in sorted(df["phase_group"].dropna().unique()):
        if p != "PHASE2":
            X[f"phase_{p}"] = (df["phase_group"] == p).astype(int)
    X["log_enrollment"] = np.log1p(df["enrollment_count"].fillna(0).clip(lower=0))
    X["log_sites"] = np.log1p(df["sites"])
    X["multi_country"] = (df["countries"] > 1).astype(int)
    X["sponsor_industry"] = (df["lead_sponsor_class"] == "INDUSTRY").astype(int)
    X["sponsor_gov"] = df["lead_sponsor_class"].isin(["NIH", "FED", "OTHER_GOV"]).astype(int)
    X["randomized"] = (df["allocation"] == "RANDOMIZED").astype(int)
    X["masked"] = (~df["masking"].isin(["NONE"]) & df["masking"].notna()).astype(int)
    X["purpose_prevention"] = (df["primary_purpose"] == "PREVENTION").astype(int)
    X["purpose_other"] = (~df["primary_purpose"].isin(["TREATMENT", "PREVENTION"])).astype(int)
    X["healthy_volunteers"] = df["healthy_volunteers"].fillna(False).astype(int)
    X["start_year"] = df["start_year"] - 2016
    X = X.loc[:, X.std() > 0]
    X["t"] = df["t"].clip(lower=1)
    X["event"] = df["event"]
    return X


def fit_models(X):
    from lifelines import CoxPHFitter, WeibullAFTFitter

    aft = WeibullAFTFitter(penalizer=0.001).fit(X, "t", "event")
    cox = CoxPHFitter(penalizer=0.001).fit(X, "t", "event")
    out = {}
    s_aft = aft.summary.loc["lambda_"]
    s_cox = cox.summary
    for var in [c for c in X.columns if c not in ("t", "event")]:
        out[var] = {
            "time_ratio": round(float(s_aft.loc[var, "exp(coef)"]), 3),
            "tr_low": round(float(s_aft.loc[var, "exp(coef) lower 95%"]), 3),
            "tr_high": round(float(s_aft.loc[var, "exp(coef) upper 95%"]), 3),
            "hazard_ratio": round(float(s_cox.loc[var, "exp(coef)"]), 3),
            "p": float(s_cox.loc[var, "p"]),
        }
    return out, {
        "aft_concordance": round(aft.concordance_index_, 3),
        "cox_concordance": round(cox.concordance_index_, 3),
    }


def decomposition(con, df, chapters, min_n):
    import pandas as pd

    frames = con.execute(
        "SELECT nct_id, time_frame FROM study_outcomes WHERE lower(outcome_type) = 'primary' AND nct_id IN "
        "(SELECT nct_id FROM cohort WHERE event = 1 AND overall_status = 'COMPLETED')"
    ).fetchall()
    longest = {}
    for nct, tf in frames:
        d = frame_days(tf)
        if d is not None:
            longest[nct] = max(d, longest.get(nct, 0))
    done = df[(df["event"] == 1) & (df["overall_status"] == "COMPLETED")].copy()
    done = done[done["primary_completion_date_type"] == "ACTUAL"]
    pc = pd.to_datetime(done["primary_completion_date"])
    st = pd.to_datetime(done["start_date"])
    co = pd.to_datetime(done["completion_date"])
    done["to_primary"] = (pc - st).dt.days
    done["primary_to_final"] = (co - pc).dt.days
    months = (done["to_primary"].clip(lower=30)) / 30.44
    done["pts_per_site_month"] = done["enrollment_count"] / done["sites"].clip(lower=1) / months
    done["primary_frame_days"] = done["nct_id"].map(longest)
    rows = []
    groups = [("All trials", done)] + [(CHAPTERS.get(c, str(c)), done[done[f"ch_{c}"] == 1]) for c in chapters]
    for name, g in groups:
        if len(g) < min_n:
            continue
        rows.append(
            {
                "group": name,
                "completed": len(g),
                "median_total_days": _num(g["t"].median()),
                "median_to_primary_days": _num(g["to_primary"].median()),
                "median_primary_to_final_days": _num(g["primary_to_final"].median()),
                "median_enrollment": _num(g["enrollment_count"].median()),
                "median_sites": _num(g["sites"].median()),
                "median_pts_per_site_month": round(float(g["pts_per_site_month"].median()), 2),
                "median_primary_frame_days": _num(g["primary_frame_days"].median()),
            }
        )
    rows.sort(key=lambda r: -r["median_total_days"] if r["group"] != "All trials" else -1e12)
    return rows


def md_table(rows, cols):
    head = "| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n"
    return head + "".join(
        "| " + " | ".join("" if r.get(c) is None else str(r.get(c)) for c in cols) + " |\n" for r in rows
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stack", default="ct-pipeline")
    ap.add_argument("--data", help="local data dir instead of the stack's bucket")
    ap.add_argument("--phase", help="comma-separated phase_group values (default: all in the snapshot)")
    ap.add_argument("--as-of", help="censoring date YYYY-MM-DD (default: snapshot date)")
    ap.add_argument("--min-n", type=int, default=300, help="smallest chapter to report")
    ap.add_argument("--out", default="duration_report", help="output directory (report.md + results.json)")
    ap.add_argument("--site", help=f"site store for {DASHBOARD_KEY} (default: the stack's SiteBucket)")
    ap.add_argument("--no-publish", action="store_true", help=f"don't write {DASHBOARD_KEY}")
    args = ap.parse_args()

    store = open_data(args)
    current = store.get_json(CURRENT_KEY)
    if current is None:
        sys.exit("No published snapshot yet (curated/CURRENT.json)")
    as_of = args.as_of or current["created_at"][:10]
    with tempfile.TemporaryDirectory(prefix="ct-duration-") as tmp:
        work = Path(tmp)
        for table in TABLES:
            store.download(current["tables"][table], work / "snapshot" / table / "data.parquet")
        con = connect(work)
        attach_snapshot(con, work / "snapshot", list(TABLES))
        con.execute(COHORT_SQL, [as_of, as_of, as_of, as_of])
        if args.phase:
            phases = [p.strip().upper() for p in args.phase.split(",") if p.strip()]
            con.execute("DELETE FROM cohort WHERE NOT list_contains(?::VARCHAR[], phase_group)", [phases])
        con.execute("DELETE FROM cohort WHERE t IS NULL OR t <= 0")
        con.execute(CHAPTER_SQL)
        counts = dict(
            con.execute(
                "SELECT chapter, count(*) FROM study_chapter WHERE nct_id IN (SELECT nct_id FROM cohort) GROUP BY 1"
            ).fetchall()
        )
        chapters = sorted(c for c, n in counts.items() if n >= args.min_n)
        pivot = ", ".join(f"max(CASE WHEN c.chapter = {c} THEN 1 ELSE 0 END) AS ch_{c}" for c in chapters)
        df = con.execute(
            f"SELECT k.*, {pivot}, max(CASE WHEN c.chapter IS NULL THEN 0 ELSE 1 END) AS coded "
            "FROM cohort k LEFT JOIN study_chapter c USING (nct_id) GROUP BY ALL"
        ).df()
        df = df[df["coded"] == 1].reset_index(drop=True)
        num = ["t", "event", "enrollment_count", "sites", "countries", "start_year"]
        df[num] = df[num].astype("float64")
        print(
            f"Snapshot {current['run_id']}, as of {as_of}: {len(df):,} started, coded interventional trials", flush=True
        )

        km = km_table(df, chapters, args.min_n)
        print("KM done", flush=True)
        X = design(df, chapters)
        models, fit = fit_models(X)
        print("Models done", flush=True)
        dec = decomposition(con, df, chapters, args.min_n)
        con.close()

    phases = df["phase_group"].value_counts().to_dict()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    results = {
        "run_id": current["run_id"],
        "as_of": as_of,
        "generated_at": date.today().isoformat(),
        "trials": len(df),
        "phases": {PHASE_GROUP_LABELS.get(k, k): int(v) for k, v in phases.items()},
        "km": km,
        "models": models,
        "fit": fit,
        "decomposition": dec,
    }
    (out / "results.json").write_text(json.dumps(results, indent=2))
    ch_rows = [{"variable": CHAPTERS.get(int(k[3:]), k) if k.startswith("ch_") else k, **v} for k, v in models.items()]
    for r in ch_rows:
        r["p"] = f"{r['p']:.1e}" if r["p"] < 0.001 else round(r["p"], 3)
    phase_text = ", ".join(f"{k}: {v:,}" for k, v in results["phases"].items())
    km_cols = ["group", "trials", "ended", "km_median_days", "km_ci_low", "km_ci_high"]
    km_cols += ["completed_only_median_days", "pct_ended_by_3y"]
    dec_cols = ["group", "completed", "median_total_days", "median_to_primary_days", "median_primary_to_final_days"]
    dec_cols += ["median_enrollment", "median_sites", "median_pts_per_site_month", "median_primary_frame_days"]
    model_cols = ["variable", "time_ratio", "tr_low", "tr_high", "hazard_ratio", "p"]
    report = [
        f"# Trial duration vs condition\n\nSnapshot `{current['run_id']}`, censored as of {as_of}; "
        f"{len(df):,} started interventional trials with an ICD-10-CM code ({phase_text}).\n",
        "## 1. Kaplan-Meier median duration (start -> completion), ongoing trials censored\n",
        md_table(
            km,
            [
                "group",
                "trials",
                "ended",
                "km_median_days",
                "km_ci_low",
                "km_ci_high",
                "completed_only_median_days",
                "pct_ended_by_3y",
            ],
        ),
        "\n## 2. Adjusted models (Weibull AFT time ratio >1 = longer; Cox hazard ratio <1 = longer)\n",
        f"Concordance: AFT {fit['aft_concordance']}, Cox {fit['cox_concordance']}.\n",
        md_table(ch_rows, model_cols),
        "\n## 3. Decomposition, completed trials with actual primary completion\n",
        md_table(dec, dec_cols),
    ]
    (out / "report.md").write_text("\n".join(report))
    print(f"Wrote {out / 'report.md'} and {out / 'results.json'}")
    site = None if args.no_publish else open_site(args)
    if site is not None:
        site.put_json(DASHBOARD_KEY, dashboard_payload(results, chapters))
        print(f"Wrote {DASHBOARD_KEY} ({site.location(DASHBOARD_KEY)})")


if __name__ == "__main__":
    main()
