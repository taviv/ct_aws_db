"""
Dashboard SQL (DuckDB dialect), shared by the build step (static overview JSON) and the
query Lambda (filtered duration views). Every query is scoped to the CTE ``s`` — the studies
matching the selected phase group — so one SQL text serves every phase.
"""

import csv
from pathlib import Path

PHASE_GROUP_LABELS = {
    "EARLY_PHASE1": "Early Phase 1",
    "PHASE1": "Phase 1",
    "PHASE1/PHASE2": "Phase 1/2",
    "PHASE2": "Phase 2",
    "PHASE2/PHASE3": "Phase 2/3",
    "PHASE3": "Phase 3",
    "PHASE4": "Phase 4",
    "NA": "Not applicable",
}

# Free-text condition names grouped by spelling variant: case, punctuation, word order, plurals and a few
# filler words are ignored, so "Covid19" = "COVID-19" and "Diabetes Mellitus, Type 2" = "Type 2 Diabetes".
_CONDITION_FILLER_WORDS = ("mellitus", "of", "the", "and")
_CONDITION_KEY = f"""array_to_string(list_sort(list_distinct(list_filter(
    list_transform(regexp_extract_all(lower(condition_name), '[a-z]+|[0-9]+'),
        t -> CASE WHEN t = 'melitus' THEN 'mellitus'
                  WHEN length(t) > 3 AND t[-1:] = 's' AND t[-2:] NOT IN ('ss', 'us', 'is') THEN t[:-2]
                  ELSE t END),
    t -> t NOT IN {_CONDITION_FILLER_WORDS}))), ' ')"""


def _healthy_pattern() -> str:
    """The curated healthy-volunteer rule (data/condition_text_rules.csv), so the chart and Z00.6 coding agree."""
    with open(Path(__file__).parent / "data" / "condition_text_rules.csv", newline="") as f:
        rows = csv.DictReader(line for line in f if not line.startswith("#"))
        return next(r["pattern"] for r in rows if r["note"].startswith("Healthy volunteers"))


_HEALTHY_LABEL = "Healthy volunteers"
_CONDITION_GROUP = f"""CASE WHEN regexp_full_match(trim(regexp_replace(lower(condition_name), '[^a-z0-9]+', ' ', 'g')),
    '{_healthy_pattern().replace("?", "{0,1}").replace("'", "''")}') THEN '~healthy' ELSE {_CONDITION_KEY} END"""

OVERVIEW_QUERIES = {
    "summary_stats": """
        SELECT
            COUNT(*)                                                         AS total_studies,
            COUNT(*) FILTER (WHERE overall_status = 'COMPLETED')             AS completed,
            COUNT(*) FILTER (WHERE overall_status = 'RECRUITING')            AS recruiting,
            COUNT(*) FILTER (WHERE overall_status = 'ACTIVE_NOT_RECRUITING') AS active_not_recruiting,
            COUNT(*) FILTER (WHERE overall_status = 'NOT_YET_RECRUITING')    AS not_yet_recruiting,
            COUNT(*) FILTER (WHERE overall_status = 'TERMINATED')            AS terminated,
            COUNT(*) FILTER (WHERE is_fda_regulated_drug)                    AS fda_regulated_drug,
            COUNT(*) FILTER (WHERE has_results)                              AS has_results,
            COUNT(*) FILTER (WHERE healthy_volunteers)                       AS healthy_volunteers
        FROM s
    """,
    "status_breakdown": """
        SELECT overall_status AS label, COUNT(*) AS value FROM s GROUP BY 1 ORDER BY 2 DESC
    """,
    "studies_by_year": """
        SELECT year(start_date) AS year, COUNT(*) AS count FROM s
        WHERE start_date IS NOT NULL AND year(start_date) BETWEEN 1990 AND year(current_date) + 5
        GROUP BY 1 ORDER BY 1
    """,
    "sponsor_class": """
        SELECT COALESCE(lead_sponsor_class, 'UNKNOWN') AS label, COUNT(*) AS value FROM s GROUP BY 1 ORDER BY 2 DESC
    """,
    "top_conditions": f"""
        SELECT CASE WHEN k = '~healthy' THEN '{_HEALTHY_LABEL}'
                    ELSE arg_min(condition_name, (-n, condition_name)) END AS label,
               COUNT(DISTINCT nct_id) AS value
        FROM (
            SELECT nct_id, condition_name, k, COUNT(*) OVER (PARTITION BY k, condition_name) AS n
            FROM (
                SELECT nct_id, condition_name, {_CONDITION_GROUP} AS k
                FROM study_conditions SEMI JOIN s USING (nct_id)
            )
        )
        GROUP BY k ORDER BY 2 DESC, 1 LIMIT 15
    """,
    "top_countries": """
        SELECT country AS label, COUNT(DISTINCT nct_id) AS value
        FROM study_locations SEMI JOIN s USING (nct_id)
        WHERE country IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 15
    """,
    "fda_regulated": """
        SELECT CASE WHEN is_fda_regulated_drug THEN 'FDA Regulated' ELSE 'Not FDA Regulated' END AS label,
               COUNT(*) AS value
        FROM s WHERE is_fda_regulated_drug IS NOT NULL GROUP BY 1 ORDER BY 2 DESC
    """,
    "intervention_types": """
        SELECT COALESCE(intervention_type, 'UNKNOWN') AS label, COUNT(*) AS value
        FROM study_interventions SEMI JOIN s USING (nct_id)
        GROUP BY 1 ORDER BY 2 DESC
    """,
    "recent_studies": """
        SELECT nct_id, brief_title, overall_status, strftime(start_date, '%Y-%m-%d') AS start_date,
               lead_sponsor_name, lead_sponsor_class, enrollment_count
        FROM s WHERE start_date IS NOT NULL AND start_date <= current_date
        ORDER BY s.start_date DESC, nct_id LIMIT 25
    """,
    "enrollment_distribution": """
        SELECT CASE
                 WHEN enrollment_count < 10   THEN '< 10'
                 WHEN enrollment_count < 50   THEN '10-49'
                 WHEN enrollment_count < 100  THEN '50-99'
                 WHEN enrollment_count < 250  THEN '100-249'
                 WHEN enrollment_count < 500  THEN '250-499'
                 WHEN enrollment_count < 1000 THEN '500-999'
                 ELSE '1000+'
               END AS label,
               COUNT(*) AS value
        FROM s WHERE enrollment_count IS NOT NULL
        GROUP BY 1 ORDER BY MIN(enrollment_count)
    """,
    "phase_groups": """
        SELECT phase_group AS label, COUNT(*) AS value FROM s GROUP BY 1 ORDER BY 1
    """,
    "countries_completed": """
        SELECT DISTINCT l.country AS label
        FROM study_locations l SEMI JOIN (SELECT nct_id FROM s WHERE overall_status = 'COMPLETED') USING (nct_id)
        WHERE l.country IS NOT NULL ORDER BY 1
    """,
}

_DURATION_BASE = "s.overall_status = 'COMPLETED' AND s.start_end IS NOT NULL AND s.start_end >= 0"

DURATION_QUERIES = {
    "duration_summary": """
        SELECT COUNT(*)                     AS total_studies,
               ROUND(AVG(start_end))        AS avg_duration_days,
               quantile_cont(start_end, 0.5) AS median_duration_days,
               MIN(start_end)               AS min_duration_days,
               MAX(start_end)               AS max_duration_days
        FROM s WHERE {where}
    """,
    "duration_histogram": """
        SELECT CASE
                 WHEN start_end < 180  THEN '< 6 mo'
                 WHEN start_end < 365  THEN '6-12 mo'
                 WHEN start_end < 730  THEN '1-2 yr'
                 WHEN start_end < 1095 THEN '2-3 yr'
                 WHEN start_end < 1825 THEN '3-5 yr'
                 ELSE '5+ yr'
               END AS bucket,
               COUNT(*) AS count,
               MIN(start_end) AS sort_key
        FROM s WHERE {where}
        GROUP BY 1 ORDER BY 3
    """,
    "duration_by_year": """
        SELECT year(s.completion_date) AS year, COUNT(*) AS studies, ROUND(AVG(s.start_end)) AS avg_duration_days
        FROM s WHERE {where} AND s.completion_date IS NOT NULL
          AND year(s.completion_date) BETWEEN 1990 AND year(current_date) + 5
        GROUP BY 1 ORDER BY 1
    """,
    "duration_by_sponsor": """
        SELECT COALESCE(s.lead_sponsor_class, 'UNKNOWN') AS sponsor_class, COUNT(*) AS studies,
               ROUND(AVG(s.start_end)) AS avg_duration_days
        FROM s WHERE {where}
        GROUP BY 1 ORDER BY 3 DESC
    """,
    "duration_by_phase": """
        SELECT s.phase_group, COUNT(*) AS studies, ROUND(AVG(s.start_end)) AS avg_duration_days,
               quantile_cont(s.start_end, 0.5) AS median_duration_days
        FROM s WHERE {where}
        GROUP BY 1 ORDER BY 1
    """,
    "duration_studies": """
        SELECT s.nct_id, s.brief_title, s.phase_group,
               strftime(s.start_date, '%Y-%m-%d')      AS start_date,
               strftime(s.completion_date, '%Y-%m-%d') AS completion_date,
               s.start_end AS duration_days, s.lead_sponsor_name, s.lead_sponsor_class,
               s.enrollment_count, s.healthy_volunteers,
               (SELECT COUNT(DISTINCT l2.country) FROM study_locations l2 WHERE l2.nct_id = s.nct_id) AS num_countries
        FROM s WHERE {where}
        ORDER BY s.start_end DESC, s.nct_id LIMIT 100
    """,
}

SINGLE_ROW_QUERIES = {"summary_stats", "duration_summary"}
ALL_QUERIES = sorted(set(OVERVIEW_QUERIES) | set(DURATION_QUERIES))


def _phase_cte(phase):
    if phase:
        return "WITH s AS (SELECT * FROM studies WHERE phase_group = ?) ", [phase]
    return "WITH s AS (SELECT * FROM studies) ", []


def build_duration_where(filters: dict):
    clauses, args = [_DURATION_BASE], []
    if filters.get("year"):
        clauses.append("year(s.completion_date) = ?")
        args.append(int(filters["year"]))
    if filters.get("healthy_volunteers") is not None:
        clauses.append("s.healthy_volunteers = ?")
        args.append(bool(filters["healthy_volunteers"]))
    if filters.get("multicountry"):
        clauses.append("(SELECT COUNT(DISTINCT l2.country) FROM study_locations l2 WHERE l2.nct_id = s.nct_id) > 1")
    elif filters.get("country"):
        clauses.append("EXISTS (SELECT 1 FROM study_locations l2 WHERE l2.nct_id = s.nct_id AND l2.country = ?)")
        args.append(str(filters["country"]))
    return " AND ".join(clauses), args


def build_query(name: str, filters: dict = None):
    """Return (sql, params) for a named query, or raise KeyError."""
    filters = filters or {}
    cte, args = _phase_cte(filters.get("phase"))
    if name in OVERVIEW_QUERIES:
        return cte + OVERVIEW_QUERIES[name], args
    if name in DURATION_QUERIES:
        where, where_args = build_duration_where(filters)
        return cte + DURATION_QUERIES[name].format(where=where), args + where_args
    raise KeyError(name)


def run_query(con, name: str, filters: dict = None):
    sql, args = build_query(name, filters)
    cur = con.execute(sql, args)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    if name in SINGLE_ROW_QUERIES:
        return dict(zip(cols, rows[0], strict=True)) if rows else {}
    return {"columns": cols, "rows": [list(r) for r in rows]}
