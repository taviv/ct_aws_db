"""
Transform one raw NDJSON page into one staging Parquet file per table, using DuckDB SQL.

Every staging row carries ``_src`` (the page name) so the build step can pick a single
winning copy of a study that appears on more than one page of the same run.
"""

import logging
import shutil
from pathlib import Path

import duckdb

from .config import STAGING_PREFIX, Settings
from .db import connect
from .storage import Store

logger = logging.getLogger(__name__)

MACROS = """
CREATE OR REPLACE MACRO ct_date(s) AS CASE
    WHEN length(s) = 10 THEN TRY_CAST(s AS DATE)
    WHEN length(s) = 7  THEN TRY_CAST(s || '-01' AS DATE)
    WHEN length(s) = 4  THEN TRY_CAST(s || '-01-01' AS DATE)
END;
CREATE OR REPLACE MACRO ct_bool(j, p) AS TRY_CAST(json_extract_string(j, p) AS BOOLEAN);
"""

RAW_SQL = """
CREATE OR REPLACE TEMP TABLE raw AS
SELECT
    json->>'$.protocolSection.identificationModule.nctId' AS nct_id,
    json->'$.protocolSection'                             AS ps,
    json->'$.derivedSection'                              AS dv,
    TRY_CAST(json->>'$.hasResults' AS BOOLEAN)            AS has_results
FROM read_ndjson_objects('{path}')
WHERE json->>'$.protocolSection.identificationModule.nctId' IS NOT NULL
"""

_STUDIES = """
SELECT
    *,
    CASE WHEN len(phases) = 0 THEN 'NA' ELSE array_to_string(list_sort(phases), '/') END AS phase_group,
    date_diff('day', start_date, completion_date)::INTEGER AS start_end
FROM (
    SELECT
        nct_id,
        identificationModule->>'$.briefTitle'                  AS brief_title,
        identificationModule->>'$.officialTitle'               AS official_title,
        identificationModule->>'$.acronym'                     AS acronym,
        identificationModule->>'$.orgStudyIdInfo.id'           AS org_study_id,
        identificationModule->>'$.organization.fullName'       AS organization_name,
        identificationModule->>'$.organization.class'          AS organization_class,
        statusModule->>'$.overallStatus'                       AS overall_status,
        statusModule->>'$.whyStopped'                          AS why_stopped,
        statusModule->>'$.statusVerifiedDate'                  AS status_verified_date,
        ct_bool(statusModule, '$.expandedAccessInfo.hasExpandedAccess') AS has_expanded_access,
        ct_date(statusModule->>'$.startDateStruct.date')       AS start_date,
        statusModule->>'$.startDateStruct.type'                AS start_date_type,
        ct_date(statusModule->>'$.primaryCompletionDateStruct.date') AS primary_completion_date,
        statusModule->>'$.primaryCompletionDateStruct.type'    AS primary_completion_date_type,
        ct_date(statusModule->>'$.completionDateStruct.date')  AS completion_date,
        statusModule->>'$.completionDateStruct.type'           AS completion_date_type,
        ct_date(statusModule->>'$.studyFirstSubmitDate')       AS study_first_submit_date,
        ct_date(statusModule->>'$.studyFirstPostDateStruct.date') AS study_first_post_date,
        ct_date(statusModule->>'$.lastUpdateSubmitDate')       AS last_update_submit_date,
        ct_date(statusModule->>'$.lastUpdatePostDateStruct.date') AS last_update_post_date,
        designModule->>'$.studyType'                           AS study_type,
        json_extract_string(designModule, '$.phases[*]')       AS phases,
        designModule->>'$.designInfo.allocation'               AS allocation,
        designModule->>'$.designInfo.interventionModel'        AS intervention_model,
        designModule->>'$.designInfo.primaryPurpose'           AS primary_purpose,
        designModule->>'$.designInfo.maskingInfo.masking'      AS masking,
        TRY_CAST(designModule->>'$.enrollmentInfo.count' AS INTEGER) AS enrollment_count,
        designModule->>'$.enrollmentInfo.type'                 AS enrollment_type,
        ct_bool(eligibilityModule, '$.healthyVolunteers')      AS healthy_volunteers,
        eligibilityModule->>'$.sex'                            AS sex,
        eligibilityModule->>'$.minimumAge'                     AS minimum_age,
        eligibilityModule->>'$.maximumAge'                     AS maximum_age,
        sponsorCollaboratorsModule->>'$.leadSponsor.name'      AS lead_sponsor_name,
        sponsorCollaboratorsModule->>'$.leadSponsor.class'     AS lead_sponsor_class,
        sponsorCollaboratorsModule->>'$.responsibleParty.type' AS responsible_party_type,
        ct_bool(oversightModule, '$.oversightHasDmc')          AS has_dmc,
        ct_bool(oversightModule, '$.isFdaRegulatedDrug')       AS is_fda_regulated_drug,
        ct_bool(oversightModule, '$.isFdaRegulatedDevice')     AS is_fda_regulated_device,
        ct_bool(oversightModule, '$.isUsExport')               AS is_us_export,
        ipdSharingStatementModule->>'$.ipdSharing'             AS ipd_sharing,
        coalesce(has_results, FALSE)                              AS has_results
    FROM (
        SELECT nct_id, has_results,
               ps->'$.designModule' AS designModule,
               ps->'$.eligibilityModule' AS eligibilityModule,
               ps->'$.identificationModule' AS identificationModule,
               ps->'$.ipdSharingStatementModule' AS ipdSharingStatementModule,
               ps->'$.oversightModule' AS oversightModule,
               ps->'$.sponsorCollaboratorsModule' AS sponsorCollaboratorsModule,
               ps->'$.statusModule' AS statusModule
        FROM raw
    )
)
"""

_MESH = """
SELECT nct_id, m->>'$.id' AS mesh_id, m->>'$.term' AS term, FALSE AS is_ancestor
FROM (SELECT nct_id, unnest(json_extract(dv, '$.{module}.meshes[*]')) AS m FROM raw)
UNION ALL
SELECT nct_id, m->>'$.id', m->>'$.term', TRUE
FROM (SELECT nct_id, unnest(json_extract(dv, '$.{module}.ancestors[*]')) AS m FROM raw)
"""

TABLES = {
    "studies": _STUDIES,
    "study_text": """
        SELECT nct_id,
               nullif(trim(ps->>'$.descriptionModule.briefSummary'), '')        AS brief_summary,
               nullif(trim(ps->>'$.descriptionModule.detailedDescription'), '') AS detailed_description,
               nullif(trim(ps->>'$.eligibilityModule.eligibilityCriteria'), '') AS eligibility_criteria
        FROM raw
    """,
    "study_phases": """
        SELECT DISTINCT nct_id, unnest(json_extract_string(ps, '$.designModule.phases[*]')) AS phase FROM raw
    """,
    "study_conditions": """
        SELECT DISTINCT nct_id, unnest(json_extract_string(ps, '$.conditionsModule.conditions[*]')) AS condition_name
        FROM raw
    """,
    "study_interventions": """
        SELECT nct_id, x->>'$.type' AS intervention_type, x->>'$.name' AS name, x->>'$.description' AS description
        FROM (SELECT nct_id, unnest(json_extract(ps, '$.armsInterventionsModule.interventions[*]')) AS x FROM raw)
    """,
    "study_outcomes": " UNION ALL ".join(
        f"""SELECT nct_id, '{otype}' AS outcome_type, o->>'$.measure' AS measure,
                   o->>'$.description' AS description, o->>'$.timeFrame' AS time_frame
            FROM (SELECT nct_id, unnest(json_extract(ps, '$.outcomesModule.{key}[*]')) AS o FROM raw)"""
        for otype, key in (
            ("primary", "primaryOutcomes"),
            ("secondary", "secondaryOutcomes"),
            ("other", "otherOutcomes"),
        )
    ),
    "study_locations": """
        SELECT nct_id, l->>'$.facility' AS facility, l->>'$.status' AS status, l->>'$.city' AS city,
               l->>'$.state' AS state, l->>'$.country' AS country, l->>'$.zip' AS zip,
               TRY_CAST(l->>'$.geoPoint.lat' AS DOUBLE) AS latitude,
               TRY_CAST(l->>'$.geoPoint.lon' AS DOUBLE) AS longitude
        FROM (SELECT nct_id, unnest(json_extract(ps, '$.contactsLocationsModule.locations[*]')) AS l FROM raw)
    """,
    "study_sponsors": """
        SELECT DISTINCT * FROM (
            SELECT nct_id, 'lead' AS sponsor_type,
                   ps->>'$.sponsorCollaboratorsModule.leadSponsor.name'  AS name,
                   ps->>'$.sponsorCollaboratorsModule.leadSponsor.class' AS class
            FROM raw
            UNION ALL
            SELECT nct_id, 'collaborator', c->>'$.name', c->>'$.class'
            FROM (SELECT nct_id, unnest(json_extract(ps, '$.sponsorCollaboratorsModule.collaborators[*]')) AS c
                  FROM raw)
        ) WHERE name IS NOT NULL
    """,
    "condition_mesh_terms": _MESH.format(module="conditionBrowseModule"),
    "intervention_mesh_terms": _MESH.format(module="interventionBrowseModule"),
}


def _sql_str(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def transform_file(con: duckdb.DuckDBPyConnection, ndjson_path: Path, out_dir: Path, src: str) -> dict:
    """Write ``out_dir/<table>.parquet`` for every table; returns row counts."""
    con.execute(MACROS)
    con.execute(RAW_SQL.format(path=str(ndjson_path).replace("'", "''")))
    out_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    for table, sql in TABLES.items():
        out = out_dir / f"{table}.parquet"
        con.execute(
            f"COPY (SELECT *, {_sql_str(src)} AS _src FROM ({sql})) "
            f"TO {_sql_str(out)} (FORMAT parquet, COMPRESSION zstd)"
        )
        counts[table] = con.execute(f"SELECT count(*) FROM read_parquet({_sql_str(out)})").fetchone()[0]
    return counts


def transform_page(settings: Settings, store: Store, run_id: str, key: str) -> dict:
    page = Path(key).stem
    work = Path(settings.work_dir) / "transform" / run_id / page
    shutil.rmtree(work, ignore_errors=True)
    local = work / Path(key).name
    store.download(key, local)
    try:
        con = connect(work)
        counts = transform_file(con, local, work / "out", page)
        con.close()
        for table in TABLES:
            store.upload(work / "out" / f"{table}.parquet", f"{STAGING_PREFIX}{run_id}/{table}/{page}.parquet")
    finally:
        shutil.rmtree(work, ignore_errors=True)
    logger.info("Transformed %s: %s", key, counts)
    return {"key": key, "studies": counts["studies"]}
