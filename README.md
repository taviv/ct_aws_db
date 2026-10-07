# ClinicalTrials.gov Pipeline — all phases, S3 Parquet + DuckDB

Weekly pipeline that pulls interventional trials of **every phase** (Early Phase 1 → Phase 4)
from the [ClinicalTrials.gov v2 API](https://clinicaltrials.gov/data-api/api), stores them as
Parquet on S3, and serves two dashboards. No database, no VPC, nothing running between runs.

```
EventBridge (weekly) ──► Step Functions
                          │
                          ├─ StartRun / FetchPage* / FinalizeFetch   (FetchFunction)
                          │     only studies updated since last run, only needed fields
                          │     → s3://data/raw/<run_id>/page_NNNN.ndjson
                          │
                          ├─ Map: TransformPage (×10 in parallel)    (TransformFunction, DuckDB)
                          │     → s3://data/staging/<run_id>/<table>/page_NNNN.parquet
                          │
                          └─ BuildSnapshot                           (BuildFunction, DuckDB)
                                current snapshot − changed studies + staged rows
                                → s3://data/curated/<run_id>/<table>/data.parquet
                                → s3://data/curated/CURRENT.json      (atomic pointer)
                                → s3://site/data/overview.json        (overview dashboard data)
                                → Glue tables (Athena)                 → state/watermark.json

CloudFront ──► /*.html, /data/*  → S3 site bucket (private, OAC)
           └─► /api/query?...    → QueryFunction URL (IAM auth, only CloudFront can call it)
                                    DuckDB over the CURRENT snapshot, cached 15 min
```

## Why this design

| | Before (Aurora) | Now (S3 Parquet) |
|---|---|---|
| Always-on cost | Aurora Serverless v2 + VPC | none (S3 storage, a few Lambda minutes/week) |
| Weekly run | refetch + reload every study | only studies with `LastUpdatePostDate` ≥ last run − 1 day |
| API payload | full JSON | `fields=protocolSection,derivedSection,hasResults` (~50% smaller) |
| Phase 2/3 scale | multi-row `INSERT` exceeds PostgreSQL's 65,535 parameter limit | no inserts; columnar files |
| Overview dashboard | 11 SQL queries per page view | one static JSON per run |
| Ad-hoc SQL | psql into the VPC | Athena (`clinical_trials` database) or DuckDB locally |
| Deployment | ~25 manual CLI steps, hardcoded ARNs | `make deploy` (SAM) |
| Rollback | — | keep last N snapshots; repoint `CURRENT.json` |

## Repository layout

```
src/ct_pipeline/      one package, four Lambda handlers (handlers.py)
  config.py           settings from env vars
  storage.py          S3 / local-directory store (local runs and tests need no AWS)
  fetch.py            API client: start / fetch_page / finalize
  transform.py        DuckDB SQL: NDJSON page → one Parquet file per table
  build.py            merge into new snapshot, overview JSON, watermark, pruning
  queries.py          dashboard SQL shared by build and query API
  query_api.py        Function URL handler
  glue.py             registers the snapshot in the Glue catalog
statemachine/         Step Functions definition (ASL)
template.yaml         SAM template: buckets, functions, state machine, schedule, CloudFront, Athena
dashboard/            static dashboards (Chart.js)
scripts/              local_pipeline.py, dev_server.py
tests/                pytest (fixtures are real API records)
```

## Data model

All tables are keyed by `nct_id`. One row per study in `studies` and `study_text`.

| Table | Contents |
|---|---|
| `studies` | status, dates, design, enrollment, lead sponsor, eligibility, FDA flags, `phases` (list), `phase_group` (e.g. `PHASE1/PHASE2`, `NA`), `start_end` (start → completion, days), `last_update_post_date` |
| `study_text` | brief summary, detailed description, eligibility criteria (kept apart so the main table stays small) |
| `study_phases` | one row per phase |
| `study_conditions` | condition names |
| `study_interventions` | type, name, description |
| `study_outcomes` | primary / secondary / other outcome measures |
| `study_locations` | facility, status, city, state, country, zip, latitude/longitude |
| `study_sponsors` | lead sponsor and collaborators (`sponsor_type`, name, class) |
| `condition_mesh_terms`, `intervention_mesh_terms` | MeSH terms and ancestors from `derivedSection` |

## Deploy

Prerequisites: AWS CLI credentials, [SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html), Python 3.13 or Docker (`make build` uses a Docker build automatically when Python 3.13 is not installed).

```bash
make deploy      # sam build + sam deploy --guided (first time) + upload dashboards
make backfill    # first load: full fetch of all phased studies (~225 pages)
make run         # incremental run now (the schedule does this weekly)
```

Stack parameters:

| Parameter | Default | |
|---|---|---|
| `QueryTerm` | `AREA[Phase](EARLY_PHASE1 OR PHASE1 OR PHASE2 OR PHASE3 OR PHASE4)` | Essie expression selecting studies; e.g. `AREA[StudyType]INTERVENTIONAL` for all interventional studies |
| `StartYear` | — | only studies starting in/after this year |
| `Schedule` | `cron(0 6 ? * MON *)` | incremental run schedule |
| `KeepSnapshots` | 3 | curated snapshots kept for rollback |
| `RawRetentionDays` | 180 | raw NDJSON lifecycle |
| `GlueDatabaseName` | `clinical_trials` | Athena database |

After changing `QueryTerm`, run `make backfill` so the snapshot matches the new filter.

The dashboard URL is the `DashboardUrl` stack output. The query API is only reachable through
CloudFront (the Function URL requires SigV4 signed by CloudFront's OAC), has reserved
concurrency 10, and returns generic error messages. Put CloudFront behind an auth layer
(e.g. Cognito / Lambda@Edge / IP allow-list) if the dashboards must not be public.

### Query API

`GET /api/query?name=<query>&phase=&year=&country=&multicountry=&healthy_volunteers=`

Queries: `duration_summary`, `duration_histogram`, `duration_by_year`, `duration_by_sponsor`,
`duration_by_phase`, `duration_studies`, plus every overview query (`summary_stats`,
`status_breakdown`, `studies_by_year`, `top_conditions`, `top_countries`, `sponsor_class`,
`top_interventions`, `enrollment_distribution`, `recent_studies`, `phase_groups`,
`countries_completed`). Duration queries cover completed studies only. `POST` with
`{"query": ..., "filters": {...}}` is also accepted.

### Ad-hoc SQL

Athena (workgroup = stack name, database `clinical_trials`):

```sql
SELECT phase_group, count(*) AS studies, approx_percentile(start_end, 0.5) AS median_days
FROM studies WHERE overall_status = 'COMPLETED' GROUP BY 1 ORDER BY 1;
```

DuckDB on a laptop, straight from S3:

```sql
INSTALL httpfs; LOAD httpfs; CREATE SECRET (TYPE s3, PROVIDER credential_chain);
SELECT * FROM read_parquet('s3://<data-bucket>/curated/<run_id>/studies/data.parquet') LIMIT 10;
```

### Rollback

Each snapshot is immutable. To roll back, copy an older `curated/<run_id>/` reference into
`curated/CURRENT.json` (the previous run id is recorded in it as `previous_run_id`). The
query API picks up the change within 5 minutes; rerun a build to regenerate `overview.json`.

## Local development (no AWS)

```bash
python3 -m pip install -r requirements-dev.txt
make test                    # pytest
make lint                    # ruff + cfn-lint
make local MAX_PAGES=3       # fetch 3 pages from the live API → ./local/{data,site}
make serve                   # dashboards + /api on http://localhost:8000
```

`scripts/local_pipeline.py --data s3://bucket --site s3://site-bucket` runs the same code
against real buckets.

## Configuration (Lambda environment)

| Variable | Default | |
|---|---|---|
| `CT_DATA_URI` | — | `s3://bucket[/prefix]` or local path |
| `CT_SITE_URI` | — | where `data/overview.json` is written |
| `CT_QUERY_TERM` | all phases | API `query.term` |
| `CT_FIELDS` | `protocolSection,derivedSection,hasResults` | API `fields` |
| `CT_PAGE_SIZE` | 1000 | API max |
| `CT_START_YEAR` | — | |
| `CT_GLUE_DATABASE` | — | register Glue tables when set |
| `CT_KEEP_SNAPSHOTS` | 3 | |
| `CT_WORK_DIR` | `/tmp/ct` | scratch space |

## Migrating from the Aurora version

1. `make deploy && make backfill`.
2. Check the new dashboards, then delete the old Aurora cluster, its VPC endpoints/security
   groups, the old Lambdas, the pg8000 layer, the old state machine and its schedule.
   The old raw NDJSON in S3 can be deleted or kept; the new pipeline does not read it.
