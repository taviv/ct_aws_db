# ClinicalTrials.gov Pipeline — all phases, S3 Parquet + DuckDB

Weekly pipeline that pulls clinical trials from the
[ClinicalTrials.gov v2 API](https://clinicaltrials.gov/data-api/api), stores them as Parquet on
S3, and serves two dashboards. No database, no VPC, nothing running between runs.

Which studies are loaded is set by two stack parameters. By default that is every phased
study (Early Phase 1 → Phase 4, ~224k). `QueryTerm=AREA[Phase]PHASE1 StartYear=2016` limits it
to Phase 1 (including Phase 1/2) studies starting in 2016 or later (~35k).

Dashboards (paths under the `DashboardUrl` stack output):
- `/`: overview (status, phases, sponsors, conditions, countries, enrollment), filterable by phase
- `/dashboard_duration.html`: start → completion duration of completed studies

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
  db.py               DuckDB connections that spill only under CT_WORK_DIR
  verify.py           data integrity checks behind `make verify`
statemachine/         Step Functions definition (ASL)
template.yaml         SAM template: buckets, functions, state machine, schedule, CloudFront, Athena
dashboard/            static dashboards (Chart.js)
scripts/              local_pipeline.py, dev_server.py, verify.py
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
[AWS CloudShell](https://console.aws.amazon.com/cloudshell) has all of these; clone the repo there and run:

```bash
make deploy      # sam build + sam deploy (--guided the first time, saving samconfig.toml) + upload dashboards
make backfill    # first load / after changing QueryTerm or StartYear: full fetch, replaces the snapshot
make run         # incremental run now (the schedule does this weekly)
make verify      # data integrity checks (see "Verifying the data")
```

Use `make deploy` rather than `sam deploy`: plain `sam deploy` does not upload the dashboard
HTML (`make dashboards` does that on its own).

Stack parameters:

| Parameter | Default | |
|---|---|---|
| `QueryTerm` | `AREA[Phase](EARLY_PHASE1 OR PHASE1 OR PHASE2 OR PHASE3 OR PHASE4)` | [Essie expression](https://clinicaltrials.gov/find-studies/constructing-complex-search-queries) selecting studies, e.g. `AREA[Phase]PHASE1` or `AREA[StudyType]INTERVENTIONAL` |
| `StartYear` | — | only studies starting in/after this year |
| `BuildMemorySize` | 3008 | Build Lambda memory (MB); new accounts are capped at 3008 |
| `QueryReservedConcurrency` | 0 | reserved concurrency for the query API (0 = none) |
| `Schedule` | `cron(0 6 ? * MON *)` | incremental run schedule |
| `KeepSnapshots` | 3 | curated snapshots kept for rollback |
| `RawRetentionDays` | 180 | raw NDJSON lifecycle |
| `GlueDatabaseName` | `clinical_trials` | Athena database |

Always filter on a field with `AREA[Field]...`. A bare word is a free-text search:
`QueryTerm=PHASE1` also matches ~2k non-Phase-1 studies that only mention "phase 1" in their text.

To change a parameter, edit `parameter_overrides` in `samconfig.toml` (or rerun
`sam deploy --guided`), then `make deploy`. `sam deploy --parameter-overrides ...` on the command
line is not saved, so the next `make deploy` would revert it. After changing `QueryTerm` or
`StartYear`, run `make backfill` so the snapshot matches the new filter.

```bash
aws cloudformation describe-stacks --stack-name ct-pipeline --query "Stacks[0].Parameters" --output table
aws cloudformation describe-stacks --stack-name ct-pipeline --query "Stacks[0].Outputs" --output table
```

The query API is only reachable through CloudFront (the Function URL requires SigV4 signed by
CloudFront's OAC) and returns generic error messages. Put CloudFront behind an auth layer
(e.g. Cognito / Lambda@Edge / IP allow-list) if the dashboards must not be public.

Lambda sizing (`template.yaml`, Python 3.13):

| Function | Memory | Timeout | `/tmp` |
|---|---|---|---|
| Fetch | 512 MB | 2 min | default |
| Transform (one 1,000-study page) | 2048 MB | 5 min | 2 GB |
| Build | `BuildMemorySize` (3008 MB) | 15 min | 10 GB |
| Query | 2048 MB | 30 s | 2 GB |

The Lambda code directory is read-only. All DuckDB work, including spill files, goes under
`CT_WORK_DIR` (`/tmp/ct`) via `ct_pipeline.db.connect`, so never call `duckdb.connect()` directly.

## Operations

The weekly schedule (an EventBridge rule created with the stack) starts an incremental run every
Monday 06:00 UTC. Each run fetches only studies updated since the previous successful run, merges
them into the current snapshot, and republishes the dashboards' data.

```bash
# schedule is enabled
aws events list-rules --query "Rules[?contains(Name,'Weekly')].[Name,State,ScheduleExpression]" --output table

# recent runs
SM=$(aws cloudformation describe-stacks --stack-name ct-pipeline --query "Stacks[0].Outputs[?OutputKey=='StateMachineArn'].OutputValue" --output text)
aws stepfunctions list-executions --state-machine-arn $SM --max-results 5 --query "executions[].[name,status,startDate,stopDate]" --output table

# why the latest run failed
ARN=$(aws stepfunctions list-executions --state-machine-arn $SM --max-results 1 --query "executions[0].executionArn" --output text)
aws stepfunctions get-execution-history --execution-arn $ARN --reverse-order --max-results 25 \
  --query "events[?ends_with(type,'Failed') || ends_with(type,'TimedOut')].[type, taskFailedEventDetails.error || executionFailedEventDetails.error, taskFailedEventDetails.cause || executionFailedEventDetails.cause]" --output json
```

A failed run changes nothing: `CURRENT.json`, the watermark and the dashboards keep the previous
snapshot, and the next run starts from that watermark. Before the first successful run,
CloudFront returns 403 for `data/overview.json` and the API returns 500, so the dashboards are
empty. After a run, the dashboards can take up to 15 minutes to show it (CloudFront cache).

### Query API

`GET /api/query?name=<query>&phase=&year=&country=&multicountry=&healthy_volunteers=`

Queries: `duration_summary`, `duration_histogram`, `duration_by_year`, `duration_by_sponsor`,
`duration_by_phase`, `duration_studies`, plus every overview query (`summary_stats`,
`status_breakdown`, `studies_by_year`, `top_conditions`, `top_countries`, `sponsor_class`,
`fda_regulated`, `intervention_types`, `enrollment_distribution`, `recent_studies`,
`phase_groups`, `countries_completed`). Duration queries cover completed studies only. `POST` with
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

### Verifying the data

`make verify` (needs AWS credentials, e.g. CloudShell) checks the published snapshot and prints PASS / WARN / FAIL per check:

| Check | Passes when |
|---|---|
| Latest pipeline run | the newest Step Functions execution SUCCEEDED |
| Snapshot freshness | `CURRENT.json` is at most 8 days old |
| Row counts | every Parquet file has the row count recorded in `CURRENT.json` |
| Unique studies | one row per `nct_id` in `studies` and `study_text` |
| Matches ClinicalTrials.gov | the snapshot's NCT IDs equal the API's for the stack's `QueryTerm`/`StartYear` (within 1%: the source changes between weekly runs) |
| Start year filter | no study starts before `StartYear` |
| Orphan rows | every child-table row has a study |
| Durations | no completion date before the start date (WARN only: that is how the source has it) |
| Dashboard overview / API | `data/overview.json` and `api/query` serve the current run with the same numbers as the snapshot |
| Spot check vs source | title, status, start/completion dates and phases of 20 random studies equal the API's (studies updated since the snapshot are skipped) |

It exits non-zero if anything fails, so it can also run after `make backfill` in scripts.

### Rollback

Each snapshot is immutable. To roll back, copy an older `curated/<run_id>/` reference into
`curated/CURRENT.json` (the previous run id is recorded in it as `previous_run_id`). The
query API picks up the change within 5 minutes; rerun a build to regenerate `overview.json`.

## Local development (no AWS)

```bash
make install                 # pip install -r requirements-dev.txt
make test                    # pytest
make lint                    # ruff + cfn-lint
make local MAX_PAGES=3       # fetch 3 pages from the live API → ./local/{data,site}
make serve                   # dashboards + /api on http://localhost:8000
```

CI (`.github/workflows/ci.yml`) runs ruff, pytest, cfn-lint, `sam validate --lint` and `sam build`
on every pull request.

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
