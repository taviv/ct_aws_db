STACK ?= ct-pipeline
PY    ?= python3

.PHONY: install test lint fmt validate build deploy dashboards backfill run reference umls-coverage llm-eval llm-code llm-try llm-code-cloud llm-xeval llm-adjudicate llm-status llm-retry condition-stats condition-audit trial-match duration-experiments verify local serve

install:
	$(PY) -m pip install -r requirements-dev.txt

test:
	$(PY) -m pytest -q

lint:
	ruff check . && ruff format --check . && cfn-lint template.yaml

fmt:
	ruff check --fix . && ruff format .

validate:
	sam validate --lint

RUNTIME := $(shell sed -n 's/^ *Runtime: python//p' template.yaml | head -1)

## Builds natively when the Lambda runtime's Python is installed, otherwise in Docker
build:
	@if command -v python$(RUNTIME) >/dev/null 2>&1 || [ "$$($(PY) -c 'import sys; print("%d.%d" % sys.version_info[:2])')" = "$(RUNTIME)" ]; then \
		sam build; \
	else \
		echo "Python $(RUNTIME) not found; building in Docker"; sam build --use-container; \
	fi

## First deploy: `make deploy` runs `sam deploy --guided` and saves samconfig.toml
deploy: build
	@if [ -f samconfig.toml ]; then sam deploy; else sam deploy --guided --stack-name $(STACK) --capabilities CAPABILITY_IAM; fi
	$(MAKE) dashboards

out = $(shell aws cloudformation describe-stacks --stack-name $(STACK) --query "Stacks[0].Outputs[?OutputKey=='$(1)'].OutputValue" --output text)

dashboards:
	aws s3 sync dashboard/ s3://$(call out,SiteBucketName)/ --delete --exclude "data/*"
	aws cloudfront create-invalidation --distribution-id $(call out,DistributionId) --paths "/*.html" >/dev/null

## Full refetch of every study (first load, or after changing QueryTerm)
backfill:
	aws stepfunctions start-execution --state-machine-arn $(call out,StateMachineArn) --input '{"mode":"full"}'

run:
	aws stepfunctions start-execution --state-machine-arn $(call out,StateMachineArn) --input '{"mode":"incremental"}'

## Refresh condition-coding reference data (ICD-10-CM, MeSH crosswalks) and recode the current snapshot
reference:
	aws lambda invoke --function-name $(call out,ReferenceFunctionName) --cli-read-timeout 900 --payload '{}' /tmp/ct-reference.json >/dev/null && cat /tmp/ct-reference.json && echo

## Measure what the UMLS layer adds (needs the UmlsSecretName secret); only writes the UMLS lookup cache
umls-coverage:
	@test -x .verify-venv/bin/python || $(VERIFY_PY) -m venv .verify-venv
	@.verify-venv/bin/python -m pip install -q -r src/requirements.txt boto3
	.verify-venv/bin/python scripts/umls_coverage.py --stack $(STACK)

## Suspicion-ranked CSV of shaky deterministic condition codes for hand review
condition-audit:
	@test -x .verify-venv/bin/python || $(VERIFY_PY) -m venv .verify-venv
	@.verify-venv/bin/python -m pip install -q -r src/requirements.txt boto3
	.verify-venv/bin/python scripts/condition_audit.py --stack $(STACK) $(ARGS)

## Condition-coding coverage per phase for the published snapshot (read-only)
condition-stats:
	@test -x .verify-venv/bin/python || $(VERIFY_PY) -m venv .verify-venv
	@.verify-venv/bin/python -m pip install -q -r src/requirements.txt boto3
	.verify-venv/bin/python scripts/condition_stats.py --stack $(STACK)

## LLM tier for condition coding: MODEL=<Bedrock model or inference-profile id> (README: "LLM tier")
llm-eval llm-code llm-try llm-code-cloud llm-xeval:
	@test -n "$(MODEL)" || (echo "Set MODEL=<Bedrock model or inference-profile id>" && exit 1)
	@test -x .verify-venv/bin/python || $(VERIFY_PY) -m venv .verify-venv
	@.verify-venv/bin/python -m pip install -q -r src/requirements.txt boto3
	.verify-venv/bin/python scripts/llm_code.py $(patsubst code-cloud,cloud,$(@:llm-%=%)) --stack $(STACK) --model "$(MODEL)" $(ARGS)

## Re-validate cached LLM answers under the current evidence rules, then re-code only
## studies still lacking a verified high-confidence category
llm-retry:
	@test -n "$(MODEL)" || (echo "Set MODEL=<Bedrock model or inference-profile id>" && exit 1)
	@test -x .verify-venv/bin/python || $(VERIFY_PY) -m venv .verify-venv
	@.verify-venv/bin/python -m pip install -q -r src/requirements.txt boto3
	.verify-venv/bin/python scripts/llm_code.py code --stack $(STACK) --model "$(MODEL)" --retry-unverified $(ARGS)

## Merge hand labels from the llm-xeval review CSV into data/eval.json (FILE=<csv>)
llm-adjudicate:
	@test -n "$(FILE)" || (echo "Set FILE=<review csv>" && exit 1)
	@test -x .verify-venv/bin/python || $(VERIFY_PY) -m venv .verify-venv
	@.verify-venv/bin/python -m pip install -q -r src/requirements.txt boto3
	.verify-venv/bin/python scripts/llm_code.py xeval --stack $(STACK) --adjudicate "$(FILE)"

## Cancer trial finder PoV: extract patient-matching fields for recruiting cancer
## trials with a US/CA/AU/UK site (phase 2+) and publish data/trials.json for the "Cancer Trials" tab
trial-match:
	@test -n "$(MODEL)" || (echo "Set MODEL=<Bedrock model or inference-profile id>" && exit 1)
	@test -x .verify-venv/bin/python || $(VERIFY_PY) -m venv .verify-venv
	@.verify-venv/bin/python -m pip install -q -r src/requirements.txt boto3
	.verify-venv/bin/python scripts/trial_match.py --stack $(STACK) --model "$(MODEL)" $(ARGS)

## Trial duration vs ICD-10-CM chapter: censored KM medians, adjusted AFT/Cox models, duration
## decomposition (writes duration_report/report.md + results.json; read-only)
# pandas/scipy/lifelines are ~500 MB: too big for CloudShell's 1 GB home, so the venv lives in /tmp
ANALYSIS_VENV ?= /tmp/ct-analysis-venv
duration-experiments:
	@test -x $(ANALYSIS_VENV)/bin/python || $(VERIFY_PY) -m venv $(ANALYSIS_VENV)
	@$(ANALYSIS_VENV)/bin/python -m pip install -q --no-cache-dir -r src/requirements.txt boto3 lifelines
	$(ANALYSIS_VENV)/bin/python scripts/duration_experiments.py --stack $(STACK) $(ARGS)

## Progress of the latest llm-code-cloud run
llm-status:
	@test -x .verify-venv/bin/python || $(VERIFY_PY) -m venv .verify-venv
	@.verify-venv/bin/python -m pip install -q -r src/requirements.txt boto3
	.verify-venv/bin/python scripts/llm_code.py status --stack $(STACK)

## Data integrity checks against the live stack (README: "Verifying the data")
VERIFY_PY := $(shell command -v python$(RUNTIME) || echo $(PY))
verify:
	@test -x .verify-venv/bin/python || $(VERIFY_PY) -m venv .verify-venv
	@.verify-venv/bin/python -m pip install -q -r src/requirements.txt boto3
	.verify-venv/bin/python scripts/verify.py --stack $(STACK)

## Local pipeline + dashboards, no AWS needed (MAX_PAGES=0 fetches everything)
MAX_PAGES ?= 3
local:
	$(PY) scripts/local_pipeline.py --max-pages $(MAX_PAGES)

serve:
	$(PY) scripts/dev_server.py
