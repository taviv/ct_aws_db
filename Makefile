STACK ?= ct-pipeline
PY    ?= python3

.PHONY: install test lint fmt validate build deploy dashboards backfill run local serve

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

build:
	sam build

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

## Local pipeline + dashboards, no AWS needed (MAX_PAGES=0 fetches everything)
MAX_PAGES ?= 3
local:
	$(PY) scripts/local_pipeline.py --max-pages $(MAX_PAGES)

serve:
	$(PY) scripts/dev_server.py
