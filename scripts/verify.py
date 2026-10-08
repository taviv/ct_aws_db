#!/usr/bin/env python3
"""
Check the live stack's data: `make verify` (or `python scripts/verify.py --stack ct-pipeline`).

Reads the stack's outputs and parameters, then runs ct_pipeline.verify against the published
snapshot, the ClinicalTrials.gov API and the dashboard. Exits 1 if any check fails.
"""

import argparse
import sys
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ct_pipeline.config import DEFAULT_QUERY_TERM  # noqa: E402
from ct_pipeline.fetch import build_query_term  # noqa: E402
from ct_pipeline.storage import S3Store  # noqa: E402
from ct_pipeline.verify import report, run_checks  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stack", default="ct-pipeline")
    ap.add_argument("--sample", type=int, default=20, help="studies to spot-check against the API (0 = skip)")
    args = ap.parse_args()

    stack = boto3.client("cloudformation").describe_stacks(StackName=args.stack)["Stacks"][0]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    params = {p["ParameterKey"]: p.get("ParameterValue", "") for p in stack.get("Parameters", [])}
    start_year = params.get("StartYear", "")
    query_term = build_query_term(params.get("QueryTerm") or DEFAULT_QUERY_TERM, start_year)

    executions = boto3.client("stepfunctions").list_executions(
        stateMachineArn=outputs["StateMachineArn"], maxResults=1
    )["executions"]

    print(f"Stack {args.stack}, query: {query_term}\n")
    checks = run_checks(
        S3Store(outputs["DataBucketName"]),
        query_term,
        start_year=start_year,
        dashboard_url=outputs["DashboardUrl"],
        latest_execution=executions[0] if executions else None,
        sample=args.sample,
    )
    sys.exit(0 if report(checks) else 1)


if __name__ == "__main__":
    main()
