#!/usr/bin/env python3
"""
LLM tier for condition coding (ROADMAP.md phase 1, steps 4-5), run from AWS CloudShell.

  make llm-eval MODEL=<id> [ARGS="--sample 300"]
      Codes a sample of studies that already have exact MeSH-derived ICD-10-CM codes (never shown to the model)
      and prints a scorecard. Writes only its own cache, reference/llm_eval_cache.json.
  make llm-code MODEL=<id> [ARGS="--phase PHASE3,PHASE2/PHASE3 --limit 1000"]
      Codes studies with no exact ICD-10-CM code into reference/llm_cache.json (resumable: rerun to continue).
      `make reference` then publishes them in study_condition_codes as via='llm'.
  make llm-retry MODEL=<id> [ARGS="--phase PHASE3"]
      Same as llm-code --retry-unverified: first re-validates every cached answer's evidence under the
      current matching rules (rescues codes a stricter gate rejected; free), then re-codes only the
      studies still lacking a verified high-confidence category.

  make llm-try MODEL=<id> [ARGS="--phase PHASE3 --limit 10 --fields brief_summary"]
      Small manual test: codes a few ambiguous studies (no exact code, vague condition name like "Solid Tumors") from
      only the chosen text fields and prints a table; --ids NCT1,NCT2 picks the studies. Writes no cache.

  make llm-code-cloud MODEL=<id> [ARGS="--phase PHASE3,PHASE2/PHASE3"]
      Same as llm-code, but runs inside AWS (LLM state machine: 12-minute Lambda passes until every study is
      answered, then the Reference function publishes the codes), so CloudShell can be closed.
  make llm-status
      Progress of the latest llm-code-cloud run: passes, studies still to code, tokens and estimated cost.

  make llm-xeval MODEL=<id> [ARGS="--sample 300 --phase PHASE3 --seed 1 --review xeval_review.csv"]
      Cross-evaluation experiment: a random sample of Phase-3 studies (healthy-volunteer and vaccine
      trials excluded) is coded by the LLM and compared to the deterministic codes already in the
      snapshot. Writes data/eval.json to the site bucket (dashboard "Coding eval" tab) and, with
      --review, a CSV of the disagreements to hand-check. Reuses the eval cache, so reruns resume.
  make llm-adjudicate FILE=<review csv>
      Folds the hand labels (your_check: llm|classic|both|neither) into data/eval.json.

MODEL is a Bedrock model or inference-profile id. --review FILE also writes every answer to a CSV for spot checks.
--price-* (USD per million tokens) turn the token counts into a cost estimate.
"""

import argparse
import csv
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ct_pipeline import llm, llm_run, xeval  # noqa: E402
from ct_pipeline.storage import LocalStore, S3Store, open_store  # noqa: E402

EVAL_CACHE_KEY = "reference/llm_eval_cache.json"
GOLD_SQL = """
    SELECT nct_id, list_sort(list(DISTINCT icd10_category)) AS gold FROM study_condition_codes
    WHERE code_system = 'ICD10CM' AND via = 'mesh' AND match_type = 'exact'
      AND icd10_category IS NOT NULL AND icd10_category <> 'Z00' {phase}
    GROUP BY 1 ORDER BY hash(nct_id || ?) LIMIT ?
"""
VAGUE_CONDITION = (
    r"^((advanced|metastatic|locally advanced|recurrent|refractory|unresectable|other)[ ,/]+)*"
    r"(solid )?(tumou?rs?|cancers?|neoplasms?|malignanc(y|ies)|carcinomas?)$"
)
AMBIGUOUS_SQL = f"""
    SELECT s.nct_id FROM studies s
    WHERE list_contains(?::VARCHAR[], s.phase_group) AND s.nct_id IN ({llm.TARGETS_SQL})
      AND EXISTS (SELECT 1 FROM study_conditions c
                  WHERE c.nct_id = s.nct_id AND regexp_matches(lower(trim(c.condition_name)), ?))
    ORDER BY hash(s.nct_id || ?) LIMIT ?
"""
CURRENT_CODES_SQL = """
    SELECT nct_id, array_to_string(list_sort(list(DISTINCT code || ' (' || match_type || ')')), ', ')
    FROM study_condition_codes WHERE code_system = 'ICD10CM' AND nct_id IN (SELECT unnest(?::VARCHAR[]))
    GROUP BY 1
"""


def make_coder(model: str, catalog: str):
    return llm.BedrockCoder(model, catalog)


def stack_outputs(stack: str) -> dict:
    import boto3

    desc = boto3.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in desc.get("Outputs", [])}


def sfn_client():
    import boto3

    return boto3.client("stepfunctions")


def open_data(args):
    if args.data:
        return LocalStore(args.data)
    return S3Store(stack_outputs(args.stack)["DataBucketName"])


def open_site(args):
    """Site store for data/eval.json: --site, else <data>/../site locally, else the stack's SiteBucket."""
    if args.site:
        return open_store(args.site)
    if args.data and not args.data.startswith("s3"):
        site = Path(args.data).parent / "site"
        return LocalStore(site) if site.exists() else None
    if args.data:
        return None
    return S3Store(stack_outputs(args.stack)["SiteBucketName"])


def prices(args) -> list:
    return [args.price_in, args.price_out, args.price_cache_read, args.price_cache_write]


def usage_line(usage: dict, price_list) -> str:
    line = ", ".join(f"{k} {usage.get(k, 0):,}" for k in llm_run.USAGE_KEYS)
    cost = llm_run.estimated_cost(usage, price_list)
    if cost is not None:
        line += f"; estimated cost ${cost:,.2f}"
    return line


def start_cloud(args, sfn, arn: str) -> None:
    payload = {
        "model": args.model,
        "phases": [p.strip().upper() for p in (args.phase or "").split(",") if p.strip()],
        "limit": args.limit or 0,
        "workers": args.workers,
        "prices": prices(args),
        "publish": not args.no_publish,
    }
    name = "llm-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    sfn.start_execution(stateMachineArn=arn, name=name, input=json.dumps(payload))
    print(f"Started {name}. CloudShell can be closed; check progress with `make llm-status`.")


def print_status(sfn, arn: str) -> None:
    runs = sfn.list_executions(stateMachineArn=arn, maxResults=1)["executions"]
    if not runs:
        print("No llm-code-cloud runs yet")
        return
    ex = sfn.describe_execution(executionArn=runs[0]["executionArn"])
    payload = json.loads(ex.get("input") or "{}")
    passes = []
    for page in sfn.get_paginator("get_execution_history").paginate(executionArn=runs[0]["executionArn"]):
        for e in page["events"]:
            if e["type"] == "TaskSucceeded":
                result = json.loads(e["taskSucceededEventDetails"].get("output") or "{}").get("Payload")
                if isinstance(result, dict) and "remaining" in result:
                    passes.append(result)
    print(f"{ex['name']}: {ex['status']} (started {ex['startDate']:%Y-%m-%d %H:%M} UTC, model {payload.get('model')})")
    if passes:
        last, usage = passes[-1], {}
        for r in passes:
            for k, v in (r.get("usage") or {}).items():
                usage[k] = usage.get(k, 0) + v
        print(
            f"  {len(passes)} passes done: {sum(r['coded'] for r in passes):,} new answers; "
            f"{last['remaining']:,} of {last['studies']:,} studies still to code"
            + (f"; {last['failed']:,} failed calls in the last pass" if last["failed"] else "")
        )
        print(f"  Tokens: {usage_line(usage, payload.get('prices'))}")
    elif ex["status"] == "RUNNING":
        print("  First pass still running (each pass codes for about 12 minutes)")
    if ex["status"] == "FAILED":
        print(f"  Error: {ex.get('error')}: {(ex.get('cause') or '')[:500]}")
    if ex["status"] == "SUCCEEDED" and payload.get("publish", True):
        print("  Codes published (the Reference function ran); the dashboard shows them within 15 minutes.")


def verified(entry: dict) -> list:
    return [c["code"] for c in entry.get("codes") or [] if c["evidence_found"]]


def write_review(path, docs, entries, gold=None) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["nct_id", "title", "conditions", "gold", "codes", "evidence", "no_code_reason"])
        for d in docs:
            e = entries.get(d["nct_id"])
            if e is None:
                continue
            codes = e.get("codes") or []
            w.writerow(
                [
                    d["nct_id"],
                    d["brief_title"],
                    "; ".join(d["conditions"]),
                    " ".join((gold or {}).get(d["nct_id"], [])),
                    " ".join(
                        f"{c['code']}:{c['confidence']}{'' if c['evidence_found'] else ':unverified'}" for c in codes
                    ),
                    " | ".join(c["evidence"] for c in codes),
                    e.get("no_code_reason", ""),
                ]
            )
    print(f"\nWrote {path}")


def try_studies(con, coder, categories, args, phases) -> dict:
    if args.ids:
        ids = sorted({i.strip().upper() for i in args.ids.split(",") if i.strip()})
    else:
        params = [phases, VAGUE_CONDITION, args.seed, args.limit or 10]
        ids = [r[0] for r in con.execute(AMBIGUOUS_SQL, params).fetchall()]
    docs = llm.study_documents(con, ids)
    missing = sorted(set(ids) - {d["nct_id"] for d in docs})
    if missing:
        print("Not in the snapshot:", ", ".join(missing))
    current = dict(con.execute(CURRENT_CODES_SQL, [ids]).fetchall()) if ids else {}
    fields = tuple(f.strip() for f in args.fields.split(",") if f.strip())
    print(f"Coding {len(docs)} studies with {args.model}, text fields sent: {', '.join(fields) or 'none'}\n")
    usage, rows = {}, []
    for d in docs:
        text = llm.render_study(d, fields)
        try:
            answer, used = coder.code(text)
        except Exception as e:  # noqa: BLE001 - show the error for this study and continue
            answer, used = {"no_code_reason": f"ERROR {type(e).__name__}: {e}"}, {}
        for k, v in used.items():
            usage[k] = usage.get(k, 0) + v
        codes = llm.validate(answer, categories, text)
        rows.append((d, current.get(d["nct_id"], ""), codes, str(answer.get("no_code_reason") or ""), text))
    for d, now, codes, reason, _ in rows:
        print(f"{d['nct_id']}  {d['brief_title'][:90]}")
        print(f"  conditions: {'; '.join(d['conditions'])[:110]}")
        print(f"  current:    {now or '-'}")
        for c in codes:
            mark = "quote found" if c["evidence_found"] else "QUOTE NOT FOUND"
            print(f"  LLM:        {c['code']:4} {categories[c['code']][:55]:55} {c['confidence']:6} {mark}")
            print(f'              "{c["evidence"][:100]}"')
        if not codes:
            print(f"  LLM:        no code - {reason[:100]}")
        print()
    if args.review:
        with open(args.review, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "nct_id",
                    "phase",
                    "title",
                    "conditions",
                    "current_codes",
                    "llm_code",
                    "llm_label",
                    "confidence",
                    "quote_found",
                    "evidence",
                    "no_code_reason",
                    "fields_sent",
                    "model_input",
                    "your_check",
                ]
            )
            for d, now, codes, reason, text in rows:
                base = [
                    d["nct_id"],
                    ",".join(phases) if not args.ids else "",
                    d["brief_title"],
                    "; ".join(d["conditions"]),
                    now,
                ]
                tail = [reason, " ".join(fields), text, ""]
                for c in codes or [None]:
                    cols = (
                        [c["code"], categories[c["code"]], c["confidence"], c["evidence_found"], c["evidence"]]
                        if c
                        else [""] * 5
                    )
                    w.writerow(base + cols + tail)
        print(f"Wrote {args.review} (one row per LLM code; fill in your_check)")
    return usage


def xscorecard(h: dict, studies: list) -> None:
    n = h["studies"]
    if not n:
        print("No studies in the sample.")
        return
    v = h["verdicts"]
    print(f"\nCross-eval ({n:,} sampled studies; 'classic' = every non-LLM via)")
    print(f"  coded by classic (any / exact)      {h['classic_coded'] / n:6.1%} / {h['classic_exact'] / n:.1%}")
    print(f"  coded by LLM (verified / exact)     {h['llm_verified'] / n:6.1%} / {h['llm_exact'] / n:.1%}")
    for verdict in xeval.VERDICTS:
        print(f"  {verdict:28} {v[verdict]:4,}  ({v[verdict] / n:.1%})")
    if h["recall_vs_exact"] is not None:
        print(
            f"  vs classic-exact as reference: recall {h['recall_vs_exact']:.1%}, "
            f"precision {h['precision_vs_exact']:.1%}"
        )
    print(
        f"  LLM exact yield where no exact classic code: {h['llm_yield_where_not_exact']:,} "
        f"({h['llm_yield_broad_only']:,} had broad only, {h['llm_yield_uncoded']:,} uncoded)"
    )
    for conf, b in sorted(h["by_confidence"].items()):
        print(
            f"  confidence {conf:6}: {b['codes']:4,} codes, "
            f"{b['match_any'] / b['codes']:.0%} match a classic code "
            f"({b['match_exact'] / b['codes']:.0%} exact)"
        )


def scorecard(docs, entries, gold: dict, chapters: dict) -> None:
    n = len(entries)
    if not n:
        print("No answers.")
        return
    codes = [c for e in entries.values() for c in e.get("codes") or []]
    preds = {k: set(verified(e)) for k, e in entries.items()}
    with_pred = [k for k in preds if preds[k]]
    hit = sum(1 for k in preds if preds[k] & set(gold[k]))
    recall = sum(len(preds[k] & set(gold[k])) / len(gold[k]) for k in preds) / n
    precision = sum(len(preds[k] & set(gold[k])) / len(preds[k]) for k in with_pred) / max(len(with_pred), 1)
    chap = sum(1 for k in preds if {chapters.get(c) for c in preds[k]} & {chapters.get(c) for c in gold[k]})
    print(f"\nScorecard ({n:,} studies with exact MeSH-derived categories, hidden from the model)")
    print(f"  answered with >=1 verified category  {len(with_pred) / n:6.1%}")
    print(f"  no code                              {sum(1 for e in entries.values() if not e.get('codes')) / n:6.1%}")
    print(f"  quoted evidence found in protocol    {sum(c['evidence_found'] for c in codes) / max(len(codes), 1):6.1%}")
    print(f"  >=1 category agrees with MeSH        {hit / n:6.1%}")
    print(f"  same chapter as MeSH                 {chap / n:6.1%}")
    print(f"  category recall / precision          {recall:6.1%} / {precision:.1%}")
    print("\nDisagreements (MeSH-derived codes can be wrong too):")
    titles = {d["nct_id"]: d["brief_title"] for d in docs}
    shown = 0
    for k in sorted(preds):
        if not preds[k] & set(gold[k]) and shown < 20:
            shown += 1
            print(f"  {k}  MeSH {' '.join(gold[k]):12} LLM {' '.join(sorted(preds[k])) or '-':12} {titles[k][:60]}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=("eval", "code", "try", "cloud", "status", "xeval"))
    ap.add_argument("--model", help="Bedrock model or inference-profile id")
    ap.add_argument("--stack", default="ct-pipeline")
    ap.add_argument("--data", help="local data directory instead of the stack's data bucket")
    ap.add_argument("--sample", type=int, default=300, help="eval: number of studies")
    ap.add_argument("--seed", default="1")
    ap.add_argument("--limit", type=int, help="code: at most this many studies (in NCT order); try: default 10")
    ap.add_argument("--ids", help="try: comma-separated NCT IDs instead of a random ambiguous sample")
    ap.add_argument(
        "--phase",
        help="comma-separated phase_group values, e.g. PHASE3,PHASE2/PHASE3 (eval/code: default all; try: PHASE3)",
    )
    ap.add_argument(
        "--fields",
        default="brief_summary",
        help=f"try: comma-separated text fields to send, from {', '.join(llm.TEXT_FIELDS)}",
    )
    ap.add_argument("--workers", type=int, default=llm.WORKERS)
    ap.add_argument(
        "--retry-unverified",
        action="store_true",
        help="code: re-validate cached answers under the current evidence rules, then re-code only "
        "studies still lacking a verified high-confidence category",
    )
    ap.add_argument("--review", help="write answers to this CSV")
    ap.add_argument(
        "--adjudicate",
        metavar="FILE",
        help="xeval: merge hand labels (your_check) from the review CSV into data/eval.json",
    )
    ap.add_argument("--site", help="xeval: site bucket/dir for data/eval.json (default: the stack's)")
    for name in ("in", "out", "cache-read", "cache-write"):
        ap.add_argument(f"--price-{name}", type=float, default=0.0)
    ap.add_argument("--no-publish", action="store_true", help="cloud: don't run the Reference function at the end")
    args = ap.parse_args()
    if args.mode == "status":
        print_status(sfn_client(), stack_outputs(args.stack)["LlmStateMachineArn"])
        return
    if not args.model and not (args.mode == "xeval" and args.adjudicate):
        sys.exit("Set MODEL to a Bedrock model or inference-profile id (see README: LLM tier)")
    if args.mode == "xeval" and args.adjudicate:
        site = open_site(args)
        if site is None:
            sys.exit("No site store for data/eval.json: pass --site or run against the stack")
        payload = site.get_json(xeval.EVAL_KEY)
        if payload is None:
            sys.exit(f"No {xeval.EVAL_KEY} yet: run the xeval first")
        payload = xeval.merge_adjudication(payload, args.adjudicate)
        site.put_json(xeval.EVAL_KEY, payload)
        print(f"Merged {payload['adjudicated']['reviewed']} hand labels into {xeval.EVAL_KEY}:")
        a = payload["adjudicated"]
        print(f"  llm right {a['llm']}, classic right {a['classic']}, both {a['both']}, neither {a['neither']}")
        return
    unknown = {f.strip() for f in args.fields.split(",") if f.strip()} - set(llm.TEXT_FIELDS)
    if unknown:
        sys.exit(f"Unknown --fields {', '.join(sorted(unknown))}; choose from {', '.join(llm.TEXT_FIELDS)}")
    if args.mode == "cloud":
        start_cloud(args, sfn_client(), stack_outputs(args.stack)["LlmStateMachineArn"])
        return

    store = open_data(args)
    with tempfile.TemporaryDirectory(prefix="ct-llm-") as tmp:
        work = Path(tmp)
        try:
            current, con, icd = llm_run.open_snapshot(store, work)
            phases = llm_run.parse_phases(con, args.phase)
        except llm.LlmError as e:
            sys.exit(str(e))
        catalog, categories = llm.category_catalog(con, icd)
        coder = make_coder(args.model, catalog)
        where, params = llm_run.phase_filter(phases)
        in_phases = f" in {', '.join(phases)}" if phases else ""
        progress = lambda d, n: print(f"  {d:,}/{n:,}", flush=True)  # noqa: E731

        if args.mode == "try":
            print(f"Snapshot {current['run_id']}", flush=True)
            usage = try_studies(con, coder, categories, args, phases or ["PHASE3"])
            print(f"Tokens: {usage_line(usage, prices(args))}")
            return
        if args.mode == "xeval":
            phases = phases or ["PHASE3"]
            pool = xeval.pool_counts(con, phases)
            ids = xeval.sample_ids(con, phases, args.seed, args.sample)
            classic = xeval.classic_codes(con, ids)
            docs = llm.study_documents(con, ids)
            print(
                f"Snapshot {current['run_id']}: {len(docs):,} {'/'.join(phases)} studies "
                f"(of {pool['eligible']:,} eligible; {pool['healthy']:,} healthy and {pool['vaccine']:,} "
                f"vaccine studies excluded); coding with {args.model}",
                flush=True,
            )
            entries, info = llm.code_studies(
                coder,
                store,
                docs,
                categories,
                workers=args.workers,
                progress=progress,
                cache_key=EVAL_CACHE_KEY,
            )
            # all levels, so block codes (e.g. C00-D49) also map to a chapter for chapter_only verdicts
            chapters = dict(con.execute(f"SELECT code, chapter FROM read_parquet('{icd}')").fetchall())
            studies = xeval.compare(docs, classic, entries, chapters)
            payload = xeval.eval_payload(current, phases, args.seed, args.model, studies, pool)
            site = open_site(args)
            if site is not None:
                site.put_json(xeval.EVAL_KEY, payload)
                print(f"\nWrote {xeval.EVAL_KEY} ({site.location(xeval.EVAL_KEY)})")
            else:
                print("\nPass --site to publish data/eval.json; nothing uploaded.")
            xscorecard(payload["headline"], studies)
            print(
                f"\nCalls: {info['coded']:,} new, {info['cached']:,} cached, {info['failed']:,} failed",
                info["errors"] or "",
            )
            print(f"Tokens: {usage_line(info['usage'], prices(args))}")
            if args.review:
                n = xeval.write_review(args.review, studies)
                print(
                    f"\nWrote {args.review}: {n} studies to hand-check "
                    "(fill your_check: llm|classic|both|neither, then `make llm-adjudicate FILE=...`)"
                )
            return
        if args.mode == "eval":
            gold = dict(con.execute(GOLD_SQL.format(phase=where), params + [args.seed, args.sample]).fetchall())
            docs = llm.study_documents(con, sorted(gold))
            print(f"Snapshot {current['run_id']}: coding {len(docs):,} studies with {args.model}", flush=True)
            entries, info = llm.code_studies(
                coder, store, docs, categories, workers=args.workers, progress=progress, cache_key=EVAL_CACHE_KEY
            )
            chapters = dict(
                con.execute(f"SELECT code, chapter FROM read_parquet('{icd}') WHERE level = 'category'").fetchall()
            )
            scorecard(docs, entries, gold, chapters)
        else:
            ids = llm_run.code_targets(con, phases)
            print(
                f"Snapshot {current['run_id']}: {len(ids):,} studies{in_phases} have no exact ICD-10-CM code",
                flush=True,
            )
            ids = ids[: args.limit] if args.limit else ids
            docs = llm.study_documents(con, ids)
            if args.retry_unverified:
                cache = (store.get_json(llm.CACHE_KEY) or {}).get("entries", {})
                retry, rescued, revalidated = [], 0, 0
                for d in docs:
                    e = cache.get(d["nct_id"])
                    if not (e or {}).get("codes"):
                        retry.append(d)
                        continue
                    revalidated += 1
                    if llm.revalidate_entry(e, llm.render_study(d), categories):
                        rescued += 1
                    if not llm.strong_codes(e):
                        retry.append(d)
                if rescued:
                    store.put_json(
                        llm.CACHE_KEY,
                        {"updated_at": datetime.now(timezone.utc).isoformat(), "entries": cache},
                    )
                print(
                    f"Re-validated {revalidated:,} cached answers under the current evidence rules "
                    f"({rescued:,} changed); {len(retry):,} studies still lack a verified "
                    "high-confidence code",
                    flush=True,
                )
                docs = retry
            print(f"Coding {len(docs):,} with {args.model} (cached answers are reused)", flush=True)
            entries, info = llm.code_studies(coder, store, docs, categories, workers=args.workers, progress=progress)
            coded = [e for e in entries.values() if verified(e)]
            print(f"\n{len(coded):,} of {len(entries):,} answered studies got >=1 verified category")
            counts = {}
            for e in coded:
                for c in verified(e):
                    counts[c] = counts.get(c, 0) + 1
            for code, n in sorted(counts.items(), key=lambda kv: -kv[1])[:15]:
                print(f"  {n:6,}  {code:5} {categories[code][:60]}")
            print("\nRun `make reference` to publish these codes (via='llm') to the dashboard data.")
            gold = None
        print(
            f"\nCalls: {info['coded']:,} new, {info['cached']:,} cached, {info['failed']:,} failed",
            info["errors"] or "",
        )
        print(f"Tokens: {usage_line(info['usage'], prices(args))}")
        if args.review:
            write_review(args.review, docs, entries, gold)


if __name__ == "__main__":
    main()
