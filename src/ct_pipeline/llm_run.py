"""Inputs of an LLM coding run, shared by scripts/llm_code.py (CloudShell) and the LLM Lambda (Step Functions loop)."""

from pathlib import Path

from . import llm
from .build import attach_snapshot
from .config import CURRENT_KEY
from .db import connect
from .reference import download_reference

TABLES = ("studies", "study_text", "study_conditions", "study_interventions", "study_condition_codes")
CODE_SQL = f"SELECT nct_id FROM ({llm.TARGETS_SQL}) WHERE TRUE {{phase}} ORDER BY nct_id"
PHASE_FILTER = "AND nct_id IN (SELECT nct_id FROM studies WHERE list_contains(?::VARCHAR[], phase_group))"
USAGE_KEYS = ("inputTokens", "outputTokens", "cacheReadInputTokens", "cacheWriteInputTokens")


def open_snapshot(store, work: Path, tables=TABLES) -> tuple:
    """Download the current snapshot's `tables` and the reference; returns (current, con, icd_path)."""
    current = store.get_json(CURRENT_KEY)
    if current is None:
        raise llm.LlmError("No published snapshot yet (curated/CURRENT.json)")
    if "study_condition_codes" not in current["tables"]:
        raise llm.LlmError("The snapshot has no study_condition_codes yet: run `make reference` first")
    for table in tables:
        store.download(current["tables"][table], work / "snapshot" / table / "data.parquet")
    if download_reference(store, work / "reference") is None:
        raise llm.LlmError("No reference yet: run `make reference` first")
    con = connect(work)
    attach_snapshot(con, work / "snapshot", list(tables))
    return current, con, work / "reference" / "icd10cm_codes" / "data.parquet"


# natural shorthands people type for the combined-phase values
PHASE_ALIASES = {"PHASE1/2": "PHASE1/PHASE2", "PHASE2/3": "PHASE2/PHASE3"}


def parse_phases(con, phases) -> list:
    """phase_group values from a list or a 'PHASE3,PHASE2/PHASE3' string, upper-cased; LlmError if unknown."""
    if isinstance(phases, str):
        phases = phases.split(",")
    phases = [PHASE_ALIASES.get(p, p) for p in (p.strip().upper() for p in phases or []) if p]
    known = {r[0] for r in con.execute("SELECT DISTINCT phase_group FROM studies").fetchall()}
    unknown = sorted(set(phases) - known)
    if unknown:
        raise llm.LlmError(f"Unknown --phase {', '.join(unknown)}; choose from {', '.join(sorted(known))}")
    return phases


def phase_filter(phases: list) -> tuple:
    return (PHASE_FILTER, [phases]) if phases else ("", [])


def code_targets(con, phases: list) -> list:
    """NCT IDs `llm-code` codes: no exact ICD-10-CM code yet, in the given phase groups (all if empty)."""
    where, params = phase_filter(phases)
    return [r[0] for r in con.execute(CODE_SQL.format(phase=where), params).fetchall()]


def estimated_cost(usage: dict, prices) -> float | None:
    """USD for Bedrock token counts, given prices per million tokens in USAGE_KEYS order; None without prices."""
    if not prices or not any(prices):
        return None
    return sum(usage.get(k, 0) * float(p) for k, p in zip(USAGE_KEYS, prices, strict=True)) / 1e6
