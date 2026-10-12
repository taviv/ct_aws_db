"""Lambda entry points (one code package, five functions)."""

import logging

from . import fetch
from .build import build_snapshot, recode_snapshot
from .config import CURRENT_KEY, Settings
from .query_api import lambda_handler as query_handler  # noqa: F401
from .reference import build_reference
from .storage import open_store
from .transform import transform_page

logging.getLogger().setLevel(logging.INFO)


def fetch_handler(event, context):
    settings = Settings.from_env()
    store = open_store(settings.data_uri)
    action = event.get("action", "start")
    if action == "start":
        return fetch.start(event.get("input") or event, settings, store)
    if action == "fetch_page":
        return fetch.fetch_page(event["state"], settings, store)
    if action == "finalize":
        return fetch.finalize(event["state"], settings, store)
    raise ValueError(f"Unknown action {action!r}; expected start | fetch_page | finalize")


def transform_handler(event, context):
    settings = Settings.from_env()
    return transform_page(settings, open_store(settings.data_uri), event["run_id"], event["key"])


def build_handler(event, context):
    import boto3

    settings = Settings.from_env()
    glue = boto3.client("glue") if settings.glue_database else None
    return build_snapshot(
        settings,
        open_store(settings.data_uri),
        open_store(settings.site_uri) if settings.site_uri else None,
        event["run_id"],
        event["mode"],
        event["fetch_started_at"],
        glue_client=glue,
    )


def reference_handler(event, context):
    """Refresh reference/<version>/ and, unless {"recode": false}, recode the current snapshot with it."""
    import boto3

    settings = Settings.from_env()
    store = open_store(settings.data_uri)
    pointer = build_reference(store, settings.work_dir)
    result = {"reference_version": pointer["version"], "row_counts": pointer["row_counts"]}
    if (event or {}).get("recode", True) and store.get_json(CURRENT_KEY) is not None:
        result["recode"] = recode_snapshot(
            settings,
            store,
            open_store(settings.site_uri) if settings.site_uri else None,
            glue_client=boto3.client("glue") if settings.glue_database else None,
        )
    return result
