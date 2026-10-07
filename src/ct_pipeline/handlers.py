"""Lambda entry points (one code package, four functions)."""

import logging

from . import fetch
from .build import build_snapshot
from .config import Settings
from .query_api import lambda_handler as query_handler  # noqa: F401
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
