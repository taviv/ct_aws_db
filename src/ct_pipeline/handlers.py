"""Lambda entry points (one code package, five functions)."""

import logging
import time

from . import fetch
from .build import build_snapshot, recode_snapshot
from .config import CURRENT_KEY, Settings
from .query_api import lambda_handler as query_handler  # noqa: F401
from .reference import build_reference, http_get
from .storage import open_store
from .transform import transform_page
from .umls import UmlsClient

logger = logging.getLogger(__name__)

logging.getLogger().setLevel(logging.INFO)


def umls_api_key(secret_name: str) -> str | None:
    """The UMLS API key from Secrets Manager; None (UMLS layer skipped) if unset or the secret doesn't exist."""
    if not secret_name:
        return None
    import boto3

    client = boto3.client("secretsmanager")
    try:
        return client.get_secret_value(SecretId=secret_name)["SecretString"].strip() or None
    except client.exceptions.ResourceNotFoundException:
        logger.warning("Secret %s not found; skipping the UMLS layer", secret_name)
        return None


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
    key = umls_api_key(settings.umls_secret_name)
    umls_client = UmlsClient(key, http_get) if key else None
    # leave ~5 minutes of the 15 for writing the reference and recoding; unfinished lookups resume next run
    deadline = time.monotonic() + context.get_remaining_time_in_millis() / 1000 - 300 if context else None
    pointer = build_reference(store, settings.work_dir, umls_client=umls_client, deadline=deadline)
    result = {"reference_version": pointer["version"], "row_counts": pointer["row_counts"]}
    if (event or {}).get("recode", True) and store.get_json(CURRENT_KEY) is not None:
        result["recode"] = recode_snapshot(
            settings,
            store,
            open_store(settings.site_uri) if settings.site_uri else None,
            glue_client=boto3.client("glue") if settings.glue_database else None,
        )
    return result
