import os
from dataclasses import dataclass

DEFAULT_QUERY_TERM = "AREA[Phase](EARLY_PHASE1 OR PHASE1 OR PHASE2 OR PHASE3 OR PHASE4)"
DEFAULT_FIELDS = "protocolSection,derivedSection,hasResults"

RAW_PREFIX = "raw/"
STAGING_PREFIX = "staging/"
CURATED_PREFIX = "curated/"
CURRENT_KEY = "curated/CURRENT.json"
WATERMARK_KEY = "state/watermark.json"
OVERVIEW_KEY = "data/overview.json"


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


@dataclass(frozen=True)
class Settings:
    data_uri: str
    site_uri: str
    query_term: str
    fields: str
    page_size: int
    start_year: str
    glue_database: str
    keep_snapshots: int
    work_dir: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            data_uri=_env("CT_DATA_URI"),
            site_uri=_env("CT_SITE_URI"),
            query_term=_env("CT_QUERY_TERM", DEFAULT_QUERY_TERM),
            fields=_env("CT_FIELDS", DEFAULT_FIELDS),
            page_size=int(_env("CT_PAGE_SIZE", "1000")),
            start_year=_env("CT_START_YEAR"),
            glue_database=_env("CT_GLUE_DATABASE"),
            keep_snapshots=int(_env("CT_KEEP_SNAPSHOTS", "3")),
            work_dir=_env("CT_WORK_DIR", "/tmp/ct"),
        )
