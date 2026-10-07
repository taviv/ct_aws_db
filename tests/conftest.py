import json
from pathlib import Path

import pytest

from ct_pipeline.config import Settings
from ct_pipeline.storage import LocalStore

FIXTURE = Path(__file__).parent / "fixtures" / "studies.ndjson"


def load_studies():
    return [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]


def ndjson(studies) -> bytes:
    return "\n".join(json.dumps(s) for s in studies).encode()


def make_study(
    nct_id,
    phases=("PHASE1",),
    start="2020-01-15",
    completion="2021-01-15",
    status="COMPLETED",
    updated="2024-01-01",
    countries=("United States",),
    title=None,
):
    return {
        "protocolSection": {
            "identificationModule": {"nctId": nct_id, "briefTitle": title or f"Study {nct_id}"},
            "statusModule": {
                "overallStatus": status,
                "startDateStruct": {"date": start},
                "completionDateStruct": {"date": completion},
                "lastUpdatePostDateStruct": {"date": updated},
            },
            "designModule": {"studyType": "INTERVENTIONAL", "phases": list(phases)},
            "sponsorCollaboratorsModule": {"leadSponsor": {"name": "Acme", "class": "INDUSTRY"}},
            "eligibilityModule": {"healthyVolunteers": False},
            "contactsLocationsModule": {"locations": [{"facility": f"Site {c}", "country": c} for c in countries]},
        },
        "hasResults": False,
    }


@pytest.fixture
def settings(tmp_path):
    return Settings(
        data_uri=str(tmp_path / "data"),
        site_uri=str(tmp_path / "site"),
        query_term="AREA[Phase](PHASE1 OR PHASE2)",
        fields="protocolSection,derivedSection,hasResults",
        page_size=100,
        start_year="",
        glue_database="",
        keep_snapshots=2,
        work_dir=str(tmp_path / "work"),
    )


@pytest.fixture
def store(settings):
    return LocalStore(settings.data_uri)


@pytest.fixture
def site_store(settings):
    return LocalStore(settings.site_uri)
