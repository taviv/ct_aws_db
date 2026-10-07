import boto3
import pytest
from moto import mock_aws

from ct_pipeline.glue import register_tables
from ct_pipeline.storage import S3Store, open_store


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        yield


def test_s3_store_roundtrip(aws, tmp_path):
    boto3.client("s3").create_bucket(Bucket="test-bucket")
    store = open_store("s3://test-bucket/pre")
    assert isinstance(store, S3Store)
    assert store.get_json("missing.json") is None
    store.put_json("state/x.json", {"a": 1})
    assert store.get_json("state/x.json") == {"a": 1}
    src = tmp_path / "f.bin"
    src.write_bytes(b"data")
    store.upload(src, "curated/R1/t/data.parquet")
    store.download("curated/R1/t/data.parquet", tmp_path / "out" / "f.bin")
    assert (tmp_path / "out" / "f.bin").read_bytes() == b"data"
    assert store.list_keys("curated/") == ["curated/R1/t/data.parquet"]
    assert store.location("curated/R1/t/") == "s3://test-bucket/pre/curated/R1/t/"
    store.delete_prefix("curated/")
    assert store.list_keys("curated/") == []


def test_glue_register_creates_then_updates(aws):
    glue = boto3.client("glue")
    glue.create_database(DatabaseInput={"Name": "ct"})
    cols = [("nct_id", "VARCHAR"), ("start_date", "DATE"), ("phases", "VARCHAR[]")]
    register_tables(glue, "ct", {"studies": ("s3://b/curated/R1/studies/", cols)})
    register_tables(glue, "ct", {"studies": ("s3://b/curated/R2/studies/", cols)})
    table = glue.get_table(DatabaseName="ct", Name="studies")["Table"]
    assert table["StorageDescriptor"]["Location"] == "s3://b/curated/R2/studies/"
    assert [c["Type"] for c in table["StorageDescriptor"]["Columns"]] == ["string", "date", "array<string>"]
