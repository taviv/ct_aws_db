"""Minimal object-store abstraction: S3 in AWS, a local directory for tests/dev."""

import json
import shutil
from pathlib import Path


class Store:
    def get_bytes(self, key: str) -> bytes | None:
        raise NotImplementedError

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        raise NotImplementedError

    def list_keys(self, prefix: str) -> list:
        raise NotImplementedError

    def download(self, key: str, path: Path) -> None:
        raise NotImplementedError

    def upload(self, path: Path, key: str, content_type: str = "application/octet-stream") -> None:
        raise NotImplementedError

    def delete_keys(self, keys: list) -> None:
        raise NotImplementedError

    def location(self, key: str) -> str:
        raise NotImplementedError

    def get_json(self, key: str):
        data = self.get_bytes(key)
        return json.loads(data) if data is not None else None

    def put_json(self, key: str, obj) -> None:
        self.put_bytes(key, json.dumps(obj, default=str).encode("utf-8"), "application/json")

    def delete_prefix(self, prefix: str) -> None:
        self.delete_keys(self.list_keys(prefix))


class LocalStore(Store):
    def __init__(self, root: str):
        self.root = Path(root)

    def _path(self, key: str) -> Path:
        return self.root / key

    def get_bytes(self, key):
        p = self._path(key)
        return p.read_bytes() if p.is_file() else None

    def put_bytes(self, key, data, content_type="application/octet-stream"):
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def list_keys(self, prefix):
        if not self.root.exists():
            return []
        keys = (p.relative_to(self.root).as_posix() for p in self.root.rglob("*") if p.is_file())
        return sorted(k for k in keys if k.startswith(prefix))

    def download(self, key, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self._path(key), path)

    def upload(self, path, key, content_type="application/octet-stream"):
        dest = self._path(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, dest)

    def delete_keys(self, keys):
        for k in keys:
            self._path(k).unlink(missing_ok=True)

    def location(self, key):
        return str(self._path(key))


class S3Store(Store):
    def __init__(self, bucket: str, prefix: str = "", client=None):
        self.bucket = bucket
        self.prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""
        self._client = client

    @property
    def client(self):
        if self._client is None:
            import boto3

            self._client = boto3.client("s3")
        return self._client

    def _key(self, key: str) -> str:
        return self.prefix + key

    def get_bytes(self, key):
        from botocore.exceptions import ClientError

        try:
            return self.client.get_object(Bucket=self.bucket, Key=self._key(key))["Body"].read()
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404", "NotFound"):
                return None
            raise

    def put_bytes(self, key, data, content_type="application/octet-stream"):
        self.client.put_object(Bucket=self.bucket, Key=self._key(key), Body=data, ContentType=content_type)

    def list_keys(self, prefix):
        keys = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._key(prefix)):
            keys.extend(obj["Key"][len(self.prefix) :] for obj in page.get("Contents", []))
        return sorted(keys)

    def download(self, key, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(self.bucket, self._key(key), str(path))

    def upload(self, path, key, content_type="application/octet-stream"):
        self.client.upload_file(str(path), self.bucket, self._key(key), ExtraArgs={"ContentType": content_type})

    def delete_keys(self, keys):
        for i in range(0, len(keys), 1000):
            batch = [{"Key": self._key(k)} for k in keys[i : i + 1000]]
            if batch:
                self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": batch, "Quiet": True})

    def location(self, key):
        return f"s3://{self.bucket}/{self._key(key)}"


def open_store(uri: str) -> Store:
    """``s3://bucket[/prefix]`` → S3Store; ``file:///path`` or a plain path → LocalStore."""
    if not uri:
        raise ValueError("Storage URI is empty (set CT_DATA_URI / CT_SITE_URI).")
    if uri.startswith("s3://"):
        bucket, _, prefix = uri[5:].partition("/")
        return S3Store(bucket, prefix)
    if uri.startswith("file://"):
        uri = uri[7:]
    return LocalStore(uri)
