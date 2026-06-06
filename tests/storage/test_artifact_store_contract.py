"""Cross-backend ArtifactStore contract suite (spec §11).

One parametrized fixture runs the same behavioural assertions against
BOTH backends:
  * LocalFsStore(tmp_path)            — filesystem
  * S3Store(bucket) on moto mock_aws  — S3

Anything that must hold identically on every backend lives here.
Backend-specific mechanics (LocalFs temp-file/traversal guard, S3
truncation stub, the empty-bucket guard) stay in their own files.
"""
from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from botnim.storage.local_fs import LocalFsStore
from botnim.storage.s3_store import S3Store


_BUCKET = "botnim-artifacts-contract-test"
_REGION = "il-central-1"


@pytest.fixture(params=["local_fs", "s3"])
def store(request, tmp_path, monkeypatch):
    """Yield each ArtifactStore backend in turn for the same test body."""
    if request.param == "local_fs":
        yield LocalFsStore(str(tmp_path))
        return

    with mock_aws():
        monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        s3 = boto3.client("s3", region_name=_REGION)
        s3.create_bucket(
            Bucket=_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": _REGION},
        )
        yield S3Store(_BUCKET, region_name=_REGION)


def test_put_then_get_roundtrip(store):
    store.put_atomic("seed/unified/a/b.json", b"hello")
    assert store.get_bytes("seed/unified/a/b.json") == b"hello"


def test_get_bytes_missing_raises_filenotfound(store):
    with pytest.raises(FileNotFoundError):
        store.get_bytes("seed/unified/missing.json")


def test_open_stream_reads_full_body(store):
    store.put_atomic("cache/unified/s.json", b"streamed-body")
    with store.open_stream("cache/unified/s.json") as fh:
        assert fh.read() == b"streamed-body"


def test_open_stream_missing_raises_filenotfound(store):
    with pytest.raises(FileNotFoundError):
        store.open_stream("cache/unified/missing.json")


def test_put_atomic_overwrite(store):
    store.put_atomic("seed/unified/v.json", b"v1")
    store.put_atomic("seed/unified/v.json", b"v2-longer")
    assert store.get_bytes("seed/unified/v.json") == b"v2-longer"


def test_exists(store):
    assert store.exists("seed/unified/e.json") is False
    store.put_atomic("seed/unified/e.json", b"e")
    assert store.exists("seed/unified/e.json") is True


def test_list_returns_keys_under_prefix(store):
    store.put_atomic("cache/wikitext/unified/aaa__v1.json", b"1")
    store.put_atomic("cache/wikitext/unified/bbb__v1.json", b"2")
    store.put_atomic("cache/wikitext/other/ccc__v1.json", b"3")
    got = sorted(store.list("cache/wikitext/unified/"))
    assert got == [
        "cache/wikitext/unified/aaa__v1.json",
        "cache/wikitext/unified/bbb__v1.json",
    ]


def test_list_empty_prefix_returns_empty(store):
    assert store.list("cache/wikitext/nope/") == []
