"""Behavioural tests for S3Store.

moto-backed S3 (per tests/backend/api/test_generate_word_doc.py) covers
round-trip / exists / list / atomic-overwrite. The truncated-read path
uses a hand-rolled fake client because moto always returns an accurate
Content-Length, so the short-read guard can only be exercised with a
stub that lies. No DB needed.
"""
from __future__ import annotations

import io

import boto3
import pytest
from moto import mock_aws

from botnim.storage.s3_store import S3Store


_BUCKET = "botnim-artifacts-test"
_REGION = "il-central-1"


@pytest.fixture
def s3_store(monkeypatch):
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


def test_put_then_get_roundtrip(s3_store):
    s3_store.put_atomic("seed/unified/a.json", b"hello")
    assert s3_store.get_bytes("seed/unified/a.json") == b"hello"


def test_get_bytes_missing_raises_filenotfound(s3_store):
    with pytest.raises(FileNotFoundError):
        s3_store.get_bytes("seed/unified/missing.json")


def test_open_stream_reads_full_body(s3_store):
    s3_store.put_atomic("cache/unified/s.json", b"streamed-body")
    with s3_store.open_stream("cache/unified/s.json") as fh:
        assert fh.read() == b"streamed-body"


def test_open_stream_missing_raises_filenotfound(s3_store):
    with pytest.raises(FileNotFoundError):
        s3_store.open_stream("cache/unified/missing.json")


def test_put_atomic_overwrite(s3_store):
    s3_store.put_atomic("seed/unified/v.json", b"v1")
    s3_store.put_atomic("seed/unified/v.json", b"v2-longer")
    assert s3_store.get_bytes("seed/unified/v.json") == b"v2-longer"


def test_exists(s3_store):
    assert s3_store.exists("seed/unified/e.json") is False
    s3_store.put_atomic("seed/unified/e.json", b"e")
    assert s3_store.exists("seed/unified/e.json") is True


def test_list_returns_keys_under_prefix(s3_store):
    s3_store.put_atomic("cache/wikitext/unified/aaa__v1.json", b"1")
    s3_store.put_atomic("cache/wikitext/unified/bbb__v1.json", b"2")
    s3_store.put_atomic("cache/wikitext/other/ccc__v1.json", b"3")
    got = sorted(s3_store.list("cache/wikitext/unified/"))
    assert got == [
        "cache/wikitext/unified/aaa__v1.json",
        "cache/wikitext/unified/bbb__v1.json",
    ]


def test_list_empty_prefix_returns_empty(s3_store):
    assert s3_store.list("cache/wikitext/nope/") == []


class _ShortReadBody:
    """A botocore StreamingBody stand-in that returns fewer bytes than
    the object's advertised ContentLength."""

    def __init__(self, data: bytes):
        self._buf = io.BytesIO(data)

    def read(self, amt=None):
        return self._buf.read(amt)

    def close(self):
        self._buf.close()


class _ShortReadClient:
    """Minimal fake S3 client whose get_object lies about ContentLength."""

    def get_object(self, Bucket, Key):
        # Advertise 100 bytes but hand back only 4.
        return {"ContentLength": 100, "Body": _ShortReadBody(b"trun")}


def test_get_bytes_short_read_raises():
    store = S3Store("any-bucket", region_name=_REGION, client=_ShortReadClient())
    with pytest.raises(OSError):
        store.get_bytes("seed/unified/truncated.json")
