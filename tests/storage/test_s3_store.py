"""S3-only mechanics for S3Store.

The shared cross-backend round-trip / exists / list / atomic-overwrite
suite lives in tests/storage/test_artifact_store_contract.py (it runs the
moto-backed S3Store too). This file pins behaviour only S3Store can
exhibit: the truncated-read guard (moto always returns an accurate
Content-Length, so the short-read path needs a stub client that lies)
and the empty-bucket constructor guard. No DB needed.
"""
from __future__ import annotations

import io

import pytest

from botnim.storage.s3_store import S3Store


_REGION = "il-central-1"


def test_init_rejects_empty_bucket():
    with pytest.raises(ValueError):
        S3Store("", region_name=_REGION)


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
