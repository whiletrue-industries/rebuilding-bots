"""Tests for the get_artifact_store() backend selector + singleton.

No DB / network: S3Store is constructed but not called (boto3.client is
lazy — it does not hit the network at construction time), and the
LocalFs branch uses a tmp root via env. We reset the cached singleton
between cases via the module-private reset hook.
"""
from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

import botnim.storage as storage
from botnim.storage.local_fs import LocalFsStore
from botnim.storage.s3_store import S3Store


@pytest.fixture(autouse=True)
def _reset_singleton():
    storage._reset_artifact_store_singleton()
    yield
    storage._reset_artifact_store_singleton()


def test_selects_localfs_when_no_bucket(monkeypatch, tmp_path):
    monkeypatch.delenv("BOTNIM_ARTIFACT_BUCKET", raising=False)
    monkeypatch.setenv("BOTNIM_ARTIFACT_LOCAL_ROOT", str(tmp_path))
    store = storage.get_artifact_store()
    assert isinstance(store, LocalFsStore)
    store.put_atomic("seed/unified/x.json", b"x")
    assert (tmp_path / "seed" / "unified" / "x.json").read_bytes() == b"x"


def test_selects_s3_when_bucket_present(monkeypatch):
    with mock_aws():
        monkeypatch.setenv("AWS_DEFAULT_REGION", "il-central-1")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        monkeypatch.setenv("BOTNIM_ARTIFACT_BUCKET", "botnim-artifacts-test")
        boto3.client("s3", region_name="il-central-1").create_bucket(
            Bucket="botnim-artifacts-test",
            CreateBucketConfiguration={"LocationConstraint": "il-central-1"},
        )
        store = storage.get_artifact_store()
        assert isinstance(store, S3Store)
        store.put_atomic("seed/unified/y.json", b"y")
        assert store.get_bytes("seed/unified/y.json") == b"y"


def test_returns_same_singleton(monkeypatch, tmp_path):
    monkeypatch.delenv("BOTNIM_ARTIFACT_BUCKET", raising=False)
    monkeypatch.setenv("BOTNIM_ARTIFACT_LOCAL_ROOT", str(tmp_path))
    assert storage.get_artifact_store() is storage.get_artifact_store()
