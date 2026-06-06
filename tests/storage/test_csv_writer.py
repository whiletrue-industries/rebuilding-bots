"""Unit tests for the shared CSV artifact writer.

Covers both on-disk shapes the legacy writers produced:
  * fixed ``fieldnames`` (DictWriter, utf-8, newline="")  -- the common case
  * dynamic fieldname extension from the union of row keys -- process_pdfs

Both backends are exercised: LocalFsStore (tmp_path) and S3Store (moto).
"""
from __future__ import annotations

import csv
import io

import boto3
import pytest
from moto import mock_aws

from botnim.storage.local_fs import LocalFsStore
from botnim.storage.s3_store import S3Store
from botnim.storage.csv_writer import write_csv_artifact, key_for_extraction


def _read_rows(data: bytes) -> list[dict]:
    return list(csv.DictReader(io.StringIO(data.decode("utf-8"))))


def test_fixed_fieldnames_localfs(tmp_path):
    store = LocalFsStore(tmp_path)
    rows = [
        {"a": "1", "b": "x"},
        {"a": "2", "b": "y"},
    ]
    write_csv_artifact(store, "cache/unified/extraction/foo.csv", rows,
                       fieldnames=["a", "b"])
    data = store.get_bytes("cache/unified/extraction/foo.csv")
    # Exact on-disk bytes: header + two rows, \r\n line terminators (csv default).
    assert data == b"a,b\r\n1,x\r\n2,y\r\n"
    assert _read_rows(data) == rows


def test_fixed_fieldnames_lands_at_key_on_s3():
    with mock_aws():
        boto3.client("s3", region_name="il-central-1").create_bucket(
            Bucket="botnim-artifacts-test",
            CreateBucketConfiguration={"LocationConstraint": "il-central-1"},
        )
        store = S3Store("botnim-artifacts-test")
        rows = [{"a": "1", "b": "x"}]
        key = "cache/unified/extraction/foo.csv"
        write_csv_artifact(store, key, rows, fieldnames=["a", "b"])
        assert store.exists(key)
        assert _read_rows(store.get_bytes(key)) == rows


def test_extend_fieldnames_matches_process_pdfs_union(tmp_path):
    """extend_fieldnames=True starts from the base list and appends any
    extra keys seen across rows, in first-seen order -- the exact behaviour
    of process_pdfs.py:181-185."""
    store = LocalFsStore(tmp_path)
    rows = [
        {"url": "u1", "revision": "R", "upstream_revision": "", "x": "1"},
        {"url": "u2", "revision": "R", "upstream_revision": "", "x": "2", "y": "9"},
    ]
    write_csv_artifact(
        store,
        "cache/unified/extraction/pdf.csv",
        rows,
        fieldnames=["url", "revision", "upstream_revision"],
        extend_fieldnames=True,
    )
    out = _read_rows(store.get_bytes("cache/unified/extraction/pdf.csv"))
    assert list(out[0].keys()) == ["url", "revision", "upstream_revision", "x", "y"]
    assert out[0]["y"] == ""   # missing in row 0 -> empty cell
    assert out[1]["y"] == "9"


def test_key_for_extraction_mirrors_relpath():
    # config_dir.name == bot slug; source['source'] == relpath under config_dir.
    assert key_for_extraction("unified", "extraction/government_decisions.csv") == \
        "cache/unified/extraction/government_decisions.csv"
    # Leading slash on the relpath is normalised away.
    assert key_for_extraction("unified", "/extraction/x.csv") == \
        "cache/unified/extraction/x.csv"
