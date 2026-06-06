"""Store-conversion tests for the bk_csv writer."""
from __future__ import annotations

import csv
import io
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from botnim.document_parser.bk_datapackage import process_bk_csv as mod
from botnim.storage.local_fs import LocalFsStore
from botnim.storage.s3_store import S3Store

_KEY = "cache/unified/extraction/government_decisions.csv"
_CSV = (
    "title,publish_date,office,government,policy_type,"
    "procedure_number_str,url_id,text\n"
    "כותרת,2026-01-01,משרד,37,החלטות ממשלה,123,abc,<p>גוף ההחלטה</p>\n"
)


def _patches():
    return [
        patch.object(mod, "_stream_upstream_rows",
                     return_value=iter(list(csv.DictReader(io.StringIO(_CSV))))),
        patch.object(mod.requests, "get"),
    ]


def _read(store, key):
    return list(csv.DictReader(io.StringIO(store.get_bytes(key).decode("utf-8"))))


def test_bk_csv_lands_at_key_localfs(tmp_path):
    store = LocalFsStore(tmp_path)
    with _patches()[0], _patches()[1] as mget:
        mget.return_value.json.return_value = {"resources": []}
        mod.process_bk_csv_source(
            store=store, key=_KEY,
            external_source_url="https://next.obudget.org/datapackages/government_decisions",
            filter_column="policy_type", filter_values=["החלטות ממשלה"],
        )
    rows = _read(store, _KEY)
    assert rows[0]["title"] == "כותרת"
    assert rows[0]["text"] == "גוף ההחלטה"          # HTML stripped
    assert list(rows[0].keys())[0] == "upstream_hash"


def test_bk_csv_lands_at_key_s3():
    with mock_aws():
        boto3.client("s3", region_name="il-central-1").create_bucket(
            Bucket="botnim-artifacts-test",
            CreateBucketConfiguration={"LocationConstraint": "il-central-1"},
        )
        store = S3Store("botnim-artifacts-test")
        with _patches()[0], _patches()[1] as mget:
            mget.return_value.json.return_value = {"resources": []}
            mod.process_bk_csv_source(
                store=store, key=_KEY,
                external_source_url="https://next.obudget.org/datapackages/government_decisions",
                filter_column="policy_type", filter_values=["החלטות ממשלה"],
            )
        assert store.exists(_KEY)
        assert _read(store, _KEY)[0]["title"] == "כותרת"
