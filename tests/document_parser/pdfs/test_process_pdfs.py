"""Unit tests for process_pdfs safety rails.

Tests cover:
- Empty upstream index raises EmptyUpstreamIndex and does not touch the output CSV
- Revision short-circuit: unchanged datapackage revision skips fetching
- Happy path writes a valid CSV (LocalFsStore and S3Store via moto)

All HTTP is mocked via unittest.mock.patch; no real network traffic.
"""
from __future__ import annotations

import csv
import io
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botnim.document_parser.pdfs.exceptions import EmptyUpstreamIndex
from botnim.document_parser.pdfs.pdf_extraction_config import (
    FieldConfig,
    SourceConfig,
)
from botnim.document_parser.pdfs import process_pdfs
from botnim.storage.local_fs import LocalFsStore
from botnim.storage.s3_store import S3Store
from botnim.storage.csv_writer import key_for_extraction
import boto3
from moto import mock_aws


def _make_config(tmp_path: Path, output_name: str = "out.csv") -> SourceConfig:
    return SourceConfig(
        fields=[FieldConfig(name="x", description="x")],
        extraction_instructions="test",
        external_source_url="https://example.com/feed",
        output_csv_path=tmp_path / output_name,
    )


def _mock_get_response(text: str, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.text = text
    resp.content = text.encode("utf-8")
    resp.status_code = status
    resp.raise_for_status = MagicMock()
    return resp


class TestEmptyUpstreamGuard:
    def test_empty_index_raises_empty_upstream(self, tmp_path: Path) -> None:
        """Upstream index.csv with only header → EmptyUpstreamIndex, no write."""
        config = _make_config(tmp_path)
        store = LocalFsStore(tmp_path / "store")
        key = key_for_extraction("unified", "extraction/out.csv")
        store.put_atomic(key, b"url,revision,x\nhttps://foo,1,old\n")

        with patch.object(process_pdfs, "get_openai_client", return_value=MagicMock()), \
             patch.object(process_pdfs.requests, "get") as mock_get:
            mock_get.return_value = _mock_get_response("url,filename,date,knesset_num\n")
            with pytest.raises(EmptyUpstreamIndex):
                process_pdfs.process_pdf_source(config, store=store, key=key)

        assert store.get_bytes(key) == b"url,revision,x\nhttps://foo,1,old\n"


class TestRevisionShortCircuit:
    def test_unchanged_upstream_revision_is_noop(self, tmp_path: Path) -> None:
        """If datapackage.json revision matches the `upstream_revision` stored
        in the first row of the existing object, skip the fetch loop entirely."""
        config = _make_config(tmp_path)
        store = LocalFsStore(tmp_path / "store")
        key = key_for_extraction("unified", "extraction/out.csv")
        store.put_atomic(
            key,
            b"url,revision,upstream_revision,x\nhttps://foo,1,2025.09.01-01,old\n",
        )

        datapackage_body = '{"revision": "2025.09.01-01", "count_of_rows": 1}'
        calls = []

        def fake_get(url: str, *args, **kwargs):
            calls.append(url)
            if url.endswith("/datapackage.json"):
                return _mock_get_response(datapackage_body)
            raise AssertionError(f"should not fetch {url}")

        with patch.object(process_pdfs, "get_openai_client", return_value=MagicMock()), \
             patch.object(process_pdfs.requests, "get", side_effect=fake_get):
            process_pdfs.process_pdf_source(config, store=store, key=key)

        assert calls == ["https://example.com/feed/datapackage.json"]
        # Object unchanged.
        assert store.get_bytes(key) == \
            b"url,revision,upstream_revision,x\nhttps://foo,1,2025.09.01-01,old\n"


class TestStoreWrite:
    def test_happy_path_lands_at_cache_key_localfs(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        store = LocalFsStore(tmp_path / "store")
        key = key_for_extraction("unified", "extraction/out.csv")

        index_body = (
            "url,filename,date,knesset_num\n"
            "https://example.com/a,a.pdf,2024-01-01,25\n"
        )

        def fake_get(url: str, *args, **kwargs):
            if url.endswith("/datapackage.json"):
                return _mock_get_response('{"revision": "rev-2"}')
            if url.endswith("/index.csv"):
                return _mock_get_response(index_body)
            return _mock_get_response("%PDF-1.4\n% fake\n")

        with patch.object(process_pdfs, "get_openai_client", return_value=MagicMock()), \
             patch.object(process_pdfs.requests, "get", side_effect=fake_get), \
             patch.object(process_pdfs, "process_single_pdf", return_value=[{"x": "new"}]):
            process_pdfs.process_pdf_source(config, store=store, key=key)

        assert store.exists(key)
        rows = list(csv.DictReader(io.StringIO(store.get_bytes(key).decode("utf-8"))))
        assert rows[0]["url"] == "https://example.com/a"
        assert rows[0]["x"] == "new"
        # Dynamic fieldname union (process_pdfs.py:181-185): base 3 + extracted 'x'.
        assert list(rows[0].keys())[:3] == ["url", "revision", "upstream_revision"]
        assert "x" in rows[0]

    def test_happy_path_lands_at_cache_key_s3(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        with mock_aws():
            boto3.client("s3", region_name="il-central-1").create_bucket(
                Bucket="botnim-artifacts-test",
                CreateBucketConfiguration={"LocationConstraint": "il-central-1"},
            )
            store = S3Store("botnim-artifacts-test")
            key = key_for_extraction("unified", "extraction/out.csv")
            index_body = (
                "url,filename,date,knesset_num\n"
                "https://example.com/a,a.pdf,2024-01-01,25\n"
            )

            def fake_get(url: str, *args, **kwargs):
                if url.endswith("/datapackage.json"):
                    return _mock_get_response('{"revision": "rev-2"}')
                if url.endswith("/index.csv"):
                    return _mock_get_response(index_body)
                return _mock_get_response("%PDF-1.4\n% fake\n")

            with patch.object(process_pdfs, "get_openai_client", return_value=MagicMock()), \
                 patch.object(process_pdfs.requests, "get", side_effect=fake_get), \
                 patch.object(process_pdfs, "process_single_pdf", return_value=[{"x": "new"}]):
                process_pdfs.process_pdf_source(config, store=store, key=key)

            assert store.exists(key)
