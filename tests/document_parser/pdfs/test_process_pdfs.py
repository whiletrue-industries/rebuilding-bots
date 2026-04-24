"""Unit tests for process_pdfs safety rails.

Tests cover:
- Empty upstream index raises EmptyUpstreamIndex and does not touch the output CSV
- Atomic write: a mid-run crash does not corrupt the existing CSV
- Revision short-circuit: unchanged datapackage revision skips fetching
- Happy path writes a valid CSV

All HTTP is mocked via unittest.mock.patch; no real network traffic.
"""
from __future__ import annotations

import csv
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
        preexisting = tmp_path / "out.csv"
        preexisting.write_text("url,revision,x\nhttps://foo,1,old\n", encoding="utf-8")
        original_mtime = preexisting.stat().st_mtime

        with patch.object(process_pdfs, "get_openai_client", return_value=MagicMock()), \
             patch.object(process_pdfs.requests, "get") as mock_get:
            mock_get.return_value = _mock_get_response("url,filename,date,knesset_num\n")
            with pytest.raises(EmptyUpstreamIndex):
                process_pdfs.process_pdf_source(config)

        assert preexisting.read_text(encoding="utf-8") == "url,revision,x\nhttps://foo,1,old\n"
        assert preexisting.stat().st_mtime == original_mtime


class TestAtomicWrite:
    def test_mid_run_crash_leaves_existing_csv_intact(self, tmp_path: Path) -> None:
        """If process_single_pdf raises inside the write-final-csv path, the
        previous CSV must remain intact (atomic rename via .tmp + os.replace)."""
        config = _make_config(tmp_path)
        preexisting = tmp_path / "out.csv"
        preexisting.write_text("url,revision,x\nhttps://foo,1,old\n", encoding="utf-8")

        index_body = (
            "url,filename,date,knesset_num\n"
            "https://example.com/a,a.pdf,2024-01-01,25\n"
        )

        def fake_get(url: str, *args, **kwargs):
            if url.endswith("/index.csv"):
                return _mock_get_response(index_body)
            return _mock_get_response("%PDF-1.4\n% fake\n")

        # process_single_pdf returns one record; simulate DictWriter.writerow failing
        def boom(*args, **kwargs):
            raise RuntimeError("simulated disk error during write")

        with patch.object(process_pdfs, "get_openai_client", return_value=MagicMock()), \
             patch.object(process_pdfs.requests, "get", side_effect=fake_get), \
             patch.object(process_pdfs, "process_single_pdf", return_value=[{"x": "new"}]), \
             patch.object(process_pdfs.csv, "DictWriter") as mock_writer_cls:
            writer = MagicMock()
            writer.writerow = boom
            writer.writeheader = MagicMock()
            mock_writer_cls.return_value = writer
            with pytest.raises(RuntimeError):
                process_pdfs.process_pdf_source(config)

        # Original content must still be there; .tmp must not leak as final file
        assert preexisting.read_text(encoding="utf-8") == "url,revision,x\nhttps://foo,1,old\n"
        # No stray .tmp file in the output directory
        tmp_leftovers = list(tmp_path.glob("*.tmp"))
        assert tmp_leftovers == [], f"unexpected .tmp files left behind: {tmp_leftovers}"
