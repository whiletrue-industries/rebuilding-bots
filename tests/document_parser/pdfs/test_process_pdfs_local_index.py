"""Tests for the local_index_csv_path branch of process_pdf_source."""
from __future__ import annotations

import csv
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botnim.document_parser.pdfs.process_pdfs import process_pdf_source
from botnim.document_parser.pdfs.pdf_extraction_config import SourceConfig, FieldConfig
from botnim.document_parser.knesset_apps.common import EmptyUpstreamIndex
from botnim.storage.local_fs import LocalFsStore
from botnim.storage.csv_writer import key_for_extraction


def _make_index_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["url", "filename", "date", "knesset_num", "title"])
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _fields():
    return [FieldConfig(name="טקסט_מלא", description="Full text", example="...", hint="...")]


def _make_store_and_key(tmp_path: Path):
    store = LocalFsStore(tmp_path / "store")
    key = key_for_extraction("unified", "extraction/out.csv")
    return store, key


def test_missing_index_raises_empty(tmp_path: Path):
    cfg = SourceConfig(
        output_csv_path=tmp_path / "out.csv",
        fields=_fields(),
        local_index_csv_path=str(tmp_path / "missing-index.csv"),
    )
    store, key = _make_store_and_key(tmp_path)
    with pytest.raises(EmptyUpstreamIndex):
        process_pdf_source(cfg, store=store, key=key)


def test_zero_row_index_with_existing_output_raises(tmp_path: Path):
    store, key = _make_store_and_key(tmp_path)
    # Pre-existing populated output in the store; we must refuse to overwrite it.
    store.put_atomic(key, b"url,revision,\xe2\x80\x8bx\nhttps://x,1,old\n")
    idx = tmp_path / "idx.csv"
    _make_index_csv(idx, [])
    cfg = SourceConfig(output_csv_path=tmp_path / "out.csv", fields=_fields(),
                       local_index_csv_path=str(idx))
    with pytest.raises(EmptyUpstreamIndex):
        process_pdf_source(cfg, store=store, key=key)


def test_zero_row_index_without_existing_writes_header(tmp_path: Path):
    """Empty index + no existing store object → writes empty header row."""
    store, key = _make_store_and_key(tmp_path)
    idx = tmp_path / "idx.csv"
    _make_index_csv(idx, [])
    cfg = SourceConfig(output_csv_path=tmp_path / "out.csv", fields=_fields(),
                       local_index_csv_path=str(idx))
    process_pdf_source(cfg, store=store, key=key)
    assert store.exists(key)
    assert store.get_bytes(key) == b"url,revision,upstream_revision\n"


def test_single_row_index_invokes_extractor_with_row_url(tmp_path: Path):
    """The new branch must use row['url'] directly, NOT external_source/filename."""
    idx = tmp_path / "idx.csv"
    _make_index_csv(idx, [{
        "url": "https://main.knesset.gov.il/.../Decision-1.pdf",
        "filename": "Decision-1.pdf",
        "date": "10.2.2026",
        "knesset_num": "25",
        "title": "החלטה",
    }])
    store, key = _make_store_and_key(tmp_path)
    cfg = SourceConfig(output_csv_path=tmp_path / "out.csv", fields=_fields(),
                       local_index_csv_path=str(idx))

    with patch("botnim.document_parser.pdfs.process_pdfs.requests.get") as mock_get, \
         patch("botnim.document_parser.pdfs.process_pdfs.process_single_pdf") as mock_process, \
         patch("botnim.document_parser.pdfs.process_pdfs.get_openai_client"):
        mock_get.return_value = MagicMock(content=b"%PDF-1.4 fake", raise_for_status=MagicMock())
        mock_process.return_value = [{"טקסט_מלא": "שלום עולם"}]
        process_pdf_source(cfg, store=store, key=key)

    # Verify the URL passed to requests.get is the row URL, NOT a constructed one.
    called_with = mock_get.call_args.args[0]
    assert called_with == "https://main.knesset.gov.il/.../Decision-1.pdf"

    # Output should have one row in the store.
    assert store.exists(key)
    rows = list(csv.DictReader(store.get_bytes(key).decode("utf-8").splitlines()))
    assert len(rows) == 1
    assert rows[0]["url"] == "https://main.knesset.gov.il/.../Decision-1.pdf"
    assert rows[0]["טקסט_מלא"] == "שלום עולם"


def test_cache_hit_skips_extractor(tmp_path: Path):
    """Existing (url, REVISION) row in output is reused — no new HTTP call."""
    from botnim.document_parser.pdfs.process_pdfs import REVISION
    idx = tmp_path / "idx.csv"
    _make_index_csv(idx, [{
        "url": "https://main.knesset.gov.il/x.pdf",
        "filename": "x.pdf",
        "date": "1.1.2026",
        "knesset_num": "25",
        "title": "Cached",
    }])
    store, key = _make_store_and_key(tmp_path)
    store.put_atomic(
        key,
        f"url,revision,upstream_revision,טקסט_מלא\n"
        f"https://main.knesset.gov.il/x.pdf,{REVISION},,already extracted\n"
        .encode("utf-8"),
    )
    cfg = SourceConfig(output_csv_path=tmp_path / "out.csv", fields=_fields(),
                       local_index_csv_path=str(idx))

    with patch("botnim.document_parser.pdfs.process_pdfs.requests.get") as mock_get, \
         patch("botnim.document_parser.pdfs.process_pdfs.process_single_pdf") as mock_process, \
         patch("botnim.document_parser.pdfs.process_pdfs.get_openai_client"):
        process_pdf_source(cfg, store=store, key=key)

    mock_get.assert_not_called()
    mock_process.assert_not_called()
    rows = list(csv.DictReader(store.get_bytes(key).decode("utf-8").splitlines()))
    assert len(rows) == 1
    assert rows[0]["טקסט_מלא"] == "already extracted"
