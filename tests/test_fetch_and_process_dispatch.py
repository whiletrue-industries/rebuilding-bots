"""Smoke tests that each new fetcher_kind dispatches to its expected callable."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


def _src(kind, **extra):
    return {"source": "extraction/x/index.csv", "fetcher": {"kind": kind, **extra}}


def test_dispatch_knesset_apps_committee(tmp_path: Path):
    src = _src("knesset_apps_committee", committee_id=2211, from_date="2022-11-15", knesset_ids="25")
    with patch(
        "botnim.document_parser.knesset_apps.committee_decisions_json.fetch_committee_decisions_index"
    ) as mock_inner:
        from botnim.fetch_and_process import fetch_and_process_source
        fetch_and_process_source("local", tmp_path, "ctx", src, "all")
        mock_inner.assert_called_once()


def test_dispatch_knesset_apps_ethics(tmp_path: Path):
    src = _src("knesset_apps_ethics", page_name="EthicsDecisions25")
    with patch(
        "botnim.document_parser.knesset_apps.ethics_decisions_html.fetch_ethics_decisions_index"
    ) as mock_inner:
        from botnim.fetch_and_process import fetch_and_process_source
        fetch_and_process_source("local", tmp_path, "ctx", src, "all")
        mock_inner.assert_called_once()


def test_dispatch_knesset_sharepoint_legal_advisor(tmp_path: Path):
    src = _src("knesset_sharepoint_legal_advisor", page_url="https://main.knesset.gov.il/x")
    with patch(
        "botnim.document_parser.knesset_sharepoint.scraper.scrape_legal_advisor_opinions"
    ) as mock_inner:
        from botnim.fetch_and_process import fetch_and_process_source
        fetch_and_process_source("local", tmp_path, "ctx", src, "all")
        mock_inner.assert_called_once()


def test_dispatch_knesset_sharepoint_legal_advisor_letters(tmp_path: Path):
    src = _src("knesset_sharepoint_legal_advisor_letters", page_url="https://main.knesset.gov.il/y")
    with patch(
        "botnim.document_parser.knesset_sharepoint.scraper.scrape_legal_advisor_letters"
    ) as mock_inner:
        from botnim.fetch_and_process import fetch_and_process_source
        fetch_and_process_source("local", tmp_path, "ctx", src, "all")
        mock_inner.assert_called_once()


def test_dispatch_indexed_pdf(tmp_path: Path):
    src = {
        "source": "extraction/x.csv",
        "fetcher": {
            "kind": "indexed_pdf",
            "local_index_csv_path": "extraction/x/index.csv",
            "fields": [{"name": "טקסט_מלא", "description": "x", "example": "y", "hint": "z"}],
        },
    }
    with patch(
        "botnim.document_parser.pdfs.process_pdfs.process_pdf_source"
    ) as mock_inner:
        from botnim.fetch_and_process import fetch_and_process_source
        fetch_and_process_source("local", tmp_path, "ctx", src, "all")
        mock_inner.assert_called_once()
