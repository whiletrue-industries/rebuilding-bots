"""Tests for the wrapper functions exposed for fap dispatch.

The plan originally referenced ``scrape_pdf_index_via_playwright``; the
real underlying callable in ``scraper.py`` is ``scrape_pdf_index`` which
takes a ``ScrapeConfig`` object. The wrappers therefore construct a
``ScrapeConfig`` from kwargs and forward to ``scrape_pdf_index``.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from botnim.storage.local_fs import LocalFsStore

_KEY = "cache/unified/extraction/legal_advisor_opinions.csv"
_KEY_LETTERS = "cache/unified/extraction/legal_advisor_letters.csv"


def test_scrape_legal_advisor_opinions_calls_underlying(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    with patch(
        "botnim.document_parser.knesset_sharepoint.scraper.scrape_pdf_index"
    ) as mock_inner:
        from botnim.document_parser.knesset_sharepoint.scraper import (
            ScrapeConfig,
            scrape_legal_advisor_opinions,
        )
        scrape_legal_advisor_opinions(
            store=store,
            key=_KEY,
            page_url="https://main.knesset.gov.il/about/departments/pages/leg/ldopinions.aspx",
        )
        mock_inner.assert_called_once()
        args, kwargs = mock_inner.call_args
        # Underlying signature: scrape_pdf_index(config: ScrapeConfig).
        cfg = args[0] if args else kwargs.get("config")
        assert isinstance(cfg, ScrapeConfig)
        assert cfg.store is store
        assert cfg.key == _KEY
        assert "ldopinions.aspx" in cfg.page_url


def test_scrape_legal_advisor_letters_calls_underlying(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    with patch(
        "botnim.document_parser.knesset_sharepoint.scraper.scrape_pdf_index"
    ) as mock_inner:
        from botnim.document_parser.knesset_sharepoint.scraper import (
            ScrapeConfig,
            scrape_legal_advisor_letters,
        )
        scrape_legal_advisor_letters(
            store=store,
            key=_KEY_LETTERS,
            page_url="https://main.knesset.gov.il/about/departments/pages/leg/ldguidelines.aspx",
        )
        mock_inner.assert_called_once()
        args, kwargs = mock_inner.call_args
        cfg = args[0] if args else kwargs.get("config")
        assert isinstance(cfg, ScrapeConfig)
        assert cfg.store is store
        assert cfg.key == _KEY_LETTERS
        assert "ldguidelines.aspx" in cfg.page_url


def test_scrape_legal_advisor_opinions_tolerates_extra_kwargs(tmp_path: Path):
    """fap may pass extra config.yaml keys; wrapper must not blow up."""
    store = LocalFsStore(tmp_path)
    with patch(
        "botnim.document_parser.knesset_sharepoint.scraper.scrape_pdf_index"
    ) as mock_inner:
        from botnim.document_parser.knesset_sharepoint.scraper import scrape_legal_advisor_opinions
        scrape_legal_advisor_opinions(
            store=store,
            key=_KEY,
            page_url="https://main.knesset.gov.il/x",
            something_unknown="ignored",
            another="also ignored",
        )
        mock_inner.assert_called_once()


def test_scrape_legal_advisor_letters_tolerates_extra_kwargs(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    with patch(
        "botnim.document_parser.knesset_sharepoint.scraper.scrape_pdf_index"
    ) as mock_inner:
        from botnim.document_parser.knesset_sharepoint.scraper import scrape_legal_advisor_letters
        scrape_legal_advisor_letters(
            store=store,
            key=_KEY_LETTERS,
            page_url="https://main.knesset.gov.il/y",
            headless=True,
            timeout_ms=30000,
        )
        mock_inner.assert_called_once()
