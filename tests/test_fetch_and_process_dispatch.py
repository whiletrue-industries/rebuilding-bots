"""Smoke tests that each new fetcher_kind dispatches to its expected callable."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


def _src(kind, **extra):
    return {"source": "extraction/x/index.csv", "fetcher": {"kind": kind, **extra}}


def test_dispatch_knesset_protocols(tmp_path: Path):
    src = _src("knesset_protocols")
    with patch(
        "botnim.document_parser.knesset_protocols.process_protocols.process_knesset_protocols_source"
    ) as mock_inner:
        from botnim.fetch_and_process import fetch_and_process_source
        fetch_and_process_source("local", tmp_path, "ctx", src, "all")
        mock_inner.assert_called_once()
        kwargs = mock_inner.call_args.kwargs
        assert "store" in kwargs
        assert "key" in kwargs
        assert kwargs["key"].startswith("cache/")


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


def test_indexed_pdf_resolves_relative_local_index_against_config_dir(tmp_path: Path):
    """yaml gives `local_index_csv_path: extraction/<slug>/index.csv` (relative).
    The dispatcher must join it onto config_dir before constructing SourceConfig
    — otherwise process_pdf_source resolves against cwd (in prod: /app, NOT
    /srv/specs/<bot>) and raises EmptyUpstreamIndex even when Stage 1 wrote
    the index. Regression test for the prod bug seen on 2026-05-06.
    """
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
        cfg = mock_inner.call_args.args[0]
        assert Path(cfg.local_index_csv_path).is_absolute()
        assert Path(cfg.local_index_csv_path) == tmp_path / "extraction/x/index.csv"


def test_indexed_pdf_keeps_absolute_local_index_unchanged(tmp_path: Path):
    """If yaml supplies an absolute path, leave it alone (escape hatch)."""
    abs_idx = tmp_path / "elsewhere" / "idx.csv"
    src = {
        "source": "extraction/x.csv",
        "fetcher": {
            "kind": "indexed_pdf",
            "local_index_csv_path": str(abs_idx),
            "fields": [{"name": "טקסט_מלא", "description": "x", "example": "y", "hint": "z"}],
        },
    }
    with patch(
        "botnim.document_parser.pdfs.process_pdfs.process_pdf_source"
    ) as mock_inner:
        from botnim.fetch_and_process import fetch_and_process_source
        fetch_and_process_source("local", tmp_path, "ctx", src, "all")
        cfg = mock_inner.call_args.args[0]
        assert Path(cfg.local_index_csv_path) == abs_idx


def test_indexed_pdf_end_to_end_empty_index(tmp_path: Path):
    """End-to-end: dispatcher → process_pdf_source. Stage 1 wrote an empty
    index (header-only) at the relative-to-config_dir path. The
    EmptyUpstreamIndex/no-output path inside process_pdf_source proves the
    dispatcher resolved the path correctly: if it didn't, we'd raise
    EmptyUpstreamIndex with 'does not exist' instead of writing an empty
    output. No OpenAI call required.
    """
    idx = tmp_path / "extraction" / "x" / "index.csv"
    idx.parent.mkdir(parents=True, exist_ok=True)
    idx.write_text("url,filename,date,knesset_num,title\n", encoding="utf-8")
    src = {
        "source": "extraction/x.csv",
        "fetcher": {
            "kind": "indexed_pdf",
            "local_index_csv_path": "extraction/x/index.csv",
            "fields": [{"name": "טקסט_מלא", "description": "x", "example": "y", "hint": "z"}],
        },
    }
    from botnim.fetch_and_process import fetch_and_process_source
    from botnim.storage import get_artifact_store
    from botnim.storage.csv_writer import key_for_extraction

    fetch_and_process_source("local", tmp_path, "ctx", src, "all")
    # 0-row index + no pre-existing output → the empty-out branch wrote the
    # header-only output CSV to the ArtifactStore (post-S3-migration), at the
    # cache/<bot>/<relpath> key derived from source['source']. The default
    # store is isolated to this test's tmp_path by the _isolate_artifact_store
    # autouse fixture (conftest.py), so the empty-index overwrite-guard does
    # not trip on a stale prior-run artifact.
    store = get_artifact_store()
    key = key_for_extraction(tmp_path.name, "extraction/x.csv")
    assert store.exists(key)
