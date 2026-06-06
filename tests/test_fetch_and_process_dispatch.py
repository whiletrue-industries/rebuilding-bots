"""Smoke tests that each new fetcher_kind dispatches to its expected callable."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

import botnim.fetch_and_process as fap
from botnim.storage.local_fs import LocalFsStore
from botnim.storage.csv_writer import key_for_extraction


def _src(kind, **extra):
    return {"source": "extraction/x/index.csv", "fetcher": {"kind": kind, **extra}}


# ---------------------------------------------------------------------------
# pdf branch
# ---------------------------------------------------------------------------

def test_pdf_branch_passes_store_and_key(tmp_path: Path, monkeypatch):
    store = LocalFsStore(tmp_path / "store")
    monkeypatch.setattr(fap, "get_artifact_store", lambda: store)
    called = {}

    def _fake_process_pdf_source(config, *, store, key):
        called["store"] = store
        called["key"] = key

    monkeypatch.setattr(
        "botnim.document_parser.pdfs.process_pdfs.process_pdf_source",
        _fake_process_pdf_source,
        raising=False,
    )
    # SourceConfig requires exactly one of external_source_url or local_index_csv_path
    src = _src(
        "pdf",
        external_source_url="https://example.com/pdfs/",
        fields=[{"name": "f", "description": "d", "example": "e", "hint": "h"}],
    )
    fap.fetch_and_process_source("local", tmp_path / "unified", "ctx", src, "all")
    assert called["store"] is store
    assert called["key"] == key_for_extraction("unified", "extraction/x/index.csv")


# ---------------------------------------------------------------------------
# lexicon branch
# ---------------------------------------------------------------------------

def test_lexicon_branch_passes_store_and_key(tmp_path: Path, monkeypatch):
    store = LocalFsStore(tmp_path / "store")
    monkeypatch.setattr(fap, "get_artifact_store", lambda: store)
    called = {}

    def _fake_scrape_lexicon(*, store, key):
        called["store"] = store
        called["key"] = key

    monkeypatch.setattr(
        "botnim.document_parser.lexicon.lexicon.scrape_lexicon",
        _fake_scrape_lexicon,
        raising=False,
    )
    src = _src("lexicon")
    # lexicon is skipped for kind='all'; use kind='lexicon'
    fap.fetch_and_process_source("local", tmp_path / "unified", "ctx", src, "lexicon")
    assert called["store"] is store
    assert called["key"] == key_for_extraction("unified", "extraction/x/index.csv")


# ---------------------------------------------------------------------------
# bk_csv branch
# ---------------------------------------------------------------------------

def test_bk_csv_branch_passes_store_and_key(tmp_path: Path, monkeypatch):
    store = LocalFsStore(tmp_path / "store")
    monkeypatch.setattr(fap, "get_artifact_store", lambda: store)
    called = {}

    def _fake_process_bk_csv(*, store, key, **kw):
        called["store"] = store
        called["key"] = key

    monkeypatch.setattr(
        "botnim.document_parser.bk_datapackage.process_bk_csv.process_bk_csv_source",
        _fake_process_bk_csv,
        raising=False,
    )
    src = _src("bk_csv", resource_url="https://example.com/data.csv")
    fap.fetch_and_process_source("local", tmp_path / "unified", "ctx", src, "all")
    assert called["store"] is store
    assert called["key"] == key_for_extraction("unified", "extraction/x/index.csv")


# ---------------------------------------------------------------------------
# knesset_odata branch
# ---------------------------------------------------------------------------

def test_knesset_odata_branch_passes_store_and_key(tmp_path: Path, monkeypatch):
    store = LocalFsStore(tmp_path / "store")
    monkeypatch.setattr(fap, "get_artifact_store", lambda: store)
    called = {}

    def _fake(*, store, key, **kw):
        called["store"] = store
        called["key"] = key

    monkeypatch.setattr(
        "botnim.document_parser.knesset_odata.process_odata.process_knesset_odata_source",
        _fake,
        raising=False,
    )
    src = _src("knesset_odata")
    fap.fetch_and_process_source("local", tmp_path / "unified", "ctx", src, "all")
    assert called["store"] is store
    assert called["key"] == key_for_extraction("unified", "extraction/x/index.csv")


# ---------------------------------------------------------------------------
# knesset_protocols branch
# ---------------------------------------------------------------------------

def test_dispatch_knesset_protocols(tmp_path: Path):
    src = _src("knesset_protocols")
    with patch(
        "botnim.document_parser.knesset_protocols.process_protocols.process_knesset_protocols_source"
    ) as mock_inner:
        fap.fetch_and_process_source("local", tmp_path, "ctx", src, "all")
        mock_inner.assert_called_once()
        kwargs = mock_inner.call_args.kwargs
        assert "store" in kwargs
        assert "key" in kwargs
        assert kwargs["key"].startswith("cache/")


def test_knesset_protocols_branch_passes_store_and_key(tmp_path: Path, monkeypatch):
    store = LocalFsStore(tmp_path / "store")
    monkeypatch.setattr(fap, "get_artifact_store", lambda: store)
    called = {}

    def _fake(*, store, key, **kw):
        called["store"] = store
        called["key"] = key

    monkeypatch.setattr(
        "botnim.document_parser.knesset_protocols.process_protocols.process_knesset_protocols_source",
        _fake,
        raising=False,
    )
    src = _src("knesset_protocols")
    fap.fetch_and_process_source("local", tmp_path / "unified", "ctx", src, "all")
    assert called["store"] is store
    assert called["key"] == key_for_extraction("unified", "extraction/x/index.csv")


# ---------------------------------------------------------------------------
# knesset_apps_committee branch
# ---------------------------------------------------------------------------

def test_dispatch_knesset_apps_committee(tmp_path: Path):
    src = _src("knesset_apps_committee", committee_id=2211, from_date="2022-11-15", knesset_ids="25")
    with patch(
        "botnim.document_parser.knesset_apps.committee_decisions_json.fetch_committee_decisions_index"
    ) as mock_inner:
        fap.fetch_and_process_source("local", tmp_path, "ctx", src, "all")
        mock_inner.assert_called_once()


def test_knesset_apps_committee_branch_passes_store_and_key(tmp_path: Path, monkeypatch):
    store = LocalFsStore(tmp_path / "store")
    monkeypatch.setattr(fap, "get_artifact_store", lambda: store)
    called = {}

    def _fake_fetch(cfg):
        called["store"] = cfg.store
        called["key"] = cfg.key

    monkeypatch.setattr(
        "botnim.document_parser.knesset_apps.committee_decisions_json.fetch_committee_decisions_index",
        _fake_fetch,
        raising=False,
    )
    src = _src("knesset_apps_committee", committee_id=2211, from_date="2022-11-15", knesset_ids="25")
    fap.fetch_and_process_source("local", tmp_path / "unified", "ctx", src, "all")
    assert called["store"] is store
    assert called["key"] == key_for_extraction("unified", "extraction/x/index.csv")


# ---------------------------------------------------------------------------
# knesset_apps_ethics branch
# ---------------------------------------------------------------------------

def test_dispatch_knesset_apps_ethics(tmp_path: Path):
    src = _src("knesset_apps_ethics", page_name="EthicsDecisions25")
    with patch(
        "botnim.document_parser.knesset_apps.ethics_decisions_html.fetch_ethics_decisions_index"
    ) as mock_inner:
        fap.fetch_and_process_source("local", tmp_path, "ctx", src, "all")
        mock_inner.assert_called_once()


def test_knesset_apps_ethics_branch_passes_store_and_key(tmp_path: Path, monkeypatch):
    store = LocalFsStore(tmp_path / "store")
    monkeypatch.setattr(fap, "get_artifact_store", lambda: store)
    called = {}

    def _fake_fetch(cfg):
        called["store"] = cfg.store
        called["key"] = cfg.key

    monkeypatch.setattr(
        "botnim.document_parser.knesset_apps.ethics_decisions_html.fetch_ethics_decisions_index",
        _fake_fetch,
        raising=False,
    )
    src = _src("knesset_apps_ethics", page_name="EthicsDecisions25")
    fap.fetch_and_process_source("local", tmp_path / "unified", "ctx", src, "all")
    assert called["store"] is store
    assert called["key"] == key_for_extraction("unified", "extraction/x/index.csv")


# ---------------------------------------------------------------------------
# knesset_sharepoint_legal_advisor branch
# ---------------------------------------------------------------------------

def test_dispatch_knesset_sharepoint_legal_advisor(tmp_path: Path):
    src = _src("knesset_sharepoint_legal_advisor", page_url="https://main.knesset.gov.il/x")
    with patch(
        "botnim.document_parser.knesset_sharepoint.scraper.scrape_legal_advisor_opinions"
    ) as mock_inner:
        fap.fetch_and_process_source("local", tmp_path, "ctx", src, "all")
        mock_inner.assert_called_once()


def test_knesset_sharepoint_legal_advisor_branch_passes_store_and_key(tmp_path: Path, monkeypatch):
    store = LocalFsStore(tmp_path / "store")
    monkeypatch.setattr(fap, "get_artifact_store", lambda: store)
    called = {}

    def _fake(*, store, key, **kw):
        called["store"] = store
        called["key"] = key

    monkeypatch.setattr(
        "botnim.document_parser.knesset_sharepoint.scraper.scrape_legal_advisor_opinions",
        _fake,
        raising=False,
    )
    src = _src("knesset_sharepoint_legal_advisor", page_url="https://main.knesset.gov.il/x")
    fap.fetch_and_process_source("local", tmp_path / "unified", "ctx", src, "all")
    assert called["store"] is store
    assert called["key"] == key_for_extraction("unified", "extraction/x/index.csv")


# ---------------------------------------------------------------------------
# knesset_sharepoint_legal_advisor_letters branch
# ---------------------------------------------------------------------------

def test_dispatch_knesset_sharepoint_legal_advisor_letters(tmp_path: Path):
    src = _src("knesset_sharepoint_legal_advisor_letters", page_url="https://main.knesset.gov.il/y")
    with patch(
        "botnim.document_parser.knesset_sharepoint.scraper.scrape_legal_advisor_letters"
    ) as mock_inner:
        fap.fetch_and_process_source("local", tmp_path, "ctx", src, "all")
        mock_inner.assert_called_once()


def test_knesset_sharepoint_legal_advisor_letters_branch_passes_store_and_key(tmp_path: Path, monkeypatch):
    store = LocalFsStore(tmp_path / "store")
    monkeypatch.setattr(fap, "get_artifact_store", lambda: store)
    called = {}

    def _fake(*, store, key, **kw):
        called["store"] = store
        called["key"] = key

    monkeypatch.setattr(
        "botnim.document_parser.knesset_sharepoint.scraper.scrape_legal_advisor_letters",
        _fake,
        raising=False,
    )
    src = _src("knesset_sharepoint_legal_advisor_letters", page_url="https://main.knesset.gov.il/y")
    fap.fetch_and_process_source("local", tmp_path / "unified", "ctx", src, "all")
    assert called["store"] is store
    assert called["key"] == key_for_extraction("unified", "extraction/x/index.csv")


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


# ---------------------------------------------------------------------------
# indexed_pdf branch — store + key assertions
# ---------------------------------------------------------------------------

def test_indexed_pdf_branch_passes_store_and_key(tmp_path: Path, monkeypatch):
    """indexed_pdf must receive store=<singleton>, key=cache/<bot>/<output relpath>,
    and index_key=cache/<bot>/<index relpath> (GAP A fix: Stage 2 reads its index
    from the store when the local file is absent)."""
    store = LocalFsStore(tmp_path / "store")
    monkeypatch.setattr(fap, "get_artifact_store", lambda: store)
    called = {}

    def _fake_process_pdf_source(config, *, store, key, index_key=None):
        called["store"] = store
        called["key"] = key
        called["index_key"] = index_key

    monkeypatch.setattr(
        "botnim.document_parser.pdfs.process_pdfs.process_pdf_source",
        _fake_process_pdf_source,
        raising=False,
    )
    src = {
        "source": "extraction/x.csv",
        "fetcher": {
            "kind": "indexed_pdf",
            "local_index_csv_path": "extraction/x/index.csv",
            "fields": [{"name": "טקסט_מלא", "description": "x", "example": "y", "hint": "z"}],
        },
    }
    fap.fetch_and_process_source("local", tmp_path / "unified", "ctx", src, "all")
    assert called["store"] is store
    assert called["key"] == key_for_extraction("unified", "extraction/x.csv")
    # GAP A: dispatcher must also pass the store key for the index so Stage 2
    # can fall back to the store when the local file is absent.
    assert called["index_key"] == key_for_extraction("unified", "extraction/x/index.csv")
