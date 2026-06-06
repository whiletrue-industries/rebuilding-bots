"""Round-trip integration tests: fap WRITES → store → sync reader READS.

These tests exercise the REAL chain through ONE store instance — no manual
fixture writes to the key the sync reader expects.  They exist because the
unit tests for GAP B and GAP A previously fixtured the store directly and
therefore missed that the fap was not writing to the correct key at all.

GAP B (wikitext): the wikitext fap must write the content_file bytes to
  key_for_extraction(bot, source['source']) after runner.run() so that
  collect_sources._collect_raw_streams_split can read it.

GAP A (indexed_pdf Stage-2): process_pdf_source must fall back to reading
  the index.csv from the store when the local file is absent (S3 backend in
  ECS), and must also honour the committed local file as a fallback when the
  store key is absent (dev path).
"""
from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botnim.storage import LocalFsStore
from botnim.storage.csv_writer import key_for_extraction


# ---------------------------------------------------------------------------
# Helpers shared across both test sections
# ---------------------------------------------------------------------------


def _make_store(root: Path) -> LocalFsStore:
    root.mkdir(parents=True, exist_ok=True)
    return LocalFsStore(root)


# ---------------------------------------------------------------------------
# GAP B — wikitext round-trip
# ---------------------------------------------------------------------------
#
# Test plan:
#   1. Build a WikitextProcessor with mocked HTTP + mocked LLM.
#   2. Run the fap via fetch_and_process_source (the real dispatcher) so it
#      exercises the GAP B fix path (writing to the sync key).
#   3. Call _collect_raw_streams_split (the sync reader) for the SAME source
#      relpath and assert it returns non-empty chunks — proving it read from
#      the store key WITHOUT us manually writing to that key.
# ---------------------------------------------------------------------------

_WIKITEXT_BOT = "unified"
# Source relpath as it appears in config.yaml (the 'source' field of the context).
_WIKITEXT_SOURCE = "extraction/Test_Page_structure_content.json"


def _valid_content_payload(html_sha256: str, version: str) -> bytes:
    """Minimal *valid* content_file that generate_markdown_dict can parse."""
    payload = {
        "metadata": {
            "input_file": "https://he.wikisource.org/wiki/Test_Page",
            "document_name": "Test_Page",
            "environment": "staging",
            "model": "gpt-4.1-mini",
            "max_tokens": None,
            "total_items": 1,
            "structure_type": "nested_hierarchy",
            "mark_type": "סעיף",
            "html_sha256": html_sha256,
            "wikitext_extractor_version": version,
        },
        "structure": [
            {
                "title": "סעיף 1",
                "id": "1",
                "content": "תוכן הסעיף הראשון.",
                "level": 1,
                "children": [],
            }
        ],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def test_wikitext_fap_writes_to_sync_key_and_reader_reads_it(tmp_path: Path, monkeypatch):
    """
    Round-trip: fap writes content_file to the sync key;
    _collect_raw_streams_split reads the SAME store key and returns chunks.
    No manual write to key_for_extraction(bot, source) is done in the test.
    """
    import hashlib
    import botnim.fetch_and_process as fap_module
    from botnim.collect_sources import _collect_raw_streams_split
    from botnim.document_parser.wikitext.process_document import WIKITEXT_EXTRACTOR_VERSION

    store_root = tmp_path / "_store"
    store = _make_store(store_root)

    # Point both the fap dispatcher and the sync reader at the same store.
    monkeypatch.setattr(fap_module, "get_artifact_store", lambda: store)

    import botnim.collect_sources as cs_module
    monkeypatch.setattr(cs_module, "get_artifact_store", lambda: store)

    html_bytes = b"<html><body>some wikitext content</body></html>"
    html_sha256 = hashlib.sha256(html_bytes).hexdigest()

    # The LLM produces a fake structure; extract_content writes the content_file.
    def _fake_extract_content(*, html_path, structure_path, content_type, output_path, **kw):
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(_valid_content_payload(html_sha256, WIKITEXT_EXTRACTOR_VERSION))

    fake_http = MagicMock()
    fake_http.content = html_bytes

    source = {
        "source": _WIKITEXT_SOURCE,
        "fetcher": {
            "kind": "wikitext",
            "input_url": "https://he.wikisource.org/wiki/Test_Page",
        },
    }
    config_dir = tmp_path / _WIKITEXT_BOT

    with patch(
        "botnim.document_parser.wikitext.pipeline_config.requests.get",
        return_value=fake_http,
    ), patch(
        "botnim.document_parser.wikitext.process_document.get_artifact_store",
        return_value=store,
    ), patch(
        "botnim.document_parser.wikitext.process_document.extract_structure_from_html",
        return_value=[],
    ), patch(
        "botnim.document_parser.wikitext.process_document.build_nested_structure",
        return_value=[],
    ), patch(
        "botnim.document_parser.wikitext.process_document.extract_content_from_html",
        side_effect=_fake_extract_content,
    ):
        fap_module.fetch_and_process_source(
            "staging", config_dir, "legal_text", source, "wikitext"
        )

    # Confirm the fap wrote to the sync-read key (not just the durable cache key).
    sync_key = key_for_extraction(_WIKITEXT_BOT, _WIKITEXT_SOURCE)
    assert store.exists(sync_key), (
        f"fap did NOT write to the sync-read key {sync_key!r}; "
        "GAP B fix is broken"
    )

    # Now call the sync reader — it must find the content without any manual
    # fixture write.
    config_dir.mkdir(parents=True, exist_ok=True)
    chunks = _collect_raw_streams_split(config_dir, "legal_text", _WIKITEXT_SOURCE)
    assert len(chunks) >= 1, (
        "sync reader returned no chunks — it could not read from the store key "
        "the fap wrote to (GAP B still present)"
    )
    # _collect_raw_streams_split returns (filename, content, ctype, extra_meta) 4-tuples.
    fname, content, ctype, extra_meta = chunks[0]
    assert "Test_Page" in fname or content.strip()


def test_wikitext_fap_also_writes_on_cache_hit(tmp_path: Path, monkeypatch):
    """
    On a CACHE HIT (durable wikitext cache present) the fap must STILL write
    to the sync key.  The local content_file is materialised from the cache
    before runner.run() returns, so the GAP B fix must pick it up on both
    code paths.
    """
    import hashlib
    import botnim.fetch_and_process as fap_module
    from botnim.document_parser.wikitext.process_document import WIKITEXT_EXTRACTOR_VERSION
    from botnim.storage.base import wikitext_cache_key

    store_root = tmp_path / "_store"
    store = _make_store(store_root)

    monkeypatch.setattr(fap_module, "get_artifact_store", lambda: store)

    html_bytes = b"<html><body>cached content</body></html>"
    html_sha256 = hashlib.sha256(html_bytes).hexdigest()

    # Pre-populate the DURABLE cache key (simulates a prior run).
    durable_key = wikitext_cache_key(_WIKITEXT_BOT, html_sha256, WIKITEXT_EXTRACTOR_VERSION)
    content_payload = _valid_content_payload(html_sha256, WIKITEXT_EXTRACTOR_VERSION)
    store.put_atomic(durable_key, content_payload)

    fake_http = MagicMock()
    fake_http.content = html_bytes

    source = {
        "source": _WIKITEXT_SOURCE,
        "fetcher": {
            "kind": "wikitext",
            "input_url": "https://he.wikisource.org/wiki/Test_Page",
        },
    }
    config_dir = tmp_path / _WIKITEXT_BOT

    llm_mock = MagicMock()
    with patch(
        "botnim.document_parser.wikitext.pipeline_config.requests.get",
        return_value=fake_http,
    ), patch(
        "botnim.document_parser.wikitext.process_document.get_artifact_store",
        return_value=store,
    ), patch(
        "botnim.document_parser.wikitext.process_document.extract_structure_from_html",
        llm_mock,
    ):
        fap_module.fetch_and_process_source(
            "staging", config_dir, "legal_text", source, "wikitext"
        )

    # It was a cache hit — LLM must not have been called.
    llm_mock.assert_not_called()

    # But the sync key must still be present.
    sync_key = key_for_extraction(_WIKITEXT_BOT, _WIKITEXT_SOURCE)
    assert store.exists(sync_key), (
        f"fap did NOT write to the sync-read key {sync_key!r} on cache-hit path; "
        "GAP B fix must handle both cache-hit and cache-miss"
    )

    # The bytes at the sync key must equal the cached payload.
    assert store.get_bytes(sync_key) == content_payload


# ---------------------------------------------------------------------------
# GAP A — indexed_pdf Stage-2 store-resident index round-trip
# ---------------------------------------------------------------------------
#
# Test plan:
#   1. Write a Stage-1 index.csv to the store at key_for_extraction(bot, idx_relpath).
#   2. Run the indexed_pdf Stage-2 fap with NO local index file on disk.
#   3. Assert Stage-2 consumed the store-resident index (processed rows / didn't
#      raise EmptyUpstreamIndex).
#   Also test the disk-fallback path: index only on disk, not in store → OK.
# ---------------------------------------------------------------------------

_PDF_BOT = "unified"
_IDX_RELPATH = "extraction/legal_advisor/index.csv"
_OUT_RELPATH = "extraction/legal_advisor.csv"


def _make_index_csv_bytes(rows: list[dict]) -> bytes:
    fieldnames = ["url", "filename", "date", "knesset_num", "title"]
    buf = io.StringIO(newline="")
    w = csv.DictWriter(buf, fieldnames=fieldnames)
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue().encode("utf-8")


def test_indexed_pdf_stage2_reads_index_from_store_when_local_absent(tmp_path: Path, monkeypatch):
    """
    Stage 1 wrote index.csv to the store; the local file does NOT exist.
    Stage 2 must read the index from the store and process the rows successfully.
    """
    import botnim.fetch_and_process as fap_module

    store_root = tmp_path / "_store"
    store = _make_store(store_root)
    monkeypatch.setattr(fap_module, "get_artifact_store", lambda: store)

    # Stage 1 writes the index to the store (simulated here).
    idx_key = key_for_extraction(_PDF_BOT, _IDX_RELPATH)
    index_bytes = _make_index_csv_bytes([
        {
            "url": "https://main.knesset.gov.il/opinion-1.pdf",
            "filename": "opinion-1.pdf",
            "date": "01.01.2026",
            "knesset_num": "25",
            "title": "חוות דעת משפטית",
        }
    ])
    store.put_atomic(idx_key, index_bytes)

    # local index file deliberately absent — config_dir / _IDX_RELPATH does NOT exist.
    config_dir = tmp_path / _PDF_BOT
    config_dir.mkdir(parents=True, exist_ok=True)
    assert not (config_dir / _IDX_RELPATH).exists()

    source = {
        "source": _OUT_RELPATH,
        "fetcher": {
            "kind": "indexed_pdf",
            "local_index_csv_path": _IDX_RELPATH,
            "fields": [
                {"name": "טקסט_מלא", "description": "Full text", "example": "...", "hint": "..."}
            ],
        },
    }

    with patch("botnim.document_parser.pdfs.process_pdfs.requests.get") as mock_get, \
         patch("botnim.document_parser.pdfs.process_pdfs.process_single_pdf") as mock_pdf, \
         patch("botnim.document_parser.pdfs.process_pdfs.get_openai_client"):
        mock_get.return_value = MagicMock(content=b"%PDF-1.4 fake", raise_for_status=MagicMock())
        mock_pdf.return_value = [{"טקסט_מלא": "תוכן חוות הדעת"}]

        # Must not raise — Stage 2 must find the index in the store.
        fap_module.fetch_and_process_source("staging", config_dir, "legal_advisor_opinions", source, "all")

    # The output CSV must be present in the store.
    out_key = key_for_extraction(_PDF_BOT, _OUT_RELPATH)
    assert store.exists(out_key), "Stage 2 did not write output — it likely never found the index"
    out_rows = list(csv.DictReader(store.get_bytes(out_key).decode("utf-8").splitlines()))
    assert len(out_rows) >= 1
    assert out_rows[0]["url"] == "https://main.knesset.gov.il/opinion-1.pdf"


def test_indexed_pdf_stage2_falls_back_to_local_disk_when_store_key_absent(tmp_path: Path, monkeypatch):
    """
    Disk-fallback path: the local index.csv exists on disk; the store has no
    object at the index key.  Stage 2 must still succeed (reads from disk).
    """
    import botnim.fetch_and_process as fap_module

    store_root = tmp_path / "_store"
    store = _make_store(store_root)
    monkeypatch.setattr(fap_module, "get_artifact_store", lambda: store)

    # Write the local index file.
    config_dir = tmp_path / _PDF_BOT
    local_idx = config_dir / _IDX_RELPATH
    local_idx.parent.mkdir(parents=True, exist_ok=True)
    local_idx.write_bytes(_make_index_csv_bytes([
        {
            "url": "https://main.knesset.gov.il/opinion-disk.pdf",
            "filename": "opinion-disk.pdf",
            "date": "01.06.2026",
            "knesset_num": "25",
            "title": "חוות דעת מדיסק",
        }
    ]))

    # Confirm the store does NOT have the index key.
    idx_key = key_for_extraction(_PDF_BOT, _IDX_RELPATH)
    assert not store.exists(idx_key)

    source = {
        "source": _OUT_RELPATH,
        "fetcher": {
            "kind": "indexed_pdf",
            "local_index_csv_path": _IDX_RELPATH,
            "fields": [
                {"name": "טקסט_מלא", "description": "Full text", "example": "...", "hint": "..."}
            ],
        },
    }

    with patch("botnim.document_parser.pdfs.process_pdfs.requests.get") as mock_get, \
         patch("botnim.document_parser.pdfs.process_pdfs.process_single_pdf") as mock_pdf, \
         patch("botnim.document_parser.pdfs.process_pdfs.get_openai_client"):
        mock_get.return_value = MagicMock(content=b"%PDF-1.4 fake", raise_for_status=MagicMock())
        mock_pdf.return_value = [{"טקסט_מלא": "תוכן מדיסק"}]

        # Must not raise — disk fallback must work.
        fap_module.fetch_and_process_source("staging", config_dir, "legal_advisor_opinions", source, "all")

    out_key = key_for_extraction(_PDF_BOT, _OUT_RELPATH)
    assert store.exists(out_key)
    out_rows = list(csv.DictReader(store.get_bytes(out_key).decode("utf-8").splitlines()))
    assert len(out_rows) >= 1
    assert out_rows[0]["url"] == "https://main.knesset.gov.il/opinion-disk.pdf"


def test_indexed_pdf_stage2_raises_when_neither_local_nor_store(tmp_path: Path, monkeypatch):
    """
    Neither the local file nor the store key exists → EmptyUpstreamIndex.
    """
    import botnim.fetch_and_process as fap_module
    from botnim.document_parser.knesset_apps.common import EmptyUpstreamIndex

    store_root = tmp_path / "_store"
    store = _make_store(store_root)
    monkeypatch.setattr(fap_module, "get_artifact_store", lambda: store)

    config_dir = tmp_path / _PDF_BOT
    config_dir.mkdir(parents=True, exist_ok=True)
    assert not (config_dir / _IDX_RELPATH).exists()
    assert not store.exists(key_for_extraction(_PDF_BOT, _IDX_RELPATH))

    source = {
        "source": _OUT_RELPATH,
        "fetcher": {
            "kind": "indexed_pdf",
            "local_index_csv_path": _IDX_RELPATH,
            "fields": [
                {"name": "טקסט_מלא", "description": "Full text", "example": "...", "hint": "..."}
            ],
        },
    }

    with patch("botnim.document_parser.pdfs.process_pdfs.get_openai_client"):
        with pytest.raises(EmptyUpstreamIndex):
            fap_module.fetch_and_process_source(
                "staging", config_dir, "legal_advisor_opinions", source, "all"
            )
