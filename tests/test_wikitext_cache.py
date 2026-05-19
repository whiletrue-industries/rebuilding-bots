"""Wikitext per-source structure-extraction cache.

The WikitextProcessor downloads Wikisource HTML and runs a single
`gpt-4.1-mini` call to derive a hierarchical structure. With ~10 wikitext
sources on the unified bot, the daily fap burns ~10 LLM calls/day on
content that rarely changes. The cache fast-path skips the LLM call when
the previous run's content_file is still on disk and was extracted from
this exact same HTML at the current WIKITEXT_EXTRACTOR_VERSION.

These tests exercise the cache decision logic without making real HTTP
requests or LLM calls.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _build_processor(tmp_path: Path, html_bytes: bytes, *, model: str = "gpt-4.1-mini"):
    """Construct a WikitextProcessor with mocked HTTP fetch + writable output."""
    from botnim.document_parser.wikitext.pipeline_config import (
        Environment,
        WikitextProcessorConfig,
    )
    from botnim.document_parser.wikitext.process_document import WikitextProcessor

    fake_resp = MagicMock()
    fake_resp.content = html_bytes
    with patch(
        "botnim.document_parser.wikitext.pipeline_config.requests.get",
        return_value=fake_resp,
    ):
        config = WikitextProcessorConfig(
            input_url="https://he.wikisource.org/wiki/Test_Page",
            output_base_dir=tmp_path,
            content_type="סעיף",
            environment=Environment.STAGING,
            model=model,
            max_tokens=None,
        )
    return WikitextProcessor(config)


def _write_existing_content_file(
    path: Path, *, html_sha256: str, version: str, model: str = "gpt-4.1-mini"
) -> None:
    """Drop a minimal valid content_file at `path` with the given cache key."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "metadata": {
            "input_file": "https://he.wikisource.org/wiki/Test_Page",
            "document_name": "Test_Page",
            "environment": "staging",
            "model": model,
            "max_tokens": None,
            "total_items": 0,
            "structure_type": "nested_hierarchy",
            "mark_type": "סעיף",
            "html_sha256": html_sha256,
            "wikitext_extractor_version": version,
        },
        "structure": [],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def test_run_cache_hit_skips_llm_call(tmp_path: Path):
    """A content_file with matching html_sha256 + version → skip LLM."""
    from botnim.document_parser.wikitext.process_document import (
        WIKITEXT_EXTRACTOR_VERSION,
    )

    html = b"<html><body>same content</body></html>"
    proc = _build_processor(tmp_path, html)
    _write_existing_content_file(
        proc.config.content_file,
        html_sha256=proc.config.input_html_sha256,
        version=WIKITEXT_EXTRACTOR_VERSION,
    )

    llm = MagicMock()
    with patch(
        "botnim.document_parser.wikitext.process_document.extract_structure_from_html",
        llm,
    ):
        ok = proc.run(generate_markdown=False)

    assert ok is True
    llm.assert_not_called()


def test_run_cache_miss_when_no_content_file(tmp_path: Path):
    """No content_file on disk → fresh extraction."""
    html = b"<html><body>brand new</body></html>"
    proc = _build_processor(tmp_path, html)
    assert not proc.config.content_file.exists()

    fake_struct = []
    fake_content = MagicMock()
    with patch(
        "botnim.document_parser.wikitext.process_document.extract_structure_from_html",
        return_value=fake_struct,
    ) as llm, patch(
        "botnim.document_parser.wikitext.process_document.extract_content_from_html",
        fake_content,
    ):
        ok = proc.run(generate_markdown=False)

    assert ok is True
    llm.assert_called_once()


def test_run_cache_miss_when_html_changed(tmp_path: Path):
    """Existing content_file at a different html_sha256 → fresh extraction."""
    from botnim.document_parser.wikitext.process_document import (
        WIKITEXT_EXTRACTOR_VERSION,
    )

    html = b"<html><body>new content</body></html>"
    proc = _build_processor(tmp_path, html)
    _write_existing_content_file(
        proc.config.content_file,
        html_sha256="0" * 64,  # deliberately mismatched
        version=WIKITEXT_EXTRACTOR_VERSION,
    )

    with patch(
        "botnim.document_parser.wikitext.process_document.extract_structure_from_html",
        return_value=[],
    ) as llm, patch(
        "botnim.document_parser.wikitext.process_document.extract_content_from_html",
    ):
        proc.run(generate_markdown=False)

    llm.assert_called_once()


def test_run_cache_miss_when_extractor_version_bumped(tmp_path: Path):
    """Existing content_file at an older version → fresh extraction."""
    html = b"<html><body>same content</body></html>"
    proc = _build_processor(tmp_path, html)
    _write_existing_content_file(
        proc.config.content_file,
        html_sha256=proc.config.input_html_sha256,
        version="v0-some-older-version",  # deliberately mismatched
    )

    with patch(
        "botnim.document_parser.wikitext.process_document.extract_structure_from_html",
        return_value=[],
    ) as llm, patch(
        "botnim.document_parser.wikitext.process_document.extract_content_from_html",
    ):
        proc.run(generate_markdown=False)

    llm.assert_called_once()


def test_stage_one_output_stamps_html_sha256_and_version(tmp_path: Path):
    """Stage 1 writes html_sha256 + wikitext_extractor_version into metadata
    so subsequent runs can compare against it."""
    from botnim.document_parser.wikitext.process_document import (
        WIKITEXT_EXTRACTOR_VERSION,
    )

    html = b"<html><body>fresh</body></html>"
    proc = _build_processor(tmp_path, html)
    expected_hash = proc.config.input_html_sha256

    with patch(
        "botnim.document_parser.wikitext.process_document.extract_structure_from_html",
        return_value=[],
    ), patch(
        "botnim.document_parser.wikitext.process_document.build_nested_structure",
        return_value=[],
    ), patch(
        "botnim.document_parser.wikitext.process_document.extract_content_from_html",
    ):
        proc.run(generate_markdown=False)

    # Stage 1 writes the structure_file with the cache key in metadata.
    assert proc.config.structure_file.exists()
    with open(proc.config.structure_file, "r", encoding="utf-8") as f:
        written = json.load(f)
    md = written["metadata"]
    assert md["html_sha256"] == expected_hash
    assert md["wikitext_extractor_version"] == WIKITEXT_EXTRACTOR_VERSION
