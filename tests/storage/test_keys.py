"""Unit tests for botnim.storage.base key-builder helpers.

No backend / network / DB needed — pure string construction.

Per the plan correction (line 34): wikitext_cache_key takes the version
as an explicit parameter; there is no WIKITEXT_EXTRACTOR_VERSION constant
in base.py (the canonical value lives in
botnim/document_parser/wikitext/process_document.py).
"""
from __future__ import annotations

import pytest

from botnim.storage.base import (
    seed_key,
    cache_key,
    wikitext_cache_key,
)


def test_seed_key_mirrors_relpath_under_bot():
    assert seed_key("unified", "legal_text/file.json") == "seed/unified/legal_text/file.json"


def test_seed_key_strips_leading_slash_on_relpath():
    assert seed_key("unified", "/legal_text/file.json") == "seed/unified/legal_text/file.json"


def test_seed_key_normalises_backslashes():
    assert seed_key("unified", "legal_text\\file.json") == "seed/unified/legal_text/file.json"


def test_cache_key_mirrors_relpath_under_bot():
    assert cache_key("unified", "common_knowledge/chunk.json") == (
        "cache/unified/common_knowledge/chunk.json"
    )


def test_wikitext_cache_key_shape():
    key = wikitext_cache_key("unified", "abc123", "v1-test")
    assert key == "cache/wikitext/unified/abc123__v1-test.json"


def test_wikitext_cache_key_different_versions_produce_different_keys():
    key_v1 = wikitext_cache_key("unified", "abc123", "v1-gpt-4.1-mini")
    key_v2 = wikitext_cache_key("unified", "abc123", "v2-gpt-4.1-mini")
    assert key_v1 != key_v2


def test_seed_key_rejects_empty_bot():
    with pytest.raises(ValueError):
        seed_key("", "x.json")


def test_seed_key_rejects_empty_relpath():
    with pytest.raises(ValueError):
        seed_key("unified", "")


def test_cache_key_rejects_empty_bot():
    with pytest.raises(ValueError):
        cache_key("", "x.json")


def test_seed_key_rejects_bot_with_forward_slash():
    with pytest.raises(ValueError):
        seed_key("uni/fied", "x.json")


def test_seed_key_rejects_bot_with_backslash():
    with pytest.raises(ValueError):
        seed_key("uni\\fied", "x.json")


def test_wikitext_cache_key_rejects_empty_html_sha256():
    with pytest.raises(ValueError):
        wikitext_cache_key("unified", "", "v1-test")


def test_wikitext_cache_key_rejects_empty_version():
    with pytest.raises(ValueError):
        wikitext_cache_key("unified", "abc123", "")
