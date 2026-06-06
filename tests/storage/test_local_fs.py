"""Behavioural tests for LocalFsStore.

No DB / network — pure tmp_path filesystem. The shared cross-backend
round-trip suite lives in tests/storage/test_artifact_store_contract.py;
this file pins LocalFs-only mechanics (atomic temp file, path mapping).
"""
from __future__ import annotations

import os

import pytest

from botnim.storage.local_fs import LocalFsStore


def test_put_then_get_roundtrip(tmp_path):
    store = LocalFsStore(str(tmp_path))
    store.put_atomic("seed/unified/a/b.json", b"hello")
    assert store.get_bytes("seed/unified/a/b.json") == b"hello"


def test_put_creates_nested_dirs(tmp_path):
    store = LocalFsStore(str(tmp_path))
    store.put_atomic("cache/unified/deep/nested/x.json", b"x")
    assert (tmp_path / "cache" / "unified" / "deep" / "nested" / "x.json").read_bytes() == b"x"


def test_get_bytes_missing_raises_filenotfound(tmp_path):
    store = LocalFsStore(str(tmp_path))
    with pytest.raises(FileNotFoundError):
        store.get_bytes("seed/unified/missing.json")


def test_open_stream_missing_raises_filenotfound(tmp_path):
    store = LocalFsStore(str(tmp_path))
    with pytest.raises(FileNotFoundError):
        store.open_stream("seed/unified/missing.json")


def test_open_stream_reads_full_body(tmp_path):
    store = LocalFsStore(str(tmp_path))
    store.put_atomic("cache/unified/s.json", b"streamed-body")
    with store.open_stream("cache/unified/s.json") as fh:
        assert fh.read() == b"streamed-body"


def test_put_atomic_overwrite(tmp_path):
    store = LocalFsStore(str(tmp_path))
    store.put_atomic("seed/unified/v.json", b"v1")
    store.put_atomic("seed/unified/v.json", b"v2-longer")
    assert store.get_bytes("seed/unified/v.json") == b"v2-longer"


def test_put_atomic_leaves_no_temp_files(tmp_path):
    store = LocalFsStore(str(tmp_path))
    store.put_atomic("seed/unified/t.json", b"data")
    leftovers = [p.name for p in (tmp_path / "seed" / "unified").iterdir()]
    assert leftovers == ["t.json"]


def test_exists(tmp_path):
    store = LocalFsStore(str(tmp_path))
    assert store.exists("seed/unified/e.json") is False
    store.put_atomic("seed/unified/e.json", b"e")
    assert store.exists("seed/unified/e.json") is True


def test_list_returns_keys_not_abs_paths(tmp_path):
    store = LocalFsStore(str(tmp_path))
    store.put_atomic("cache/wikitext/unified/aaa__v1.json", b"1")
    store.put_atomic("cache/wikitext/unified/bbb__v1.json", b"2")
    store.put_atomic("cache/wikitext/other/ccc__v1.json", b"3")
    got = sorted(store.list("cache/wikitext/unified/"))
    assert got == [
        "cache/wikitext/unified/aaa__v1.json",
        "cache/wikitext/unified/bbb__v1.json",
    ]


def test_list_missing_prefix_returns_empty(tmp_path):
    store = LocalFsStore(str(tmp_path))
    assert store.list("cache/wikitext/nope/") == []


def test_traversal_key_is_rejected(tmp_path):
    store = LocalFsStore(str(tmp_path))
    with pytest.raises(ValueError):
        store.put_atomic("../escape.json", b"x")
