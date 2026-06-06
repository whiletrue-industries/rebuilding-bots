"""LocalFs-only mechanics for LocalFsStore.

The shared cross-backend round-trip / exists / list / atomic-overwrite
suite lives in tests/storage/test_artifact_store_contract.py; this file
pins behaviour that only the filesystem backend can exhibit (atomic
temp file, on-disk path mapping, traversal guard, the stat-vs-read
truncation guard).

No DB / network — pure tmp_path filesystem.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from botnim.storage.local_fs import LocalFsStore


def test_put_creates_nested_dirs(tmp_path):
    store = LocalFsStore(str(tmp_path))
    store.put_atomic("cache/unified/deep/nested/x.json", b"x")
    assert (tmp_path / "cache" / "unified" / "deep" / "nested" / "x.json").read_bytes() == b"x"


def test_put_atomic_leaves_no_temp_files(tmp_path):
    store = LocalFsStore(str(tmp_path))
    store.put_atomic("seed/unified/t.json", b"data")
    leftovers = [p.name for p in (tmp_path / "seed" / "unified").iterdir()]
    assert leftovers == ["t.json"]


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


def test_get_bytes_truncation_raises(tmp_path, monkeypatch):
    """If stat reports a larger size than the file actually holds (e.g. a
    concurrent truncation between stat and read), get_bytes must raise
    OSError, not return a short body. This guard is load-bearing for the
    later reconcile-safety fix."""
    store = LocalFsStore(str(tmp_path))
    store.put_atomic("seed/unified/short.json", b"abc")

    real_stat = Path.stat

    class _FatStat:
        """Wraps a real stat_result but advertises a bigger st_size."""

        def __init__(self, base):
            self._base = base

        @property
        def st_size(self):
            return self._base.st_size + 100

        def __getattr__(self, name):
            return getattr(self._base, name)

    def fake_stat(self, *args, **kwargs):
        result = real_stat(self, *args, **kwargs)
        if self.name == "short.json":
            return _FatStat(result)
        return result

    monkeypatch.setattr(Path, "stat", fake_stat)

    with pytest.raises(OSError):
        store.get_bytes("seed/unified/short.json")
