"""Regression: opening a KVFile when its parent directory is missing
raises ``sqlite3.OperationalError: unable to open database file``.

Hit live in staging on 2026-05-10 when ``botnim sync ... --replace-context
committee_decisions --force-rebuild`` ran in the botnim-api ECS task: the
2026-05-09 entrypoint cleanup removed ``mkdir /srv/cache`` from
api_server.sh under the (mistaken) belief that Aurora replaces the kvfile
caches. ``collect_sources.py`` and ``vector_store_es.py`` still init
sqlite-backed kvfiles under ``<repo_root>/cache/`` regardless of backend,
so the wipe phase succeeded and the re-embed phase crashed — leaving the
context empty in Aurora.

The fix puts ``location.parent.mkdir(parents=True, exist_ok=True)`` next
to each ``KVFile(location=...)`` call. These tests exercise the helpers
that wrap that pattern.
"""
from __future__ import annotations

from pathlib import Path

import pytest


def test_open_metadata_cache_creates_missing_parent_dir(tmp_path, monkeypatch):
    """``collect_sources._open_metadata_cache`` must auto-create
    ``<repo_root>/cache/`` if it doesn't exist, since the kvfile/sqlite3
    layer won't and the dir is absent in the prod docker image."""
    from botnim import collect_sources

    fake_module_file = tmp_path / "no-cache-here" / "botnim" / "collect_sources.py"
    monkeypatch.setattr(collect_sources, "__file__", str(fake_module_file))

    expected_parent = fake_module_file.parent.parent / "cache"
    assert not expected_parent.exists(), "precondition: cache dir does not yet exist"

    cache = collect_sources._open_metadata_cache()

    assert expected_parent.exists(), "helper should mkdir the parent"
    cache.set("k", "v")
    assert cache.get("k") == "v"


def test_open_embedding_cache_creates_missing_parent_dir(tmp_path, monkeypatch):
    """``vector_store_es._open_embedding_cache`` must auto-create
    ``<repo_root>/cache/`` for the same reason — the embedding kvfile
    sits next to the metadata kvfile under the repo's ``cache/`` dir."""
    from botnim.vector_store import vector_store_es

    fake_module_file = (
        tmp_path / "no-cache-here" / "botnim" / "vector_store" / "vector_store_es.py"
    )
    monkeypatch.setattr(vector_store_es, "__file__", str(fake_module_file))

    expected_parent = fake_module_file.parent.parent.parent / "cache"
    assert not expected_parent.exists(), "precondition: cache dir does not yet exist"

    cache = vector_store_es._open_embedding_cache()

    assert expected_parent.exists(), "helper should mkdir the parent"
    cache.set("k", "v")
    assert cache.get("k") == "v"
