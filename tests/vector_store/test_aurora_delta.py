"""Delta-sync semantics: re-running sync only embeds new/changed chunks.

These tests pin down the new force_rebuild kwarg semantics. Pattern
mirrors tests/test_vector_store_aurora.py — uses the same aurora_db
fixture (per-test postgres DB + alembic upgrade head + DATABASE_URL +
cached-engine reset) and the same _get_embedding_client patch target.
"""
from __future__ import annotations

import hashlib
import io
import os
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import text


REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def _alembic_upgrade(database_url: str) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    subprocess.run(
        ["alembic", "--config", "alembic.ini", "upgrade", "head"],
        cwd=REPO_ROOT, env=env, check=True, capture_output=True,
    )


@pytest.fixture
def aurora_db(database_url, monkeypatch):
    """Aurora backend pointed at a fresh per-test DB with schema applied."""
    _alembic_upgrade(database_url)
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("OPENAI_API_KEY_STAGING", "sk-test")
    from botnim.db import session as s
    s._engine = None
    return database_url


class _FakeEmbeddingClient:
    """Counts embed calls so we can prove the content-hash skip works."""
    def __init__(self):
        self.calls: list[str] = []

    def embed(self, content: str) -> list:
        self.calls.append(content)
        h = hashlib.sha256(content.encode()).digest()
        return [(b / 255.0) for b in h] * 48  # 1536-dim


def _file_streams(items: list[tuple[str, str, dict]]):
    """items: [(filename, content_str, metadata)]. Returns the
    (fname, file, type, metadata) tuple shape upload_files expects."""
    return [
        (fname, io.BytesIO(content.encode()), "md", metadata)
        for fname, content, metadata in items
    ]


def _doc_count(cid: str) -> int:
    from botnim.db.session import get_engine
    eng = get_engine()
    with eng.connect() as conn:
        return conn.execute(
            text("SELECT count(*) FROM documents WHERE context_id=:cid"),
            {"cid": cid},
        ).scalar_one()


def test_delta_default_inserts_new_chunks_only(aurora_db, monkeypatch):
    """force_rebuild=False: keep existing rows, embed only what's missing."""
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora

    fake = _FakeEmbeddingClient()
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: fake,
    )
    store = VectorStoreAurora(
        config={"slug": "unified", "name": "Unified"},
        config_dir=".", environment="staging",
    )

    cid = store.get_or_create_vector_store(
        {"slug": "ctx_delta"}, "ctx_delta",
        replace_context=False, force_rebuild=False,
    )

    # Pre-seed one row that should be REUSED on the next upload.
    streams_seed = _file_streams([("a.md", "alpha", {"title": "A"})])
    store.upload_files({"slug": "ctx_delta"}, "ctx_delta", cid, streams_seed,
                      callback=lambda x: None)
    assert _doc_count(cid) == 1
    seed_calls = len(fake.calls)
    assert seed_calls == 1, f"seed should embed once, got {seed_calls}"

    # Now upload again with the same chunk + a new chunk. Default
    # force_rebuild=False: existing chunk is reused (no embed),
    # new chunk is inserted (one embed).
    streams_delta = _file_streams([
        ("a.md", "alpha", {"title": "A"}),
        ("b.md", "beta",  {"title": "B"}),
    ])
    store.upload_files({"slug": "ctx_delta"}, "ctx_delta", cid, streams_delta,
                      callback=lambda x: None)
    assert _doc_count(cid) == 2
    assert len(fake.calls) - seed_calls == 1, (
        f"expected exactly 1 new embed call (for 'beta'), got "
        f"{len(fake.calls) - seed_calls}: {fake.calls[seed_calls:]}"
    )


def test_force_rebuild_wipes_and_reembeds_all(aurora_db, monkeypatch):
    """force_rebuild=True: DELETE pre-existing rows, re-embed every chunk."""
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora

    fake = _FakeEmbeddingClient()
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: fake,
    )
    store = VectorStoreAurora(
        config={"slug": "unified", "name": "Unified"},
        config_dir=".", environment="staging",
    )

    cid = store.get_or_create_vector_store(
        {"slug": "ctx_rebuild"}, "ctx_rebuild",
        replace_context=False, force_rebuild=False,
    )
    streams_seed = _file_streams([
        ("x.md", "ex content", {"title": "X"}),
        ("y.md", "wy content", {"title": "Y"}),
    ])
    store.upload_files({"slug": "ctx_rebuild"}, "ctx_rebuild", cid, streams_seed,
                      callback=lambda x: None)
    assert _doc_count(cid) == 2
    seed_calls = len(fake.calls)
    assert seed_calls == 2

    # Now force_rebuild=True: get_or_create wipes the table for this cid.
    cid_after = store.get_or_create_vector_store(
        {"slug": "ctx_rebuild"}, "ctx_rebuild",
        replace_context=False, force_rebuild=True,
    )
    assert cid_after == cid, f"force_rebuild must preserve the contexts row id; expected {cid}, got {cid_after}"
    assert _doc_count(cid) == 0, "force_rebuild should have wiped documents"

    # Re-upload — every chunk is a fresh embed (no cache to hit).
    # Build a fresh stream list with new BytesIO instances, since
    # streams_seed's BytesIO read pointers are at EOF after the first upload.
    streams_reupload = _file_streams([
        ("x.md", "ex content", {"title": "X"}),
        ("y.md", "wy content", {"title": "Y"}),
    ])
    store.upload_files({"slug": "ctx_rebuild"}, "ctx_rebuild", cid, streams_reupload,
                      callback=lambda x: None)
    assert _doc_count(cid) == 2
    assert len(fake.calls) - seed_calls == 2, (
        f"expected 2 new embeds after wipe, got {len(fake.calls) - seed_calls}"
    )


def test_replace_context_none_with_force_rebuild_false_is_noop(aurora_db, monkeypatch):
    """vector_store_update with replace_context='none' must not call
    upload_files for any context. This tests the should_process logic in
    vector_store_base.vector_store_update."""
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora

    fake = _FakeEmbeddingClient()
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: fake,
    )
    store = VectorStoreAurora(
        config={"slug": "unified", "name": "Unified"},
        config_dir=".", environment="staging",
    )

    # Pre-create a context + one row so we can verify it's preserved.
    cid = store.get_or_create_vector_store(
        {"slug": "ctx_noop"}, "ctx_noop",
        replace_context=False, force_rebuild=False,
    )
    streams_seed = _file_streams([("p.md", "preserve", {"title": "P"})])
    store.upload_files({"slug": "ctx_noop"}, "ctx_noop", cid, streams_seed,
                      callback=lambda x: None)
    pre_count = _doc_count(cid)
    assert pre_count == 1

    # Now run vector_store_update with replace_context='none'. Patch the
    # downstream methods so we can verify upload_files is NEVER reached.
    contexts = [{"slug": "ctx_noop", "name": "ctx_noop", "type": "csv"}]
    with patch.object(store, "upload_files") as mock_upload, \
         patch.object(store, "update_tool_resources"), \
         patch.object(store, "update_tools"), \
         patch.object(store, "delete_existing_files") as mock_delete:
        store.vector_store_update(
            contexts, replace_context="none", force_rebuild=False,
        )

    assert mock_upload.call_count == 0, (
        f"replace_context='none' must not upload; got {mock_upload.call_count} call(s)"
    )
    assert mock_delete.call_count == 0, (
        f"replace_context='none' must not delete; got {mock_delete.call_count} call(s)"
    )
    assert _doc_count(cid) == pre_count, "row count unchanged"
