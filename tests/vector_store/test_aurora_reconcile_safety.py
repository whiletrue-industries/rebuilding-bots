"""Reconcile-safety regression tests for VectorStoreAurora.upload_files.

Closes the 2026-05-27 data-loss path: a per-chunk failure mid-file used
to leave the file in the reconcile-scoped `files_processed` set with a
truncated `seen_hashes`, so the reconcile DELETE wiped the file's
still-valid rows that the failed run never re-produced.

These pin ALL-OR-NOTHING-PER-FILE: a file only contributes its filename
and hashes to the reconcile scope after its ENTIRE chunk stream is
consumed without exception. The short-read injected here mirrors the
read-integrity raise from ArtifactStore.get_bytes (raises on a
truncated/short read vs Content-Length).

Fixtures (`aurora_db`, `database_url`) come from tests/conftest.py and
need the test-pg container:
    docker compose -f docker-compose.test.yml up -d test-pg
"""
from __future__ import annotations

import hashlib
import io
import logging
import os
import subprocess
from pathlib import Path

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
    """Counts embed calls; returns a deterministic 1536-dim vector."""
    def __init__(self):
        self.calls: list[str] = []

    def embed(self, content: str) -> list:
        self.calls.append(content)
        h = hashlib.sha256(content.encode()).digest()
        return [(b / 255.0) for b in h] * 48  # 1536-dim


class _ShortReadFile:
    """A read-side file-like whose .read() raises a truncated-read error,
    mirroring ArtifactStore.get_bytes raising when the bytes returned are
    shorter than Content-Length. Used to simulate a mid-file failure on
    the artifact-backed read path (content_file.read() in upload_files).
    """
    def __init__(self, declared_len: int, returned: bytes):
        self._declared = declared_len
        self._returned = returned

    def read(self, *args, **kwargs) -> bytes:
        if len(self._returned) < self._declared:
            raise IOError(
                f"short read: got {len(self._returned)} bytes, "
                f"expected {self._declared} (Content-Length)"
            )
        return self._returned


def _file_streams(items: list[tuple[str, str, dict]]):
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


def _filenames(cid: str) -> set[str]:
    from botnim.db.session import get_engine
    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(
            text("SELECT metadata->>'filename' FROM documents WHERE context_id=:cid"),
            {"cid": cid},
        ).fetchall()
    return {r[0] for r in rows}


def test_short_read_midfile_does_not_delete_live_rows(aurora_db, monkeypatch, caplog):
    """A truncated/short artifact read mid-file must:
      (a) be raised by the read (and swallowed per-chunk so the batch
          finishes),
      (b) exclude that file from the reconcile scope, and
      (c) delete ZERO live documents rows.
    This is the 2026-05-27 data-loss regression.
    """
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
        {"slug": "ctx_safe"}, "ctx_safe",
        replace_context=False, force_rebuild=False,
    )

    # Seed: two healthy chunks for a.md (split body) + one for b.md.
    store.upload_files(
        {"slug": "ctx_safe"}, "ctx_safe", cid,
        _file_streams([
            ("a.md", "alpha body one", {"title": "A"}),
            ("b.md", "beta body", {"title": "B"}),
        ]),
        callback=lambda _: None,
    )
    assert _doc_count(cid) == 2
    assert _filenames(cid) == {"a.md", "b.md"}
    seed_calls = len(fake.calls)

    # Re-run: a.md is now an artifact-backed stream whose read() raises a
    # short read (truncated mid-file) BEFORE any chunk is produced; b.md
    # is unchanged. The failed a.md must NOT enter files_processed, so the
    # reconcile must NOT delete a.md's still-live row.
    streams = [
        ("a.md", _ShortReadFile(declared_len=64, returned=b"alpha"),
         "md", {"title": "A"}),
        ("b.md", io.BytesIO(b"beta body"), "md", {"title": "B"}),
    ]
    with caplog.at_level(logging.ERROR,
                         logger="botnim.vector_store.vector_store_aurora"):
        store.upload_files(
            {"slug": "ctx_safe"}, "ctx_safe", cid, streams,
            callback=lambda _: None,
        )

    # (a) the short read raised (logged at ERROR, file-level read failure).
    assert any("short read" in r.getMessage() for r in caplog.records), (
        "expected the truncated/short read to raise and be logged"
    )
    # (c) ZERO live rows deleted — both seed rows survive.
    assert _doc_count(cid) == 2, (
        "no live rows may be deleted when a file failed mid-read"
    )
    # (b) a.md still present — it was excluded from the reconcile scope.
    assert _filenames(cid) == {"a.md", "b.md"}, (
        "a.md's live row must survive: the failed file must not be "
        "reconcile-scoped"
    )
    # a.md was never embedded this run; b.md unchanged → no new embeds.
    assert len(fake.calls) == seed_calls, (
        f"failed/unchanged files embed nothing new; got "
        f"{len(fake.calls) - seed_calls} new calls"
    )


def test_genuinely_removed_chunk_still_reconciles(aurora_db, monkeypatch):
    """Sanity: with no failures, re-uploading the SAME filename with new
    content still drops the stale chunk (the legit reconcile path keeps
    working — the safety fix must not disable reconcile)."""
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
        {"slug": "ctx_reconcile_ok"}, "ctx_reconcile_ok",
        replace_context=False, force_rebuild=False,
    )
    store.upload_files(
        {"slug": "ctx_reconcile_ok"}, "ctx_reconcile_ok", cid,
        _file_streams([("a.md", "version-one body", {"title": "A"})]),
        callback=lambda _: None,
    )
    assert _doc_count(cid) == 1

    # Re-upload same filename, new body → fresh content_hash; the file
    # streams cleanly, so it IS reconcile-scoped and the stale hash drops.
    store.upload_files(
        {"slug": "ctx_reconcile_ok"}, "ctx_reconcile_ok", cid,
        _file_streams([("a.md", "version-two body — drifted", {"title": "A"})]),
        callback=lambda _: None,
    )
    assert _doc_count(cid) == 1, (
        "a cleanly-streamed re-upload of the same filename must reconcile "
        "the stale chunk away (net stays 1)"
    )
