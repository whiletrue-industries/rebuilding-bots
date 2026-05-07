"""Aurora-backed read-through cache for dynamic_extraction outputs.

These tests cover the ExtractionCache surface in isolation; the
integration with collect_sources is covered later in this plan.
"""
from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import text

from botnim.extraction_cache import ExtractionCache
from botnim.db.session import get_session


REPO_ROOT = Path(__file__).resolve().parent.parent


def _alembic_upgrade(database_url: str) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    subprocess.run(
        ["alembic", "--config", "alembic.ini", "upgrade", "head"],
        cwd=REPO_ROOT, env=env, check=True, capture_output=True,
    )


@pytest.fixture
def cache(database_url, monkeypatch):
    """An ExtractionCache talking to a fresh per-test postgres DB.

    Mirrors the aurora_db pattern from tests/vector_store/test_aurora_delta.py:
    apply alembic head, expose DATABASE_URL to the cached engine, and reset
    the module-level engine so get_session() rebinds to this test's DB.
    """
    _alembic_upgrade(database_url)
    monkeypatch.setenv("DATABASE_URL", database_url)
    from botnim.db import session as s
    s._engine = None
    return ExtractionCache(environment="test")


def _row_count() -> int:
    with get_session() as sess:
        return sess.execute(text("SELECT count(*) FROM extraction_cache")).scalar_one()


def test_get_returns_none_on_miss(cache):
    assert cache.get("nonexistent_hash", "v1-gpt-4o-mini") is None


def test_put_then_get_roundtrips_payload(cache):
    payload = {
        "title": "Knesset Protocol 47",
        "DocumentMetadata": {"DocumentTitle": "Plenary Session"},
        "Topics": ["budget", "education"],
    }
    cache.put(
        content_hash="h1",
        extractor_version="v1-gpt-4o-mini",
        payload=payload,
        bot="unified",
        context="knesset_protocols",
        document_type="text/markdown",
    )
    got = cache.get("h1", "v1-gpt-4o-mini")
    assert got == payload
    assert _row_count() == 1


def test_put_is_idempotent_on_conflict(cache):
    p1 = {"title": "First"}
    p2 = {"title": "Second"}
    cache.put("h2", "v1", payload=p1, bot="unified", context="ctxA", document_type="text/markdown")
    cache.put("h2", "v1", payload=p2, bot="unified", context="ctxA", document_type="text/markdown")
    assert cache.get("h2", "v1") == p2
    assert _row_count() == 1


def test_purge_scopes_to_bot_context_version(cache):
    cache.put("hA", "v1", payload={"x": 1}, bot="unified", context="ctxA", document_type="text/markdown")
    cache.put("hB", "v1", payload={"x": 2}, bot="unified", context="ctxB", document_type="text/markdown")
    cache.put("hC", "v2", payload={"x": 3}, bot="unified", context="ctxA", document_type="text/markdown")
    cache.put("hD", "v1", payload={"x": 4}, bot="other", context="ctxA", document_type="text/markdown")
    assert _row_count() == 4

    purged = cache.purge(bot="unified", context="ctxA", extractor_version="v1")

    assert purged == 1
    assert cache.get("hA", "v1") is None      # purged
    assert cache.get("hB", "v1") is not None  # different context
    assert cache.get("hC", "v2") is not None  # different version
    assert cache.get("hD", "v1") is not None  # different bot
