"""Tests for VectorStoreAurora — mirrors VectorStoreES test shape."""
import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import text


REPO_ROOT = Path(__file__).resolve().parent.parent


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
    # reset cached engine
    from botnim.db import session as s
    s._engine = None
    return database_url


def test_init_with_environment(aurora_db):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    assert store.environment == "staging"
    assert store.production is False


def test_init_rejects_missing_environment(aurora_db):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    with pytest.raises(ValueError, match="Environment must be explicitly specified"):
        VectorStoreAurora(config={"slug": "u"}, config_dir=".", environment=None)


def test_get_or_create_vector_store_returns_uuid_and_inserts(aurora_db):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.db.session import get_engine

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    context_obj = {"slug": "legal_text"}

    cid = store.get_or_create_vector_store(context_obj, "legal_text", replace_context=False)

    # Returned value is a uuid string
    assert isinstance(cid, str)
    assert len(cid) == 36  # uuid

    # Row landed in contexts
    eng = get_engine()
    with eng.connect() as conn:
        row = conn.execute(text(
            "SELECT id, bot, name FROM contexts WHERE bot=:bot AND name=:name"
        ), {"bot": "unified", "name": "legal_text"}).fetchone()
    assert row is not None
    assert str(row[0]) == cid


def test_get_or_create_is_idempotent(aurora_db):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid1 = store.get_or_create_vector_store({"slug": "x"}, "x", False)
    cid2 = store.get_or_create_vector_store({"slug": "x"}, "x", False)
    assert cid1 == cid2  # same row, not a duplicate


def test_get_or_create_replace_context_clears_documents(aurora_db):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.db.session import get_engine

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "x"}, "x", False)

    # Manually insert a document
    eng = get_engine()
    with eng.connect() as conn:
        conn.execute(text(
            "INSERT INTO documents (context_id, content, content_hash) "
            "VALUES (:cid, 'hello', 'h1')"
        ), {"cid": cid})
        conn.commit()

    # replace_context=True should preserve the context row but clear its docs
    cid2 = store.get_or_create_vector_store({"slug": "x"}, "x", replace_context=True)
    assert cid2 == cid
    with eng.connect() as conn:
        n = conn.execute(text(
            "SELECT count(*) FROM documents WHERE context_id=:cid"
        ), {"cid": cid}).scalar()
    assert n == 0
