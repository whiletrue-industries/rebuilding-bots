"""Tests for alembic migrations.

These tests apply the migration to an isolated pytest-postgresql DB
and assert structural properties + idempotence + downgrade inverse.
"""
import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text


REPO_ROOT = Path(__file__).resolve().parent.parent


def _alembic(args: list[str], database_url: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    return subprocess.run(
        ["alembic", "--config", "alembic.ini", *args],
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def test_0001_creates_three_tables(database_url):
    _alembic(["upgrade", "0001"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' "
            "AND tablename IN ('contexts','documents','agent_prompts') "
            "ORDER BY tablename"
        )).fetchall()
    assert [r[0] for r in rows] == ["agent_prompts", "contexts", "documents"]


def test_0001_documents_has_vector_column(database_url):
    _alembic(["upgrade", "0001"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        row = conn.execute(text(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name='documents' AND column_name='embedding'"
        )).fetchone()
    assert row is not None
    assert row[0] == "USER-DEFINED"  # 'vector' is a user-defined type from pgvector


def test_0001_downgrade_drops_all_tables(database_url):
    _alembic(["upgrade", "0001"], database_url)
    _alembic(["downgrade", "base"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' "
            "AND tablename IN ('contexts','documents','agent_prompts')"
        )).fetchall()
    assert rows == []


def test_0001_upgrade_idempotent(database_url):
    """Calling upgrade twice in a row is a no-op (alembic's job, but verify)."""
    _alembic(["upgrade", "0001"], database_url)
    _alembic(["upgrade", "0001"], database_url)  # second call is a no-op
    eng = create_engine(database_url)
    with eng.connect() as conn:
        n = conn.execute(text(
            "SELECT count(*) FROM pg_tables WHERE tablename='contexts'"
        )).scalar()
    assert n == 1


def test_0002_creates_required_indexes(database_url):
    _alembic(["upgrade", "0002"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT indexname FROM pg_indexes WHERE schemaname='public' "
            "ORDER BY indexname"
        )).fetchall()
    names = {r[0] for r in rows}
    assert "documents_embedding_ivfflat" in names
    assert "documents_tsv_gin" in names
    assert "documents_metadata_gin" in names
    assert "documents_context_id" in names
    assert "agent_prompts_one_current" in names


def test_0002_partial_unique_enforces_one_current(database_url):
    _alembic(["upgrade", "0002"], database_url)
    eng = create_engine(database_url)
    with eng.begin() as conn:
        # Two non-current rows for the same section_key — fine
        conn.execute(text("""
            INSERT INTO agent_prompts (section_key, version, content, is_current)
            VALUES ('intro', 1, '{}'::jsonb, false), ('intro', 2, '{}'::jsonb, false)
        """))
        # One current row — fine
        conn.execute(text("""
            INSERT INTO agent_prompts (section_key, version, content, is_current)
            VALUES ('intro', 3, '{}'::jsonb, true)
        """))
    # Second current row for same section — should fail in its own transaction
    with eng.begin() as conn:
        with pytest.raises(Exception) as excinfo:
            conn.execute(text("""
                INSERT INTO agent_prompts (section_key, version, content, is_current)
                VALUES ('intro', 4, '{}'::jsonb, true)
            """))
        assert "duplicate key" in str(excinfo.value).lower() or "unique" in str(excinfo.value).lower()


def test_0002_downgrade_drops_indexes(database_url):
    _alembic(["upgrade", "0002"], database_url)
    _alembic(["downgrade", "0001"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT indexname FROM pg_indexes WHERE indexname='documents_embedding_ivfflat'"
        )).fetchall()
    assert rows == []
