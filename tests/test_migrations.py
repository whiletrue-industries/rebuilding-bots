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
    assert "active_by_agent_section" in names
    assert "agent_prompts_section_recent" in names


def test_0002_partial_unique_enforces_one_current(database_url):
    _alembic(["upgrade", "0002"], database_url)
    eng = create_engine(database_url)
    with eng.begin() as conn:
        # Two non-active rows for the same (agent_type, section_key) — fine
        conn.execute(text("""
            INSERT INTO agent_prompts (agent_type, section_key, body, active)
            VALUES ('unified', 'intro', 'body text', false),
                   ('unified', 'intro', 'body text 2', false)
        """))
        # One active row — fine
        conn.execute(text("""
            INSERT INTO agent_prompts (agent_type, section_key, body, active)
            VALUES ('unified', 'intro', 'active body', true)
        """))
    # Second active row for same (agent_type, section_key) — should fail in its own transaction
    with eng.begin() as conn:
        with pytest.raises(Exception) as excinfo:
            conn.execute(text("""
                INSERT INTO agent_prompts (agent_type, section_key, body, active)
                VALUES ('unified', 'intro', 'another active body', true)
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


def test_0003_creates_test_questions_table(database_url):
    _alembic(["upgrade", "0003"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT tablename FROM pg_tables "
            "WHERE tablename='agent_prompt_test_questions'"
        )).fetchall()
    assert len(rows) == 1


def test_0003_test_questions_has_required_columns(database_url):
    _alembic(["upgrade", "0003"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='agent_prompt_test_questions' ORDER BY column_name"
        )).fetchall()
    names = {r[0] for r in rows}
    assert names == {
        "id", "agent_type", "text", "ordinal", "enabled",
        "created_at", "created_by",
    }


def test_0003_downgrade_drops_table(database_url):
    _alembic(["upgrade", "0003"], database_url)
    _alembic(["downgrade", "0002"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT tablename FROM pg_tables "
            "WHERE tablename='agent_prompt_test_questions'"
        )).fetchall()
    assert rows == []


def test_0005_adds_source_id_column_and_index(database_url):
    _alembic(["upgrade", "0005"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        cols = conn.execute(text(
            "SELECT column_name, is_nullable FROM information_schema.columns "
            "WHERE table_name='documents' AND column_name='source_id'"
        )).fetchall()
        assert cols == [("source_id", "YES")], "source_id should exist and be nullable"
        idx = conn.execute(text(
            "SELECT 1 FROM pg_indexes WHERE indexname='documents_context_source'"
        )).fetchone()
        assert idx is not None, "documents_context_source index missing"


def test_0005_downgrade_drops_source_id(database_url):
    _alembic(["upgrade", "0005"], database_url)
    _alembic(["downgrade", "0004"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        cols = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='documents' AND column_name='source_id'"
        )).fetchall()
    assert cols == [], "source_id should be dropped"
