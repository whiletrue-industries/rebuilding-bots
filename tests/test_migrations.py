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


def test_0006_creates_context_snapshots_table(database_url):
    _alembic(["upgrade", "0006"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        cols = sorted(conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='context_snapshots' ORDER BY column_name"
        )).fetchall())
        assert cols == [
            ("bot",), ("context",), ("doc_count",), ("id",),
            ("snapshot_at",), ("source_id",),
        ], f"unexpected columns: {cols}"
        idx = conn.execute(text(
            "SELECT 1 FROM pg_indexes WHERE indexname='context_snapshots_lookup'"
        )).fetchone()
        assert idx is not None, "context_snapshots_lookup index missing"


def test_0006_downgrade_drops_table(database_url):
    _alembic(["upgrade", "0006"], database_url)
    _alembic(["downgrade", "0005"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT to_regclass('public.context_snapshots')"
        )).fetchone()
    assert rows[0] is None, "context_snapshots should be dropped"


def test_0007_swaps_ivfflat_for_hnsw(database_url):
    _alembic(["upgrade", "0007"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        names = {r[0] for r in conn.execute(text(
            "SELECT indexname FROM pg_indexes WHERE tablename='documents'"
        )).fetchall()}
    assert "documents_embedding_hnsw" in names
    assert "documents_embedding_ivfflat" not in names


def test_0007_downgrade_restores_ivfflat(database_url):
    _alembic(["upgrade", "0007"], database_url)
    _alembic(["downgrade", "0006"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        names = {r[0] for r in conn.execute(text(
            "SELECT indexname FROM pg_indexes WHERE tablename='documents'"
        )).fetchall()}
    assert "documents_embedding_ivfflat" in names
    assert "documents_embedding_hnsw" not in names


# ---------------------------------------------------------------------------
# 0009 — agent_tool_overrides table + agent_prompt_snapshots view
# (UPE Task 2 — see docs/superpowers/specs/2026-05-07-unified-prompt-editor-design.md §5.1)
# ---------------------------------------------------------------------------


def test_0009_creates_agent_tool_overrides_table(database_url):
    _alembic(["upgrade", "0009_unified_prompt_editor"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT tablename FROM pg_tables "
            "WHERE schemaname='public' AND tablename='agent_tool_overrides'"
        )).fetchall()
    assert len(rows) == 1


def test_0009_agent_tool_overrides_has_required_columns(database_url):
    _alembic(["upgrade", "0009_unified_prompt_editor"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='agent_tool_overrides' ORDER BY column_name"
        )).fetchall()
    names = {r[0] for r in rows}
    assert names == {
        "id", "agent_type", "tool_name", "description", "active", "is_draft",
        "parent_version_id", "change_note", "created_by", "created_at",
        "published_at",
    }


def test_0009_partial_unique_active_index_exists_with_predicate(database_url):
    """The partial unique index must include ``WHERE active = true`` so that
    multiple historical (active=false) rows for the same (agent_type, tool_name)
    are allowed, but two simultaneously-active rows are forbidden."""
    _alembic(["upgrade", "0009_unified_prompt_editor"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        row = conn.execute(text(
            "SELECT indexdef FROM pg_indexes "
            "WHERE indexname='agent_tool_overrides_active_uniq'"
        )).fetchone()
    assert row is not None, "agent_tool_overrides_active_uniq index missing"
    indexdef = row[0]
    assert "UNIQUE" in indexdef.upper(), f"index not unique: {indexdef}"
    assert "active" in indexdef.lower() and "true" in indexdef.lower(), (
        f"index predicate missing 'WHERE active = true': {indexdef}"
    )


def test_0009_active_uniqueness_is_enforced_per_agent_tool(database_url):
    _alembic(["upgrade", "0009_unified_prompt_editor"], database_url)
    eng = create_engine(database_url)
    with eng.begin() as conn:
        # Multiple historical rows (active=false) — fine.
        conn.execute(text("""
            INSERT INTO agent_tool_overrides (agent_type, tool_name, description, active)
            VALUES ('unified', 'search_unified__legal_text', 'first', false),
                   ('unified', 'search_unified__legal_text', 'second', false)
        """))
        # Exactly one active row — fine.
        conn.execute(text("""
            INSERT INTO agent_tool_overrides (agent_type, tool_name, description, active)
            VALUES ('unified', 'search_unified__legal_text', 'currently active', true)
        """))
    # A second active row for the same (agent_type, tool_name) must fail.
    with eng.begin() as conn:
        with pytest.raises(Exception) as excinfo:
            conn.execute(text("""
                INSERT INTO agent_tool_overrides (agent_type, tool_name, description, active)
                VALUES ('unified', 'search_unified__legal_text', 'duplicate active', true)
            """))
        assert (
            "duplicate key" in str(excinfo.value).lower()
            or "unique" in str(excinfo.value).lower()
        )


def test_0009_lookup_index_exists(database_url):
    _alembic(["upgrade", "0009_unified_prompt_editor"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        row = conn.execute(text(
            "SELECT 1 FROM pg_indexes "
            "WHERE indexname='agent_tool_overrides_lookup'"
        )).fetchone()
    assert row is not None


def test_0009_creates_agent_prompt_snapshots_view(database_url):
    _alembic(["upgrade", "0009_unified_prompt_editor"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        row = conn.execute(text(
            "SELECT viewname FROM pg_views "
            "WHERE schemaname='public' AND viewname='agent_prompt_snapshots'"
        )).fetchone()
    assert row is not None, "agent_prompt_snapshots view missing"


def test_0009_view_groups_published_sections_by_minute(database_url):
    """Two ``agent_prompts`` rows published at the same minute must collapse
    into a single snapshot row whose ``section_version_ids`` array contains
    both ids ordered by ``ordinal``. A third row published a minute later
    must surface as a separate snapshot."""
    _alembic(["upgrade", "0009_unified_prompt_editor"], database_url)
    eng = create_engine(database_url)
    with eng.begin() as conn:
        # Two sections published at the same minute (different seconds is fine —
        # date_trunc('minute', ...) collapses them).
        conn.execute(text("""
            INSERT INTO agent_prompts (
                agent_type, section_key, ordinal, body, active, is_draft,
                published_at, created_by
            ) VALUES
                ('unified', 'intro',   0, 'intro body',   true, false,
                 timestamptz '2026-05-07 12:00:01+00', 'alice'),
                ('unified', 'methods', 1, 'methods body', true, false,
                 timestamptz '2026-05-07 12:00:30+00', 'alice'),
                -- A row published a different minute → separate snapshot.
                ('unified', 'closing', 2, 'closing body', true, false,
                 timestamptz '2026-05-07 12:01:05+00', 'bob'),
                -- An unpublished draft must NOT surface.
                ('unified', 'draft_only', 3, 'draft body', false, true,
                 NULL, 'alice')
        """))

        rows = conn.execute(text(
            "SELECT agent_type, snapshot_minute, section_keys, published_by "
            "FROM agent_prompt_snapshots "
            "ORDER BY snapshot_minute"
        )).fetchall()

    assert len(rows) == 2, f"expected 2 snapshot rows, got {len(rows)}: {rows}"

    first = rows[0]
    assert first.agent_type == "unified"
    # Two sections grouped at minute 12:00.
    assert list(first.section_keys) == ["intro", "methods"], (
        f"section ordering wrong: {first.section_keys}"
    )
    assert first.published_by == "alice"

    second = rows[1]
    assert second.agent_type == "unified"
    assert list(second.section_keys) == ["closing"]
    assert second.published_by == "bob"


def test_0009_downgrade_drops_view_and_table(database_url):
    _alembic(["upgrade", "0009_unified_prompt_editor"], database_url)
    _alembic(["downgrade", "0008_extraction_cache"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        view = conn.execute(text(
            "SELECT to_regclass('public.agent_prompt_snapshots')"
        )).fetchone()
        table = conn.execute(text(
            "SELECT to_regclass('public.agent_tool_overrides')"
        )).fetchone()
    assert view[0] is None, "agent_prompt_snapshots view should be dropped"
    assert table[0] is None, "agent_tool_overrides table should be dropped"


def test_0009_round_trips(database_url):
    """upgrade head → downgrade -1 → upgrade head must be a clean round-trip."""
    _alembic(["upgrade", "head"], database_url)
    _alembic(["downgrade", "-1"], database_url)
    _alembic(["upgrade", "head"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        table = conn.execute(text(
            "SELECT to_regclass('public.agent_tool_overrides')"
        )).fetchone()
        view = conn.execute(text(
            "SELECT to_regclass('public.agent_prompt_snapshots')"
        )).fetchone()
    assert table[0] is not None
    assert view[0] is not None
