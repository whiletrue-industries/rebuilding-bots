"""Tests for alembic migration 0012_agent_turns.

Verifies that `agent_turns` and `trace_audit_log` tables are created
with the expected columns, primary keys, and indexes.

Note: the revision is named 0012 on disk (0010 and 0011 were already
taken); this test file is named 0010 per the phoenix-tracing spec's
task numbering.
"""
import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text


REPO_ROOT = Path(__file__).resolve().parent.parent.parent


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


def test_agent_turns_table_exists(database_url):
    _alembic(["upgrade", "0012_agent_turns"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        # Check table exists
        row = conn.execute(text(
            "SELECT to_regclass('public.agent_turns')"
        )).fetchone()
        assert row[0] is not None, "agent_turns table should exist"

        # Check required columns
        cols = {r[0] for r in conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='agent_turns'"
        )).fetchall()}
        assert cols >= {
            "turn_id", "conversation_id", "message_id", "trace_id",
            "summary", "cited_chunks", "env", "created_at",
        }, f"missing columns: {cols}"

        # Check primary key
        pks = {r[0] for r in conn.execute(text(
            "SELECT a.attname FROM pg_index i "
            "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
            "WHERE i.indrelid = 'agent_turns'::regclass AND i.indisprimary"
        )).fetchall()}
        assert pks == {"turn_id"}, f"expected PK on turn_id, got {pks}"

        # Check indexes
        idx_names = {r[0] for r in conn.execute(text(
            "SELECT indexname FROM pg_indexes WHERE tablename='agent_turns'"
        )).fetchall()}
        assert "ix_agent_turns_trace_id" in idx_names, \
            f"ix_agent_turns_trace_id missing from {idx_names}"
        assert "ix_agent_turns_message_id" in idx_names, \
            f"ix_agent_turns_message_id missing from {idx_names}"
        assert "ix_agent_turns_conversation_id" in idx_names, \
            f"ix_agent_turns_conversation_id missing from {idx_names}"


def test_trace_audit_log_table_exists(database_url):
    _alembic(["upgrade", "0012_agent_turns"], database_url)
    eng = create_engine(database_url)
    with eng.connect() as conn:
        # Check table exists
        row = conn.execute(text(
            "SELECT to_regclass('public.trace_audit_log')"
        )).fetchone()
        assert row[0] is not None, "trace_audit_log table should exist"

        # Check required columns
        cols = {r[0] for r in conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='trace_audit_log'"
        )).fetchall()}
        assert cols >= {
            "id", "admin_user_id", "trace_id", "conversation_id",
            "owning_user_id", "ts",
        }, f"missing columns: {cols}"

        # Check primary key
        pks = {r[0] for r in conn.execute(text(
            "SELECT a.attname FROM pg_index i "
            "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
            "WHERE i.indrelid = 'trace_audit_log'::regclass AND i.indisprimary"
        )).fetchall()}
        assert pks == {"id"}, f"expected PK on id, got {pks}"

        # Check indexes
        idx_names = {r[0] for r in conn.execute(text(
            "SELECT indexname FROM pg_indexes WHERE tablename='trace_audit_log'"
        )).fetchall()}
        assert "ix_trace_audit_log_trace_id" in idx_names, \
            f"ix_trace_audit_log_trace_id missing from {idx_names}"
        assert "ix_trace_audit_log_admin_user_id" in idx_names, \
            f"ix_trace_audit_log_admin_user_id missing from {idx_names}"
        assert "ix_trace_audit_log_ts" in idx_names, \
            f"ix_trace_audit_log_ts missing from {idx_names}"
