"""Tests for botnim.db.tool_overrides.get_active_tool_overrides.

Pattern matches tests/test_extraction_cache.py: per-test postgres DB
from the ``database_url`` fixture in conftest.py, alembic ``upgrade head``
to create ``agent_tool_overrides``, then raw INSERT seeding so the test
shape is independent of any future ORM helpers.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import text

from botnim.db.session import get_session
from botnim.db.tool_overrides import get_active_tool_overrides


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
    """Fresh per-test postgres DB with alembic head applied + cached
    engine reset so get_session() rebinds to this test's DB.
    """
    _alembic_upgrade(database_url)
    monkeypatch.setenv("DATABASE_URL", database_url)
    from botnim.db import session as s
    s._engine = None
    s._SessionFactory = None
    return database_url


def _insert_override(
    *,
    agent_type: str,
    tool_name: str,
    description: str,
    active: bool = True,
) -> None:
    with get_session() as sess:
        sess.execute(text(
            "INSERT INTO agent_tool_overrides "
            "(agent_type, tool_name, description, active, published_at) "
            "VALUES (:a, :t, :d, :act, "
            "        CASE WHEN :act THEN now() ELSE NULL END)"
        ), {"a": agent_type, "t": tool_name, "d": description, "act": active})


def test_returns_empty_when_no_rows(aurora_db):
    assert get_active_tool_overrides("unified") == {}


def test_returns_active_overrides(aurora_db):
    _insert_override(
        agent_type="unified",
        tool_name="search_unified__legal_text",
        description="Custom legal_text description.",
    )
    _insert_override(
        agent_type="unified",
        tool_name="fetchWordDocument",
        description="Custom fetchWordDocument description.",
    )

    got = get_active_tool_overrides("unified")
    assert got == {
        "search_unified__legal_text": "Custom legal_text description.",
        "fetchWordDocument": "Custom fetchWordDocument description.",
    }


def test_skips_inactive_rows(aurora_db):
    _insert_override(
        agent_type="unified",
        tool_name="search_unified__legal_text",
        description="Inactive desc.",
        active=False,
    )
    assert get_active_tool_overrides("unified") == {}


def test_scopes_to_bot_slug(aurora_db):
    _insert_override(
        agent_type="other_bot",
        tool_name="some_tool",
        description="Other bot's override.",
    )
    assert get_active_tool_overrides("unified") == {}
    assert get_active_tool_overrides("other_bot") == {
        "some_tool": "Other bot's override.",
    }


def test_db_unreachable_returns_empty(monkeypatch):
    """When Aurora is unreachable (no DB env vars at all), the function
    must NOT raise — callers in ``load_bot_config`` rely on this."""
    # Reset cached engine + scrub all DB-related env vars so
    # _build_database_url() raises and our try/except converts to {}.
    from botnim.db import session as s
    s._engine = None
    s._SessionFactory = None
    for var in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD",
                "DATABASE_URL", "BOTNIM_DATABASE_URL"):
        monkeypatch.delenv(var, raising=False)
    assert get_active_tool_overrides("unified") == {}
