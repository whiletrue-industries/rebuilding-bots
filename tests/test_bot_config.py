"""Integration tests for botnim.bot_config.load_bot_config wiring of
agent_tool_overrides into the resulting tool list.

The default-behavior path is tested without a DB at all (the override
hook returns ``{}`` on connection failure, so the tool descriptions
fall back to their canonical config.yaml / OpenAPI YAML values). The
override paths use the same per-test postgres + alembic-head pattern
as tests/test_tool_overrides.py.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import text

from botnim.bot_config import load_bot_config
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
def aurora_db(database_url, monkeypatch):
    _alembic_upgrade(database_url)
    monkeypatch.setenv("DATABASE_URL", database_url)
    from botnim.db import session as s
    s._engine = None
    s._SessionFactory = None
    return database_url


def _seed_override(*, agent_type: str, tool_name: str, description: str,
                   active: bool = True) -> None:
    with get_session() as sess:
        sess.execute(text(
            "INSERT INTO agent_tool_overrides "
            "(agent_type, tool_name, description, active, published_at) "
            "VALUES (:a, :t, :d, :act, "
            "        CASE WHEN :act THEN now() ELSE NULL END)"
        ), {"a": agent_type, "t": tool_name, "d": description, "act": active})


def _tool_by_name(tools, name):
    matches = [t for t in tools if t.get("name") == name]
    assert matches, f"tool {name!r} not in {[t.get('name') for t in tools]}"
    return matches[0]


# ---------------------------------------------------------------------------
# Default-behavior baseline (no DB connection, no overrides) — guards against
# regressions where adding the override kwarg breaks existing callers.
# ---------------------------------------------------------------------------

def test_load_bot_config_no_overrides_uses_canonical_descriptions(monkeypatch):
    """Without DB / overrides, tool descriptions match config.yaml /
    OpenAPI YAML — i.e. the override hook is a no-op when the table is
    empty (or unreachable, as it is here)."""
    # Scrub all DB env vars so get_active_tool_overrides hits its
    # except branch and returns {}.
    from botnim.db import session as s
    s._engine = None
    s._SessionFactory = None
    for var in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD",
                "DATABASE_URL", "BOTNIM_DATABASE_URL"):
        monkeypatch.delenv(var, raising=False)

    cfg = load_bot_config("unified", "staging")

    legal = _tool_by_name(cfg.tools, "search_unified__legal_text__dev")
    assert legal["description"] == "Knesset Bylaws (תקנון הכנסת) and related laws"
    # OpenAPI tool's description matches its `description` field in the spec.
    di = _tool_by_name(cfg.tools, "DatasetInfo")
    assert "BudgetKey" in di["description"]


# ---------------------------------------------------------------------------
# Override-applied paths — both surface (search_*) and OpenAPI (operationId).
# ---------------------------------------------------------------------------

def test_search_tool_override_replaces_description(aurora_db):
    """Seeded override for the env-suffixed search tool name is applied."""
    _seed_override(
        agent_type="unified",
        tool_name="search_unified__legal_text__dev",
        description="OVERRIDDEN legal_text description.",
    )

    cfg = load_bot_config("unified", "staging")

    legal = _tool_by_name(cfg.tools, "search_unified__legal_text__dev")
    assert legal["description"] == "OVERRIDDEN legal_text description."


def test_openapi_tool_override_by_operation_id(aurora_db):
    """Seeded override keyed by operationId replaces the OpenAPI description."""
    _seed_override(
        agent_type="unified",
        tool_name="DatasetInfo",
        description="OVERRIDDEN BudgetKey DatasetInfo.",
    )

    cfg = load_bot_config("unified", "staging")

    di = _tool_by_name(cfg.tools, "DatasetInfo")
    assert di["description"] == "OVERRIDDEN BudgetKey DatasetInfo."


def test_inactive_override_is_ignored(aurora_db):
    """``active = false`` rows must not influence the tool description."""
    _seed_override(
        agent_type="unified",
        tool_name="DatasetInfo",
        description="STALE override.",
        active=False,
    )

    cfg = load_bot_config("unified", "staging")

    di = _tool_by_name(cfg.tools, "DatasetInfo")
    assert "STALE override." not in di["description"]
    assert "BudgetKey" in di["description"]


def test_override_for_other_bot_does_not_leak(aurora_db):
    """An override for a different agent_type must not be applied."""
    _seed_override(
        agent_type="some_other_bot",
        tool_name="DatasetInfo",
        description="Other bot's override.",
    )

    cfg = load_bot_config("unified", "staging")

    di = _tool_by_name(cfg.tools, "DatasetInfo")
    assert di["description"] != "Other bot's override."
