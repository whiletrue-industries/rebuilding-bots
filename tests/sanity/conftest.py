"""Shared fixtures for the botnim.sanity test suite."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from botnim.sanity.types import RunSummary


REPO_ROOT = Path(__file__).resolve().parents[2]


def _alembic_upgrade(database_url: str) -> None:
    """Run ``alembic upgrade head`` against *database_url*.

    Follows the same subprocess pattern used by test_vector_store_aurora.py
    and test_aurora_writer.py — the schema is owned by alembic; storage is
    pure CRUD and must not auto-create tables.
    """
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    venv_alembic = Path(sys.executable).parent / "alembic"
    alembic = str(venv_alembic) if venv_alembic.exists() else "alembic"
    subprocess.run(
        [alembic, "--config", "alembic.ini", "upgrade", "head"],
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
    )


@pytest.fixture(autouse=True)
def _apply_migrations(database_url):
    """Apply ``alembic upgrade head`` to the per-test DB before each test.

    ``database_url`` is function-scoped (a fresh DB per test), so the
    migration runs once per test — exactly when the fresh schema is needed.
    Storage tests depend on ``database_url`` (via the ``db_url`` fixture in
    test_storage.py), so the autouse fixture fires in the right order.
    """
    _alembic_upgrade(database_url)


def _summary(**overrides) -> RunSummary:
    base = dict(
        total_rows=11,
        ab_new_wins=5,
        ab_old_wins=3,
        ab_ties=3,
        rubric_pass=8,
        rubric_fail=1,
        rubric_xfail=2,
        rubric_infra=0,
        pass_rate=8 / (8 + 1),
    )
    base.update(overrides)
    return RunSummary(**base)


@pytest.fixture
def make_summary():
    """Factory: returns a RunSummary with defaults overridable per call."""
    return _summary
