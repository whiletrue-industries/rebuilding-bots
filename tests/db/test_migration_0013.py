"""Tests for the phoenix database + role provisioning migration.

Verifies that migration 0013_phoenix_db creates:
- a ``phoenix`` database owned by the ``phoenix_app`` role
- a ``phoenix_app`` role with LOGIN privilege

Note: ``phoenix`` database and ``phoenix_app`` role are cluster-wide
objects, not scoped to a single per-test database.  We clean up any
pre-existing phoenix objects before each test so upgrades always start
from a consistent state.
"""
import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest
from sqlalchemy import create_engine, text


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_TEST_PG_HOST = "localhost"
_TEST_PG_PORT = 54329
_TEST_PG_USER = "test"
_TEST_PG_PASSWORD = "test"

# Resolve alembic from the same Python environment that's running pytest,
# so the subprocess picks up the venv binary even when alembic isn't on PATH.
_ALEMBIC = str(Path(sys.executable).parent / "alembic")


def _alembic(args: list[str], database_url: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    return subprocess.run(
        [_ALEMBIC, "--config", "alembic.ini", *args],
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def _clean_phoenix_objects() -> None:
    """Drop phoenix database and phoenix_app role if they exist.

    These are cluster-wide objects.  We clean them before each test so
    that calling ``alembic upgrade 0013_phoenix_db`` always starts from
    a known-clean state, regardless of what a prior test (or a previous
    test run) left behind.
    """
    with psycopg.connect(
        host=_TEST_PG_HOST,
        port=_TEST_PG_PORT,
        user=_TEST_PG_USER,
        password=_TEST_PG_PASSWORD,
        dbname="test",
        autocommit=True,
    ) as conn:
        # Terminate any open connections to the phoenix database before
        # dropping it, so we don't get "database is being accessed by
        # other users" errors from leftover alembic connections.
        conn.execute(
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity "
            "WHERE datname = 'phoenix' AND pid <> pg_backend_pid()"
        )
        conn.execute("DROP DATABASE IF EXISTS phoenix")
        conn.execute("DROP ROLE IF EXISTS phoenix_app")


@pytest.fixture(autouse=True)
def clean_phoenix(database_url):
    """Ensure phoenix cluster objects are absent before each test and after."""
    _clean_phoenix_objects()
    yield
    # Downgrade so the per-test alembic_version table is tidy.
    try:
        _alembic(["downgrade", "-1"], database_url)
    except subprocess.CalledProcessError:
        pass  # best-effort; cluster objects are cleaned by the next test's setup
    _clean_phoenix_objects()


def test_phoenix_database_exists(database_url):
    _alembic(["upgrade", "0013_phoenix_db"], database_url)
    with create_engine(database_url).connect() as c:
        rows = c.execute(text(
            "SELECT datname, pg_get_userbyid(datdba) "
            "FROM pg_database WHERE datname = 'phoenix'"
        )).fetchall()
    assert len(rows) == 1, f"expected exactly one 'phoenix' database row, got {rows}"
    name, owner = rows[0]
    assert name == "phoenix"
    assert owner == "phoenix_app"


def test_phoenix_app_role_can_login(database_url):
    _alembic(["upgrade", "0013_phoenix_db"], database_url)
    with create_engine(database_url).connect() as c:
        rows = c.execute(text(
            "SELECT rolcanlogin FROM pg_roles WHERE rolname = 'phoenix_app'"
        )).fetchall()
    assert len(rows) == 1, f"expected exactly one 'phoenix_app' role row, got {rows}"
    assert rows[0][0] is True, "phoenix_app role should have LOGIN privilege"
