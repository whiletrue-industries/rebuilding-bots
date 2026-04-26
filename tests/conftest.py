from __future__ import annotations

import socket

import psycopg
import pytest
from pytest_postgresql import factories


_TEST_PG_HOST = "localhost"
_TEST_PG_PORT = 54329


def pytest_configure(config):
    """Fail fast (with a clear message) if the test-pg container isn't running.

    Without this, psycopg.connect() in _load_vector_extension hangs ~33s
    waiting for the healthcheck retry budget, then errors with a generic
    "could not connect to server" — masking the real cause (the developer
    forgot to start Docker).
    """
    try:
        with socket.create_connection((_TEST_PG_HOST, _TEST_PG_PORT), timeout=1):
            pass
    except (OSError, socket.timeout):
        pytest.exit(
            f"\n\ntest-pg not reachable at {_TEST_PG_HOST}:{_TEST_PG_PORT}.\n"
            "Start it with:\n"
            "    docker compose -f docker-compose.test.yml up -d test-pg\n",
            returncode=2,
        )


def _load_vector_extension(
    host: str,
    port: int,
    user: str,
    dbname: str,
    password: str,
) -> None:
    """Create the pgvector extension on the template DB so every
    per-test database inherits it automatically."""
    with psycopg.connect(
        host=host,
        port=port,
        user=user,
        dbname=dbname,
        password=password,
        autocommit=True,
    ) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS vector")


# Connect to the Docker-managed Postgres at localhost:54329.
# pytest-postgresql's noproc factory uses an external server; we don't
# spawn pg_ctl, so no local Postgres binary is required.
#
# NOTE: dbname must NOT match the pre-existing "test" DB created by the
# Docker container's POSTGRES_DB env var. The DatabaseJanitor will try to
# CREATE DATABASE <dbname> TEMPLATE <dbname>_tmpl, which would fail with
# "already exists" if we reused "test". Using "pytest_db" as the base
# name avoids that collision.
postgresql_proc = factories.postgresql_noproc(
    host=_TEST_PG_HOST,
    port=_TEST_PG_PORT,
    user="test",
    password="test",
    dbname="pytest_db",
    load=[_load_vector_extension],
)

postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
def pg_connection(postgresql):
    """A psycopg connection to a per-test isolated Postgres database
    with the vector extension loaded. Backed by the test-pg Docker
    container (start with: docker compose -f docker-compose.test.yml up -d test-pg).
    """
    return postgresql


@pytest.fixture
def database_url(postgresql) -> str:
    info = postgresql.info
    # Use the psycopg v3 dialect; psycopg2 is not installed in this project.
    return f"postgresql+psycopg://{info.user}:{info.password}@{info.host}:{info.port}/{info.dbname}"
