"""Route-level test for POST /botnim/admin/sanity.

Asserts auth + 202 + thread spawn. The thread itself is mocked away.
"""
from __future__ import annotations

import sys
import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


def _import_server(monkeypatch: pytest.MonkeyPatch):
    """Import server with firebase stubs and a clean module state."""
    monkeypatch.setenv("DATABASE_URL", "postgres://fake")
    sys.path.insert(0, "backend/api")
    for mod in ["server", "sanity_auth", "refresh_auth", "resolve_firebase_user"]:
        sys.modules.pop(mod, None)
    with patch("firebase_admin.initialize_app"), \
         patch("firebase_admin.credentials.Certificate"):
        import server  # noqa: F401 — import side-effect
    return server


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> tuple:
    monkeypatch.setenv("BOTNIM_SANITY_ADMIN_API_KEY", "shh-secret")
    server = _import_server(monkeypatch)
    return TestClient(server.app), server


def test_unauth_returns_401(client: tuple) -> None:
    c, _ = client
    r = c.post("/botnim/admin/sanity")
    assert r.status_code == 401


def test_wrong_key_returns_401(client: tuple) -> None:
    c, _ = client
    r = c.post("/botnim/admin/sanity", headers={"X-API-Key": "wrong"})
    assert r.status_code == 401


def test_correct_key_returns_202_and_starts_thread(client: tuple) -> None:
    c, server = client
    with patch.object(server, "_run_sanity_job_background") as mock_run:
        r = c.post("/botnim/admin/sanity", headers={"X-API-Key": "shh-secret"})
    assert r.status_code == 202
    assert r.json() == {"status": "accepted"}
    time.sleep(0.05)
    mock_run.assert_called_once()


def test_legacy_path_alias_also_works(client: tuple) -> None:
    """/admin/sanity (no /botnim prefix) is the alias mounted alongside."""
    c, server = client
    with patch.object(server, "_run_sanity_job_background"):
        r = c.post("/admin/sanity", headers={"X-API-Key": "shh-secret"})
    assert r.status_code == 202


def test_missing_key_env_returns_503(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BOTNIM_SANITY_ADMIN_API_KEY", raising=False)
    server = _import_server(monkeypatch)
    c = TestClient(server.app)
    r = c.post("/botnim/admin/sanity", headers={"X-API-Key": "anything"})
    assert r.status_code == 503


def test_run_sanity_body_uses_db_host_shape_without_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ECS regression: DATABASE_URL is NOT set on the live task, only
    DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD. The body must resolve the URL
    via the same path get_engine() uses (libpq form for raw psycopg) — not via
    ``os.environ["DATABASE_URL"]``, which previously raised KeyError and made
    every scheduled sanity invocation fail before reaching the runner."""
    # `test_query_error_handling.py` substitutes botnim.db.session with a
    # MagicMock at module-load time so server.py imports without needing a
    # real DB; that pollution leaks across files inside the same pytest
    # session. Load the REAL module from disk under a private alias and
    # patch the assertion target directly — avoids fighting the other
    # test's sys.modules setup at all.
    import importlib.util
    import pathlib
    real_session_path = (
        pathlib.Path(__file__).resolve().parents[2]
        / "botnim" / "db" / "session.py"
    )
    spec = importlib.util.spec_from_file_location(
        "_real_botnim_db_session_for_test", real_session_path
    )
    real_session = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(real_session)
    # Override JUST the function the body imports — monkeypatch reverts
    # on test exit, leaving the other test's MagicMock-module untouched.
    import sys as _sys
    target_module = _sys.modules.get("botnim.db.session")
    if target_module is not None:
        monkeypatch.setattr(
            target_module, "build_libpq_database_url",
            real_session.build_libpq_database_url, raising=False,
        )

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("BOTNIM_DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_HOST", "aurora.example")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "botnim_prod")
    monkeypatch.setenv("DB_USER", "botnim_app")
    monkeypatch.setenv("DB_PASSWORD", "secret")
    monkeypatch.setenv("BOTNIM_SANITY_ADMIN_API_KEY", "shh-secret")

    server = _import_server(monkeypatch)

    received: dict = {}

    def _fake_run_sanity(*, env: str, db_url: str) -> str:
        received["env"] = env
        received["db_url"] = db_url
        return "fake-run-id"

    fake_runner = MagicMock()
    fake_runner.run_sanity = _fake_run_sanity
    monkeypatch.setitem(sys.modules, "botnim.sanity.runner", fake_runner)

    # Bypass the advisory-lock wrapper so the body runs in-line.
    monkeypatch.setattr(server, "_try_run_with_advisory_lock", lambda _k, _l, body: body())

    server._run_sanity_job_background()

    assert received["env"] == server.DEFAULT_ENVIRONMENT or received["env"]  # not None
    assert received["db_url"].startswith("postgresql://"), received["db_url"]
    assert "+psycopg" not in received["db_url"]
    assert "aurora.example" in received["db_url"]
    assert "botnim_prod" in received["db_url"]
