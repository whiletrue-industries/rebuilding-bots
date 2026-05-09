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
