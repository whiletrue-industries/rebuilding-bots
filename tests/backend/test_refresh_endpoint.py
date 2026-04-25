"""Integration tests for POST /admin/refresh.

Uses FastAPI TestClient. The body of the refresh work (fetch + sync) is
patched out so the tests run offline; we only verify endpoint semantics:
auth, 202 response, background dispatch, and REFRESH_FAILED logging.
"""
from __future__ import annotations

import logging
import sys
import time
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("BOTNIM_ADMIN_API_KEY", "s3cret")
    sys.path.insert(0, "backend/api")
    # Force fresh imports so the dependency picks up the new env var
    for mod in [
        "server",
        "refresh_auth",
        "resolve_firebase_user",
    ]:
        sys.modules.pop(mod, None)
    # Stub out firebase at module level so resolve_firebase_user imports cleanly
    # without a real credentials file.
    with patch("firebase_admin.initialize_app"), \
         patch("firebase_admin.credentials.Certificate"):
        import server  # noqa: F401 — import side-effect
    return TestClient(server.app)


def test_refresh_requires_auth(client: TestClient) -> None:
    resp = client.post("/botnim/admin/refresh")
    assert resp.status_code == 401


def test_refresh_wrong_key_rejected(client: TestClient) -> None:
    resp = client.post("/botnim/admin/refresh", headers={"X-API-Key": "wrong"})
    assert resp.status_code == 401


def test_refresh_accepted_returns_202(client: TestClient) -> None:
    with patch("server._run_refresh_job") as mock_run:
        resp = client.post("/botnim/admin/refresh", headers={"X-API-Key": "s3cret"})
        assert resp.status_code == 202
        time.sleep(0.05)
        mock_run.assert_called_once()


def test_refresh_failure_logs_refresh_failed_prefix(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    def blow_up() -> None:
        raise RuntimeError("boom")

    caplog.set_level(logging.ERROR)
    with patch("server._run_refresh_job", side_effect=blow_up):
        resp = client.post("/botnim/admin/refresh", headers={"X-API-Key": "s3cret"})
        assert resp.status_code == 202
        time.sleep(0.1)
    matching = [r for r in caplog.records if "REFRESH_FAILED" in r.getMessage()]
    assert matching, f"expected REFRESH_FAILED in logs; got: {[r.getMessage() for r in caplog.records]}"
