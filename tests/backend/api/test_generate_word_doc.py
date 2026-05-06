"""Integration tests for POST /tools/generate_word_doc.

Uses FastAPI TestClient + moto S3. Follows the same fixture pattern as
``tests/backend/test_refresh_endpoint.py``: forces a fresh ``server``
import while ``firebase_admin.initialize_app`` is patched, so the
module-level firebase client doesn't try to read real credentials.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import patch

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws


def _import_server_fresh():
    """Reload backend/api/server.py with firebase patched out."""
    sys.path.insert(0, "backend/api")
    for mod in ["server", "refresh_auth", "resolve_firebase_user"]:
        sys.modules.pop(mod, None)
    with patch("firebase_admin.initialize_app"), \
         patch("firebase_admin.credentials.Certificate"):
        import server  # noqa: F401 — import side-effect
    return sys.modules["server"]


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch):
    """TestClient backed by an in-memory moto S3 with a pre-created bucket."""
    with mock_aws():
        monkeypatch.setenv("AWS_DEFAULT_REGION", "il-central-1")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        monkeypatch.setenv("WORD_DOCS_BUCKET", "botnim-word-docs-test")
        s3 = boto3.client("s3", region_name="il-central-1")
        s3.create_bucket(
            Bucket="botnim-word-docs-test",
            CreateBucketConfiguration={"LocationConstraint": "il-central-1"},
        )
        server_mod = _import_server_fresh()
        with TestClient(server_mod.app) as c:
            yield c


def test_happy_path(client: TestClient) -> None:
    body = {
        "title": "סיכום",
        "sections": [{"heading": "רקע", "level": 1, "body_md": "פסקה"}],
    }
    r = client.post("/tools/generate_word_doc", json=body)
    assert r.status_code == 200, r.text
    data = r.json()
    assert "url" in data and data["url"].startswith("https://")
    assert "expires_at" in data
    assert data["filename"].endswith(".docx")


def test_malformed_body_returns_422(client: TestClient) -> None:
    # No "sections" key at all → pydantic validation error.
    r = client.post("/tools/generate_word_doc", json={"title": "x"})
    assert r.status_code == 422


def test_empty_sections_returns_422(client: TestClient) -> None:
    r = client.post(
        "/tools/generate_word_doc",
        json={"title": "x", "sections": []},
    )
    assert r.status_code == 422


def test_missing_bucket_returns_503(monkeypatch: pytest.MonkeyPatch) -> None:
    """When WORD_DOCS_BUCKET is unset, the endpoint should return 503.

    This test deliberately does NOT use the shared `client` fixture
    (which sets the env var); it builds its own TestClient with the env
    explicitly cleared. The endpoint reads os.getenv at request time so
    no module reload is needed.
    """
    monkeypatch.delenv("WORD_DOCS_BUCKET", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "il-central-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    server_mod = _import_server_fresh()
    with TestClient(server_mod.app) as c:
        body = {
            "title": "x",
            "sections": [{"heading": "h", "level": 1, "body_md": "b"}],
        }
        r = c.post("/tools/generate_word_doc", json=body)
        assert r.status_code == 503
        assert "WORD_DOCS_BUCKET" in r.json()["detail"]
