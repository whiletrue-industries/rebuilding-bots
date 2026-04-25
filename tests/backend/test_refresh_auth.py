"""Unit tests for the refresh API-key auth dependency."""
from __future__ import annotations

import os

import pytest
from fastapi import HTTPException


def _load_module():
    # Import lazily so monkeypatching BOTNIM_ADMIN_API_KEY takes effect.
    import importlib
    import sys
    import os
    # Use absolute path: pytest may not run from the worktree root
    backend_api_path = os.path.join(os.path.dirname(__file__), "..", "..", "backend", "api")
    sys.path.insert(0, backend_api_path)
    import refresh_auth
    importlib.reload(refresh_auth)
    return refresh_auth


def test_missing_env_var_rejects_any_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BOTNIM_ADMIN_API_KEY", raising=False)
    refresh_auth = _load_module()
    with pytest.raises(HTTPException) as exc_info:
        refresh_auth.require_refresh_api_key(x_api_key="anything")
    assert exc_info.value.status_code == 503  # service not configured


def test_correct_key_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BOTNIM_ADMIN_API_KEY", "s3cret")
    refresh_auth = _load_module()
    # Should not raise
    refresh_auth.require_refresh_api_key(x_api_key="s3cret")


def test_wrong_key_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BOTNIM_ADMIN_API_KEY", "s3cret")
    refresh_auth = _load_module()
    with pytest.raises(HTTPException) as exc_info:
        refresh_auth.require_refresh_api_key(x_api_key="wrong")
    assert exc_info.value.status_code == 401


def test_missing_header_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BOTNIM_ADMIN_API_KEY", "s3cret")
    refresh_auth = _load_module()
    with pytest.raises(HTTPException) as exc_info:
        refresh_auth.require_refresh_api_key(x_api_key=None)
    assert exc_info.value.status_code == 401
