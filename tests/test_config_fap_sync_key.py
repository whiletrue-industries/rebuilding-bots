"""Tests for the OPENAI_API_KEY_<ENV>_FAP_SYNC override.

The daily refresh (botnim.refresh_endpoint → server.py:_run_refresh_job)
enters botnim.config.fap_sync_context() for the entire fetch+sync run.
Inside that context, OpenAI client construction prefers the
OPENAI_API_KEY_<ENV>_FAP_SYNC env var so the refresh can use a dedicated
key without disturbing chat retrieval or sanity, which build their
clients outside the context.
"""
from __future__ import annotations

import asyncio
import threading

import pytest

from botnim.config import _resolve_openai_api_key, fap_sync_context


def test_resolver_uses_regular_key_outside_context(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY_PRODUCTION", "sk-regular-prod")
    monkeypatch.setenv("OPENAI_API_KEY_PRODUCTION_FAP_SYNC", "sk-fap-prod")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    assert _resolve_openai_api_key("production") == "sk-regular-prod"


def test_resolver_prefers_fap_sync_key_inside_context(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY_PRODUCTION", "sk-regular-prod")
    monkeypatch.setenv("OPENAI_API_KEY_PRODUCTION_FAP_SYNC", "sk-fap-prod")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with fap_sync_context():
        assert _resolve_openai_api_key("production") == "sk-fap-prod"
    # Context exits cleanly.
    assert _resolve_openai_api_key("production") == "sk-regular-prod"


def test_resolver_falls_back_to_regular_key_when_fap_var_unset(monkeypatch):
    """If OPENAI_API_KEY_<ENV>_FAP_SYNC isn't configured, the context is
    a no-op — important for envs that haven't opted in to key separation."""
    monkeypatch.setenv("OPENAI_API_KEY_STAGING", "sk-regular-staging")
    monkeypatch.delenv("OPENAI_API_KEY_STAGING_FAP_SYNC", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with fap_sync_context():
        assert _resolve_openai_api_key("staging") == "sk-regular-staging"


def test_context_does_not_leak_to_concurrent_threads(monkeypatch):
    """Verifies the contextvar is properly isolated: a thread spawned
    OUTSIDE the context never sees fap-sync-mode, even if the main
    thread is currently inside the context. This is the property that
    keeps chat retrieval on the regular key while the daily refresh is
    running on the same process.
    """
    monkeypatch.setenv("OPENAI_API_KEY_PRODUCTION", "sk-regular")
    monkeypatch.setenv("OPENAI_API_KEY_PRODUCTION_FAP_SYNC", "sk-fap")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    other_thread_result: list[str] = []
    barrier = threading.Barrier(2)
    proceed = threading.Event()

    def chat_retrieval_lookalike() -> None:
        barrier.wait(timeout=2)  # Sync up so main is inside the context.
        other_thread_result.append(_resolve_openai_api_key("production"))
        proceed.set()

    t = threading.Thread(target=chat_retrieval_lookalike)
    t.start()
    with fap_sync_context():
        barrier.wait(timeout=2)
        assert proceed.wait(timeout=2)
        # The OTHER thread, started before this context, must see the
        # regular key — contextvars do not bleed across thread boundaries.
        assert other_thread_result == ["sk-regular"]
        # And THIS thread, inside the context, sees the fap-sync key.
        assert _resolve_openai_api_key("production") == "sk-fap"
    t.join(timeout=2)


def test_context_propagates_through_asyncio_tasks(monkeypatch):
    """asyncio.Task creation copies the current Context, so OpenAI
    clients constructed inside coroutines spawned from the refresh
    pipeline inherit the fap-sync key.
    """
    monkeypatch.setenv("OPENAI_API_KEY_PRODUCTION", "sk-regular")
    monkeypatch.setenv("OPENAI_API_KEY_PRODUCTION_FAP_SYNC", "sk-fap")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    async def inner() -> str:
        # Run inside an asyncio.Task — context should still be fap-sync.
        return _resolve_openai_api_key("production")

    async def runner() -> str:
        with fap_sync_context():
            return await asyncio.gather(inner())

    result = asyncio.run(runner())
    assert result == ["sk-fap"]


def test_resolver_unprefixed_key_when_no_environment_passed(monkeypatch):
    """No environment arg + unprefixed OPENAI_API_KEY still works
    (CLI / local-dev convenience). Fap-sync context is no-op without
    an env."""
    monkeypatch.delenv("OPENAI_API_KEY_PRODUCTION", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY_STAGING", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY_LOCAL", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY_PRODUCTION_FAP_SYNC", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-unprefixed")
    with fap_sync_context():
        assert _resolve_openai_api_key() == "sk-unprefixed"


def test_resolver_falls_through_to_other_env_suffix_in_context(monkeypatch):
    """If OPENAI_API_KEY_PRODUCTION_FAP_SYNC is unset BUT another
    env's _FAP_SYNC var is set, prefer that over any regular key.
    Catches mis-configured stages where only one env got the new
    secret wired up.
    """
    monkeypatch.delenv("OPENAI_API_KEY_PRODUCTION", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY_PRODUCTION_FAP_SYNC", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY_STAGING_FAP_SYNC", "sk-staging-fap")
    monkeypatch.setenv("OPENAI_API_KEY_STAGING", "sk-staging-regular")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    # Note: caller asked for production, but only staging's keys exist.
    # Inside fap-sync, the staging fap-sync key should be preferred over
    # the staging regular key.
    with fap_sync_context():
        assert _resolve_openai_api_key("production") == "sk-staging-fap"
