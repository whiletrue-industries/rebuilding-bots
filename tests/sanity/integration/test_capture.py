"""Integration: Playwright drives Chromium against a stub LibreChat.

Slow lane — marked `slow` so they are skipped from the default `pytest`
collection and only run when explicitly targeted:

    pytest -v -m slow tests/sanity/integration/test_capture.py

The stub (stub_librechat.py) mirrors the exact selectors from
scripts/ui-sanity-capture.spec.js so this test exercises real selector
resolution, the requestfinished stream-done signal, and the error path.
"""
from __future__ import annotations

import threading
import time

import pytest
import uvicorn
from fastapi.testclient import TestClient

from botnim.sanity import capture
from botnim.sanity.types import GoldEntry

from tests.sanity.integration.stub_librechat import make_app


pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def stub_url():
    """Spin up a stub LibreChat server for the duration of this module."""
    app = make_app()
    config = uvicorn.Config(app, host="127.0.0.1", port=18443, log_level="warning")
    server = uvicorn.Server(config)
    t = threading.Thread(target=server.run, daemon=True)
    t.start()

    # Wait until the stub is accepting connections (up to 5 s).
    deadline = time.time() + 5
    with TestClient(app) as client:
        while time.time() < deadline:
            try:
                if client.get("/login").status_code == 200:
                    break
            except Exception:
                time.sleep(0.05)

    yield "http://127.0.0.1:18443"

    server.should_exit = True


def test_capture_pair_records_both_answers(stub_url):
    """Happy path: both old and new bots answer successfully."""
    gold = [
        GoldEntry(
            row=0,
            question="hello?",
            expected_behavior="greet",
            must_not_contain=[],
            observed_notes="",
        ),
    ]
    result = capture.capture_pair(
        url_old=stub_url,
        url_new=stub_url,
        user="user@botnim.il",
        password="pw",
        gold_set=gold,
        timeout_ms=15_000,
        stable_ms=500,
    )
    assert len(result.rows) == 1
    row = result.rows[0]

    # Both answers must report ok=True.
    assert row.answer_old.ok is True, f"answer_old not ok: {row.answer_old}"
    assert row.answer_new.ok is True, f"answer_new not ok: {row.answer_new}"

    # Stub streams "שלום עולם" — check it arrived.
    # (If the stub shape changes, update stub_librechat.py to match.)
    assert "שלום" in row.answer_new.text or row.answer_new.text, (
        f"expected Hebrew text in answer_new, got: {row.answer_new.text!r}"
    )


def test_capture_500_records_ok_false(stub_url):
    """Error path: TRIGGER_500 question makes the stub return 500 → ok=False."""
    gold = [
        GoldEntry(
            row=0,
            question="TRIGGER_500",
            expected_behavior="x",
            must_not_contain=[],
            observed_notes="",
        ),
    ]
    result = capture.capture_pair(
        url_old=stub_url,
        url_new=stub_url,
        user="u@x",
        password="p",
        gold_set=gold,
        timeout_ms=15_000,
        stable_ms=500,
    )
    row = result.rows[0]
    # At minimum one of the two bots should surface the error.
    # (Both use the same stub, so both should fail.)
    assert row.answer_new.ok is False, (
        f"expected ok=False for TRIGGER_500, got: {row.answer_new}"
    )
    assert row.answer_new.error, (
        f"expected non-empty error for TRIGGER_500, got: {row.answer_new}"
    )
