"""Drives Playwright + headless Chromium against both bots.

Two-turn capture (post-2026-05-10):
- ONE browser, ONE shared BrowserContext, TWO pages — one per env.
- Login once per page (no per-row context churn → faster + dodges local-stack
  rate limits).
- Per gold-set row: capture turn 1 on each page (navigate /c/new + ask), then
  if `entry.followup_prompt` is set, capture turn 2 on the SAME conversation
  (no nav, just type + Enter).

Selectors and the SSE-stream-stability heuristic stay in lock-step with
parlibot/scripts/ui-sanity-capture.spec.js. Keep that JS spec and this
module in sync — they ARE the same logic in two languages.

Key design decisions ported from the JS spec:
- Stream-done signal: Playwright `requestfinished` on a URL matching
  /api/agents/chat/stream/<id>  OR  /api/assistants/v{N}/chat
  This fires when the SSE body fully closes, which is the correct
  "answer is final" boundary — NOT a length/stability poll.
- Answer extraction: `.agent-turn` last element's innerText.
- Send trigger: pressing Enter on the textarea (NOT a button click).
- Login wait: waitForURL away from /login.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Optional

from playwright.sync_api import Page, Request, TimeoutError as PWTimeoutError, sync_playwright

from botnim.sanity.types import (
    Answer,
    CaptureResult,
    CaptureRow,
    GoldEntry,
    SideCapture,
)

logger = logging.getLogger(__name__)

# Selectors — sourced from scripts/ui-sanity-capture.spec.js (canonical).
SELECTORS: dict[str, str] = {
    "email": 'input[name="email"]',
    "password": 'input[name="password"]',
    "submit_login": 'button[type="submit"]',
    "chat_input": 'textarea[name="text"]',
    "answer": ".agent-turn",
}

_STREAM_DONE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"/api/agents/chat/stream/"),
    re.compile(r"/api/assistants/v\d+/chat(\?|$)"),
]

_DEFAULT_TIMEOUT_MS: int = 120_000
_DEFAULT_STABLE_MS: int = 4_000  # kept for API compat
_POST_STREAM_SETTLE_MS: int = 800
_BAD_REQUEST_SIGNAL: str = "agent_id is required in request body"


def capture_pair(
    *,
    url_old: str,
    url_new: str,
    user: str,
    password: str,
    gold_set: list[GoldEntry],
    timeout_ms: int = _DEFAULT_TIMEOUT_MS,
    stable_ms: int = _DEFAULT_STABLE_MS,  # accepted for API compat
    user_old: Optional[str] = None,
    password_old: Optional[str] = None,
    user_new: Optional[str] = None,
    password_new: Optional[str] = None,
) -> CaptureResult:
    """Capture turn-1 (and optional turn-2) per (bot, gold-set entry) pair.

    Single shared BrowserContext with ONE page per env, logged-in once each.
    Returns a CaptureResult with rows in the same order as gold_set.
    """
    user_old = user_old or user
    password_old = password_old or password
    user_new = user_new or user
    password_new = password_new or password

    rows: list[CaptureRow] = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            ctx = browser.new_context()
            old_page = ctx.new_page()
            new_page = ctx.new_page()
            try:
                _login(old_page, url_old, user_old, password_old, timeout_ms)
                _login(new_page, url_new, user_new, password_new, timeout_ms)

                for entry in gold_set:
                    logger.info(
                        "capturing row=%d question=%r followup=%s",
                        entry.row,
                        entry.question[:60],
                        bool(entry.followup_prompt),
                    )
                    side_old = _capture_side(
                        old_page, url_old, entry, timeout_ms,
                    )
                    side_new = _capture_side(
                        new_page, url_new, entry, timeout_ms,
                    )
                    rows.append(
                        CaptureRow(
                            row=entry.row,
                            question=entry.question,
                            expected_behavior=entry.expected_behavior,
                            must_not_contain=entry.must_not_contain,
                            observed_notes=entry.observed_notes,
                            followup_prompt=entry.followup_prompt,
                            expected_after_followup=entry.expected_after_followup,
                            answer_old=side_old,
                            answer_new=side_new,
                        )
                    )
            finally:
                ctx.close()
        finally:
            browser.close()
    return CaptureResult(rows=rows)


def _capture_side(
    page: Page, base_url: str, entry: GoldEntry, timeout_ms: int,
) -> SideCapture:
    """Capture turn 1 (and optional turn 2 on the same conversation).

    If turn 1 timed out, skip turn 2 — the LibreChat input often stays
    half-locked when the SSE never closed, causing the follow-up to either
    hang again or return NO_NEW_TURN. Better to mark turn 2 as
    SKIPPED_T1_TIMEOUT and move on. (Cascading-hang observed 2026-05-10.)
    """
    turn1 = _capture_turn1(page, base_url, entry.question, timeout_ms)
    turn2: Optional[Answer] = None
    if entry.followup_prompt:
        if turn1.error == "TIMEOUT":
            turn2 = Answer(text="", ok=False, error="SKIPPED_T1_TIMEOUT", duration_ms=0)
        else:
            turn2 = _capture_turn2(page, entry.followup_prompt, timeout_ms)
    return SideCapture(turn1=turn1, turn2=turn2)


def _login(page: Page, base_url: str, user: str, password: str, timeout_ms: int) -> None:
    """Navigate to /login, fill creds, submit, wait until URL leaves /login."""
    url = f"{base_url.rstrip('/')}/login"
    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    page.fill(SELECTORS["email"], user)
    page.fill(SELECTORS["password"], password)
    with page.expect_navigation(
        url=lambda u: "/login" not in str(u),
        timeout=30_000,
        wait_until="domcontentloaded",
    ):
        page.click(SELECTORS["submit_login"])


def _capture_turn1(
    page: Page, base_url: str, question: str, timeout_ms: int,
) -> Answer:
    """Navigate to /c/new, send the question, wait for SSE close."""
    started = time.time()
    try:
        page.goto(
            f"{base_url.rstrip('/')}/c/new",
            wait_until="domcontentloaded",
            timeout=timeout_ms,
        )
        text, error = _send_and_wait(page, question, timeout_ms)
        ok = error is None and bool(text.strip())
        return Answer(
            text=text, ok=ok, error=error,
            duration_ms=int((time.time() - started) * 1000),
        )
    except (PWTimeoutError, Exception) as exc:  # noqa: BLE001
        logger.warning("_capture_turn1 error: %s", exc)
        return Answer(
            text="", ok=False, error=f"{type(exc).__name__}: {exc}",
            duration_ms=int((time.time() - started) * 1000),
        )


def _capture_turn2(page: Page, followup_prompt: str, timeout_ms: int) -> Answer:
    """Send the follow-up on the SAME conversation; verify a new turn rendered."""
    started = time.time()
    try:
        prior_turns = page.locator(SELECTORS["answer"]).count()
        text, error = _send_and_wait(page, followup_prompt, timeout_ms)
        ok = error is None and bool(text.strip())
        # Sanity: a new .agent-turn should have appeared.
        final_turns = page.locator(SELECTORS["answer"]).count()
        if ok and final_turns <= prior_turns:
            ok = False
            error = "NO_NEW_TURN"
        return Answer(
            text=text, ok=ok, error=error,
            duration_ms=int((time.time() - started) * 1000),
        )
    except (PWTimeoutError, Exception) as exc:  # noqa: BLE001
        logger.warning("_capture_turn2 error: %s", exc)
        return Answer(
            text="", ok=False, error=f"{type(exc).__name__}: {exc}",
            duration_ms=int((time.time() - started) * 1000),
        )


def _send_and_wait(
    page: Page, message: str, timeout_ms: int,
) -> tuple[str, Optional[str]]:
    """Type message, press Enter, wait for SSE close (or BAD_REQUEST). Returns
    (text, error?). Error is None on success."""
    inp = page.locator(SELECTORS["chat_input"]).first
    inp.wait_for(state="visible", timeout=30_000)
    inp.fill(message)

    stream_done_holder: dict = {"value": None}
    bad_request_holder: dict = {"value": None}

    def _on_request_finished(req: Request) -> None:
        url = req.url
        if any(p.search(url) for p in _STREAM_DONE_PATTERNS):
            stream_done_holder["value"] = req

    page.on("requestfinished", _on_request_finished)

    try:
        inp.press("Enter")

        deadline = time.time() + (timeout_ms / 1000)
        while time.time() < deadline:
            if stream_done_holder["value"] is not None:
                break
            bad_req_loc = page.get_by_text(_BAD_REQUEST_SIGNAL, exact=False).first
            try:
                if bad_req_loc.is_visible():
                    bad_request_holder["value"] = True
                    break
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(100)
        else:
            text = _extract_last_agent_turn(page)
            return text, "TIMEOUT"

        if bad_request_holder["value"]:
            text = _extract_last_agent_turn(page)
            return text, "BAD_REQUEST_SIGNAL"

        page.wait_for_timeout(_POST_STREAM_SETTLE_MS)

        req = stream_done_holder["value"]
        try:
            resp = req.response()
            if resp and resp.status >= 400:
                text = _extract_last_agent_turn(page)
                return text, f"HTTP_{resp.status}"
        except Exception:  # noqa: BLE001
            pass

        text = _extract_last_agent_turn(page)
        return text, None
    finally:
        page.remove_listener("requestfinished", _on_request_finished)


def _extract_last_agent_turn(page: Page) -> str:
    try:
        return page.locator(SELECTORS["answer"]).last.inner_text(timeout=2_000)
    except Exception:  # noqa: BLE001
        return ""
