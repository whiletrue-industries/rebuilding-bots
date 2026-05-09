"""Drives Playwright + headless Chromium against both bots.

Selectors and the SSE-stream-stability heuristic are ported verbatim from
parlibot/scripts/ui-sanity-capture.spec.js. Keep that JS script and this
module in sync — they ARE the same logic in two languages.

Key design decisions from the JS spec:
- Stream-done signal: Playwright `requestfinished` on a URL matching
  /api/agents/chat/stream/<id>  OR  /api/assistants/v{N}/chat
  This fires when the SSE body fully closes, which is the correct
  "answer is final" boundary — NOT a length/stability poll.
- Answer extraction: `.agent-turn` last element's innerText.
- Send trigger: pressing Enter on the textarea (NOT a button click).
- Login wait: waitForURL away from /login.

SELECTORS dict — sourced from the JS spec, verbatim:
  email:         input[name="email"]
  password:      input[name="password"]
  submit_login:  button[type="submit"]
  chat_input:    textarea[name="text"]         # JS: page.locator('textarea[name="text"]').first()
  answer:        .agent-turn                   # JS: page.locator('.agent-turn').last().innerText()
"""
from __future__ import annotations

import logging
import re
import time
from typing import Optional

from playwright.sync_api import Page, Request, TimeoutError as PWTimeoutError, sync_playwright

from botnim.sanity.types import Answer, CaptureResult, CaptureRow, GoldEntry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Selectors — sourced from scripts/ui-sanity-capture.spec.js (canonical).
# If LibreChat upgrades its DOM, edit BOTH the JS spec and this dict.
# ---------------------------------------------------------------------------
SELECTORS: dict[str, str] = {
    # Login page  (JS: loginOnBot)
    "email": 'input[name="email"]',
    "password": 'input[name="password"]',
    "submit_login": 'button[type="submit"]',
    # Chat page  (JS: captureChatAnswer)
    "chat_input": 'textarea[name="text"]',          # .first() in JS
    # Answer extraction  (JS: page.locator('.agent-turn').last().innerText())
    "answer": ".agent-turn",
}

# URL patterns for the "stream finished" signal — requestfinished event.
# Matches both the new (ECS) and old (legacy droplet) LibreChat shapes.
# JS: /\/api\/agents\/chat\/stream\//.test(req.url()) ||
#     /\/api\/assistants\/v\d+\/chat(\?|$)/.test(req.url())
_STREAM_DONE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"/api/agents/chat/stream/"),
    re.compile(r"/api/assistants/v\d+/chat(\?|$)"),
]

# Constants mirror the JS spec defaults.
_DEFAULT_TIMEOUT_MS: int = 120_000
_DEFAULT_STABLE_MS: int = 4_000  # kept for API compat; actual wait is SSE-close-based

# Short post-stream settle delay (JS: await page.waitForTimeout(800))
_POST_STREAM_SETTLE_MS: int = 800

# BAD_REQUEST regression signal (JS: BAD_REQUEST_SIGNAL)
_BAD_REQUEST_SIGNAL: str = "agent_id is required in request body"


def capture_pair(
    *,
    url_old: str,
    url_new: str,
    user: str,
    password: str,
    gold_set: list[GoldEntry],
    timeout_ms: int = _DEFAULT_TIMEOUT_MS,
    stable_ms: int = _DEFAULT_STABLE_MS,  # accepted for API compat; not used in SSE path
) -> CaptureResult:
    """Capture one answer per (bot, question) pair using headless Chromium.

    Returns a CaptureResult with rows in the same order as gold_set.
    Each row records Answer(ok, text, duration_ms, error?) for the old and
    new bots. A single Playwright browser instance is reused across all
    questions but each question gets a fresh BrowserContext (no cookie bleed).
    """
    rows: list[CaptureRow] = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            for entry in gold_set:
                logger.info(
                    "capturing row=%d question=%r",
                    entry.row,
                    entry.question[:60],
                )
                ans_old = _ask_one(
                    browser, url_old, user, password, entry.question, timeout_ms
                )
                ans_new = _ask_one(
                    browser, url_new, user, password, entry.question, timeout_ms
                )
                rows.append(
                    CaptureRow(
                        row=entry.row,
                        question=entry.question,
                        expected_behavior=entry.expected_behavior,
                        must_not_contain=entry.must_not_contain,
                        observed_notes=entry.observed_notes,
                        answer_old=ans_old,
                        answer_new=ans_new,
                    )
                )
        finally:
            browser.close()
    return CaptureResult(rows=rows)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ask_one(
    browser,
    base_url: str,
    user: str,
    password: str,
    question: str,
    timeout_ms: int,
) -> Answer:
    """Open a fresh context, login, ask the question, return the answer."""
    started = time.time()
    ctx = browser.new_context()
    page = ctx.new_page()
    try:
        _login(page, base_url, user, password, timeout_ms)
        text, error = _capture_chat_answer(page, base_url, question, timeout_ms)
        ok = error is None and bool(text.strip())
        return Answer(
            text=text,
            ok=ok,
            error=error,
            duration_ms=int((time.time() - started) * 1000),
        )
    except (PWTimeoutError, Exception) as exc:  # noqa: BLE001
        logger.warning("_ask_one error: %s", exc)
        return Answer(
            text="",
            ok=False,
            error=f"{type(exc).__name__}: {exc}",
            duration_ms=int((time.time() - started) * 1000),
        )
    finally:
        ctx.close()


def _login(page: Page, base_url: str, user: str, password: str, timeout_ms: int) -> None:
    """Navigate to /login, fill creds, submit, wait until URL leaves /login.

    Mirrors loginOnBot() in the JS spec.
    """
    url = f"{base_url.rstrip('/')}/login"
    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    page.fill(SELECTORS["email"], user)
    page.fill(SELECTORS["password"], password)
    # JS: Promise.all([waitForURL(!startsWith('/login')), click(submit)])
    with page.expect_navigation(
        url=lambda u: "/login" not in str(u),
        timeout=30_000,
        wait_until="domcontentloaded",
    ):
        page.click(SELECTORS["submit_login"])


def _capture_chat_answer(
    page: Page,
    base_url: str,
    question: str,
    timeout_ms: int,
) -> tuple[str, Optional[str]]:
    """Navigate to /c/new, send the question, wait for SSE close, return (text, error).

    Mirrors captureChatAnswer() in the JS spec. Uses the requestfinished event
    (not a stability poll) as the stream-done signal.

    Returns:
        (text, None)          — on success
        (text, "TIMEOUT")     — when requestfinished never fired
        (text, "BAD_REQUEST") — when BAD_REQUEST_SIGNAL appeared in DOM
        (text, "HTTP_NNN")    — when the stream endpoint returned an error status
    """
    page.goto(
        f"{base_url.rstrip('/')}/c/new",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    inp = page.locator(SELECTORS["chat_input"]).first
    inp.wait_for(state="visible", timeout=30_000)
    inp.fill(question)

    # Register the requestfinished waiter BEFORE pressing Enter (JS does the
    # same: const streamDone = page.waitForEvent('requestfinished', {...}))
    stream_done_holder: dict = {"value": None}
    bad_request_holder: dict = {"value": None}

    def _on_request_finished(req: Request) -> None:
        url = req.url
        if any(p.search(url) for p in _STREAM_DONE_PATTERNS):
            stream_done_holder["value"] = req

    page.on("requestfinished", _on_request_finished)

    try:
        inp.press("Enter")

        # Poll until stream finished, bad-request signal, or timeout.
        deadline = time.time() + (timeout_ms / 1000)
        while time.time() < deadline:
            # Check stream done first.
            if stream_done_holder["value"] is not None:
                break

            # Check BAD_REQUEST regression signal.
            bad_req_loc = page.get_by_text(_BAD_REQUEST_SIGNAL, exact=False).first
            try:
                if bad_req_loc.is_visible():
                    bad_request_holder["value"] = True
                    break
            except Exception:  # noqa: BLE001
                pass

            page.wait_for_timeout(100)
        else:
            # Timeout branch
            text = _extract_last_agent_turn(page)
            return text, "TIMEOUT"

        if bad_request_holder["value"]:
            text = _extract_last_agent_turn(page)
            return text, "BAD_REQUEST_SIGNAL"

        # SSE closed — settle for DOM flush (JS: page.waitForTimeout(800))
        page.wait_for_timeout(_POST_STREAM_SETTLE_MS)

        # Check HTTP status of the finished request.
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
    """Return innerText of the last .agent-turn element, or '' if none exists.

    JS: page.locator('.agent-turn').last().innerText().catch(() => '')
    """
    try:
        return page.locator(SELECTORS["answer"]).last.inner_text(timeout=2_000)
    except Exception:  # noqa: BLE001
        return ""
