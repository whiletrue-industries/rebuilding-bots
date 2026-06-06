"""Thin client wrapping the gov.il "openapi" decisions gateway.

Endpoint migration (2026-05)
----------------------------
gov.il moved its decisions API off ``www.gov.il`` to the
``openapi-gc.digital.gov.il`` gateway. The old hosts now return the SPA
HTML shell (HTTP 200, ``text/html``) for these paths, which used to
surface as an opaque ``JSONDecodeError`` and silently stalled the
``government_decisions`` context for ~a month. Current endpoints:

* GET {API_BASE}/collectors/v1/api/DataCollector/GetResults  — paginated listing
* GET {API_BASE}/contentpage/v1/api/content-pages/{pageId}   — full content
* GET <attachment_url>                                       — PDF / DOCX bytes

The gateway requires, in addition to the Cloudflare TLS clearance that
``curl_cffi`` with ``impersonate="chrome"`` provides:

* header ``x-client-id`` — the SPA's public API key, shipped in its JS
  bundle. Overridable via the ``GOV_IL_CLIENT_ID`` env var in case gov.il
  rotates it.
* ``Referer`` / ``Origin`` of ``https://www.gov.il``.
* a session cookie seeded by one warmup GET to the gateway host. A bare
  (never-warmed) session gets HTTP 500 from the gateway; a warmed session
  gets 200. The warmup is lazy (fires on the first real call) so unit
  tests that inject a mock ``_session`` and pass ``warmup=False`` never
  touch the network.

The client exposes ``_session`` so tests can inject a ``MagicMock``;
production callers never touch ``_session`` directly.
"""
from __future__ import annotations

import os
import time
from typing import Any, Optional

from curl_cffi import requests as _cc_requests

from ...config import get_logger
from .exceptions import GovIlApiError

logger = get_logger(__name__)


# Host gov.il decisions still live under for human-facing source_urls and
# (historically) attachment downloads.
GOV_BASE = "https://www.gov.il"

# The "openapi" gateway the SPA migrated to (2026-05).
API_BASE = "https://openapi-gc.digital.gov.il/pub/cio/govil/rest"
LISTING_PATH = "/collectors/v1/api/DataCollector/GetResults"
CONTENT_PATH = "/contentpage/v1/api/content-pages"
GOV_RESOLUTIONS_TYPE = "30280ed5-306f-4f0b-a11d-cacf05d36648"

# The SPA ships this public client id in its JS bundle; the gateway 500s
# without it. Overridable via env in case gov.il rotates the key.
DEFAULT_CLIENT_ID = "9KFgciHHGDyNiqz5MdQS0eK2ApeJYMc6YnElUICpN1atirZc"

# One GET here (returns 404, but Set-Cookie seeds the cookie the gateway's
# DataCollector / contentpage endpoints require) before the first real call.
_WARMUP_URL = "https://openapi-gc.digital.gov.il/"


def _build_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "x-client-id": os.environ.get("GOV_IL_CLIENT_ID", DEFAULT_CLIENT_ID),
        "Referer": "https://www.gov.il/",
        "Origin": "https://www.gov.il",
    }


class GovIlClient:
    """Stateful client; one instance per scrape run.

    ``delay_seconds`` is the polite interval between any two requests,
    enforced by the client itself so callers don't have to remember.
    Tal's guide recommends 0.3–0.5s.

    ``warmup`` controls the lazy cookie-warmup GET; pass ``warmup=False``
    in unit tests (which inject a mock ``_session``) to keep the network
    out of the assertions.
    """

    def __init__(
        self,
        *,
        delay_seconds: float = 0.3,
        timeout: int = 30,
        warmup: bool = True,
    ) -> None:
        self._impersonate = "chrome"
        self._session = _cc_requests.Session()
        self._session.impersonate = self._impersonate
        self._delay = delay_seconds
        self._timeout = timeout
        self._last_call = 0.0
        self._headers = _build_headers()
        self._warmup_enabled = warmup
        self._warmed = False

    def _sleep_if_needed(self) -> None:
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._delay:
            time.sleep(self._delay - elapsed)
        self._last_call = time.monotonic()

    def _ensure_warmed(self) -> None:
        """Seed the gateway session cookie before the first real request.

        A bare session gets HTTP 500 from the gateway; one warmup GET (even
        a 404 on the gateway root) sets the cookie that the API endpoints
        require. Best-effort: a failed warmup is logged and the call
        proceeds — the API call itself surfaces any real problem (a
        non-JSON body raises ``GovIlApiError`` via ``_json_or_raise``).
        """
        if self._warmed or not self._warmup_enabled:
            return
        # Set first so a raising warmup never loops on the next call.
        self._warmed = True
        try:
            self._session.get(_WARMUP_URL, headers=self._headers, timeout=self._timeout)
        except Exception as exc:  # noqa: BLE001
            logger.warning("gov_il warmup request failed (continuing): %s", exc)

    def _json_or_raise(self, resp, *, url: str) -> Any:
        """Return ``resp.json()``, or raise ``GovIlApiError`` for non-JSON.

        Guards on ``Content-Type``: the gateway (and the old, now-dead
        ``www.gov.il`` endpoints) return an HTML SPA shell with HTTP 200
        when a path moves or the request is blocked. ``.json()`` on that
        raises an opaque ``JSONDecodeError`` — the exact failure that hid
        the 2026-05 migration. Fail loudly with the URL + content-type.
        """
        try:
            ctype = (resp.headers.get("content-type") or "").lower()
        except Exception:  # noqa: BLE001 — headers shape varies across clients
            ctype = ""
        if "json" not in ctype:
            snippet = ""
            try:
                snippet = (resp.text or "")[:160].replace("\n", " ")
            except Exception:  # noqa: BLE001
                pass
            raise GovIlApiError(
                f"gov.il returned non-JSON (content-type={ctype!r}) from {url} — "
                f"the endpoint likely moved or is WAF-blocked. Body head: {snippet!r}"
            )
        return resp.json()

    def list_decisions(self, *, skip: int, limit: int) -> dict[str, Any]:
        """One page of the listing API (newest-first)."""
        self._ensure_warmed()
        self._sleep_if_needed()
        url = f"{API_BASE}{LISTING_PATH}"
        resp = self._session.get(
            url,
            params={
                "CollectorType": ["policy", "pmopolicy"],
                "Type": GOV_RESOLUTIONS_TYPE,
                "skip": skip,
                "limit": limit,
                "culture": "he",
            },
            headers=self._headers,
            timeout=self._timeout,
        )
        resp.raise_for_status()
        return self._json_or_raise(resp, url=url)

    def fetch_content(self, page_id: str) -> Optional[dict[str, Any]]:
        """Full content page. Returns ``None`` for 404 (deleted/moved)."""
        self._ensure_warmed()
        self._sleep_if_needed()
        url = f"{API_BASE}{CONTENT_PATH}/{page_id}"
        resp = self._session.get(
            url,
            params={"culture": "he"},
            headers=self._headers,
            timeout=self._timeout,
        )
        if resp.status_code == 404:
            logger.info("content 404 for %s — skipping", page_id)
            return None
        resp.raise_for_status()
        return self._json_or_raise(resp, url=url)

    def download_attachment(self, url: str) -> bytes:
        """Raw bytes for a PDF / DOCX attachment URL."""
        self._ensure_warmed()
        self._sleep_if_needed()
        resp = self._session.get(url, headers=self._headers, timeout=self._timeout * 2)
        resp.raise_for_status()
        return resp.content
