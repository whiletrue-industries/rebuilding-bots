"""Thin client wrapping the three gov.il endpoints we use.

Endpoints documented in Tal's GOV_DEC_DEV_GUIDE.md:
* GET /CollectorsWebApi/api/DataCollector/GetResults — paginated listing
* GET /ContentPageWebApi/api/content-pages/{pageId} — full content
* GET <attachment_url>                              — PDF / DOCX bytes

All three sit behind Cloudflare's TLS-fingerprinting WAF; from Python
only ``curl_cffi`` with ``impersonate="chrome"`` reliably gets a 200.
Standard ``requests`` / ``httpx`` get a 403.

The client exposes ``_session`` so tests can inject a MagicMock without
real network calls; production callers never touch ``_session`` directly.
"""
from __future__ import annotations

import time
from typing import Any, Optional

from curl_cffi import requests as _cc_requests

from ...config import get_logger

logger = get_logger(__name__)


GOV_BASE = "https://www.gov.il"
LISTING_PATH = "/CollectorsWebApi/api/DataCollector/GetResults"
CONTENT_PATH = "/ContentPageWebApi/api/content-pages"
GOV_RESOLUTIONS_TYPE = "30280ed5-306f-4f0b-a11d-cacf05d36648"


class GovIlClient:
    """Stateful client; one instance per scrape run.

    ``delay_seconds`` is the polite interval between any two requests,
    enforced by the client itself so callers don't have to remember.
    Tal's guide recommends 0.3–0.5s.
    """

    def __init__(self, *, delay_seconds: float = 0.3, timeout: int = 30) -> None:
        self._impersonate = "chrome"
        self._session = _cc_requests.Session()
        self._session.impersonate = self._impersonate
        self._delay = delay_seconds
        self._timeout = timeout
        self._last_call = 0.0

    def _sleep_if_needed(self) -> None:
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._delay:
            time.sleep(self._delay - elapsed)
        self._last_call = time.monotonic()

    def list_decisions(self, *, skip: int, limit: int) -> dict[str, Any]:
        """One page of the listing API."""
        self._sleep_if_needed()
        resp = self._session.get(
            f"{GOV_BASE}{LISTING_PATH}",
            params={
                "CollectorType": ["policy", "pmopolicy"],
                "Type": GOV_RESOLUTIONS_TYPE,
                "skip": skip,
                "limit": limit,
                "culture": "he",
            },
            timeout=self._timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def fetch_content(self, page_id: str) -> Optional[dict[str, Any]]:
        """Full content page. Returns ``None`` for 404 (deleted/moved)."""
        self._sleep_if_needed()
        resp = self._session.get(
            f"{GOV_BASE}{CONTENT_PATH}/{page_id}",
            params={"culture": "he"},
            timeout=self._timeout,
        )
        if resp.status_code == 404:
            logger.info("content 404 for %s — skipping", page_id)
            return None
        resp.raise_for_status()
        return resp.json()

    def download_attachment(self, url: str) -> bytes:
        """Raw bytes for a PDF / DOCX attachment URL."""
        self._sleep_if_needed()
        resp = self._session.get(url, timeout=self._timeout * 2)
        resp.raise_for_status()
        return resp.content
