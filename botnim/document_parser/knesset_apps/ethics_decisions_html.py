"""Fetch ethics committee decisions via the WebSiteApi Pages endpoint.

The SharePoint page at ``main.knesset.gov.il/APPS/committees/2217/
pages/EthicsDecisions25`` is rendered by an XHR GET to:

  https://www.knesset.gov.il/WebSiteApi/knessetapi/Pages/GetPage/?\
  PageName=EthicsDecisions25&Route=...&Project=committees

returning::

    {
      "Title": "החלטות ועדת האתיקה...",
      "Lang": "",
      "Html":  "<!DOCTYPE html>...full SharePoint page body...</html>"
    }

The ``Html`` payload is a fully-rendered SharePoint page that
contains the PDF anchors (~50 of them for Knesset 25 as of 2026-05).
We parse those out with pyquery — same approach BK's ethics pipeline
used, just against a different (server-rendered, no-Reblaze) URL.

Like the committee_decisions_json fetcher, the host
``www.knesset.gov.il`` is not behind Reblaze; plain ``requests`` works.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional

import requests
from pyquery import PyQuery as pq

from .common import (
    DocRow,
    atomic_write_csv,
    ensure_at_least_one_row,
    normalize_pdf_url,
)

logger = logging.getLogger(__name__)


_API_URL = "https://www.knesset.gov.il/WebSiteApi/knessetapi/Pages/GetPage/"

# CommitteeId for ועדת האתיקה.
DEFAULT_ETHICS_COMMITTEE_ID = 2217


@dataclass
class EthicsDecisionsConfig:
    """Parameters for one ethics_decisions fetch.

    output_csv_path:
        Where to write the resulting ``index.csv``.
    page_name:
        ``EthicsDecisions25`` for current Knesset 25; bump for a new
        Knesset (the SharePoint scheme is one page per Knesset
        number).
    knesset_num:
        The Knesset number tag we attach to each row. Must be kept
        in sync with ``page_name`` since the API doesn't return it
        per-row.
    committee_id:
        Defaults to 2217. Used only to compose the canonical
        front-end ``Route`` query parameter the API expects.
    """

    output_csv_path: Path
    page_name: str = "EthicsDecisions25"
    knesset_num: int = 25
    committee_id: int = DEFAULT_ETHICS_COMMITTEE_ID
    api_url: str = _API_URL
    timeout_s: int = 60
    extra_headers: dict = field(default_factory=dict)


_DATE_NEAR_PDF = re.compile(r"(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})")


def _nearest_ancestor_text(node) -> str:
    """Walk up to the closest ``<tr|td|li|div|p>`` ancestor and return
    its text content. pyquery doesn't ship a jQuery-style ``.closest()``
    so we walk parents() ourselves."""
    p = pq(node).parents("tr,td,li,div,p")
    if not p:
        return ""
    return (pq(p[0]).text() or "")


def _extract_pdf_anchors(html: str, knesset_num: int) -> Iterable[DocRow]:
    """Yield one DocRow per ``<a>`` whose href ends in ``.pdf`` in
    the rendered page HTML.

    We try to recover a publication date from text near the anchor,
    matching common Hebrew formats like ``12/05/2024``. Missing dates
    are left empty rather than guessed.
    """
    if not html or not html.strip():
        return
    doc = pq(html)
    seen: set[str] = set()
    for anchor in doc("a"):
        a = pq(anchor)
        href = a.attr("href") or ""
        if not href.lower().endswith(".pdf"):
            continue
        url = normalize_pdf_url(_absolute(href))
        if not url or url in seen:
            continue
        seen.add(url)
        title = (a.text() or "").strip()
        # Look for a date in surrounding container text (Hebrew dd.mm.yyyy /
        # dd/mm/yyyy variants).
        container_text = _nearest_ancestor_text(anchor)
        m = _DATE_NEAR_PDF.search(container_text) if container_text else None
        date = m.group(1) if m else ""
        # Filename: prefer the last URL path segment if it looks
        # plausible; fall back to a safe md5-based name.
        filename = url.rsplit("/", 1)[-1].split("?")[0]
        if not filename or len(filename) > 200 or "%" in filename:
            import hashlib
            filename = hashlib.md5(url.encode()).hexdigest()[:16] + ".pdf"
        yield DocRow(
            url=url,
            filename=filename,
            date=date,
            knesset_num=knesset_num,
            title=title,
        )


def _absolute(href: str) -> str:
    """Resolve relative anchor to an absolute URL.

    The Knesset HTML mixes absolute fs.knesset.gov.il URLs with
    site-relative paths. We resolve relatives against
    ``main.knesset.gov.il`` since that's where the original page
    lives; PDF downloads then redirect / serve from
    ``fs.knesset.gov.il`` natively.
    """
    if href.startswith(("http://", "https://")):
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://main.knesset.gov.il" + href
    return "https://main.knesset.gov.il/" + href


def fetch_ethics_decisions_index(
    config: EthicsDecisionsConfig,
    *,
    http_get=requests.get,
) -> list[DocRow]:
    """Call the JSON-wrapped Pages API and write ``index.csv``."""
    route = (
        f"https://main.knesset.gov.il/APPS/committees/"
        f"{config.committee_id}/pages/{config.page_name}"
    )
    params = {
        "PageName": config.page_name,
        "Route": route,
        "Project": "committees",
    }
    headers = {
        "Accept": "application/json",
        **config.extra_headers,
    }
    logger.info("fetch_ethics_decisions: GET %s page_name=%s",
                config.api_url, config.page_name)
    resp = http_get(config.api_url, params=params, headers=headers, timeout=config.timeout_s)
    resp.raise_for_status()
    payload = resp.json()
    html = payload.get("Html") or ""
    if not html:
        logger.warning("fetch_ethics_decisions: empty Html field in payload")

    rows = list(_extract_pdf_anchors(html, knesset_num=config.knesset_num))
    logger.info("fetch_ethics_decisions: extracted %d PDF rows", len(rows))

    ensure_at_least_one_row(rows, config.output_csv_path)
    atomic_write_csv(config.output_csv_path, rows)
    logger.info(
        "fetch_ethics_decisions: wrote %d rows to %s",
        len(rows), config.output_csv_path,
    )
    return rows
