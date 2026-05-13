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

import csv
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional

import requests
from pyquery import PyQuery as pq

from .common import (
    CSV_FIELDS,
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
    historical_archive_csv:
        Optional path (relative to config_dir, resolved by the
        dispatcher) to a committed CSV of older-Knesset decisions
        in the same ``url,filename,date,knesset_num,title`` shape.
        When set, rows from that CSV are merged into the live API
        results before writing index.csv; the LIVE rows win on URL
        collision so a freshly-edited K25 entry isn't overwritten by
        a stale archive row. This lets us extend coverage back to
        K15 (≈1999) without standing up a live fetcher for archive
        pages that are gated behind the Knesset CDN's JS challenge.
    """

    output_csv_path: Path
    page_name: str = "EthicsDecisions25"
    knesset_num: int = 25
    committee_id: int = DEFAULT_ETHICS_COMMITTEE_ID
    api_url: str = _API_URL
    timeout_s: int = 60
    extra_headers: dict = field(default_factory=dict)
    historical_archive_csv: Optional[Path] = None


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


def _load_archive_rows(archive_csv: Path) -> list[DocRow]:
    """Load a committed historical-archive CSV in the same shape as our
    live output. Missing/blank ``knesset_num`` is coerced to 0 so a
    malformed row still flows through (downstream readers tolerate 0).

    Returns an empty list on a missing file rather than raising — the
    parameter is optional and an absent archive should not break the
    live K25 fetch.
    """
    if not archive_csv.exists():
        logger.warning(
            "historical_archive_csv=%s missing; skipping merge", archive_csv,
        )
        return []
    rows: list[DocRow] = []
    with open(archive_csv, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = set(CSV_FIELDS) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"historical_archive_csv {archive_csv} is missing columns: "
                f"{sorted(missing)} (expected {CSV_FIELDS})"
            )
        for r in reader:
            try:
                knesset_num = int(r.get("knesset_num") or 0)
            except ValueError:
                knesset_num = 0
            rows.append(DocRow(
                url=(r.get("url") or "").strip(),
                filename=(r.get("filename") or "").strip(),
                date=(r.get("date") or "").strip(),
                knesset_num=knesset_num,
                title=(r.get("title") or "").strip(),
            ))
    # Drop rows with no URL — they're not actionable for the Stage 2
    # PDF downloader and only inflate counts.
    rows = [r for r in rows if r.url]
    return rows


def _merge_rows(live: list[DocRow], archive: list[DocRow]) -> list[DocRow]:
    """Merge live + archive rows, deduping by URL with live winning.

    Live rows are appended first; archive rows for URLs not in the live
    set follow. The order preserves "newest first" within each source
    when callers pass them in that order.
    """
    seen = {r.url for r in live}
    merged = list(live)
    for r in archive:
        if r.url in seen:
            continue
        seen.add(r.url)
        merged.append(r)
    return merged


def fetch_ethics_decisions_index(
    config: EthicsDecisionsConfig,
    *,
    http_get=requests.get,
) -> list[DocRow]:
    """Call the JSON-wrapped Pages API and write ``index.csv``.

    When ``config.historical_archive_csv`` is set, the live K25 rows
    are merged with the committed archive CSV — live wins on URL
    collisions, archive fills in the older Knessets that the live API
    can't reach (those live behind the Knesset CDN's JS challenge and
    aren't realistically scrapeable from a backend job).
    """
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

    live_rows = list(_extract_pdf_anchors(html, knesset_num=config.knesset_num))
    logger.info("fetch_ethics_decisions: extracted %d live PDF rows", len(live_rows))

    if config.historical_archive_csv is not None:
        archive_rows = _load_archive_rows(Path(config.historical_archive_csv))
        logger.info(
            "fetch_ethics_decisions: loaded %d archive rows from %s",
            len(archive_rows), config.historical_archive_csv,
        )
        rows = _merge_rows(live_rows, archive_rows)
        logger.info(
            "fetch_ethics_decisions: merged total %d rows "
            "(%d live + %d archive, %d dedup'd)",
            len(rows), len(live_rows), len(archive_rows),
            len(live_rows) + len(archive_rows) - len(rows),
        )
    else:
        rows = live_rows

    ensure_at_least_one_row(rows, config.output_csv_path)
    atomic_write_csv(config.output_csv_path, rows)
    logger.info(
        "fetch_ethics_decisions: wrote %d rows to %s",
        len(rows), config.output_csv_path,
    )
    return rows
