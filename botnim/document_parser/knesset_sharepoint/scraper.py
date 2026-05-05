"""Playwright + stealth based scraper for ``main.knesset.gov.il`` pages.

Why this exists
---------------

Three of the BudgetKey datapackages we used to consume have stopped
publishing rows since 2025-01:

  - ``knesset_legal_advisor`` (legal-opinion PDFs)
  - ``knesset_legal_advisor_letters`` (LD letters/replies)
  - ``ethics_committee_decisions`` (per-Knesset ethics decisions)

Their pipelines all share the same shape: navigate a SharePoint listing
page on ``main.knesset.gov.il`` with headless Chrome, extract anchor
hrefs that end in ``.pdf``, then download each PDF via plain HTTP from
``fs.knesset.gov.il``. The host is gated by the Reblaze WAF (the
``winsocks()`` JS challenge served from
``/kramericaindustries.ac_v2.lib.js``); plain ``requests``/``curl_cffi``
get the 585-byte challenge stub instead of content. Real headless
Chrome alone is also detected; we need the ``playwright-stealth``
patches (navigator.webdriver, etc.) to pass.

This module exposes one function — ``scrape_pdf_index`` — that produces
the same shape of ``index.csv`` we used to read from BK. The downstream
``process_pdf_source`` pipeline (OCR + LLM extraction) is unchanged.

Operational properties
----------------------

* **Single-purpose**: only navigates, scrapes, and writes
  ``url, title, filename`` (+ optional date / knesset_num for ethics).
  PDF download is delegated to ``process_pdf_source`` (plain HTTP from
  ``fs.knesset.gov.il``, no Reblaze wall) so this module never holds
  binary content.
* **Idempotent CSV write**: writes to a tempfile then ``os.replace`` so
  a crashed run doesn't corrupt the existing CSV.
* **Empty-result safety guard**: raises ``EmptyUpstreamIndex`` rather
  than overwriting a populated CSV with zero rows — same contract the
  PDF processor enforces, so a Reblaze flap or selector drift surfaces
  as a refresh failure (REFRESH_FAILED) rather than silent corruption.
"""
from __future__ import annotations

import csv
import hashlib
import logging
import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Optional

logger = logging.getLogger(__name__)

PREFIX = "https://main.knesset.gov.il"

# Reblaze JS challenge cookie sticks for the duration of the browser
# context. Hitting the root first reliably triggers the challenge and
# subsequent target-page hits get the bypass.
_WARMUP_URL = PREFIX + "/"

# Default User-Agent. Matches what we use elsewhere
# (see backend/api/server.py and document_parser/gov_il_decisions/api.py).
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


class EmptyUpstreamIndex(Exception):
    """Mirror of ``document_parser.pdfs.exceptions.EmptyUpstreamIndex``.

    Defined locally so this module has no compile-time dependency on the
    PDF processor — operators can import this scraper standalone.
    """


@dataclass
class PdfRow:
    """One row of the index.csv we produce — same shape as BK's."""

    url: str
    title: str
    filename: str
    date: Optional[str] = None  # populated for ethics pages
    knesset_num: Optional[int] = None  # populated for ethics pages


@dataclass
class ScrapeConfig:
    """Parameters for one SharePoint listing scrape.

    page_url:
        Absolute URL of the SharePoint listing page (e.g.
        ``https://main.knesset.gov.il/about/departments/pages/leg/ldguidelines.aspx``).
    anchor_selector:
        CSS selector for the PDF anchors on that page. ``a.LDDocLink``
        for the legal-advisor pages; ``table a`` for ethics (paired
        with extra date-row logic — see ``ethics_extractor``).
    output_csv_path:
        Where to write the resulting ``index.csv``.
    sub_index_extractor:
        Optional callable that, given the rendered listing HTML, yields
        sub-page URLs to also scrape (used by ethics whose top-level
        page links to per-Knesset CommitteeDecisions{N}.aspx pages).
    row_extractor:
        Callable ``(page) -> Iterable[PdfRow]`` that extracts the rows
        from one listing page. Default: every ``a[href$=".pdf"]`` under
        ``anchor_selector``, with ``url`` + ``title`` + md5-derived
        ``filename``. Override for ethics-style date+knesset rows.
    extra_browser_args:
        Passed through to ``chromium.launch(args=...)`` — escape hatch
        for environments that need ``--no-sandbox`` etc.
    timeout_ms:
        Per-navigation timeout in milliseconds.
    """

    page_url: str
    anchor_selector: str = "a"
    output_csv_path: Path = field(default_factory=lambda: Path("/tmp/index.csv"))
    sub_index_extractor: Optional[Callable[[str], list[str]]] = None
    row_extractor: Optional[Callable[[object], Iterable[PdfRow]]] = None
    extra_browser_args: list[str] = field(default_factory=list)
    timeout_ms: int = 60_000


def _filename_for(url: str) -> str:
    """Stable filename from a PDF URL — same convention as BK uses."""
    return hashlib.md5(url.encode()).hexdigest()[:16] + ".pdf"


def _absolute(href: str) -> str:
    return href if href.startswith("http") else PREFIX + href


def _default_row_extractor(anchor_selector: str) -> Callable[[object], Iterable[PdfRow]]:
    """Build a row extractor that picks every PDF anchor under the
    selector — this matches the legal_advisor / legal_advisor_letters
    BK pipelines exactly."""

    def _extract(page) -> Iterable[PdfRow]:
        anchors = page.query_selector_all(anchor_selector)
        rows: list[PdfRow] = []
        for a in anchors:
            href = a.get_attribute("href") or ""
            if not href.endswith(".pdf"):
                continue
            url = _absolute(href)
            title = (a.inner_text() or "").strip()
            rows.append(PdfRow(url=url, title=title, filename=_filename_for(url)))
        return rows

    return _extract


def _atomic_write_csv(path: Path, rows: list[PdfRow]) -> None:
    """Write rows to ``path`` via a tempfile + ``os.replace`` so the
    existing CSV is never partially-overwritten."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["url", "title", "filename", "date", "knesset_num"]
    fd, tmp = tempfile.mkstemp(prefix=".index-", suffix=".csv", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for r in rows:
                w.writerow({
                    "url": r.url,
                    "title": r.title,
                    "filename": r.filename,
                    "date": r.date or "",
                    "knesset_num": r.knesset_num if r.knesset_num is not None else "",
                })
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _ensure_at_least_one_row(rows: list[PdfRow], page_url: str, csv_path: Path) -> None:
    """Refuse to overwrite an existing populated CSV with an empty list.

    Mirrors the EmptyUpstreamIndex guard the PDF processor enforces.
    A first-run (no existing CSV) is allowed to write an empty file —
    that just means the page is currently empty, not that the scrape
    silently broke.
    """
    if rows:
        return
    if csv_path.exists() and csv_path.stat().st_size > 0:
        # Best-effort: count existing rows to make the error message
        # concrete for the operator.
        existing = -1
        try:
            with open(csv_path, encoding="utf-8") as f:
                existing = max(0, sum(1 for _ in csv.DictReader(f)))
        except Exception:  # noqa: BLE001
            pass
        raise EmptyUpstreamIndex(
            f"{page_url}: scraped 0 rows; refusing to overwrite "
            f"{csv_path} which has {existing} existing rows. Likely cause: "
            "Reblaze JS challenge not bypassed, or the page's anchor "
            "selector changed."
        )


def scrape_pdf_index(config: ScrapeConfig) -> list[PdfRow]:
    """Scrape one SharePoint listing page and write its ``index.csv``.

    Returns the list of rows written so callers can assert without
    reading the CSV back.
    """
    # Lazy imports keep playwright / stealth optional at module load time —
    # we only fail at scrape time if the deps aren't installed, which makes
    # unit tests (which mock the browser) cheap.
    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "knesset_sharepoint scraper requires `playwright` and "
            "`playwright-stealth` plus a Chromium browser binary. "
            "Install with: `pip install playwright playwright-stealth && "
            "python -m playwright install chromium`."
        ) from e

    extractor = config.row_extractor or _default_row_extractor(config.anchor_selector)
    rows: list[PdfRow] = []
    seen_urls: set[str] = set()

    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=True, args=config.extra_browser_args)
        ctx = browser.new_context(user_agent=_UA)
        page = ctx.new_page()
        try:
            # Reblaze cookie warmup. ``networkidle`` lets the JS challenge
            # complete and set the bypass cookie before we navigate to
            # the target listing.
            logger.info("scrape_pdf_index: warmup at %s", _WARMUP_URL)
            page.goto(_WARMUP_URL, wait_until="networkidle", timeout=config.timeout_ms)
            cookies = ctx.cookies()
            logger.info("scrape_pdf_index: warmup got %d cookies", len(cookies))

            urls_to_scrape = [config.page_url]
            if config.sub_index_extractor is not None:
                page.goto(config.page_url, wait_until="networkidle", timeout=config.timeout_ms)
                page.wait_for_timeout(1500)
                sub_urls = list(config.sub_index_extractor(page.content()))
                logger.info("scrape_pdf_index: %d sub-pages discovered from %s",
                            len(sub_urls), config.page_url)
                urls_to_scrape = sub_urls or [config.page_url]

            for url in urls_to_scrape:
                logger.info("scrape_pdf_index: fetching %s", url)
                page.goto(url, wait_until="networkidle", timeout=config.timeout_ms)
                page.wait_for_timeout(1500)
                page_rows = list(extractor(page))
                logger.info("scrape_pdf_index: %s -> %d rows", url, len(page_rows))
                for r in page_rows:
                    if r.url in seen_urls:
                        continue
                    seen_urls.add(r.url)
                    rows.append(r)
        finally:
            browser.close()

    _ensure_at_least_one_row(rows, config.page_url, config.output_csv_path)
    _atomic_write_csv(config.output_csv_path, rows)
    logger.info("scrape_pdf_index: wrote %d rows to %s", len(rows), config.output_csv_path)
    return rows


# ---------------------------------------------------------------------------
# Per-pipeline preset builders. Operators (and `fetch_and_process_source`)
# call these to get a ScrapeConfig for one of the three known sources
# without restating the page URLs and selectors.
# ---------------------------------------------------------------------------

def legal_advisor_opinions_config(output_csv_path: Path) -> ScrapeConfig:
    """Replaces BK's ``knesset_legal_advisor`` pipeline."""
    return ScrapeConfig(
        page_url=PREFIX + "/about/departments/pages/leg/ldguidelines.aspx",
        anchor_selector="a.LDDocLink",
        output_csv_path=output_csv_path,
    )


def legal_advisor_letters_config(output_csv_path: Path) -> ScrapeConfig:
    """Replaces BK's ``knesset_legal_advisor_letters`` pipeline."""
    return ScrapeConfig(
        page_url=PREFIX + "/about/departments/pages/leg/ldguidelines2.aspx",
        anchor_selector="a.LDDocLink",
        output_csv_path=output_csv_path,
    )


_ETHICS_SUB_PAGE_PATTERN = re.compile(r"/Activity/committees/Ethics/pages/CommitteeDecisions\d+\.aspx", re.IGNORECASE)
_ETHICS_KNESSET_NUM_PATTERN = re.compile(r"CommitteeDecisions(\d+)", re.IGNORECASE)


def _ethics_sub_index_extractor(html: str) -> list[str]:
    """Find per-Knesset year pages from the ethics 'past' index page."""
    seen: list[str] = []
    for m in _ETHICS_SUB_PAGE_PATTERN.findall(html):
        url = _absolute(m)
        if url not in seen:
            seen.append(url)
    # Ensure the current-Knesset page (CommitteeDecisions25.aspx) is
    # included; BK hardcodes it as a fallback when the listing on
    # CommitteeDecisionsPast doesn't link back to itself.
    current = PREFIX + "/Activity/committees/Ethics/pages/CommitteeDecisions25.aspx"
    if current not in seen:
        seen.append(current)
    return seen


def _ethics_row_extractor(page) -> Iterable[PdfRow]:
    """Each ethics sub-page lists decisions in <tr>'s with .ComEthicsTdDate
    (or .link-item with .ComEthicsDivDate). Mirror BK's row logic."""
    rows: list[PdfRow] = []
    page_url = page.url
    knesset_num: Optional[int] = None
    knesset_match = _ETHICS_KNESSET_NUM_PATTERN.search(page_url)
    if knesset_match:
        knesset_num = int(knesset_match.group(1))

    # Each <tr> or <div.link-item> may contain one anchor and one date cell.
    for container in page.query_selector_all("tr, .link-item"):
        anchors = container.query_selector_all("a")
        date_nodes = container.query_selector_all(".ComEthicsTdDate, .ComEthicsDivDate")
        if len(anchors) != 1 or len(date_nodes) != 1:
            continue
        anchor = anchors[0]
        href = anchor.get_attribute("href") or ""
        if not href.endswith(".pdf"):
            continue
        url = _absolute(href)
        title = (anchor.inner_text() or "").strip()
        date = (date_nodes[0].inner_text() or "").strip()
        rows.append(PdfRow(
            url=url,
            title=title,
            filename=_filename_for(url),
            date=date,
            knesset_num=knesset_num,
        ))
    return rows


def ethics_committee_decisions_config(output_csv_path: Path) -> ScrapeConfig:
    """Replaces BK's ``ethics_committee_decisions`` pipeline."""
    return ScrapeConfig(
        page_url=PREFIX + "/Activity/committees/Ethics/Pages/CommitteeDecisionsPast.aspx",
        anchor_selector="table a",
        output_csv_path=output_csv_path,
        sub_index_extractor=_ethics_sub_index_extractor,
        row_extractor=_ethics_row_extractor,
    )


# ---------------------------------------------------------------------------
# Operator-facing wrappers used by ``fetch_and_process_source`` dispatch.
# Each wrapper builds a ``ScrapeConfig`` from the kwargs supplied via
# ``config.yaml`` and forwards to ``scrape_pdf_index``. Extra kwargs
# (e.g. ``headless``, ``timeout_ms`` if a future config passes them)
# are tolerated and forwarded only when ``ScrapeConfig`` accepts them;
# truly unknown kwargs are discarded so the dispatcher need not know
# about every per-fetcher field.
# ---------------------------------------------------------------------------

# ScrapeConfig fields that a config.yaml entry may legitimately override
# beyond page_url/output_csv_path. Anything else in **_extra is dropped.
_SCRAPE_CONFIG_PASSTHROUGH_FIELDS = {"timeout_ms", "extra_browser_args"}


def _select_passthrough(extra: dict) -> dict:
    return {k: v for k, v in extra.items() if k in _SCRAPE_CONFIG_PASSTHROUGH_FIELDS}


def scrape_legal_advisor_opinions(*, output_csv_path, page_url, **_extra):
    """fap dispatch wrapper for the legal-advisor opinions page.

    Builds a ``ScrapeConfig`` with the ``a.LDDocLink`` selector used by
    ``legal_advisor_opinions_config`` and forwards to ``scrape_pdf_index``.
    Extra kwargs are silently ignored unless they map to a known
    ``ScrapeConfig`` field (``timeout_ms``, ``extra_browser_args``).
    """
    config = ScrapeConfig(
        page_url=page_url,
        anchor_selector="a.LDDocLink",
        output_csv_path=Path(output_csv_path),
        **_select_passthrough(_extra),
    )
    return scrape_pdf_index(config)


def scrape_legal_advisor_letters(*, output_csv_path, page_url, **_extra):
    """fap dispatch wrapper for the legal-advisor letters page."""
    config = ScrapeConfig(
        page_url=page_url,
        anchor_selector="a.LDDocLink",
        output_csv_path=Path(output_csv_path),
        **_select_passthrough(_extra),
    )
    return scrape_pdf_index(config)
