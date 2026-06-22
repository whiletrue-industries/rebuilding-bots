"""Enumerate every law/regulation in ספר החוקים הפתוח from the WikiSource index.

Network surface is exactly one function (`fetch_index_titles`) so tests stay
hermetic. The MediaWiki `parse&prop=links` call returns every main-namespace
page linked from the index (transclusions followed), which we classify by
leading token and filter through the legal_text skip-list.
"""
from pathlib import Path
import requests

from ...config import get_logger
from .classify import classify_title
from .manifest import LawBookEntry
from .skip_list import legal_text_skip_titles

logger = get_logger(__name__)

API_URL = "https://he.wikisource.org/w/api.php"
INDEX_PAGE = "ספר_החוקים_הפתוח"
_HEADERS = {"User-Agent": "botnim-law-book/1.0 (+https://botnim.build-up.team)"}


class CoverageShrinkError(Exception):
    """Discovery returned materially fewer items than expected — refuse to
    overwrite a good manifest with a truncated one."""


def url_for_title(title: str) -> str:
    # Keep Hebrew (non-ASCII) characters unencoded — MediaWiki and browsers accept them;
    # only encode ASCII special chars. Spaces become underscores per wiki convention.
    return "https://he.wikisource.org/wiki/" + title.replace(" ", "_")


def fetch_index_titles(api_url: str = API_URL, index_page: str = INDEX_PAGE) -> list[str]:
    """Return all main-namespace (ns==0) page titles linked from the index."""
    resp = requests.get(api_url, headers=_HEADERS, params={
        "action": "parse", "page": index_page, "prop": "links", "format": "json",
    }, timeout=60)
    resp.raise_for_status()
    links = resp.json()["parse"]["links"]
    return [l["*"] for l in links if l.get("ns") == 0 and "exists" in l]


def discover_law_pages(config_dir, *, include_regulations: bool,
                       min_expected_laws: int = 200,
                       apply_skip_list: bool = True,
                       prior: list[LawBookEntry] | None = None) -> list[LawBookEntry]:
    config_dir = Path(config_dir)
    # The skip-list is derived from the legal_text context so israeli_laws does
    # not double-index those laws. During the single-source consolidation we set
    # apply_skip_list=False so israeli_laws finally ingests the legal_text laws
    # (incl. תקנון הכנסת) while legal_text still exists for the parity gate.
    skip = legal_text_skip_titles(config_dir) if apply_skip_list else set()
    raw_titles = fetch_index_titles()

    wanted = {"law", "regulation"} if include_regulations else {"law"}
    entries: list[LawBookEntry] = []
    laws_found = regs_found = skipped = 0
    seen: set[str] = set()
    for title in raw_titles:
        kind = classify_title(title)
        if kind == "law":
            laws_found += 1
        elif kind == "regulation":
            regs_found += 1
        if kind not in wanted:
            continue
        if title in skip:
            skipped += 1
            continue
        if title in seen:
            continue
        seen.add(title)
        entries.append(LawBookEntry(title=title, url=url_for_title(title), kind=kind))

    logger.info("LAW_BOOK_DISCOVER laws_found=%d regulations_found=%d skipped=%d selected=%d",
                laws_found, regs_found, skipped, len(entries))

    # Guard #1: absolute floor — a broken enumerator returns ~0.
    if laws_found < min_expected_laws:
        raise CoverageShrinkError(
            f"only {laws_found} laws discovered (< floor {min_expected_laws}); "
            f"refusing to overwrite manifest")
    # Guard #2: relative shrink vs. last committed manifest.
    if prior:
        prior_count = len([e for e in prior if e.kind in wanted])
        if prior_count and len(entries) < prior_count * 0.9:
            raise CoverageShrinkError(
                f"discovery shrank to {len(entries)} from {prior_count} (>10% drop); "
                f"refusing to overwrite manifest")
    return entries
