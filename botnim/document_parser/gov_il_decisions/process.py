"""Orchestrator: refresh gov.il government decisions directly into Aurora.

Bypasses the usual ``extraction/<context>.csv`` → ``botnim sync``
pipeline because the corpus is too large to commit as CSV (26K+ rows)
and re-running the LLM categorizer at sync time would be wasteful. The
fetcher fetches → extracts → categorizes → writes to Aurora in one pass.

After this fetcher runs, ``botnim sync`` still needs to run for
``_write_snapshots`` to populate ``context_snapshots`` (which powers
the ``/admin/sources`` view). The sync itself is a no-op on this
context because there's no ``extraction/government_decisions.csv``
for it to read.

If the listing returns zero results AND the context has no existing
rows, raise ``EmptyUpstreamIndex`` — protects against a gov.il outage
silently wiping the indexed corpus on a fresh install.
"""
from __future__ import annotations

from datetime import date
from typing import Optional

from ...config import get_logger
from .api import GovIlClient
from .aurora_writer import (
    existing_page_ids,
    get_or_create_context,
    newest_publish_date,
    write_decision,
)
from .categorize import categorize
from .exceptions import EmptyUpstreamIndex
from .extract import docx_to_text, html_to_text, pdf_to_text

logger = get_logger(__name__)


def _page_id_from_listing(item: dict) -> Optional[str]:
    url = item.get("url") or ""
    if not url:
        return None
    return url.rstrip("/").rsplit("/", 1)[-1] or None


def _meta(item: dict, group: str, key: str) -> str:
    try:
        return item["tags"][group][key][0]["title"]
    except (KeyError, IndexError, TypeError):
        return ""


def _parse_government_number(government: str) -> str:
    """Parse '37' from 'הממשלה ה- 37, בנימין נתניהו'."""
    if "ה-" not in government:
        return ""
    try:
        return government.split("ה-")[1].split(",")[0].strip()
    except IndexError:
        return ""


def _extract_body_and_attachments(content: dict, client: GovIlClient) -> tuple[str, list[str]]:
    """Pull HTML body + (if attachments) inline their extracted text."""
    main = content.get("contentMain") or {}
    sections = main.get("htmlContents") or []
    inline_html = "\n".join(s.get("sectionData") or "" for s in sections)
    body = html_to_text(inline_html)

    attachment_urls: list[str] = []
    sub = content.get("contentSub") or {}
    files = (sub.get("filesToDownload") or {}).get("filesGroupItems") or []
    for group in files:
        for f in group.get("items") or []:
            url = f.get("url")
            if not url:
                continue
            attachment_urls.append(url)
            ext = (f.get("extension") or "").lower()
            try:
                blob = client.download_attachment(url)
                if ext == "pdf":
                    body += "\n\n" + pdf_to_text(blob)
                elif ext in ("docx", "doc"):
                    body += "\n\n" + docx_to_text(blob)
            except Exception as exc:
                logger.warning("attachment %s extraction failed: %s", url, exc)

    return body.strip(), attachment_urls


def _ingest_one(
    *,
    page_id: str,
    item: dict,
    client: GovIlClient,
    context_id: str,
    environment: str,
) -> int:
    """Fetch + extract + categorize + write one decision. Returns rows written."""
    content = client.fetch_content(page_id)
    if content is None:
        return 0  # 404 — skip

    body, attachment_urls = _extract_body_and_attachments(content, client)
    if not body:
        logger.info("empty body for %s — writing metadata only", page_id)

    title = item.get("title") or ""
    cats = categorize(title=title, text=body)
    government = _meta(item, "metaData", "ממשלה")

    metadata = {
        "page_id": page_id,
        "decision_number": _meta(item, "promotedMetaData", "מספר החלטה"),
        "government_number": _parse_government_number(government),
        "government": government,
        "title": title,
        "publish_date": _meta(item, "metaData", "תאריך פרסום"),
        "effective_date": _meta(item, "metaData", "תאריך תחולה"),
        "office": _meta(item, "metaData", "משרד"),
        "action_type": cats["action_type"],
        "domain": cats["domain"],
        "has_attachment": bool(attachment_urls),
        "source_url": f"https://www.gov.il/he/departments/policies/{page_id}",
        "attachment_urls": attachment_urls,
    }

    return write_decision(
        context_id,
        page_id=page_id,
        title=title,
        text=body,
        metadata=metadata,
        environment=environment,
    )


def process_gov_il_decisions_source(
    *,
    environment: str,
    page_size: int = 50,
    max_pages: int = 1000,
    bot_slug: str = "unified",
    context_name: str = "government_decisions",
    stale_after_days: int = 30,
) -> None:
    """Refresh gov.il government decisions into Aurora.

    Pages through the listing API; for each ``page_id`` not already in
    the context, fetches content, extracts body+attachments,
    categorizes, and writes (chunked + embedded) to Aurora.
    """
    context_id = get_or_create_context(bot_slug, context_name)
    seen = existing_page_ids(context_id)
    logger.info(
        "gov_il_decisions: starting refresh for %s/%s, %d existing page_ids",
        bot_slug, context_name, len(seen),
    )

    client = GovIlClient()
    skip = 0
    seen_total: Optional[int] = None
    pages = 0
    new_count = 0
    total_listing_results = 0
    while pages < max_pages:
        page = client.list_decisions(skip=skip, limit=page_size)
        if seen_total is None:
            seen_total = page.get("total", 0)
            # Cold-scrape guard. The fetcher is designed for delta refreshes
            # against an already-bootstrapped context. If the context is empty
            # (operator hasn't run scripts/bootstrap_gov_decisions.py yet) and
            # upstream is large, we'd silently start a 2.5-hour scrape + ~$30
            # of LLM calls — almost always a mistake (e.g. a fresh deploy ran
            # before bootstrap). Refuse and tell the operator what to do. The
            # threshold (1000) is generous so legitimate dev use against a
            # small dataset still works.
            if not seen and seen_total > 1000:
                raise EmptyUpstreamIndex(
                    f"Refusing cold scrape: context ({bot_slug}, {context_name}) "
                    f"is empty and gov.il has {seen_total} upstream decisions. "
                    f"Run scripts/bootstrap_gov_decisions.py once against this "
                    f"Aurora to seed the context, then re-run."
                )
        results = page.get("results") or []
        if not results:
            break
        total_listing_results += len(results)
        for item in results:
            pid = _page_id_from_listing(item)
            if not pid or pid in seen:
                continue
            try:
                rows_written = _ingest_one(
                    page_id=pid,
                    item=item,
                    client=client,
                    context_id=context_id,
                    environment=environment,
                )
                if rows_written > 0:
                    new_count += 1
                    seen.add(pid)
            except Exception as exc:
                logger.warning("ingest failed for %s: %s", pid, exc)
        skip += page_size
        pages += 1

    if total_listing_results == 0 and not seen:
        raise EmptyUpstreamIndex(
            "gov.il listing returned 0 results and context has no existing rows — "
            "refusing to proceed"
        )

    logger.info(
        "gov_il_decisions: upstream_total=%s new_this_run=%d total_in_context=%d",
        seen_total, new_count, len(seen),
    )

    # Freshness alarm. The 2026-05 endpoint migration stalled this context
    # for a month while every refresh "succeeded" (the broken fetch was
    # swallowed by fetch_and_process's per-context isolation). Emit a loud,
    # greppable line when the newest decision we hold is older than the
    # threshold so a CloudWatch metric-filter alarm on GOV_IL_DECISIONS_STALE
    # can page instead of the rot going unnoticed.
    try:
        newest = newest_publish_date(context_id)
        if newest is not None:
            age_days = (date.today() - newest).days
            if age_days > stale_after_days:
                logger.error(
                    "GOV_IL_DECISIONS_STALE: newest decision in (%s, %s) is %s "
                    "(%d days old > %d-day threshold) — fetcher may be broken "
                    "(new_this_run=%d, upstream_total=%s)",
                    bot_slug, context_name, newest.isoformat(), age_days,
                    stale_after_days, new_count, seen_total,
                )
            else:
                logger.info(
                    "gov_il_decisions: freshness OK — newest decision %s (%d days old)",
                    newest.isoformat(), age_days,
                )
    except Exception as exc:  # noqa: BLE001 — freshness is advisory, never fatal
        logger.warning("gov_il_decisions freshness check failed: %s", exc)
