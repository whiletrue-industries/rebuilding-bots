"""Fetch decisions of one Knesset committee via the WebSiteApi JSON endpoint.

The SharePoint-rendered page at ``main.knesset.gov.il/apps/committees/
<id>/decisions?...`` is powered by a single XHR POST to:

  https://www.knesset.gov.il/WebSiteApi/knessetapi/CommitteeDecisions/GetCommitteePortalsDecisions

with body::

    {
      "CommitteeId": <int>,
      "FromDate":    "YYYY-MM-DDT00:00:00",
      "ToDate":      null | "YYYY-MM-DDT00:00:00",
      "KnessetIDs":  "25",
      "Language":    "he",
      "Subject":     ""
    }

Response shape (verified live 2026-05-05)::

    {
      "TotalItems": 89,
      "Items": [
        {
          "ItemId": 2242119,
          "DecisionDate": "2026-03-25T10:00:00",
          "DecisionDateText": "...Hebrew long-form...",
          "DocumentTitle": "החלטת ועדת הכנסת בדבר ...",
          "DocumentPath": "https://fs.knesset.gov.il/25\\Committees\\25_cs_dec_12116253.pdf",
          "committeeId": 2726,        # the COMMITTEE-SESSION id, not the committee
          "FileFormat": "pdf"
        },
        ...
      ]
    }

The endpoint host (``www.knesset.gov.il``) is not behind Reblaze;
plain ``requests`` works without TLS impersonation or browser
automation.

Used by the unified bot's ``committee_decisions`` context. Default
config targets ``ועדת הכנסת`` (CommitteeId=2211) — change the config
to scrape another committee's decisions.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

from .common import (
    DocRow,
    atomic_write_csv,
    ensure_at_least_one_row,
    normalize_pdf_url,
)

logger = logging.getLogger(__name__)


_API_URL = (
    "https://www.knesset.gov.il/WebSiteApi/knessetapi/"
    "CommitteeDecisions/GetCommitteePortalsDecisions"
)

# CommitteeId for ``ועדת הכנסת`` (the Knesset Committee). Surfaced on the
# canonical SharePoint apps page operators link to:
#   https://main.knesset.gov.il/apps/committees/2211/decisions?...
DEFAULT_KNESSET_COMMITTEE_ID = 2211

# The bot's user-facing question scope is "current Knesset"; the
# upstream API expects a comma-separated string of Knesset numbers
# even when there's only one. We default to 25 (current as of 2026-05).
DEFAULT_KNESSET_IDS = "25"


@dataclass
class CommitteeDecisionsConfig:
    """Parameters for one committee_decisions fetch.

    output_csv_path:
        Where to write the resulting ``index.csv``.
    committee_id:
        The Knesset CommitteeId to fetch (default: 2211 = ועדת הכנסת).
    knesset_ids:
        Comma-separated string of Knesset numbers to query, e.g. ``"25"``
        or ``"24,25"``. The API filters server-side.
    from_date:
        ISO date (``YYYY-MM-DD``); ``None`` => beginning of time.
        Coerced to ``YYYY-MM-DDT00:00:00`` in the request body.
    to_date:
        ISO date or ``None`` (= end of time / "today").
    api_url:
        Override only for tests / mirrors.
    timeout_s:
        HTTP timeout.
    """

    output_csv_path: Path
    committee_id: int = DEFAULT_KNESSET_COMMITTEE_ID
    knesset_ids: str = DEFAULT_KNESSET_IDS
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    api_url: str = _API_URL
    timeout_s: int = 60
    # Server returns 10 rows per call regardless of any PageNum/PageSize
    # we tried (verified live 2026-05). We paginate by progressively
    # narrowing ToDate. ``max_iterations`` is a runaway safety; one
    # committee's full corpus typically needs <= ceil(TotalItems / 10)
    # iterations.
    max_iterations: int = 200
    extra_headers: dict = field(default_factory=dict)


def _to_api_datetime(d: Optional[str]) -> Optional[str]:
    if d is None:
        return None
    if "T" in d:
        return d
    return f"{d}T00:00:00"


def _knesset_num_from_ids(knesset_ids: str) -> int:
    """Pick the highest Knesset number from a comma-separated list.

    The output CSV has a single ``knesset_num`` column. When the
    config queries multiple knessets, we tag rows with the highest
    one — same convention BK's ethics pipeline used (the per-Knesset
    sub-page's number).
    """
    nums = [int(p.strip()) for p in knesset_ids.split(",") if p.strip()]
    return max(nums) if nums else 0


def _decrement_one_second(api_dt: str) -> str:
    """Subtract 1s from an ``YYYY-MM-DDTHH:MM:SS`` string.

    Used as cursor decrement when paginating: the API filters items
    inclusive of the boundary, so naive ``ToDate = oldest`` would re-
    return rows that share the boundary timestamp and the loop sees
    new=0 even though uniques remain.
    """
    dt = datetime.fromisoformat(api_dt)
    dt -= timedelta(seconds=1)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _items_to_rows(items: list[dict], knesset_num: int) -> list[DocRow]:
    rows: list[DocRow] = []
    for item in items:
        if (item.get("FileFormat") or "").lower() != "pdf":
            continue
        url = normalize_pdf_url(item.get("DocumentPath"))
        if not url:
            continue
        rows.append(DocRow(
            url=url,
            filename=f"{item.get('ItemId')}.pdf",
            date=item.get("DecisionDate") or "",
            knesset_num=knesset_num,
            title=(item.get("DocumentTitle") or "").strip(),
        ))
    return rows


def fetch_committee_decisions_index(
    config: CommitteeDecisionsConfig,
    *,
    http_post=requests.post,
) -> list[DocRow]:
    """Call the JSON API (cursor-paginated by ToDate) and write ``index.csv``.

    Returns the list of rows so callers can assert without reading
    back the CSV.
    """
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        **config.extra_headers,
    }
    knesset_num = _knesset_num_from_ids(config.knesset_ids)
    seen_urls: set[str] = set()
    rows: list[DocRow] = []
    cursor_to_date: Optional[str] = _to_api_datetime(config.to_date)
    last_total: Optional[int] = None

    for iteration in range(config.max_iterations):
        body = {
            "CommitteeId": config.committee_id,
            "FromDate": _to_api_datetime(config.from_date),
            "ToDate": cursor_to_date,
            "KnessetIDs": config.knesset_ids,
            "Language": "he",
            "Subject": "",
        }
        logger.info(
            "fetch_committee_decisions: iter=%d ToDate=%s",
            iteration, cursor_to_date or "(none)",
        )
        resp = http_post(config.api_url, json=body, headers=headers,
                         timeout=config.timeout_s)
        resp.raise_for_status()
        payload = resp.json()
        items = payload.get("Items") or []
        last_total = payload.get("TotalItems")
        logger.info(
            "fetch_committee_decisions: TotalItems=%s fetched=%s",
            last_total, len(items),
        )
        if not items:
            break

        # Dedupe at the URL level, NOT ItemId. ``ItemId`` is a meeting
        # id and one meeting can have multiple decision documents (each
        # with a distinct DocumentPath). Combined with the API's date-
        # level ToDate truncation, the same meeting can surface across
        # pages with different docs each time — so an ItemId-keyed
        # filter would silently drop documents on the second page.
        page_rows = _items_to_rows(items, knesset_num)
        new_rows_this_page = [r for r in page_rows if r.url not in seen_urls]
        for r in new_rows_this_page:
            seen_urls.add(r.url)
            rows.append(r)

        if not new_rows_this_page:
            # All PDF URLs in this page were already collected — exhausted.
            break
        # Advance cursor to one second before the oldest DecisionDate
        # we saw, so the next call returns strictly older rows.
        oldest = min(it["DecisionDate"] for it in items)
        cursor_to_date = _decrement_one_second(oldest)

    logger.info(
        "fetch_committee_decisions: collected %d unique PDFs (server TotalItems=%s)",
        len(rows), last_total,
    )

    ensure_at_least_one_row(rows, config.output_csv_path)
    atomic_write_csv(config.output_csv_path, rows)
    logger.info(
        "fetch_committee_decisions: wrote %d rows to %s",
        len(rows), config.output_csv_path,
    )
    return rows
