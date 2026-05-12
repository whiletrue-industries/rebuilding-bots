"""Fetcher for the Knesset OData ParliamentInfo service.

Targets the plenary-schedule entities ``KNS_PlenumSession`` (one row per
sitting, with ``StartDate``/``FinishDate``/``Name``) and
``KNS_PlmSessionItem`` (one row per agenda item within a sitting). The
public service base is
``https://knesset.gov.il/Odata/ParliamentInfo.svc/`` and supports
standard OData v2 ``$filter`` / ``$orderby`` / ``$top`` / ``$format=json``.

The output is a single CSV — one row per ``(session, item)`` pair with
the session metadata duplicated alongside each item. That shape lets
the downstream collect_sources csv pipeline turn each row into one
embedded document, so a query like
"what's on the plenary schedule for next week?" can match individual
agenda items by date + item text rather than blob-matching whole
sittings. Sessions with zero items still produce one row (with empty
item columns) so an empty agenda is searchable too.

Safety rails follow the bk_csv pattern:

* ``EmptyUpstreamIndex`` if zero sessions come back — refuses to
  overwrite the existing on-disk CSV (e.g. the API is down or returns
  200 with an empty result during a deploy window).
* Atomic write via ``.tmp`` + ``os.replace``.
* Hash short-circuit: SHA-256 of the sorted ``(PlenumSessionID,
  LastUpdatedDate)`` tuples is stored in column ``upstream_hash``;
  unchanged → leave the file alone. We compute our own hash because
  the OData service does not expose an ETag we can rely on and the
  per-entity ``LastUpdatedDate`` only catches edits to that entity
  (not deletions of items).
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

import requests

from ...config import get_logger
from ..pdfs.exceptions import EmptyUpstreamIndex

logger = get_logger(__name__)


_DEFAULT_BASE = "https://knesset.gov.il/Odata/ParliamentInfo.svc"
_DEFAULT_DAYS_PAST = 365
_DEFAULT_DAYS_FUTURE = 90
_PAGE_SIZE = 250  # OData service caps page size; 250 keeps round trips low.


def _odata_datetime(dt: datetime) -> str:
    """Format a datetime as an OData v2 ``datetime'YYYY-MM-DDTHH:MM:SS'`` literal."""
    return f"datetime'{dt.strftime('%Y-%m-%dT%H:%M:%S')}'"


def _fetch_paged(url: str, *, base_params: dict, timeout: int = 60) -> Iterable[dict]:
    """Iterate over OData v2 results, following ``__next`` links if present.

    The Knesset service returns ``{"value": [...], "@odata.nextLink": ...}``
    in JSON-light mode. We page until exhausted.
    """
    params = dict(base_params)
    next_url: Optional[str] = url
    while next_url:
        if next_url == url:
            resp = requests.get(next_url, params=params, timeout=timeout,
                                headers={"Accept": "application/json"})
        else:
            # Subsequent calls use the absolute next link verbatim.
            resp = requests.get(next_url, timeout=timeout,
                                headers={"Accept": "application/json"})
        resp.raise_for_status()
        payload = resp.json()
        rows = payload.get("value")
        if rows is None:
            # OData v2 verbose envelope fallback (unlikely with default
            # JSON content negotiation, but harmless).
            rows = payload.get("d", {}).get("results", [])
        for r in rows:
            yield r
        # OData v2 in JSON-light uses ``odata.nextLink`` (no @); OData v4
        # uses ``@odata.nextLink``. The Knesset service is v2, so check the
        # unprefixed key first. Without this, the first page is the only
        # page we ever see — and the year-wide window silently drops ~50
        # sessions starting around the 100-row OData page cap.
        raw_next = payload.get("odata.nextLink") or payload.get("@odata.nextLink")
        # The Knesset service returns the next-link as a RELATIVE path
        # ("KNS_PlenumSession?$filter=…&$skiptoken=…"), not an absolute URL.
        # Resolve it against the request URL so requests.get gets a fully
        # qualified URL. urljoin is a no-op when raw_next is already absolute.
        from urllib.parse import urljoin
        next_url = urljoin(url, raw_next) if raw_next else None


def _normalize_dt(value: Optional[str]) -> str:
    """OData returns ``"2026-04-27T15:00:00"`` style strings already.

    Pass them through unchanged; defensively handle ``None``. The
    downstream embedding only cares about the YYYY-MM-DD prefix for
    date queries, so we keep the full timestamp.
    """
    if not value:
        return ""
    return value


def _compute_hash(
    sessions: list[dict],
    items: list[dict],
    stenograms: list[dict] | None = None,
) -> str:
    """SHA-256 over the (id, last_updated) tuples, sorted for determinism.

    Stenograms participate in the hash so that a session gaining its
    stenogram (some hours/days after it ends) triggers a CSV rewrite —
    we want the new ``source_url`` to land in the next sync, not stay
    stale until something else in the session changes.
    """
    h = hashlib.sha256()
    for row in sorted(sessions, key=lambda r: r.get("PlenumSessionID") or 0):
        h.update(f"S:{row.get('PlenumSessionID')}:{row.get('LastUpdatedDate', '')}\n".encode())
    for row in sorted(items, key=lambda r: r.get("plmPlenumSessionID") or 0):
        h.update(
            f"I:{row.get('plmPlenumSessionID')}:{row.get('LastUpdatedDate', '')}\n".encode()
        )
    for row in sorted(stenograms or [], key=lambda r: r.get("DocumentPlenumSessionID") or ""):
        h.update(
            f"D:{row.get('DocumentPlenumSessionID')}:{row.get('LastUpdatedDate', '')}\n".encode()
        )
    return h.hexdigest()


def _existing_upstream_hash(output_csv: Path) -> Optional[str]:
    """Read the upstream hash stored alongside the first row, if any."""
    if not output_csv.exists():
        return None
    with open(output_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            return row.get("upstream_hash") or None
    return None


def _hebrew_date(iso_dt: str) -> str:
    """``"2026-04-27T15:00:00"`` → ``"27/04/2026 15:00"`` for human-readable rows.

    Returns the empty string for bad / missing input rather than raising.
    """
    if not iso_dt:
        return ""
    try:
        dt = datetime.fromisoformat(iso_dt)
    except ValueError:
        return iso_dt
    return dt.strftime("%d/%m/%Y %H:%M")


def fetch_plenum_sessions(
    base_url: str,
    start_dt: datetime,
    end_dt: datetime,
    timeout: int = 60,
) -> list[dict]:
    """Fetch plenum sessions in the half-open window [start_dt, end_dt)."""
    url = f"{base_url}/KNS_PlenumSession"
    filt = (
        f"StartDate ge {_odata_datetime(start_dt)} and "
        f"StartDate lt {_odata_datetime(end_dt)}"
    )
    params = {
        "$filter": filt,
        "$orderby": "StartDate asc",
        "$top": _PAGE_SIZE,
        "$format": "json",
    }
    return list(_fetch_paged(url, base_params=params, timeout=timeout))


def fetch_session_items(
    base_url: str,
    session_ids: list[int],
    timeout: int = 60,
) -> list[dict]:
    """Fetch all agenda items for the given session IDs.

    The service's ``$filter`` URL has a length limit, so we batch
    session IDs into chunks of 25 IDs per request via the
    ``PlenumSessionID in (...)`` idiom (OData v2 doesn't support ``in``;
    we use repeated ``or`` clauses).
    """
    if not session_ids:
        return []
    url = f"{base_url}/KNS_PlmSessionItem"
    out: list[dict] = []
    chunk_size = 25
    for i in range(0, len(session_ids), chunk_size):
        chunk = session_ids[i:i + chunk_size]
        clauses = " or ".join(f"PlenumSessionID eq {sid}" for sid in chunk)
        params = {
            "$filter": clauses,
            "$orderby": "PlenumSessionID,Ordinal",
            "$top": _PAGE_SIZE,
            "$format": "json",
        }
        out.extend(_fetch_paged(url, base_params=params, timeout=timeout))
    return out


# GroupTypeID for the stenogram (סטנוגרמה) — the full plenary transcript.
# Stenograms are published after the session completes, so they're absent
# for upcoming sessions; that's intentional (semantic search shouldn't
# return upcoming sessions for content questions anyway; live-tool does).
_STENOGRAM_GROUP_TYPE_ID = 43


def fetch_session_stenograms(
    base_url: str,
    session_ids: list[int],
    timeout: int = 60,
) -> list[dict]:
    """Fetch stenogram document rows (GroupTypeID=43) for the given session IDs.

    Returns rows from KNS_DocumentPlenumSession filtered to the stenogram
    group type. Each row carries ``PlenumSessionID``, ``FilePath`` (the
    fs.knesset.gov.il URL), and ``LastUpdatedDate`` (used to pick the
    latest if a session somehow has multiple stenogram rows).
    """
    if not session_ids:
        return []
    url = f"{base_url}/KNS_DocumentPlenumSession"
    out: list[dict] = []
    chunk_size = 25
    for i in range(0, len(session_ids), chunk_size):
        chunk = session_ids[i:i + chunk_size]
        clauses = " or ".join(f"PlenumSessionID eq {sid}" for sid in chunk)
        params = {
            "$filter": f"({clauses}) and GroupTypeID eq {_STENOGRAM_GROUP_TYPE_ID}",
            "$top": _PAGE_SIZE,
            "$format": "json",
        }
        out.extend(_fetch_paged(url, base_params=params, timeout=timeout))
    return out


def process_knesset_odata_source(
    *,
    output_csv_path: Path,
    base_url: str = _DEFAULT_BASE,
    days_past: int = _DEFAULT_DAYS_PAST,
    days_future: int = _DEFAULT_DAYS_FUTURE,
    now: Optional[datetime] = None,
    _http_timeout: int = 60,
):
    """Download Knesset plenum-schedule data and write a normalized CSV.

    Parameters
    ----------
    output_csv_path:
        Where to write the CSV. Atomically replaced on success.
    base_url:
        OData service base URL (without trailing slash). Override for tests.
    days_past:
        How many days of historical sessions to keep. Useful so
        "what was on last week's plenary" still works.
    days_future:
        How far ahead to fetch. Plenary schedules are typically
        published 1–4 weeks out; 90 days gives plenty of headroom.
    now:
        Override the reference timestamp for the window. Test seam.
    """
    output_csv = Path(output_csv_path)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    if now is None:
        now = datetime.utcnow()
    start_dt = now - timedelta(days=days_past)
    end_dt = now + timedelta(days=days_future)

    base_url = base_url.rstrip("/")

    logger.info(
        "Fetching Knesset plenum schedule from %s (%s -> %s)",
        base_url, start_dt.isoformat(timespec='seconds'),
        end_dt.isoformat(timespec='seconds'),
    )
    sessions = fetch_plenum_sessions(base_url, start_dt, end_dt, timeout=_http_timeout)
    if not sessions:
        raise EmptyUpstreamIndex(
            f"{base_url}/KNS_PlenumSession: no sessions in window "
            f"[{start_dt.date()}, {end_dt.date()}) — refusing to overwrite "
            f"{output_csv}"
        )
    logger.info("Got %d sessions", len(sessions))

    session_ids = [s["PlenumSessionID"] for s in sessions]
    items = fetch_session_items(base_url, session_ids, timeout=_http_timeout)
    logger.info("Got %d agenda items across %d sessions", len(items), len(sessions))

    stenograms = fetch_session_stenograms(base_url, session_ids, timeout=_http_timeout)
    logger.info("Got %d stenograms for %d sessions", len(stenograms), len(sessions))
    # Latest-wins if a session has multiple stenogram rows (rare).
    stenogram_url_by_session: dict[int, str] = {}
    for doc in sorted(stenograms, key=lambda d: d.get("LastUpdatedDate") or ""):
        sid = doc.get("PlenumSessionID")
        fp = (doc.get("FilePath") or "").strip()
        if sid and fp:
            stenogram_url_by_session[sid] = fp

    upstream_hash = _compute_hash(sessions, items, stenograms)
    stored_hash = _existing_upstream_hash(output_csv)
    if stored_hash and stored_hash == upstream_hash:
        logger.info(
            "Knesset OData hash %s unchanged; leaving %s as-is",
            upstream_hash, output_csv,
        )
        return

    items_by_session: dict[int, list[dict]] = {}
    for it in items:
        items_by_session.setdefault(it["PlenumSessionID"], []).append(it)
    for lst in items_by_session.values():
        lst.sort(key=lambda r: (r.get("Ordinal") or 0, r.get("ItemID") or 0))

    fieldnames = [
        "upstream_hash",
        "session_id",
        "session_number",
        "knesset_num",
        "session_name",
        "session_date",       # YYYY-MM-DD
        "session_time",       # HH:MM
        "session_start_iso",
        "session_finish_iso",
        "session_human_date", # DD/MM/YYYY HH:MM (Hebrew-locale-friendly)
        "is_special_meeting",
        "source_url",
        "item_ordinal",
        "item_type",
        "item_name",
        "item_status_id",
        "item_is_discussion",
    ]

    out_rows: list[dict] = []
    for s in sessions:
        s_iso = _normalize_dt(s.get("StartDate"))
        f_iso = _normalize_dt(s.get("FinishDate"))
        s_date = s_iso[:10] if s_iso else ""
        s_time = s_iso[11:16] if s_iso else ""
        common = {
            "upstream_hash": upstream_hash,
            "session_id": s.get("PlenumSessionID") or "",
            "session_number": s.get("Number") or "",
            "knesset_num": s.get("KnessetNum") or "",
            "session_name": (s.get("Name") or "").strip(),
            "session_date": s_date,
            "session_time": s_time,
            "session_start_iso": s_iso,
            "session_finish_iso": f_iso,
            "session_human_date": _hebrew_date(s_iso),
            "is_special_meeting": "כן" if s.get("IsSpecialMeeting") else "לא",
            "source_url": stenogram_url_by_session.get(s.get("PlenumSessionID"), ""),
        }
        sid = s["PlenumSessionID"]
        rows_for_session = items_by_session.get(sid, [])
        if not rows_for_session:
            row = dict(common)
            row.update({
                "item_ordinal": "",
                "item_type": "",
                "item_name": "",
                "item_status_id": "",
                "item_is_discussion": "",
            })
            out_rows.append(row)
            continue
        for it in rows_for_session:
            row = dict(common)
            row.update({
                "item_ordinal": it.get("Ordinal") or "",
                "item_type": (it.get("ItemTypeDesc") or "").strip(),
                "item_name": (it.get("Name") or "").strip(),
                "item_status_id": it.get("StatusID") or "",
                "item_is_discussion": it.get("IsDiscussion") or "",
            })
            out_rows.append(row)

    tmp_output = output_csv.with_suffix(output_csv.suffix + ".tmp")
    try:
        with open(tmp_output, "w", encoding="utf-8", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in out_rows:
                writer.writerow(row)
        os.replace(tmp_output, output_csv)
    except Exception:
        try:
            tmp_output.unlink()
        except FileNotFoundError:
            pass
        raise

    logger.info(
        "Wrote %d rows (%d sessions × items) to %s [hash=%s]",
        len(out_rows), len(sessions), output_csv, upstream_hash,
    )
