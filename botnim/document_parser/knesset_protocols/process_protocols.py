"""Fetcher for Knesset committee + plenum protocol transcripts.

Pulls protocol document references from the Knesset OData service
(``KNS_DocumentCommitteeSession`` + ``KNS_DocumentPlenumSession``,
filtered to ``GroupTypeDesc`` values that designate transcripts —
``פרוטוקול ועדה``, ``דברי הכנסת``, ``סטנוגרמה``), downloads each
``.doc`` file from ``fs.knesset.gov.il``, parses it with
:mod:`parse_protocol` into speaker turns, and writes one CSV row per
turn for the downstream sync to embed.

Per-speaker-turn chunking is the recommended granularity per the Monday
plan: each turn becomes one searchable document with structured metadata
(committee, date, agenda item, speaker, party, role). Aurora does the
heavy lifting via pgvector + BM25.

Scale rails — without them this fetcher will happily try to download
~162K docs and burn days:

* ``days_history`` — how far back to look (default: 365 days). Filters
  the OData query by ``LastUpdatedDate`` so we only pick up docs that
  were created or revised inside the window.
* ``max_protocols`` — hard cap on the number of .doc files processed
  per run (default 5000). Acts as a circuit breaker for the first
  staging deploy.
* ``rate_limit_seconds`` — minimum delay between successive .doc
  downloads (default 0.25s = 4 req/s, well under what fs.knesset.gov.il
  serves).

Safety rails:

* Atomic CSV write via .tmp + os.replace.
* Hash short-circuit by SHA-256 over (DocumentID, LastUpdatedDate)
  tuples — re-running with no upstream changes is a no-op.
* :class:`EmptyUpstreamIndex` if the OData query returns zero docs.
* Per-document failures are logged and skipped (don't abort the whole
  fetch when a single doc 404s or fails to parse).
"""
from __future__ import annotations

import csv
import hashlib
import time
from datetime import datetime, timedelta
from typing import Iterable, Optional

import requests

from ...config import get_logger
from ...storage.base import ArtifactStore
from ...storage.csv_writer import write_csv_artifact
from ..pdfs.exceptions import EmptyUpstreamIndex
from .parse_protocol import parse_protocol

logger = get_logger(__name__)


_DEFAULT_BASE = "https://knesset.gov.il/Odata/ParliamentInfo.svc"
_PAGE_SIZE = 250

# Bigger CSV cells than usual — full speaker turns can be long.
csv.field_size_limit(10 * 1024 * 1024)

# Default GroupTypeDesc values that designate actual transcript content.
# Background docs / decisions / agendas are intentionally excluded.
_DEFAULT_COMMITTEE_TYPES = ("פרוטוקול ועדה",)
_DEFAULT_PLENUM_TYPES = ("דברי הכנסת",)  # סטנוגרמה is older + tiny corpus


def _odata_datetime(dt: datetime) -> str:
    return f"datetime'{dt.strftime('%Y-%m-%dT%H:%M:%S')}'"


def _odata_str_eq(field: str, values: tuple[str, ...]) -> str:
    """Build a `field eq 'a' or field eq 'b'` clause for OData v2.

    OData v2 doesn't support ``in`` and the Knesset service is stricter
    than most about quoting. Keep the values short — the service rejects
    URLs over ~8KB.
    """
    parts = [f"{field} eq '{v}'" for v in values]
    return "(" + " or ".join(parts) + ")"


def _fetch_paged(url: str, *, base_params: dict, timeout: int = 60) -> Iterable[dict]:
    """Iterate OData v2 results, following the next-page link.

    The Knesset OData service is v2 in JSON-light mode. Two non-obvious
    things to get right (both were latent bugs until plenary_schedule was
    backfilled past a single page on 2026-05-11):

    1. v2 uses ``odata.nextLink`` (no @). v4 uses ``@odata.nextLink``.
       Check the unprefixed key first; fall back to @ for forward
       compatibility if anyone ever points this at a v4 service.
    2. The next-link is a RELATIVE path ("KNS_DocumentCommitteeSession?
       $filter=…&$skiptoken=…"), not an absolute URL. ``urljoin`` against
       the request URL produces a fully qualified URL; it's a no-op when
       the value is already absolute.
    """
    from urllib.parse import urljoin
    params = dict(base_params)
    next_url: Optional[str] = url
    while next_url:
        if next_url == url:
            resp = requests.get(next_url, params=params, timeout=timeout,
                                headers={"Accept": "application/json"})
        else:
            resp = requests.get(next_url, timeout=timeout,
                                headers={"Accept": "application/json"})
        resp.raise_for_status()
        payload = resp.json()
        rows = payload.get("value")
        if rows is None:
            rows = payload.get("d", {}).get("results", [])
        for r in rows:
            yield r
        raw_next = payload.get("odata.nextLink") or payload.get("@odata.nextLink")
        next_url = urljoin(url, raw_next) if raw_next else None


def _list_committee_docs(base_url: str, since: datetime, max_docs: int,
                         types: tuple[str, ...]) -> list[dict]:
    """Fetch committee protocol document index entries."""
    url = f"{base_url}/KNS_DocumentCommitteeSession"
    filt = (
        f"{_odata_str_eq('GroupTypeDesc', types)} and "
        f"LastUpdatedDate ge {_odata_datetime(since)}"
    )
    params = {"$filter": filt, "$orderby": "LastUpdatedDate desc",
              "$top": _PAGE_SIZE, "$format": "json"}
    out: list[dict] = []
    for row in _fetch_paged(url, base_params=params):
        out.append(row)
        if len(out) >= max_docs:
            break
    return out


def _list_plenum_docs(base_url: str, since: datetime, max_docs: int,
                      types: tuple[str, ...]) -> list[dict]:
    """Fetch plenum protocol document index entries."""
    url = f"{base_url}/KNS_DocumentPlenumSession"
    filt = (
        f"{_odata_str_eq('GroupTypeDesc', types)} and "
        f"LastUpdatedDate ge {_odata_datetime(since)}"
    )
    params = {"$filter": filt, "$orderby": "LastUpdatedDate desc",
              "$top": _PAGE_SIZE, "$format": "json"}
    out: list[dict] = []
    for row in _fetch_paged(url, base_params=params):
        out.append(row)
        if len(out) >= max_docs:
            break
    return out


def _compute_hash(committee_docs: list[dict], plenum_docs: list[dict]) -> str:
    h = hashlib.sha256()
    for row in sorted(committee_docs, key=lambda r: int(r["DocumentCommitteeSessionID"])):
        h.update(f"C:{row['DocumentCommitteeSessionID']}:{row.get('LastUpdatedDate','')}\n".encode())
    for row in sorted(plenum_docs, key=lambda r: int(r["DocumentPlenumSessionID"])):
        h.update(f"P:{row['DocumentPlenumSessionID']}:{row.get('LastUpdatedDate','')}\n".encode())
    return h.hexdigest()


def _existing_upstream_hash(store: ArtifactStore, key: str) -> Optional[str]:
    if not store.exists(key):
        return None
    import io as _io
    text = store.get_bytes(key).decode("utf-8")
    reader = csv.DictReader(_io.StringIO(text))
    for row in reader:
        return row.get("upstream_hash") or None
    return None


def _download(url: str, *, timeout: int = 60) -> Optional[bytes]:
    """Download a single .doc file; return bytes or None on failure."""
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.content
    except Exception as exc:  # noqa: BLE001 — log and skip, don't abort the run
        logger.warning("download failed for %s: %s", url, exc)
        return None


def process_knesset_protocols_source(
    *,
    store: ArtifactStore,
    key: str,
    base_url: str = _DEFAULT_BASE,
    days_history: int = 365,
    max_protocols: int = 5000,
    rate_limit_seconds: float = 0.25,
    include_committees: bool = True,
    include_plenum: bool = True,
    committee_types: tuple[str, ...] = _DEFAULT_COMMITTEE_TYPES,
    plenum_types: tuple[str, ...] = _DEFAULT_PLENUM_TYPES,
    now: Optional[datetime] = None,
    _http_timeout: int = 60,
):
    """Download + parse Knesset protocols and write a per-turn CSV."""
    if now is None:
        now = datetime.utcnow()
    since = now - timedelta(days=days_history)

    base_url = base_url.rstrip("/")

    logger.info(
        "Listing Knesset protocol docs since %s (committees=%s, plenum=%s, max=%d)",
        since.date(), include_committees, include_plenum, max_protocols,
    )
    committee_docs = (_list_committee_docs(base_url, since, max_protocols, committee_types)
                      if include_committees else [])
    remaining = max(0, max_protocols - len(committee_docs))
    plenum_docs = (_list_plenum_docs(base_url, since, remaining, plenum_types)
                   if include_plenum and remaining > 0 else [])

    total = len(committee_docs) + len(plenum_docs)
    logger.info("Got %d committee docs + %d plenum docs (total %d)",
                len(committee_docs), len(plenum_docs), total)
    if total == 0:
        raise EmptyUpstreamIndex(
            f"OData returned 0 protocol documents in window "
            f"[{since.date()}, {now.date()}) — refusing to overwrite {key}"
        )

    upstream_hash = _compute_hash(committee_docs, plenum_docs)
    stored = _existing_upstream_hash(store, key)
    if stored and stored == upstream_hash:
        logger.info("Knesset protocols hash %s unchanged; leaving %s as-is",
                    upstream_hash, key)
        return

    # Per-document delta: even when the overall upstream_hash changed (one new
    # protocol uploaded, or a single existing one re-edited), the vast
    # majority of documents are unchanged. Re-downloading + re-parsing 13K
    # .doc files because one was added is wasteful. Build a reuse index
    # keyed by (document_id, file_last_updated) — if a document's
    # LastUpdatedDate from OData matches what we already have on disk, we
    # reuse the per-turn rows verbatim (only refreshing the upstream_hash
    # column so the file remains internally consistent). When LastUpdatedDate
    # differs (or the doc is new) we fall through to a fresh download+parse.
    existing_rows_by_doc: dict[str, list[dict]] = {}
    existing_last_updated_by_doc: dict[str, str] = {}
    if store.exists(key):
        import io as _io
        for row in csv.DictReader(_io.StringIO(store.get_bytes(key).decode("utf-8"))):
            doc_id = row.get("document_id") or ""
            if not doc_id:
                continue
            existing_rows_by_doc.setdefault(doc_id, []).append(row)
            # All rows for the same document share the same file_last_updated;
            # last writer wins but they should be identical.
            existing_last_updated_by_doc[doc_id] = row.get("file_last_updated") or ""

    reused_doc_count = 0
    downloaded_doc_count = 0

    fieldnames = [
        "upstream_hash",
        "doc_kind",                # "committee" | "plenum"
        "doc_group_type",          # the original GroupTypeDesc
        "document_id",             # DocumentCommitteeSessionID / DocumentPlenumSessionID
        "session_id",              # CommitteeSessionID / PlenumSessionID (parent session)
        "file_url",
        "file_last_updated",
        "knesset_num",
        "session_label",
        "committee_name",
        "session_date",
        "agenda_item",
        "turn_ordinal",
        "speaker_role",            # chair | speaker | speaker_continued | interjection
        "speaker_name",
        "speaker_party",
        "turn_text",
    ]

    out_rows: list[dict] = []
    failures = 0

    def _process_doc(row: dict, kind: str, doc_id_field: str, sess_id_field: str):
        nonlocal failures, reused_doc_count, downloaded_doc_count
        url = row.get("FilePath") or ""
        if not url.lower().endswith((".doc", ".docx")):
            return
        doc_id = row.get(doc_id_field) or ""
        upstream_last_updated = row.get("LastUpdatedDate") or ""
        # Per-document cache hit: reuse the existing per-turn rows verbatim,
        # only updating upstream_hash so the file's internal consistency
        # marker reflects the current run. No download, no parse.
        if (
            doc_id
            and doc_id in existing_rows_by_doc
            and upstream_last_updated
            and existing_last_updated_by_doc.get(doc_id) == upstream_last_updated
        ):
            for cached in existing_rows_by_doc[doc_id]:
                refreshed = dict(cached)
                refreshed["upstream_hash"] = upstream_hash
                out_rows.append(refreshed)
            reused_doc_count += 1
            return
        downloaded_doc_count += 1
        body = _download(url, timeout=_http_timeout)
        if rate_limit_seconds > 0:
            time.sleep(rate_limit_seconds)
        if body is None:
            failures += 1
            return
        try:
            header, turns = parse_protocol(body)
        except Exception as exc:  # noqa: BLE001
            logger.warning("parse failed for %s: %s", url, exc)
            failures += 1
            return
        common = {
            "upstream_hash": upstream_hash,
            "doc_kind": kind,
            "doc_group_type": row.get("GroupTypeDesc") or "",
            "document_id": row.get(doc_id_field) or "",
            "session_id": row.get(sess_id_field) or "",
            "file_url": url,
            "file_last_updated": row.get("LastUpdatedDate") or "",
            "knesset_num": header.knesset_num,
            "session_label": header.session_label,
            "committee_name": header.committee_name,
            "session_date": header.session_date,
        }
        for t in turns:
            r = dict(common)
            r.update({
                "agenda_item": t.agenda_item,
                "turn_ordinal": t.ordinal,
                "speaker_role": t.role,
                "speaker_name": t.speaker_name,
                "speaker_party": t.speaker_party,
                "turn_text": t.text,
            })
            out_rows.append(r)

    for i, row in enumerate(committee_docs, 1):
        _process_doc(row, "committee", "DocumentCommitteeSessionID", "CommitteeSessionID")
        if i % 25 == 0:
            logger.info("  committee %d/%d processed (rows so far: %d)",
                        i, len(committee_docs), len(out_rows))
    for i, row in enumerate(plenum_docs, 1):
        _process_doc(row, "plenum", "DocumentPlenumSessionID", "PlenumSessionID")
        if i % 25 == 0:
            logger.info("  plenum %d/%d processed (rows so far: %d)",
                        i, len(plenum_docs), len(out_rows))

    if not out_rows:
        raise EmptyUpstreamIndex(
            f"All {total} candidate protocols failed to download or parse — "
            f"refusing to overwrite {key}"
        )

    write_csv_artifact(store, key, out_rows, fieldnames=fieldnames)

    logger.info(
        "Wrote %d turn rows from %d documents (%d failures) to %s [hash=%s]",
        len(out_rows), total - failures, failures, key, upstream_hash,
    )
    logger.info(
        "KNESSET_PROTOCOLS_CACHE_SUMMARY: reused=%d downloaded=%d failures=%d "
        "total_upstream=%d",
        reused_doc_count, downloaded_doc_count, failures, total,
    )
