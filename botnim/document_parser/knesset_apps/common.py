"""Shared types + helpers for the knesset_apps fetchers."""
from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from botnim.document_parser.pdfs.exceptions import (
    EmptyUpstreamIndex as _PdfsEmptyUpstreamIndex,
)
from botnim.storage.base import ArtifactStore
from botnim.storage.csv_writer import write_csv_artifact


class EmptyUpstreamIndex(_PdfsEmptyUpstreamIndex):
    """Stage 1 fetcher's empty-index guard. Inherits from pdfs.exceptions
    variant so that downstream ``except`` clauses on either symbol catch both
    raise sites — preserves the per-context error isolation contract."""
    pass


@dataclass
class DocRow:
    """One row of the index.csv we produce — same shape as the BK
    datapackage CSVs that ``process_pdf_source`` already reads.

    ``title`` is included even though BK's CSV doesn't carry it (the
    downstream PDF processor recovers it from the PDF body); the
    Knesset apps APIs DO return titles, so we keep them for
    debuggability and for downstream metadata enrichment.
    """

    url: str
    filename: str
    date: str
    knesset_num: int
    title: str = ""


CSV_FIELDS = ["url", "filename", "date", "knesset_num", "title"]


def atomic_write_csv(store: ArtifactStore, key: str, rows: list[DocRow]) -> None:
    """Write ``rows`` to ``key`` atomically through the artifact store."""
    write_csv_artifact(
        store,
        key,
        [
            {
                "url": r.url,
                "filename": r.filename,
                "date": r.date,
                "knesset_num": r.knesset_num,
                "title": r.title,
            }
            for r in rows
        ],
        fieldnames=CSV_FIELDS,
    )


def ensure_at_least_one_row(rows: list[DocRow], store: ArtifactStore, key: str) -> None:
    """Refuse to overwrite an existing populated CSV with empty rows.

    A first-run (no existing object) is allowed to write zero rows.
    """
    if rows:
        return
    if store.exists(key):
        text = store.get_bytes(key).decode("utf-8")
        try:
            existing = max(0, sum(1 for _ in csv.DictReader(io.StringIO(text))))
        except Exception:  # noqa: BLE001
            existing = -1
        if existing <= 0:
            return
        raise EmptyUpstreamIndex(
            f"Knesset apps API returned 0 rows; refusing to overwrite "
            f"{key} which has {existing} existing rows. Likely cause: "
            "filter mismatch (committee_id / date range / knesset number) "
            "or upstream API down."
        )


# Knesset's DocumentPath returns Windows-style backslashes inside the
# URL path (e.g. https://fs.knesset.gov.il/25\Committees\..._dec_*.pdf).
# fs.knesset.gov.il accepts forward-slash equivalents; we normalize so
# downstream HTTP clients don't see a malformed URL.
def normalize_pdf_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return url
    return url.replace("\\", "/")
