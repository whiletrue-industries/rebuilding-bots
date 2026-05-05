"""Shared types + helpers for the knesset_apps fetchers."""
from __future__ import annotations

import csv
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


class EmptyUpstreamIndex(Exception):
    """Raised when the upstream API returned zero rows but the existing
    CSV is populated — refusing to overwrite real data with nothing."""


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


def atomic_write_csv(path: Path, rows: list[DocRow]) -> None:
    """Write ``rows`` to ``path`` via tempfile + ``os.replace``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".index-", suffix=".csv", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            w.writeheader()
            for r in rows:
                w.writerow({
                    "url": r.url,
                    "filename": r.filename,
                    "date": r.date,
                    "knesset_num": r.knesset_num,
                    "title": r.title,
                })
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def ensure_at_least_one_row(rows: list[DocRow], csv_path: Path) -> None:
    """Refuse to overwrite an existing populated CSV with empty rows.

    Mirrors the safety guard in ``document_parser.pdfs.process_pdfs``.
    A first-run with no existing CSV is allowed to write zero rows
    (means the upstream is genuinely empty for this filter).
    """
    if rows:
        return
    if csv_path.exists() and csv_path.stat().st_size > 0:
        try:
            with open(csv_path, encoding="utf-8") as f:
                existing = max(0, sum(1 for _ in csv.DictReader(f)))
        except Exception:  # noqa: BLE001
            existing = -1
        raise EmptyUpstreamIndex(
            f"Knesset apps API returned 0 rows; refusing to overwrite "
            f"{csv_path} which has {existing} existing rows. Likely cause: "
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
