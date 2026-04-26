"""Fetcher for OpenBudget BudgetKey single-CSV datapackages.

These datapackages live at ``next.obudget.org/datapackages/<feed>/`` and
expose a `datapackage.json` manifest plus one CSV resource. Unlike the
``pdf`` fetcher (which downloads PDF binaries listed in an ``index.csv``
and then runs OpenAI extraction per file), these CSVs are the structured
content directly — one row per record, columns already parsed by
BudgetKey's upstream pipeline.

Designed for the ``government_decisions`` context (35K+ rows of gov.il
decisions with title, text, date, ministry, decision number, etc.) but
kept generic for any single-CSV BudgetKey datapackage.

Safety rails — same shape as ``process_pdfs.py``:

* ``EmptyUpstreamIndex`` if the upstream CSV is empty (header-only or
  zero rows) — refuses to overwrite the existing on-disk CSV.
* Atomic write: ``.tmp`` + ``os.replace``.
* Revision short-circuit via the ``datapackage.json`` ``hash`` field
  (BudgetKey's datapackages don't have ``revision`` like the knesset
  ones; use the resource ``hash`` instead).

The fetcher writes columns the bot will use for retrieval and citation:
``title``, ``text`` (HTML stripped), ``publish_date``, ``office``,
``government``, ``policy_type``, ``procedure_number_str``, ``url_id``.
The full upstream row count is configurable via ``max_rows`` so we can
keep the indexed corpus to a tractable size during initial rollout
(default: most-recent ``max_rows`` by ``publish_date``).
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
from pathlib import Path
from typing import Iterable, List, Optional

import requests

from ...config import get_logger
from ..pdfs.exceptions import EmptyUpstreamIndex

logger = get_logger(__name__)


# Bump CSV field-size limit — gov decision body html can be hundreds of KB.
csv.field_size_limit(10 * 1024 * 1024)


_HTML_TAG = re.compile(r"<[^>]+>")
_HTML_ENTITY = re.compile(r"&#?\w+;")
_WS = re.compile(r"\s+")


def _html_to_text(html: str) -> str:
    """Conservative HTML stripper for the BudgetKey body fields.

    Keeps line breaks at block boundaries, drops every tag, decodes the
    most common entities, collapses whitespace runs. Avoids pulling
    BeautifulSoup as a hard dep — the inputs are well-formed enough that
    a regex pass + manual entity map covers it.
    """
    if not html:
        return ""
    # Convert block-level boundaries to newlines so paragraphs survive.
    text = re.sub(r"</(p|div|li|h[1-6]|tr|br)\s*>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<br\s*/?\s*>", "\n", text, flags=re.IGNORECASE)
    text = _HTML_TAG.sub("", text)
    # Decode the entities that actually show up in the gov-decisions feed.
    text = (
        text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&apos;", "'")
        .replace("&#13;", "")
        .replace("&#10;", "\n")
    )
    # Drop any other numeric/named entities we didn't explicitly map.
    text = _HTML_ENTITY.sub("", text)
    # Collapse runs of whitespace (but keep newlines).
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _existing_upstream_hash(output_csv: Path) -> Optional[str]:
    """Read the upstream hash stored alongside the first row, if any."""
    if not output_csv.exists():
        return None
    with open(output_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            return row.get("upstream_hash") or None
    return None


def _stream_upstream_rows(csv_url: str) -> Iterable[dict]:
    """Stream rows from the upstream CSV without buffering the whole file.

    The gov_decisions CSV is ~100MB. Streaming keeps peak memory bounded
    and makes the EmptyUpstreamIndex check fast on a header-only response.
    """
    with requests.get(csv_url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        # iter_lines(decode_unicode=True) is finicky across requests versions;
        # decode explicitly so we always feed csv.DictReader str lines.
        line_iter = (line.decode("utf-8", errors="replace") for line in resp.iter_lines())
        reader = csv.DictReader(line_iter)
        for row in reader:
            yield row


def _select_recent(rows: Iterable[dict], date_field: str, max_rows: int) -> List[dict]:
    """Keep the most-recent ``max_rows`` rows by ``date_field``.

    ``rows`` is fully consumed (we need to sort the whole stream to pick
    the top-N by date). That's fine at 35K rows × ~3KB each ≈ 100MB
    in memory — same order as the upstream file size — and the slice we
    keep is bounded by ``max_rows``.
    """
    materialized = list(rows)
    materialized.sort(key=lambda r: r.get(date_field) or "", reverse=True)
    return materialized[:max_rows]


def process_bk_csv_source(
    *,
    external_source_url: str,
    output_csv_path: Path,
    csv_filename: str = "data/government_decisions.csv",
    max_rows: int = 2000,
    date_field: str = "publish_date",
    keep_columns: Optional[List[str]] = None,
    text_columns: Optional[List[str]] = None,
    filter_column: Optional[str] = None,
    filter_values: Optional[List[str]] = None,
):
    """Download a BudgetKey single-CSV datapackage and write a normalized
    extraction CSV.

    Parameters
    ----------
    external_source_url:
        Datapackage root, e.g.
        ``https://next.obudget.org/datapackages/government_decisions``.
        The fetcher fetches ``{root}/datapackage.json`` for the hash and
        ``{root}/{csv_filename}`` for the rows.
    output_csv_path:
        Where to write the normalized CSV. Atomically replaced on success.
    csv_filename:
        Path within the datapackage to the CSV resource. Defaults to the
        government_decisions layout.
    max_rows:
        Cap on the number of rows kept (most recent by ``date_field``).
    date_field:
        Column name to sort by for the recency filter.
    keep_columns:
        Columns to copy verbatim. Anything in ``text_columns`` is also
        kept. Defaults to the gov-decisions schema.
    text_columns:
        Columns to HTML-strip. Defaults to ``['text']``.
    filter_column / filter_values:
        Optional row filter — only keep rows where
        ``row[filter_column] in filter_values``. The gov_decisions feed
        mixes ~10 document types; using
        ``filter_column='policy_type'`` and
        ``filter_values=['החלטות ממשלה']`` narrows to actual cabinet
        decisions (which is the only type with reliable body text).
    """
    output_csv = Path(output_csv_path)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    if keep_columns is None:
        keep_columns = [
            "title",
            "publish_date",
            "office",
            "government",
            "policy_type",
            "procedure_number_str",
            "url_id",
        ]
    if text_columns is None:
        text_columns = ["text"]

    dp_url = f"{external_source_url.rstrip('/')}/datapackage.json"
    csv_url = f"{external_source_url.rstrip('/')}/{csv_filename.lstrip('/')}"

    # 1. Hash short-circuit.
    upstream_hash: Optional[str] = None
    try:
        dp = requests.get(dp_url, timeout=60).json()
        for r in dp.get("resources", []):
            if r.get("path") == csv_filename:
                upstream_hash = r.get("hash")
                break
    except Exception as e:
        logger.warning(f"Could not fetch datapackage.json from {dp_url}: {e}")

    stored_hash = _existing_upstream_hash(output_csv)
    if (
        upstream_hash is not None
        and stored_hash is not None
        and upstream_hash == stored_hash
    ):
        logger.info(
            f"{external_source_url}: upstream hash {upstream_hash} unchanged; "
            f"leaving {output_csv} as-is"
        )
        return

    # 2. Stream + filter.
    logger.info(f"Streaming {csv_url} (max_rows={max_rows})...")
    all_rows = list(_stream_upstream_rows(csv_url))
    if len(all_rows) == 0:
        raise EmptyUpstreamIndex(
            f"{csv_url}: upstream CSV is empty - refusing to overwrite {output_csv}"
        )
    if filter_column and filter_values:
        before = len(all_rows)
        allowed = set(filter_values)
        all_rows = [r for r in all_rows if r.get(filter_column) in allowed]
        logger.info(
            f"Filtered {before} -> {len(all_rows)} rows by {filter_column} in {filter_values}"
        )
        if len(all_rows) == 0:
            raise EmptyUpstreamIndex(
                f"{csv_url}: filter {filter_column} in {filter_values} matched zero rows "
                f"- refusing to overwrite {output_csv}"
            )
    logger.info(
        f"Got {len(all_rows)} upstream rows; selecting most-recent {max_rows} by {date_field}"
    )
    rows = _select_recent(all_rows, date_field, max_rows)

    # 3. Project + normalize.
    out_rows = []
    fieldnames = ["upstream_hash", *keep_columns, *text_columns]
    for r in rows:
        out = {"upstream_hash": upstream_hash or ""}
        for col in keep_columns:
            out[col] = r.get(col, "") or ""
        for col in text_columns:
            out[col] = _html_to_text(r.get(col, "") or "")
        out_rows.append(out)

    # 4. Atomic write.
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

    logger.info(f"Wrote {len(out_rows)} rows to {output_csv}")
