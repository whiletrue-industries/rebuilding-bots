import os
import json
import requests
from io import StringIO
import csv
from pathlib import Path
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from ...config import get_openai_client, get_logger
from ...storage.base import ArtifactStore
from ...storage.csv_writer import write_csv_artifact

from .pdf_extraction_config import SourceConfig
from .config import REVISION
from .pdf_processor import process_single_pdf
from .exceptions import EmptyUpstreamIndex

logger = get_logger(__name__)

# Per-PDF work is dominated by Tesseract OCR (releases the GIL) and one
# OpenAI field-extraction call (network I/O), so threads parallelize cleanly.
# Override at runtime with PDF_PROCESSING_WORKERS=N when re-extracting a
# large context (committee_decisions, knesset_protocols, ...).
PDF_PROCESSING_WORKERS = int(os.environ.get('PDF_PROCESSING_WORKERS', '8'))


def _process_one_pdf(row, external_source, config, openai_client, upstream_revision):
    url = row['url']
    if external_source is not None:
        pdf_url = f'{external_source}/{row["filename"]}'
    else:
        pdf_url = row['url']
    out_rows: list[dict] = []
    with tempfile.NamedTemporaryFile(suffix='.pdf') as tmp_file:
        try:
            logger.info(f'Processing PDF: {pdf_url}')
            resp = requests.get(pdf_url)
            resp.raise_for_status()
            tmp_file.write(resp.content)
            tmp_file.flush()
            records = process_single_pdf(Path(tmp_file.name), config, openai_client)
            for record in records:
                out_rows.append({
                    'url': url,
                    'revision': REVISION,
                    'upstream_revision': upstream_revision or '',
                    **record,
                })
        except Exception as e:
            print(f"Error processing {pdf_url}: {e}")
    return out_rows


def _existing_upstream_revision(store: ArtifactStore, key: str) -> str | None:
    """Read the upstream revision stored alongside the first row, if any.

    Returns None if the object is missing, empty, or doesn't have the column.
    """
    if not store.exists(key):
        return None
    text = store.get_bytes(key).decode("utf-8")
    reader = csv.DictReader(StringIO(text))
    for row in reader:
        return row.get('upstream_revision') or None
    return None


def process_pdf_source(config: SourceConfig, *, store: ArtifactStore, key: str, index_key: str | None = None):
    openai_client = get_openai_client()

    # NEW: local-index branch — index.csv already on disk (Stage 1 wrote it).
    # The two-stage source pattern: a separate fetcher (e.g. knesset_apps,
    # knesset_sharepoint) writes a BK-shape index.csv to disk; here we just
    # consume it. row['url'] is the absolute PDF URL, so we don't need to
    # build it from external_source/filename.
    #
    # GAP A fix: Stage 1 now writes the index to the STORE at index_key
    # (key_for_extraction(bot, raw_idx)).  If the local file is absent (S3
    # backend in ECS, or a fresh dev checkout) we fall back to the store.
    # If neither exists we raise EmptyUpstreamIndex as before.
    if config.local_index_csv_path is not None:
        from ..knesset_apps.common import EmptyUpstreamIndex as KnessetEmptyUpstreamIndex
        index_path = Path(config.local_index_csv_path)
        if index_path.exists():
            # Local file present — read directly (dev / committed-index path).
            with open(index_path, "r", encoding="utf-8") as f:
                input_records = list(csv.DictReader(f))
        elif index_key is not None and store.exists(index_key):
            # Store-resident index written by Stage 1 (S3 backend in ECS).
            logger.info(
                "local index %s absent; reading from store key %s",
                index_path, index_key,
            )
            raw = store.get_bytes(index_key).decode("utf-8")
            input_records = list(csv.DictReader(raw.splitlines()))
        else:
            raise KnessetEmptyUpstreamIndex(
                f"local index {index_path} does not exist and store key "
                f"{index_key!r} is also absent — Stage 1 has not run yet; "
                f"refusing to overwrite {key}"
            )
        if len(input_records) == 0:
            if store.exists(key):
                raise KnessetEmptyUpstreamIndex(
                    f"local index {index_path} is empty — refusing to "
                    f"overwrite {key}"
                )
            # First-run on a fresh setup with empty index — write empty out, exit.
            store.put_atomic(key, b"url,revision,upstream_revision\n")
            return
        upstream_revision: str | None = ""
        external_source = None  # NOT used for pdf URL in this branch
    else:
        # EXISTING: BK external source branch.
        external_source = config.external_source_url

        # Revision short-circuit: if the upstream datapackage revision matches what
        # we already have, skip the rest. The per-row (url, revision) cache would
        # catch this too, but this is cheaper (one HTTP round trip, no OpenAI calls
        # at all).
        upstream_revision = None
        try:
            dp_resp = requests.get(f'{external_source}/datapackage.json')
            dp_resp.raise_for_status()
            upstream_revision = json.loads(dp_resp.text).get('revision')
        except Exception as e:
            logger.warning(f'Could not fetch datapackage.json for {external_source}: {e}')

        stored_revision = _existing_upstream_revision(store, key)
        if (
            upstream_revision is not None
            and stored_revision is not None
            and upstream_revision == stored_revision
        ):
            logger.info(
                f'{external_source}: upstream revision {upstream_revision} unchanged; '
                f'leaving {key} as-is'
            )
            return

        input_csv = requests.get(f'{external_source}/index.csv').text
        input_csv = StringIO(input_csv)
        input_csv = csv.DictReader(input_csv)
        input_records = list(input_csv)

        if len(input_records) == 0:
            raise EmptyUpstreamIndex(
                f"{external_source}: upstream index.csv is empty — refusing to "
                f"overwrite {key}"
            )

    existing_urls = dict()
    if store.exists(key):
        existing_csv = csv.DictReader(StringIO(store.get_bytes(key).decode("utf-8")))
        for row in existing_csv:
            existing_urls[(row['url'], row['revision'])] = row

    out = []
    to_process = []
    for row in input_records:
        url = row['url']
        if (url, REVISION) in existing_urls:
            out.append(existing_urls[(url, REVISION)])
            logger.info(f'Skipping existing URL: {url}')
            continue
        to_process.append(row)

    if to_process:
        logger.info(
            f'Processing {len(to_process)} PDFs with {PDF_PROCESSING_WORKERS} '
            f'workers (skipped {len(out)} already-cached rows)'
        )
        with ThreadPoolExecutor(max_workers=PDF_PROCESSING_WORKERS) as ex:
            futures = [
                ex.submit(_process_one_pdf, row, external_source, config, openai_client, upstream_revision)
                for row in to_process
            ]
            for fut in as_completed(futures):
                out.extend(fut.result())

    # Atomic write through the store: base fieldnames + dynamic union (was
    # process_pdfs.py:181-185).
    write_csv_artifact(
        store,
        key,
        out,
        fieldnames=['url', 'revision', 'upstream_revision'],
        extend_fieldnames=True,
    )
