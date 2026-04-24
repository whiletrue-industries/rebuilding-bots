import os
import json
import requests
from io import StringIO
import csv
from pathlib import Path
import tempfile

from ...config import get_openai_client, get_logger

from .pdf_extraction_config import SourceConfig
from .config import REVISION
from .pdf_processor import process_single_pdf
from .exceptions import EmptyUpstreamIndex

logger = get_logger(__name__)


def _existing_upstream_revision(output_csv: Path) -> str | None:
    """Read the upstream revision stored alongside the first row, if any.

    Returns None if the file is missing, empty, or doesn't have the column.
    """
    if not output_csv.exists():
        return None
    with open(output_csv, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            return row.get('upstream_revision') or None
    return None


def process_pdf_source(config: SourceConfig):
    openai_client = get_openai_client()

    external_source = config.external_source_url
    output_csv = Path(config.output_csv_path)

    # Revision short-circuit: if the upstream datapackage revision matches what
    # we already have, skip the rest. The per-row (url, revision) cache would
    # catch this too, but this is cheaper (one HTTP round trip, no OpenAI calls
    # at all).
    upstream_revision: str | None = None
    try:
        dp_resp = requests.get(f'{external_source}/datapackage.json')
        dp_resp.raise_for_status()
        upstream_revision = json.loads(dp_resp.text).get('revision')
    except Exception as e:
        logger.warning(f'Could not fetch datapackage.json for {external_source}: {e}')

    stored_revision = _existing_upstream_revision(output_csv)
    if (
        upstream_revision is not None
        and stored_revision is not None
        and upstream_revision == stored_revision
    ):
        logger.info(
            f'{external_source}: upstream revision {upstream_revision} unchanged; '
            f'leaving {output_csv} as-is'
        )
        return

    input_csv = requests.get(f'{external_source}/index.csv').text
    input_csv = StringIO(input_csv)
    input_csv = csv.DictReader(input_csv)
    input_records = list(input_csv)

    if len(input_records) == 0:
        raise EmptyUpstreamIndex(
            f"{external_source}: upstream index.csv is empty — refusing to "
            f"overwrite {output_csv}"
        )

    existing_urls = dict()
    if output_csv.exists():
        with open(output_csv, 'r') as csv_file:
            existing_csv = csv.DictReader(csv_file)
            for row in existing_csv:
                existing_urls[(row['url'], row['revision'])] = row

    out = []
    for row in input_records:
        url = row['url']
        pdf_url = f'{external_source}/{row["filename"]}'
        if (url, REVISION) in existing_urls:
            out.append(existing_urls[(url, REVISION)])
            logger.info(f'Skipping existing URL: {url}')
            continue
        with tempfile.NamedTemporaryFile(suffix='.pdf') as tmp_file:
            try:
                logger.info(f'Processing PDF: {pdf_url}')
                resp = requests.get(pdf_url)
                resp.raise_for_status()
                tmp_file.write(resp.content)
                tmp_file.flush()
                records = process_single_pdf(Path(tmp_file.name), config, openai_client)
                for record in records:
                    out.append({
                        'url': url,
                        'revision': REVISION,
                        'upstream_revision': upstream_revision or '',
                        **record
                    })
            except Exception as e:
                print(f"Error processing {pdf_url}: {e}")

    # Write the output CSV atomically: write to a sibling .tmp, then os.replace.
    tmp_output = output_csv.with_suffix(output_csv.suffix + '.tmp')
    try:
        with open(tmp_output, 'w', newline='') as csv_file:
            fieldnames = ['url', 'revision', 'upstream_revision']
            for r in out:
                for k in r.keys():
                    if k not in fieldnames:
                        fieldnames.append(k)
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in out:
                writer.writerow(row)
        os.replace(tmp_output, output_csv)
    except Exception:
        try:
            tmp_output.unlink()
        except FileNotFoundError:
            pass
        raise
