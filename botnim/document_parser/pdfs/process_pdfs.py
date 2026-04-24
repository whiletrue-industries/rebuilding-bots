import os
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

def process_pdf_source(config: SourceConfig):
    openai_client = get_openai_client()

    external_source = config.external_source_url
    output_csv = Path(config.output_csv_path)

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
                        **record
                    })
            except Exception as e:
                print(f"Error processing {pdf_url}: {e}")

    # Write the output CSV atomically: write to a sibling .tmp, then os.replace.
    # Kernel guarantees the rename is atomic on the same filesystem (including
    # EFS) so readers never see a half-written file, and a mid-loop crash
    # leaves the previous CSV untouched.
    tmp_output = output_csv.with_suffix(output_csv.suffix + '.tmp')
    try:
        with open(tmp_output, 'w', newline='') as csv_file:
            fieldnames = ['url', 'revision']
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
        # Clean up the partial .tmp and re-raise so the caller (refresh job) sees the failure
        try:
            tmp_output.unlink()
        except FileNotFoundError:
            pass
        raise