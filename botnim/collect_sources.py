import asyncio
import io
import csv
from pathlib import Path
from typing import Union
import hashlib
import dataflows as DF
from kvfile.kvfile_sqlite import CachedKVFileSQLite as KVFile
import json

from .config import get_logger
from .dynamic_extraction import extract_structured_content, extract_structured_content_async
from .document_parser.wikitext.generate_markdown_files import generate_markdown_dict
from .document_parser.wikitext.pipeline_config import sanitize_filename
from ._concurrency import SyncConcurrency, get_sync_concurrency, run_async


logger = get_logger(__name__)
cache: KVFile = None

def _cache_key(content: str) -> str:
    return hashlib.sha256(content.strip().encode('utf-8')).hexdigest()[:16]


def _cached_metadata_for_content(content: str) -> dict | None:
    """Return the cached metadata dict if the cached content exactly matches.

    Reads are safe without a lock (sqlite handles concurrent readers), so the
    concurrent sync pipeline can call this BEFORE acquiring the semaphore —
    satisfying DoD "cache check happens BEFORE the semaphore acquire so cache
    hits do not consume slots".
    """
    key = _cache_key(content)
    item = cache.get(key, default=None)
    if item and item.get('content') == content:
        logger.info(f'Cache hit for {key}, cached content: {item.get("content")[:100]!r}')
        return item['metadata']
    return None


def _build_metadata_record(content: str, file_path: str, document_type: str, extracted_data: dict | None, error: Exception | None) -> dict:
    metadata = {'title': file_path, 'status': 'processed'}
    if error is not None:
        metadata['status'] = 'extraction_error'
        metadata['error'] = str(error)
        return metadata
    if extracted_data is None:
        return metadata
    metadata['document_type'] = document_type
    if extracted_data.get('DocumentMetadata', {}).get('DocumentTitle'):
        metadata['title'] = extracted_data['DocumentMetadata']['DocumentTitle']
    metadata.update(extracted_data)
    logger.info(f"Added enhanced metadata for {file_path}")
    return metadata


def get_metadata_for_content(content: str, file_path: str, document_type: str) -> dict:
    """Synchronous extraction — preserved for non-async callers.

    Concurrent callers should use ``get_metadata_for_content_async``.
    """
    cached = _cached_metadata_for_content(content)
    if cached is not None:
        return cached

    try:
        logger.info(f"Extracting structured content for {file_path} with document type: {document_type}")
        extracted_data = extract_structured_content(content, document_type=document_type)
        metadata = _build_metadata_record(content, file_path, document_type, extracted_data, None)
    except Exception as e:
        logger.error(f"Error extracting structured content from {file_path}")
        metadata = _build_metadata_record(content, file_path, document_type, None, e)

    cache.set(_cache_key(content), {'content': content, 'metadata': metadata})
    return metadata


async def _get_metadata_for_content_async(
    content: str,
    file_path: str,
    document_type: str,
    concurrency: SyncConcurrency,
    client=None,
) -> dict:
    """Concurrent extraction.

    Order of operations is load-bearing:
      1. Cache lookup WITHOUT holding the semaphore. Cache hits skip the
         pool entirely so they don't crowd out genuine OpenAI work (DoD).
      2. Semaphore-bounded OpenAI call.
      3. Cache write under the async lock so the sqlite KVFile never sees
         concurrent writers.
    """
    cached = _cached_metadata_for_content(content)
    if cached is not None:
        return cached

    try:
        extracted_data = await concurrency.run_bounded(
            extract_structured_content_async,
            content,
            document_type=document_type,
            client=client,
        )
        metadata = _build_metadata_record(content, file_path, document_type, extracted_data, None)
    except Exception as e:
        logger.error(f"Error extracting structured content from {file_path}: {e}")
        metadata = _build_metadata_record(content, file_path, document_type, None, e)

    async with concurrency.cache_lock:
        cache.set(_cache_key(content), {'content': content, 'metadata': metadata})
    return metadata

def _prepare_file_content(filename: str, content: Union[str, io.BufferedReader], content_type: str) -> tuple[str, str, str]:
    """Normalize the raw content to (filename, utf-8 text, content_type).

    Used by the async path so we can do the I/O sync but defer the OpenAI
    extraction to the bounded pool.
    """
    if not isinstance(content, str):
        content = content.read().decode('utf-8')
    return filename, content.strip(), content_type


async def _process_file_stream_async(
    filename: str,
    content: Union[str, io.BufferedReader],
    content_type: str,
    concurrency: SyncConcurrency,
    client,
):
    fname, text, ctype = _prepare_file_content(filename, content, content_type)
    metadata = await _get_metadata_for_content_async(text, fname, ctype, concurrency, client=client)
    return (fname, io.BytesIO(text.encode('utf-8')), ctype, metadata)

def _collect_raw_streams_files(config_dir: Path, source):
    """Return [(filename, content, content_type)] without the OpenAI step."""
    files = list(config_dir.glob(source))
    return [(f.name, f.open('rb'), 'text/markdown') for f in files]


def _collect_raw_streams_split(config_dir: Path, context_name, source, offset=0):
    filename = config_dir / source
    if filename.suffix == '.json':
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        document_name = data.get('metadata', {}).get('document_name', '')
        if not document_name:
            input_file = data.get('metadata', {}).get('input_file', '')
            document_name = Path(input_file).stem
        document_name = sanitize_filename(document_name)
        structure = data.get('structure', [])
        markdown_dict = generate_markdown_dict(structure, document_name)
        return [(fname, content, 'text/markdown') for fname, content in markdown_dict.items()]
    else:
        content = filename.read_text()
        parts = content.split('\n---\n')
        return [(f'{context_name}_{i+offset}.md', c, 'text/markdown') for i, c in enumerate(parts)]


def _collect_raw_streams_google_spreadsheet(context_name, source, offset=0):
    resources, dp, _ = DF.Flow(
        DF.load(source, name='rows'),
    ).results()
    rows = resources[0]
    headers = [f.name for f in dp.resources[0].schema.fields]
    raw = []
    for idx, row in enumerate(rows):
        content = ''
        if len(headers) > 1:
            for i, header in enumerate(headers):
                if row.get(header):
                    if i > 0:
                        content += f'{header}:\n{row[header]}\n\n'
                    else:
                        content += f'{row[header]}\n\n'
        if content:
            raw.append((f'{context_name}_{idx+offset}.md', content, 'text/markdown'))
    return raw


def _collect_raw_streams_csv(config_dir, context_name, source, offset=0):
    with open(config_dir / source, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        raw = []
        for idx, row in enumerate(reader):
            content = ''
            for k, v in row.items():
                content += f'{k}:\n{v}\n\n'
            raw.append((f'{context_name}_{idx+offset}.md', content, 'text/markdown'))
        return raw

def _raw_streams_for_context(config_dir, context_name, context_, offset=0):
    """Gather raw (filename, content, content_type) without calling OpenAI.

    Mirrors ``file_streams_for_context`` but defers the extraction step.
    """
    context_type = context_['type']
    source = context_['source']
    if context_type == 'files':
        return _collect_raw_streams_files(config_dir, source)
    if context_type == 'split':
        return _collect_raw_streams_split(config_dir, context_name, source, offset=offset)
    if context_type == 'google-spreadsheet':
        return _collect_raw_streams_google_spreadsheet(context_name, source, offset=offset)
    if context_type == 'csv':
        return _collect_raw_streams_csv(config_dir, context_name, source, offset=offset)
    raise ValueError(f'Unknown context type: {context_type}')


async def collect_context_sources_async(
    context_,
    config_dir: Path,
    concurrency: SyncConcurrency,
    client=None,
):
    """Async variant of ``collect_context_sources``.

    Extraction for the N source files happens concurrently under the
    bounded semaphore. Cache hits skip the semaphore entirely so a warm
    sync never blocks on API slots.
    """
    global cache
    cache = KVFile(location=str(Path(__file__).parent.parent / 'cache' / 'metadata'))

    context_name = context_['name']
    raw: list[tuple[str, object, str]] = []
    if 'sources' in context_:
        for source in context_['sources']:
            raw.extend(_raw_streams_for_context(config_dir, context_name, source, offset=len(raw)))
    else:
        raw.extend(_raw_streams_for_context(config_dir, context_name, context_))

    # asyncio.gather preserves input order in its output list — this is
    # what keeps SYNC_CONCURRENCY=1 byte-equal to the serial implementation.
    tasks = [
        _process_file_stream_async(fn, content, ct, concurrency, client)
        for fn, content, ct in raw
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    file_streams: list = []
    for r, (fn, _, _) in zip(results, raw):
        if isinstance(r, BaseException):
            # Error isolation: one failed document must not poison the batch.
            logger.error(f"Extraction failed for {fn}: {r}")
            continue
        file_streams.append(r)

    cache.close()
    return file_streams


def collect_context_sources(context_, config_dir: Path):
    """Synchronous entry point — now fans out to the async pipeline under a
    bounded ``SYNC_CONCURRENCY`` pool.

    Kept sync-signature-compatible so existing callers (including
    ``VectorStoreBase.vector_store_update`` and ad-hoc scripts) don't need
    to change. With SYNC_CONCURRENCY=1 the async path is fully serial and
    produces byte-equal output.
    """
    concurrency = SyncConcurrency()
    return run_async(collect_context_sources_async(context_, config_dir, concurrency))

def collect_all_sources(context_list, config_dir):
    all_sources = []
    for context in context_list:
        all_sources.append(dict(
            **context,
            file_streams=collect_context_sources(context, config_dir)
        ))
    return all_sources
