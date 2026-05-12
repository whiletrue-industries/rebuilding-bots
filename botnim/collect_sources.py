import asyncio
import io
import csv
import re
from pathlib import Path
from typing import Union
import hashlib
import dataflows as DF
from kvfile.kvfile_sqlite import CachedKVFileSQLite as KVFile
import json

# Bump CSV field-size limit globally for this module. Some BudgetKey-sourced
# extraction CSVs (notably government_decisions, where the `text` column is
# the full HTML-stripped decision body) carry rows whose largest field
# exceeds Python's default 131072-byte cap. Without this, _collect_raw_streams_csv
# raises `_csv.Error: field larger than field limit (131072)` at row N and
# the whole sync aborts mid-loop. 10 MB ceiling is well above any single
# decision body we've seen in practice (~500 KB max).
csv.field_size_limit(10 * 1024 * 1024)

from .config import get_logger
from .dynamic_extraction import extract_structured_content, extract_structured_content_async
from .document_parser.wikitext.generate_markdown_files import generate_markdown_dict
from .document_parser.wikitext.pipeline_config import sanitize_filename
from ._concurrency import SyncConcurrency, get_sync_concurrency, run_async


logger = get_logger(__name__)
cache: KVFile = None


def _open_metadata_cache() -> KVFile:
    """Open the per-process L1 extraction-result cache.

    Lives at ``<repo_root>/cache/metadata.sqlite``. The parent dir
    ``<repo_root>/cache/`` is committed in the repo for dev/CI but is
    NOT copied into the prod docker image (no ``COPY cache/`` in
    ``backend/api/Dockerfile``), so a fresh ECS task has no
    ``/srv/cache/``. Without this mkdir, ``KVFile(...)`` →
    ``sqlite3.connect(...)`` raises ``OperationalError: unable to open
    database file`` and the sync aborts mid-run — and if the
    ``--force-rebuild`` wipe already ran, the context is left empty in
    Aurora until a successful re-sync.
    """
    location = Path(__file__).parent.parent / 'cache' / 'metadata'
    location.parent.mkdir(parents=True, exist_ok=True)
    return KVFile(location=str(location))


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


# CSV-row contexts whose fap step fans one source document out into many
# rows (knesset_protocols → speaker turns, plenary_schedule → (session,
# item) pairs) flatten the row into the per-chunk markdown content as
# ``key:\nvalue\n\n`` blocks. Lift the *source-doc* identifier out of that
# content into a dedicated metadata field so `/admin/sources` can count
# distinct source documents (`100 protocols`) instead of distinct chunks
# (`27K speaker turns`). The key list is ordered by specificity — `file_url`
# is the most stable per-source identifier when available, falling back to
# OData entity IDs for contexts that have no upstream URL. Returns None for
# contexts where no candidate key appears (most legal/lexicon corpora — each
# row IS the source doc; `title` is already the right count there).
_SOURCE_DOC_CANDIDATE_KEYS = ("file_url", "url", "document_id", "session_id")
_SOURCE_DOC_LINE_RE = re.compile(
    r"(?m)^(" + "|".join(_SOURCE_DOC_CANDIDATE_KEYS) + r"):\n([^\n]+)"
)


_SOURCE_URL_CANDIDATE_KEYS = ("source_url", "file_url", "url")
_SOURCE_URL_LINE_RE = re.compile(
    r"(?m)^(?:" + "|".join(_SOURCE_URL_CANDIDATE_KEYS) + r"):\n(https?://[^\n]+)"
)


def _extract_source_url(content: str) -> str | None:
    """Return the first https?:// URL found under a URL-typed column, or None."""
    m = _SOURCE_URL_LINE_RE.search(content)
    return m.group(1).strip() if m else None


def _extract_source_doc(content: str) -> str | None:
    """Return the first non-empty candidate-key value from CSV-flattened content."""
    for m in _SOURCE_DOC_LINE_RE.finditer(content):
        v = m.group(2).strip()
        if v:
            return v
    return None


def _build_metadata_record(content: str, file_path: str, document_type: str, extracted_data: dict | None, error: Exception | None) -> dict:
    metadata = {'title': file_path, 'status': 'processed'}
    source_doc = _extract_source_doc(content)
    if source_doc:
        metadata['source_doc'] = source_doc
    source_url = _extract_source_url(content)
    if source_url:
        metadata['source_url'] = source_url
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
    *,
    bot: str | None = None,
    context_name: str | None = None,
    extraction_cache=None,
    client=None,
) -> dict:
    """Concurrent extraction.

    Order of operations is load-bearing:
      1. L1 cache lookup (per-process KVFile). Cache hits skip the pool
         entirely so they don't crowd out genuine OpenAI work (DoD).
      2. L2 cache lookup (Aurora extraction_cache). Hits warm L1 and skip
         the pool too.
      3. Semaphore-bounded OpenAI call. RpdExhausted trips the shared
         flag (so siblings short-circuit) before re-raising.
      4. L2 + L1 cache write — L2 best-effort (failures log + continue),
         L1 always written under the async lock.
    """
    from .dynamic_extraction import (
        EXTRACTION_VERSION, RpdExhausted, extract_structured_content_async,
    )

    content_hash = hashlib.sha256(content.strip().encode("utf-8")).hexdigest()

    # L1: per-process KVFile (legacy fast path).
    cached_local = _cached_metadata_for_content(content)
    if cached_local is not None:
        return cached_local

    # L2: Aurora cache.
    if extraction_cache is not None:
        try:
            cached_aurora = extraction_cache.get(content_hash, EXTRACTION_VERSION)
        except Exception as e:
            logger.warning("extraction_cache.get failed for %s: %s", file_path, e)
            cached_aurora = None
        if cached_aurora is not None:
            async with concurrency.cache_lock:
                cache.set(_cache_key(content), {"content": content, "metadata": cached_aurora})
            return cached_aurora

    # Miss. Bounded LLM call.
    try:
        extracted_data = await concurrency.run_bounded(
            extract_structured_content_async,
            content,
            document_type=document_type,
            client=client,
        )
        metadata = _build_metadata_record(content, file_path, document_type, extracted_data, None)
    except RpdExhausted:
        # Set the trip flag so other in-flight tasks short-circuit.
        concurrency.rpd_tripped.set()
        raise
    except Exception as e:
        logger.error(f"Error extracting structured content from {file_path}: {e}")
        metadata = _build_metadata_record(content, file_path, document_type, None, e)
        # Don't cache errors; next run retries.
        return metadata

    # Persist to L2 (Aurora). Failures here log + continue (L1 still gets it).
    if extraction_cache is not None and bot and context_name:
        try:
            extraction_cache.put(
                content_hash, EXTRACTION_VERSION,
                payload=metadata, bot=bot, context=context_name,
                document_type=document_type,
            )
        except Exception as e:
            logger.warning("extraction_cache.put failed for %s: %s", file_path, e)

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
    source_id: str,
    concurrency: SyncConcurrency,
    client,
    *,
    bot: str | None = None,
    context_name: str | None = None,
    extraction_cache=None,
):
    fname, text, ctype = _prepare_file_content(filename, content, content_type)
    metadata = await _get_metadata_for_content_async(
        text, fname, ctype, concurrency,
        bot=bot, context_name=context_name, extraction_cache=extraction_cache,
        client=client,
    )
    metadata = dict(metadata or {})
    metadata['source_id'] = source_id
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
    """Gather raw (filename, content, content_type, source_id) tuples without calling OpenAI.

    Each tuple now carries the derived `source_id` so per-source attribution
    survives through the async extraction pipeline into upload_files.
    """
    from .sync import _source_id_for  # local import avoids circular at module load
    context_type = context_['type']
    source = context_['source']
    fetcher = context_.get('fetcher')
    source_id = _source_id_for(fetcher, source)

    if context_type == 'files':
        raw = _collect_raw_streams_files(config_dir, source)
    elif context_type == 'split':
        raw = _collect_raw_streams_split(config_dir, context_name, source, offset=offset)
    elif context_type == 'google-spreadsheet':
        raw = _collect_raw_streams_google_spreadsheet(context_name, source, offset=offset)
    elif context_type == 'csv':
        raw = _collect_raw_streams_csv(config_dir, context_name, source, offset=offset)
    else:
        raise ValueError(f'Unknown context type: {context_type}')

    return [(fname, content, ctype, source_id) for fname, content, ctype in raw]


async def collect_context_sources_async(
    context_,
    config_dir: Path,
    concurrency: SyncConcurrency,
    *,
    bot: str | None = None,
    extraction_cache=None,
    client=None,
):
    """Async variant of ``collect_context_sources``.

    Extraction for the N source files happens concurrently under the
    bounded semaphore. Cache hits skip the semaphore entirely so a warm
    sync never blocks on API slots.

    When ``extraction_cache`` is provided, the Aurora-backed L2 cache is
    consulted before each LLM call and populated on miss. RPD short-
    circuits via ``RpdExhausted`` — exhausted siblings are dropped, but
    successful + cached extractions are still returned so the run can
    embed partial progress.
    """
    from .dynamic_extraction import RpdExhausted

    global cache
    cache = _open_metadata_cache()

    context_name = context_['name']
    raw: list[tuple[str, object, str, str]] = []
    if 'sources' in context_:
        for source in context_['sources']:
            raw.extend(_raw_streams_for_context(config_dir, context_name, source, offset=len(raw)))
    elif 'type' in context_ and 'source' in context_:
        raw.extend(_raw_streams_for_context(config_dir, context_name, context_))
    else:
        # Context with neither `sources` nor a single inline source — used by
        # direct-Aurora fetchers (e.g. gov_il_decisions) that bypass the
        # extraction/<x>.csv pipeline entirely. Sync becomes a no-op for the
        # data side; the context row is still upserted by
        # get_or_create_vector_store so /admin/sources still sees it.
        logger.info(
            "Context %s has no sources to collect (direct-Aurora fetcher).",
            context_name,
        )

    # asyncio.gather preserves input order in its output list — this is
    # what keeps SYNC_CONCURRENCY=1 byte-equal to the serial implementation.
    tasks = [
        _process_file_stream_async(
            fn, content, ct, sid, concurrency, client,
            bot=bot, context_name=context_name, extraction_cache=extraction_cache,
        )
        for fn, content, ct, sid in raw
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    file_streams: list = []
    rpd_count = 0
    for r, (fn, _, _, _) in zip(results, raw):
        if isinstance(r, RpdExhausted):
            rpd_count += 1
            continue
        if isinstance(r, BaseException):
            # Error isolation: one failed document must not poison the batch.
            logger.error(f"Extraction failed for {fn}: {r}")
            continue
        file_streams.append(r)

    if rpd_count > 0:
        logger.warning(
            "EXTRACTION RPD HIT: %d/%d files left un-extracted in context %s. "
            "%d files were extracted (cache+fresh) and will be embedded this run. "
            "RESUME: re-run `botnim sync <env> <bot>` after the daily limit "
            "resets (midnight UTC). Cached extractions persist in Aurora; the "
            "next run will only call gpt-4o-mini for the remaining %d files.",
            rpd_count, len(tasks), context_name, len(file_streams), rpd_count,
        )

    cache.close()
    return file_streams


def collect_context_sources(context_, config_dir: Path, *, bot=None, extraction_cache=None):
    """Synchronous entry point — now fans out to the async pipeline under a
    bounded ``SYNC_CONCURRENCY`` pool.

    Kept sync-signature-compatible so existing callers (including
    ``VectorStoreBase.vector_store_update`` and ad-hoc scripts) don't need
    to change. With SYNC_CONCURRENCY=1 the async path is fully serial and
    produces byte-equal output.

    The new ``bot`` / ``extraction_cache`` kwargs default to ``None`` so
    legacy callers stay on the cache-free path; only the post-2026-04-26
    aurora sync wires them through.
    """
    concurrency = SyncConcurrency()
    return run_async(collect_context_sources_async(
        context_, config_dir, concurrency,
        bot=bot, extraction_cache=extraction_cache,
    ))

def collect_all_sources(context_list, config_dir):
    all_sources = []
    for context in context_list:
        all_sources.append(dict(
            **context,
            file_streams=collect_context_sources(context, config_dir)
        ))
    return all_sources
