"""Direct-to-Aurora writer shared by the gov_il_decisions fetcher and
the one-time bootstrap script.

Bypasses the usual extraction/<context>.csv → ``botnim sync`` pipeline
because the gov.il decisions corpus is too large (26K+ rows) to commit
as CSV and the LLM-derived categorization makes the cache regeneration
too expensive to redo at sync time. See
``botnim/document_parser/gov_il_decisions/process.py`` and
CLAUDE.md ("The architectural decision") for the rationale.

The functions here mirror the INSERT pattern used by
``VectorStoreAurora.upload_files`` so that ``/admin/sources`` and the
existing retrieval path keep working without modification.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime

from openai import OpenAI
from sqlalchemy import text as sql_text

from ...config import DEFAULT_EMBEDDING_MODEL, get_logger
from ...db.session import get_session
from ...vector_store.vector_store_aurora import (
    _chunk_for_embedding,
    _get_embedding_client,
)

logger = get_logger(__name__)

SOURCE_ID = "gov_il_decisions"

# OpenAI's embeddings endpoint accepts up to 2048 inputs per call. Default
# bootstrap batch size is well under that — leaves headroom for chunking
# expanding the count above the per-decision input batch.
MAX_EMBEDDING_BATCH_SIZE = 2048


def get_or_create_context(bot: str, name: str) -> str:
    """UPSERT into ``contexts`` and return the context_id (uuid str)."""
    with get_session() as sess:
        row = sess.execute(sql_text(
            "INSERT INTO contexts (bot, name) VALUES (:bot, :name) "
            "ON CONFLICT (bot, name) DO UPDATE SET updated_at=now() "
            "RETURNING id"
        ), {"bot": bot, "name": name}).fetchone()
        return str(row[0])


def existing_page_ids(context_id: str) -> set[str]:
    """Return the set of distinct ``metadata->>'page_id'`` for this context."""
    with get_session() as sess:
        rows = sess.execute(sql_text(
            "SELECT DISTINCT metadata->>'page_id' AS pid "
            "FROM documents WHERE context_id = :cid AND metadata ? 'page_id'"
        ), {"cid": context_id}).fetchall()
    return {r[0] for r in rows if r[0]}


def write_decision(
    context_id: str,
    *,
    page_id: str,
    title: str,
    text: str,
    metadata: dict,
    environment: str,
) -> int:
    """Chunk + embed + insert a single decision into ``documents``.

    Returns the number of newly-inserted rows. ON CONFLICT (context_id,
    content_hash) DO NOTHING handles dedup so re-running over an
    already-imported decision is a no-op.

    All chunks share ``metadata.page_id`` (so ``existing_page_ids``
    finds them), plus ``chunk_index`` / ``total_chunks`` for the LLM
    citation layer to collapse if it wants. Caller-supplied
    ``metadata`` is merged in.
    """
    chunks = _chunk_for_embedding(text)
    total_chunks = len(chunks)
    if total_chunks > 1:
        logger.info("Chunked decision %s into %d pieces", page_id, total_chunks)

    client = _get_embedding_client(environment)
    inserted = 0
    extracted_at = datetime.utcnow().isoformat()

    with get_session() as sess:
        for chunk_index, chunk_content in enumerate(chunks):
            try:
                chunk_hash = hashlib.sha256(chunk_content.encode("utf-8")).hexdigest()
                doc_metadata = dict(metadata or {})
                doc_metadata["page_id"] = page_id
                doc_metadata["title"] = doc_metadata.get("title", title)
                doc_metadata["chunk_index"] = chunk_index
                doc_metadata["total_chunks"] = total_chunks
                doc_metadata["extracted_at"] = extracted_at

                embedding = client.embed(chunk_content)

                result = sess.execute(sql_text(
                    "INSERT INTO documents "
                    "(context_id, content, content_hash, metadata, embedding, source_id) "
                    "VALUES (:cid, :c, :h, CAST(:m AS jsonb), CAST(:e AS vector), :sid) "
                    "ON CONFLICT (context_id, content_hash) DO NOTHING"
                ), {
                    "cid": context_id,
                    "c": chunk_content,
                    "h": chunk_hash,
                    "m": json.dumps(doc_metadata),
                    "e": str(embedding),
                    "sid": SOURCE_ID,
                })
                if result.rowcount and result.rowcount > 0:
                    inserted += 1
            except Exception as exc:
                logger.error(
                    "Failed to write chunk %d/%d for page_id=%s: %s",
                    chunk_index + 1, total_chunks, page_id, exc,
                )
                continue

    return inserted


def _resolve_openai_api_key(environment: str) -> str | None:
    """Delegate to the canonical resolver in botnim.config.

    The central resolver also honours OPENAI_API_KEY_<ENV>_FAP_SYNC when
    invoked inside a fap_sync_context() (e.g. from _run_refresh_job),
    routing the daily refresh to a dedicated OpenAI key while keeping
    chat retrieval on the regular key.
    """
    from botnim.config import _resolve_openai_api_key as _resolve
    try:
        return _resolve(environment)
    except ValueError:
        return None


def write_decisions_batched(
    records: list[dict],
    *,
    environment: str,
    embedding_batch_size: int = 1000,
) -> dict[str, int]:
    """Embed + UPSERT many decisions in batched OpenAI calls.

    ``records`` is a list of dicts with the same keys ``write_decision``
    accepts: ``context_id, page_id, title, text, metadata``. ``context_id``
    is per-record so callers can mix contexts in one call if they want; in
    practice the bootstrap script passes the same context_id for every
    record.

    Pipeline:
        1. For each record: chunk text via ``_chunk_for_embedding``, build
           a plan of ``(record_idx, chunk_idx, total_chunks, chunk_text,
           content_hash)``.
        2. Batch the plan's chunks into groups of ``embedding_batch_size``
           and call ``client.embeddings.create(input=[batch])`` once per
           batch.
        3. INSERT each chunk row with ``ON CONFLICT (context_id,
           content_hash) DO NOTHING`` so re-runs over an already-imported
           Aurora are no-ops.

    Returns ``{"chunks_planned": N, "chunks_written": M, "decisions": K}``
    where ``chunks_written`` reflects ``cursor.rowcount`` summed across
    INSERTs — i.e., the count AFTER ON CONFLICT filtering.
    """
    if embedding_batch_size <= 0:
        raise ValueError("embedding_batch_size must be positive")
    if embedding_batch_size > MAX_EMBEDDING_BATCH_SIZE:
        raise ValueError(
            f"embedding_batch_size {embedding_batch_size} exceeds OpenAI's "
            f"per-request maximum of {MAX_EMBEDDING_BATCH_SIZE}"
        )

    if not records:
        return {"chunks_planned": 0, "chunks_written": 0, "decisions": 0}

    # ---- Phase 1: build the plan ------------------------------------------
    extracted_at = datetime.utcnow().isoformat()
    plan: list[dict] = []
    for record_idx, rec in enumerate(records):
        text_value = rec.get("text") or ""
        chunks = _chunk_for_embedding(text_value)
        # _chunk_for_embedding always returns at least one element (the
        # empty-string case yields ['']), so total_chunks >= 1.
        total_chunks = len(chunks)
        for chunk_idx, chunk_content in enumerate(chunks):
            chunk_hash = hashlib.sha256(chunk_content.encode("utf-8")).hexdigest()
            doc_metadata = dict(rec.get("metadata") or {})
            doc_metadata["page_id"] = rec["page_id"]
            doc_metadata["title"] = doc_metadata.get("title", rec.get("title", ""))
            doc_metadata["chunk_index"] = chunk_idx
            doc_metadata["total_chunks"] = total_chunks
            doc_metadata["extracted_at"] = extracted_at
            plan.append({
                "record_idx": record_idx,
                "context_id": rec["context_id"],
                "chunk_content": chunk_content,
                "content_hash": chunk_hash,
                "metadata": doc_metadata,
            })

    chunks_planned = len(plan)
    if chunks_planned == 0:
        return {"chunks_planned": 0, "chunks_written": 0, "decisions": len(records)}

    # ---- Phase 2 + 3: batch embed + INSERT --------------------------------
    api_key = _resolve_openai_api_key(environment)
    client = OpenAI(api_key=api_key)

    total_batches = (chunks_planned + embedding_batch_size - 1) // embedding_batch_size
    chunks_written = 0
    cumulative = 0

    for batch_idx in range(total_batches):
        start = batch_idx * embedding_batch_size
        end = min(start + embedding_batch_size, chunks_planned)
        batch = plan[start:end]
        inputs = [item["chunk_content"] for item in batch]

        response = client.embeddings.create(
            input=inputs,
            model=DEFAULT_EMBEDDING_MODEL,
        )
        # OpenAI guarantees response.data is in the same order as inputs.
        if len(response.data) != len(batch):
            raise RuntimeError(
                f"OpenAI returned {len(response.data)} embeddings for a "
                f"batch of {len(batch)} inputs"
            )

        with get_session() as sess:
            for item, datum in zip(batch, response.data):
                result = sess.execute(sql_text(
                    "INSERT INTO documents "
                    "(context_id, content, content_hash, metadata, embedding, source_id) "
                    "VALUES (:cid, :c, :h, CAST(:m AS jsonb), CAST(:e AS vector), :sid) "
                    "ON CONFLICT (context_id, content_hash) DO NOTHING"
                ), {
                    "cid": item["context_id"],
                    "c": item["chunk_content"],
                    "h": item["content_hash"],
                    "m": json.dumps(item["metadata"]),
                    "e": str(datum.embedding),
                    "sid": SOURCE_ID,
                })
                if result.rowcount and result.rowcount > 0:
                    chunks_written += 1

        cumulative += len(batch)
        logger.info(
            "embedded batch %d/%d (chunks=%d, cumulative=%d)",
            batch_idx + 1, total_batches, len(batch), cumulative,
        )

    return {
        "chunks_planned": chunks_planned,
        "chunks_written": chunks_written,
        "decisions": len(records),
    }
