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
from datetime import datetime

from sqlalchemy import text as sql_text

from ...config import get_logger
from ...db.session import get_session
from ...vector_store.vector_store_aurora import (
    _chunk_for_embedding,
    _get_embedding_client,
)

logger = get_logger(__name__)

SOURCE_ID = "gov_il_decisions"


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
