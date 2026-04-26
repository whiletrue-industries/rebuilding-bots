"""Aurora (PostgreSQL + pgvector) vector store backend.

Mirrors the surface of VectorStoreES so sync.py can swap backends
via the --backend flag. See docs/superpowers/specs/2026-04-26-aurora-migration-design.md
for design rationale.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from typing import Any

from openai import OpenAI
from sqlalchemy import text

from ..config import is_production, get_logger, DEFAULT_EMBEDDING_SIZE, DEFAULT_EMBEDDING_MODEL
from ..db.session import get_engine, get_session
from .vector_store_base import VectorStoreBase

logger = get_logger(__name__)


def _get_embedding_client(environment: str):
    """Return an object with an .embed(text) -> list[float] method.

    Real impl returns a thin wrapper around OpenAI; tests monkeypatch
    this function to inject a fake. Kept as a module-level function
    (not a method) so monkeypatch works without subclassing.
    """
    api_key = (
        os.getenv("OPENAI_API_KEY_PRODUCTION")
        if environment == "production"
        else os.getenv("OPENAI_API_KEY_STAGING")
    )
    client = OpenAI(api_key=api_key)

    class _Wrapper:
        def embed(self, text: str) -> list:
            response = client.embeddings.create(
                input=text,
                model=DEFAULT_EMBEDDING_MODEL,
            )
            return response.data[0].embedding

    return _Wrapper()


class VectorStoreAurora(VectorStoreBase):
    """Vector store backed by Aurora Serverless v2 (PostgreSQL 16.4 + pgvector)."""

    def __init__(self, config: dict, config_dir, environment: str | None = None):
        if environment is None:
            raise ValueError(
                "Environment must be explicitly specified. "
                "Use 'local', 'staging', or 'production'"
            )
        env_name = environment.lower()
        if env_name not in {"local", "staging", "production"}:
            raise ValueError(
                f"Invalid environment: {environment}. "
                "Must be one of: local, staging, production"
            )
        production = is_production(env_name)
        super().__init__(config, config_dir, production=production)
        self.environment = env_name

        # Trigger engine creation early so connection failures surface here, not later
        get_engine()
        logger.info("VectorStoreAurora initialized for environment=%s", env_name)

    # ---- abstract method overrides -----------------------------------------

    def get_or_create_vector_store(self, context, context_name, replace_context):
        """Return the context_id (uuid str) for (bot, context_name).

        - Inserts a row into contexts if it doesn't exist.
        - If replace_context is True, deletes all rows in documents that
          reference this context (CASCADE handles the join).
        """
        bot = self.config["slug"]
        with get_session() as sess:
            row = sess.execute(text(
                "INSERT INTO contexts (bot, name) VALUES (:bot, :name) "
                "ON CONFLICT (bot, name) DO UPDATE SET updated_at=now() "
                "RETURNING id"
            ), {"bot": bot, "name": context_name}).fetchone()
            cid = str(row[0])

            if replace_context:
                sess.execute(text(
                    "DELETE FROM documents WHERE context_id = :cid"
                ), {"cid": cid})
                logger.info("Cleared documents for context %s/%s (id=%s)", bot, context_name, cid)
        return cid

    def upload_files(self, context, context_name, vector_store, file_streams, callback):
        """Insert one row per markdown file. Skips embedding API calls
        for content whose hash already exists in documents (content-hash
        skip — replaces the EFS sqlite embedding cache).

        Per-file errors (embedding failures, oversize content, malformed
        files) are logged and skipped — they do not abort the whole sync.
        Mirrors VectorStoreES._upload_files_async's `return_exceptions=True`
        semantics, which lets one bad doc not poison the batch.
        """
        cid = vector_store  # this is the context_id uuid (returned by get_or_create)
        client = _get_embedding_client(self.environment)
        successful = 0
        skipped = 0

        with get_session() as sess:
            for fname, content_file, file_type, metadata in file_streams:
                if not fname.endswith(".md"):
                    logger.debug("Skipping non-markdown file: %s", fname)
                    continue

                try:
                    content = content_file.read().decode("utf-8")
                    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

                    # Content-hash skip: if this exact (context_id, content_hash)
                    # is already present, do nothing.
                    existing = sess.execute(text(
                        "SELECT id FROM documents "
                        "WHERE context_id=:cid AND content_hash=:h"
                    ), {"cid": cid, "h": content_hash}).fetchone()
                    if existing:
                        logger.debug("Skipping unchanged content for %s", fname)
                        successful += 1
                        continue

                    # New or changed content — embed and insert
                    embedding = client.embed(content)
                    doc_metadata = dict(metadata or {})
                    doc_metadata["filename"] = fname
                    doc_metadata["context_name"] = context_name
                    doc_metadata["context_type"] = context.get("type", "")
                    doc_metadata["extracted_at"] = datetime.utcnow().isoformat()

                    sess.execute(text(
                        "INSERT INTO documents "
                        "(context_id, content, content_hash, metadata, embedding) "
                        "VALUES (:cid, :c, :h, CAST(:m AS jsonb), CAST(:e AS vector))"
                    ), {
                        "cid": cid,
                        "c": content,
                        "h": content_hash,
                        "m": json.dumps(doc_metadata),
                        "e": str(embedding),
                    })
                    successful += 1
                except Exception as exc:
                    logger.error("Failed to process file %s: %s", fname, exc)
                    skipped += 1
                    continue

        if callable(callback):
            callback(successful)
        if skipped:
            logger.warning(
                "Uploaded %d files to context_id=%s; skipped %d files with errors",
                successful, cid, skipped,
            )
        else:
            logger.info("Uploaded %d files to context_id=%s", successful, cid)

    def delete_existing_files(self, context_, vector_store, file_names):
        """Delete documents whose metadata.filename matches any in file_names.
        Returns the count of deleted rows.
        """
        cid = vector_store
        with get_session() as sess:
            result = sess.execute(text(
                "DELETE FROM documents "
                "WHERE context_id = :cid AND metadata->>'filename' = ANY(:names)"
            ), {"cid": cid, "names": list(file_names)})
            return result.rowcount

    def search(
        self,
        context_name: str,
        query_text: str,
        search_mode,           # SearchModeConfig — kept for ES-parity signature
        embedding: list[float],
        num_results: int = 7,
        explain: bool = False,
        metadata_filter: dict | None = None,
    ) -> dict:
        """Hybrid retrieval: pgvector cosine + tsvector BM25, fused via
        reciprocal-rank-fusion. Mirrors VectorStoreES.search's return shape
        so downstream code (search_modes.py, the LLM tool layer) doesn't
        need to know which backend it talked to.

        Returns: {"hits": {"hits": [{"_id", "_score", "_source": {...}}, ...]}}
        """
        bot = self.config["slug"]
        fetch = num_results * 3  # over-fetch then RRF-trim

        # Resolve context_id from (bot, name) — small extra round-trip but
        # keeps the search call self-contained and resilient to context
        # rows being added/removed mid-process.
        with get_session() as sess:
            row = sess.execute(text(
                "SELECT id FROM contexts WHERE bot=:bot AND name=:name"
            ), {"bot": bot, "name": context_name}).fetchone()
            if not row:
                logger.warning("search: context (%s, %s) not found", bot, context_name)
                return {"hits": {"hits": []}}
            cid = str(row[0])

            md_filter_sql = ""
            md_params = {}
            if metadata_filter:
                md_filter_sql = " AND metadata @> CAST(:mfilter AS jsonb)"
                md_params["mfilter"] = json.dumps(metadata_filter)

            vector_rows = sess.execute(text(
                f"""
                SELECT id, content, metadata, 1 - (embedding <=> CAST(:emb AS vector)) AS score
                FROM documents
                WHERE context_id = :cid{md_filter_sql}
                ORDER BY embedding <=> CAST(:emb AS vector)
                LIMIT :limit
                """
            ), {"cid": cid, "emb": str(embedding), "limit": fetch, **md_params}).fetchall()

            bm25_rows = sess.execute(text(
                f"""
                SELECT id, content, metadata,
                       ts_rank(tsv, plainto_tsquery('simple', :q)) AS score
                FROM documents
                WHERE context_id = :cid
                  AND tsv @@ plainto_tsquery('simple', :q){md_filter_sql}
                ORDER BY score DESC
                LIMIT :limit
                """
            ), {"cid": cid, "q": query_text, "limit": fetch, **md_params}).fetchall()

        return _rrf_fuse(vector_rows, bm25_rows, num_results)

    def update_tools(self, context_, vector_store):
        """Emit an OpenAI function-tool definition for this context.
        Uses the context_name (not the uuid) as the tool-name suffix so
        the LLM sees the same tool names as today's ES backend.
        """
        from .search_modes import SEARCH_MODES, DEFAULT_SEARCH_MODE
        context_name = context_.get("slug", "unknown")
        tool_description = self._tool_description(context_)
        search_mode_description = self._search_mode_description(context_)

        self.tools.append({
            "type": "function",
            "function": {
                "name": f"search_{context_name}",
                "description": tool_description,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The query string to use for semantic/free text search",
                        },
                        "search_mode": {
                            "type": "string",
                            "description": search_mode_description,
                            "enum": [mode.name for mode in SEARCH_MODES.values()],
                            "default": DEFAULT_SEARCH_MODE.name,
                        },
                        "num_results": {
                            "type": "integer",
                            "description": "Number of results to return. Leave empty to use the default for the search mode.",
                            "default": 7,
                        },
                    },
                    "required": ["query"],
                },
            },
        })

    def update_tool_resources(self, context_, vector_store):
        """Aurora doesn't use OpenAI tool_resources (those are for OpenAI's
        own vector stores). Mirror the ES backend by setting None."""
        self.tool_resources = None

    # Helpers — copied verbatim from VectorStoreES so behavior is identical
    def _tool_description(self, context_) -> str:
        description = context_.get("description", "")
        examples = context_.get("examples", "")
        if description and examples:
            return f"{description}. Examples: {examples}"
        if description:
            return description
        context_name = context_.get("slug", "unknown")
        return f"Semantic search the '{context_name}' vector store"

    def _search_mode_description(self, context_) -> str:
        base = "Search mode. "
        slug = context_.get("slug", "")
        if any(k in slug for k in ("legal_text", "common_knowledge")):
            modes = [
                "'SECTION_NUMBER': Specialized search for finding legal text sections by their number "
                "(e.g. 'סעיף 12'). Requires both section number and resource name (default 3 results)",
                "'REGULAR': Semantic + full text search across all main fields (default 7 results)",
                "'METADATA_BROWSE': Browse documents with structured metadata summaries instead of full content (25 results)",
            ]
        elif any(k in slug for k in ("legal_advisor_opinions", "legal_advisor_letters",
                                      "committee_decisions", "ethics_decisions")):
            modes = [
                "'METADATA_BROWSE': Browse documents with structured metadata summaries instead of full content (25 results)",
                "'REGULAR': Semantic + full text search across all main fields (7 results)",
            ]
        else:
            modes = [
                "'REGULAR': Semantic + full text search across all main fields (default 7 results)",
                "'METADATA_BROWSE': Browse documents with structured metadata summaries instead of full content (25 results)",
            ]
        return base + ". ".join(modes) + "."


def _rrf_fuse(
    vector_rows: list,
    bm25_rows: list,
    num_results: int,
    k: int = 60,
) -> dict:
    """Reciprocal-rank-fusion with the standard k=60 constant.
    Returns the ES-shaped hits dict so callers don't notice the backend swap.
    """
    scores: dict[str, float] = {}
    docs: dict[str, tuple] = {}

    for rank, row in enumerate(vector_rows):
        doc_id = str(row[0])
        scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank + 1)
        docs[doc_id] = row

    for rank, row in enumerate(bm25_rows):
        doc_id = str(row[0])
        scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank + 1)
        docs.setdefault(doc_id, row)

    ordered = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:num_results]

    hits = []
    for doc_id, fused_score in ordered:
        row = docs[doc_id]
        hits.append({
            "_id": doc_id,
            "_score": fused_score,
            "_source": {
                "content": row[1],
                "metadata": row[2] if isinstance(row[2], dict) else json.loads(row[2]),
            },
        })
    return {"hits": {"hits": hits}}
