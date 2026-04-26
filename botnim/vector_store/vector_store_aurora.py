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
        """
        cid = vector_store  # this is the context_id uuid (returned by get_or_create)
        client = _get_embedding_client(self.environment)
        successful = 0

        with get_session() as sess:
            for fname, content_file, file_type, metadata in file_streams:
                if not fname.endswith(".md"):
                    logger.debug("Skipping non-markdown file: %s", fname)
                    continue

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

        if callable(callback):
            callback(successful)
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

    def update_tools(self, context_, vector_store):
        raise NotImplementedError("update_tools: implemented in Task 2.4")

    def update_tool_resources(self, context_, vector_store):
        raise NotImplementedError("update_tool_resources: implemented in Task 2.4")
