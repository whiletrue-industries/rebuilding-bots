"""Aurora (PostgreSQL + pgvector) vector store backend.

Mirrors the surface of VectorStoreES so sync.py can swap backends
via the --backend flag. See docs/superpowers/specs/2026-04-26-aurora-migration-design.md
for design rationale.
"""
from __future__ import annotations

import hashlib
import os
from typing import Any

from sqlalchemy import text

from ..config import is_production, get_logger, DEFAULT_EMBEDDING_SIZE
from ..db.session import get_engine, get_session
from .vector_store_base import VectorStoreBase

logger = get_logger(__name__)


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
        raise NotImplementedError("upload_files: implemented in Task 2.2")

    def delete_existing_files(self, context_, vector_store, file_names):
        raise NotImplementedError("delete_existing_files: implemented in Task 2.2")

    def update_tools(self, context_, vector_store):
        raise NotImplementedError("update_tools: implemented in Task 2.4")

    def update_tool_resources(self, context_, vector_store):
        raise NotImplementedError("update_tool_resources: implemented in Task 2.4")
