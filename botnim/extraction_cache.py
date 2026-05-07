"""Aurora-backed read-through cache for dynamic_extraction.py outputs.

Lookup key is (content_hash, extractor_version). Two contexts that ingest
the same raw text share one row — saves cost when content overlap exists
between contexts (e.g. plenary_schedule vs knesset_protocols citing the
same Knesset minute).

The class is intentionally thin: get / put / purge. No connection pool of
its own — reuses :func:`botnim.db.session.get_session` so this module
inherits the same env-var convention as every other Aurora caller.
"""
from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text

from .config import get_logger
from .db.session import get_session

logger = get_logger(__name__)


class ExtractionCache:
    """Aurora-backed read-through cache for dynamic_extraction outputs."""

    def __init__(self, environment: str):
        # `environment` kept on the instance for log/diagnostic context.
        # The actual DB target is encoded in get_session()'s engine binding,
        # which is already environment-scoped via env vars.
        self.environment = environment

    def get(self, content_hash: str, extractor_version: str) -> dict[str, Any] | None:
        """Return cached payload dict for the given key, or None on miss."""
        with get_session() as sess:
            row = sess.execute(text(
                "SELECT payload FROM extraction_cache "
                "WHERE content_hash = :h AND extractor_version = :v"
            ), {"h": content_hash, "v": extractor_version}).fetchone()
        if row is None:
            return None
        # SQLAlchemy returns the JSONB column as a python dict already.
        return row[0] if isinstance(row[0], dict) else json.loads(row[0])

    def put(
        self,
        content_hash: str,
        extractor_version: str,
        *,
        payload: dict[str, Any],
        bot: str,
        context: str,
        document_type: str | None,
    ) -> None:
        """Idempotent upsert. Last writer wins on payload."""
        with get_session() as sess:
            sess.execute(text(
                "INSERT INTO extraction_cache "
                "(content_hash, extractor_version, payload, bot, context, document_type) "
                "VALUES (:h, :v, CAST(:p AS jsonb), :b, :c, :dt) "
                "ON CONFLICT (content_hash, extractor_version) DO UPDATE SET "
                "    payload = EXCLUDED.payload, "
                "    extracted_at = now()"
            ), {
                "h": content_hash,
                "v": extractor_version,
                "p": json.dumps(payload, ensure_ascii=False),
                "b": bot,
                "c": context,
                "dt": document_type,
            })

    def purge(
        self,
        bot: str,
        context: str,
        extractor_version: str | None = None,
    ) -> int:
        """Delete rows for (bot, context [, extractor_version]). Returns count."""
        with get_session() as sess:
            if extractor_version is None:
                result = sess.execute(text(
                    "DELETE FROM extraction_cache WHERE bot = :b AND context = :c"
                ), {"b": bot, "c": context})
            else:
                result = sess.execute(text(
                    "DELETE FROM extraction_cache "
                    "WHERE bot = :b AND context = :c AND extractor_version = :v"
                ), {"b": bot, "c": context, "v": extractor_version})
            count = result.rowcount or 0
        if count:
            logger.info(
                "Purged %d extraction_cache rows for %s/%s @ %s",
                count, bot, context, extractor_version or "all-versions",
            )
        return count
