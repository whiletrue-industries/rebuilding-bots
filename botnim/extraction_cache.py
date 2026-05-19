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

    def get_with_fallback(
        self, content_hash: str, current_version: str
    ) -> dict[str, Any] | None:
        """Return ``{"payload", "from_version", "stale"}`` or ``None``.

        Prefers an exact match at ``current_version``; if absent, returns
        the most-recently-extracted row at any other version for the same
        ``content_hash``. ``stale`` indicates which case was hit. Single
        round-trip — the ``ORDER BY (extractor_version = :v) DESC``
        clause is a boolean rank that puts the exact match first; ties
        break by recency. The existing primary key on
        ``(content_hash, extractor_version)`` already gives a fast index
        range scan, and per-hash row counts are tiny (1-3 in practice)
        so the sort cost is negligible.

        See ``docs/superpowers/specs/2026-05-19-extraction-cache-delta-design.md``
        for why this exists.
        """
        with get_session() as sess:
            row = sess.execute(text(
                "SELECT payload, extractor_version "
                "FROM extraction_cache "
                "WHERE content_hash = :h "
                "ORDER BY (extractor_version = :v) DESC, extracted_at DESC "
                "LIMIT 1"
            ), {"h": content_hash, "v": current_version}).fetchone()
        if row is None:
            return None
        payload = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        from_version = row[1]
        return {
            "payload": payload,
            "from_version": from_version,
            "stale": from_version != current_version,
        }

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
