"""extraction_cache: Aurora-backed cache for dynamic_extraction.py outputs

Revision ID: 0008_extraction_cache
Revises: 0007
Create Date: 2026-05-07

Adds the extraction_cache table that survives ECS task replacements
and prevents per-file gpt-4o-mini calls on every sync. Lookup key is
(content_hash, extractor_version); bot/context are stored only for
the operator-grade purge query.
"""
from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "0008_extraction_cache"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE extraction_cache (
            id                 BIGSERIAL PRIMARY KEY,
            content_hash       TEXT        NOT NULL,
            extractor_version  TEXT        NOT NULL,
            payload            JSONB       NOT NULL,
            bot                TEXT        NOT NULL,
            context            TEXT        NOT NULL,
            document_type      TEXT,
            extracted_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

            CONSTRAINT extraction_cache_key_unique UNIQUE (content_hash, extractor_version)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX idx_extraction_cache_purge
            ON extraction_cache (bot, context, extractor_version);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_extraction_cache_purge;")
    op.execute("DROP TABLE IF EXISTS extraction_cache;")
