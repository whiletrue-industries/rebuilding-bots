"""documents.source_id column for per-fetcher attribution

Revision ID: 0005
Revises: 0004
Create Date: 2026-04-27

Adds a nullable text column `source_id` to `documents` so each chunk can be
attributed to its underlying fetcher (wikitext URL, pdf CSV, lexicon, etc.).
Populated by sync code in a follow-up commit; existing rows stay NULL until
the one-shot backfill (botnim/db/migrations/data/0005_backfill_source_id.sql)
runs against staging/prod.
"""
from alembic import op


revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE documents ADD COLUMN source_id TEXT")
    op.execute(
        "CREATE INDEX documents_context_source "
        "ON documents (context_id, source_id)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS documents_context_source")
    op.execute("ALTER TABLE documents DROP COLUMN IF EXISTS source_id")
