"""context_snapshots table for per-sync drift history

Revision ID: 0006
Revises: 0005
Create Date: 2026-04-27

Append-only audit log of (bot, context, source_id, doc_count) per sync.
The aggregate row uses source_id='*'. No foreign keys — context/source
rows may disappear from live tables, their history must remain readable.
"""
from alembic import op


revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE context_snapshots (
            id           UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
            snapshot_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            bot          TEXT NOT NULL,
            context      TEXT NOT NULL,
            source_id    TEXT NOT NULL,
            doc_count    INTEGER NOT NULL
        )
    """)
    op.execute("""
        CREATE INDEX context_snapshots_lookup
            ON context_snapshots (bot, context, source_id, snapshot_at DESC)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS context_snapshots_lookup")
    op.execute("DROP TABLE IF EXISTS context_snapshots")
