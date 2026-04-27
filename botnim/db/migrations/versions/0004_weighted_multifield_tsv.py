"""weighted multi-field tsv (mirror ES REGULAR_CONFIG)

Revision ID: 0004
Revises: 0003
Create Date: 2026-04-27

Replaces the content-only tsv with a weighted multi-field tsvector that
mirrors what ES indexed under REGULAR_CONFIG: DocumentTitle gets the
highest weight (A), structured metadata fields (Summary, Description,
OfficialSource, AdditionalKeywords, Topics) get weight B, and the markdown
content gets weight D as the lowest-priority "haystack" field. Combined
with `ts_rank_cd` in the read path (vector_store_aurora.search), this
restores parity with the ES REGULAR mode's title-weighted retrieval.

GIN index on tsv is dropped and recreated because the tsvector column is
recreated. The GENERATED-ALWAYS-STORED form means existing rows recompute
their tsv at column re-add time — no explicit backfill needed.
"""
from alembic import op


revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


_NEW_TSV_EXPR = (
    # Weight A: most authoritative — document title fields.
    "setweight(to_tsvector('simple', coalesce(metadata->>'DocumentTitle','')), 'A') "
    "|| setweight(to_tsvector('simple', coalesce(metadata->>'title','')), 'A') "
    # Weight B: descriptive metadata that summarises content.
    "|| setweight(to_tsvector('simple', coalesce(metadata->>'Summary','')), 'B') "
    "|| setweight(to_tsvector('simple', coalesce(metadata->>'Description','')), 'B') "
    "|| setweight(to_tsvector('simple', coalesce(metadata->>'OfficialSource','')), 'B') "
    "|| setweight(to_tsvector('simple', coalesce(metadata->>'AdditionalKeywords','')), 'B') "
    "|| setweight(to_tsvector('simple', coalesce(metadata->>'Topics','')), 'B') "
    # Weight D: full markdown content. Largest field, lowest priority — a
    # title hit beats any number of body hits with ts_rank_cd's default
    # weights {0.1, 0.2, 0.4, 1.0}.
    "|| setweight(to_tsvector('simple', content), 'D')"
)


def upgrade() -> None:
    # GIN index references tsv column — drop first, recreate after column swap.
    op.execute("DROP INDEX IF EXISTS documents_tsv_gin")
    op.execute("ALTER TABLE documents DROP COLUMN tsv")
    op.execute(
        f"ALTER TABLE documents ADD COLUMN tsv tsvector "
        f"GENERATED ALWAYS AS ({_NEW_TSV_EXPR}) STORED"
    )
    op.execute("CREATE INDEX documents_tsv_gin ON documents USING gin (tsv)")
    op.execute("ANALYZE documents")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS documents_tsv_gin")
    op.execute("ALTER TABLE documents DROP COLUMN tsv")
    op.execute(
        "ALTER TABLE documents ADD COLUMN tsv tsvector "
        "GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED"
    )
    op.execute("CREATE INDEX documents_tsv_gin ON documents USING gin (tsv)")
    op.execute("ANALYZE documents")
