"""Trigram GIN index on metadata->>'law_name' for fast law-name resolution.

_resolve_law_name (vector_store_aurora.py) maps a colloquial/partial/variant law
mention to the formal law name via pg_trgm similarity over the distinct law_name
values. Without this index the `metadata->>'law_name' % :mention` lookup seq-scans
the whole context (~185K docs); the trigram GIN index makes it an indexed
candidate lookup. pg_trgm is already installed (migration 0015). Same
autocommit_block + CREATE INDEX CONCURRENTLY pattern as 0015/0016.
"""
from alembic import op

revision = "0017_documents_law_name_trgm"
down_revision = "0016_documents_law_name_norm_idx"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS documents_law_name_trgm "
            "ON documents USING gin ((metadata->>'law_name') gin_trgm_ops) "
            "WHERE metadata ? 'law_name'"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS documents_law_name_trgm")
