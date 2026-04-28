"""replace ivfflat with hnsw for vector index

Revision ID: 0007
Revises: 0006
Create Date: 2026-04-28

IVFFlat with `lists=100` is the wrong index choice for our context sizes
(45–4837 docs per context). Per pgvector guidance, `lists ≈ rows/1000`,
so a corpus of 45 docs should have `lists=1` — not 100. The 100-partition
index leaves most partitions empty and lets the wrong partition win for
short-doc queries (observed in the budget knowledge corpus during the
2026-04-28 chat regression: "מה תקציב מדינת ישראל לשנת 2026" surfaced
ministry-code one-liners over the actual instruction-bearing docs).

HNSW gives near-exact recall without partition tuning, and `ef_search`
is set per-transaction in the search code path. Build cost on ~6k docs
is a few seconds; storage overhead is small.

We keep `vector_cosine_ops` (the embedding distance function used by the
search code).
"""
from alembic import op


revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS documents_embedding_ivfflat")
    op.execute(
        "CREATE INDEX documents_embedding_hnsw "
        "ON documents USING hnsw (embedding vector_cosine_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS documents_embedding_hnsw")
    op.execute(
        "CREATE INDEX documents_embedding_ivfflat "
        "ON documents USING ivfflat (embedding vector_cosine_ops) "
        "WITH (lists = 100)"
    )
