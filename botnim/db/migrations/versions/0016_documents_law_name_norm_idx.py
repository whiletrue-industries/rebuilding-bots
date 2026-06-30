"""Expression index on the normalized law_name for scoped-first law retrieval.

The scoped-first retrieval path (vector_store_aurora.search()) filters
israeli_laws docs by NORMALIZED law_name equality inside a MATERIALIZED CTE.
Without a matching expression index that filter evaluates the 4x replace() +
regexp_replace + trim chain (_LAW_NAME_NORM_SQL) on EVERY row in the context
(O(context_size), ~50-500ms per law-scoped call on a 185K-doc context). This
index makes the filter O(law_size).

The indexed expression MUST be byte-identical to _LAW_NAME_NORM_SQL in
botnim/vector_store/vector_store_aurora.py (same maqaf '־' U+05BE, gershayim
'״' U+05F4, geresh '׳' U+05F3) — PostgreSQL only uses a functional index when
the query expression matches it exactly. The byte-identity is verified by the
EXPLAIN test (test_law_name_norm_index_is_used). The partial predicate keeps
the index small (only law-bearing rows).

CONCURRENTLY must run outside a transaction; alembic's autocommit_block()
handles it (same pattern as 0015).
"""
from alembic import op

revision = "0016_documents_law_name_norm_idx"
down_revision = "0015_documents_content_trgm_idx"
branch_labels = None
depends_on = None

# Byte-identical to _LAW_NAME_NORM_SQL (vector_store_aurora.py:96-100).
_LAW_NAME_NORM_EXPR = (
    "trim(regexp_replace("
    "replace(replace(replace(replace(metadata->>'law_name', '־', '-'), ':', ' '), '״', '\"'), '׳', ''''),"
    " '\\s+', ' ', 'g'))"
)


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS documents_law_name_norm "
            "ON documents (context_id, (" + _LAW_NAME_NORM_EXPR + ")) "
            "WHERE metadata ? 'law_name'"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS documents_law_name_norm")
