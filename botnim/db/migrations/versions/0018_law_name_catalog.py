"""law_name_catalog: materialized view of distinct (context_id, law_name).

Resolution (_best_law_match in vector_store_aurora.py) and query-side detection
(_detect_law_in_query) trigram-match a mention against the distinct law names.
Over the 185K-row `documents` table that costs ~2.6s (the % bitmap heap recheck
dominates); over this ~14K-row matview it is ~200ms. The unique (context_id,
law_name) index enables REFRESH ... CONCURRENTLY (run at end of sync); the
gin_trgm index serves the % lookup. pg_trgm installed in 0015.
"""
from alembic import op

revision = "0018_law_name_catalog"
down_revision = "0017_documents_law_name_trgm"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE MATERIALIZED VIEW law_name_catalog AS "
        "SELECT DISTINCT context_id, metadata->>'law_name' AS law_name "
        "FROM documents "
        "WHERE metadata ? 'law_name' AND coalesce(metadata->>'law_name', '') <> '' "
        "WITH DATA"
    )
    op.execute(
        "CREATE UNIQUE INDEX law_name_catalog_uq ON law_name_catalog (context_id, law_name)"
    )
    op.execute(
        "CREATE INDEX law_name_catalog_trgm ON law_name_catalog USING gin (law_name gin_trgm_ops)"
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS law_name_catalog")
