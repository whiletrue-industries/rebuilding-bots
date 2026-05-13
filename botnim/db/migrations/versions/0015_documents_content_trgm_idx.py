"""GIN trigram index on documents.content for Hebrew-friendly lexical retrieval.

Adds a `gin_trgm_ops` index so `word_similarity()`-based ranking on
`documents.content` runs in O(log n) instead of O(n). This is the
data-layer prerequisite for the `lexical_strategy: trigram` per-context
flag wired in vector_store_aurora.search() — without the index, the
trigram branch would seq-scan 27k-row corpora and blow past the 12s
retrieve deadline (probed 2026-05-13: knesset_protocols word_similarity
without index = 2.2s).

Trigram ranking beats the existing `to_tsquery('simple', …)` BM25 path
on Hebrew because pg_trgm matches character 3-grams, which inherently
bridge construct-state alternation (ועדת↔ועדה↔ועדות, הכנסת↔לכנסת).
Local A/B on the prod-style query "מה הדרך ליזום ועדת חקירה ממלכתית?"
surfaced all three prod-cited sections (§22 חוק-יסוד הכנסת, §129 + §135
תקנון הכנסת) in top-8 trigram results vs 0/3 for the current tsquery
path.

CREATE INDEX CONCURRENTLY is used so production reads aren't blocked
while the index builds (documents is ~1.7 GB; non-concurrent would
write-lock the table for ~5-10 min). CONCURRENTLY requires running
OUTSIDE a transaction; alembic's autocommit_block() handles that.
"""
from alembic import op


revision = "0015_documents_content_trgm_idx"
down_revision = "0014_phoenix_pw_from_secret"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # pg_trgm is already installed on the local pgvector image and on Aurora
    # (verified 2026-05-13). The IF NOT EXISTS makes this idempotent in case
    # the extension was added out-of-band.
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # CONCURRENTLY must not be inside a transaction.
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "documents_content_trgm "
            "ON documents USING gin (content gin_trgm_ops)"
        )


def downgrade() -> None:
    # DROP INDEX CONCURRENTLY for symmetry; keeps reads unblocked during
    # rollback. We don't drop the pg_trgm extension itself — it's broadly
    # useful and other future migrations may rely on it.
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS documents_content_trgm")
