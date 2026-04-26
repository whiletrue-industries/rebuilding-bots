"""indexes: ivfflat, gin, partial unique

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-26

"""
from alembic import op


revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX documents_embedding_ivfflat "
        "ON documents USING ivfflat (embedding vector_cosine_ops) "
        "WITH (lists = 100)"
    )
    op.execute("CREATE INDEX documents_tsv_gin ON documents USING gin (tsv)")
    op.execute(
        "CREATE INDEX documents_metadata_gin "
        "ON documents USING gin (metadata jsonb_path_ops)"
    )
    op.execute("CREATE INDEX documents_context_id ON documents(context_id)")
    op.execute(
        "CREATE UNIQUE INDEX agent_prompts_one_current "
        "ON agent_prompts (section_key) WHERE is_current = true"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS agent_prompts_one_current")
    op.execute("DROP INDEX IF EXISTS documents_context_id")
    op.execute("DROP INDEX IF EXISTS documents_metadata_gin")
    op.execute("DROP INDEX IF EXISTS documents_tsv_gin")
    op.execute("DROP INDEX IF EXISTS documents_embedding_ivfflat")
