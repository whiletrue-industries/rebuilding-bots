"""agent_prompt_test_questions table

Revision ID: 0003
Revises: 0002
Create Date: 2026-04-26

"""
from alembic import op


revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE agent_prompt_test_questions (
            id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            agent_type   text NOT NULL,
            text         text NOT NULL,
            ordinal      int NOT NULL DEFAULT 0,
            enabled      boolean NOT NULL DEFAULT true,
            created_at   timestamptz NOT NULL DEFAULT now(),
            created_by   text
        )
    """)
    op.execute("""
        CREATE INDEX agent_prompt_test_questions_agent_type
        ON agent_prompt_test_questions (agent_type)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS agent_prompt_test_questions_agent_type")
    op.execute("DROP TABLE IF EXISTS agent_prompt_test_questions")
