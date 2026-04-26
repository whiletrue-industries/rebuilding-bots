"""init contexts, documents, agent_prompts

Revision ID: 0001
Revises:
Create Date: 2026-04-26

"""
from alembic import op


revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")  # for gen_random_uuid()

    op.execute("""
        CREATE TABLE contexts (
            id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            bot          text NOT NULL,
            name         text NOT NULL,
            config       jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at   timestamptz NOT NULL DEFAULT now(),
            updated_at   timestamptz NOT NULL DEFAULT now(),
            UNIQUE (bot, name)
        )
    """)

    op.execute("""
        CREATE TABLE documents (
            id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            context_id    uuid NOT NULL REFERENCES contexts(id) ON DELETE CASCADE,
            content       text NOT NULL,
            content_hash  text NOT NULL,
            metadata      jsonb NOT NULL DEFAULT '{}'::jsonb,
            embedding     vector(1536),
            tsv           tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED,
            created_at    timestamptz NOT NULL DEFAULT now(),
            updated_at    timestamptz NOT NULL DEFAULT now(),
            UNIQUE (context_id, content_hash)
        )
    """)

    op.execute("""
        CREATE TABLE agent_prompts (
            id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            agent_type         text NOT NULL,
            section_key        text NOT NULL,
            ordinal            int NOT NULL DEFAULT 0,
            header_text        text NOT NULL DEFAULT '',
            body               text NOT NULL,
            active             boolean NOT NULL DEFAULT false,
            is_draft           boolean NOT NULL DEFAULT true,
            parent_version_id  uuid,
            change_note        text,
            created_at         timestamptz NOT NULL DEFAULT now(),
            created_by         text,
            published_at       timestamptz
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS agent_prompts")
    op.execute("DROP TABLE IF EXISTS documents")
    op.execute("DROP TABLE IF EXISTS contexts")
    # Leave extensions in place — they may be used by other consumers
