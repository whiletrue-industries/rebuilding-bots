"""unified_prompt_editor: agent_tool_overrides table + agent_prompt_snapshots view

Revision ID: 0009_unified_prompt_editor
Revises: 0008_extraction_cache
Create Date: 2026-05-07

Adds the data-layer scaffolding for the Unified Prompt Editor (UPE):

* ``agent_tool_overrides`` — mirrors ``agent_prompts`` semantics one-for-one
  (active uniqueness, draft/parent/restore handling). Falls back to the
  canonical description in ``config.yaml`` / OpenAPI YAML when no row with
  ``active = true`` exists for a given ``(agent_type, tool_name)``.
* ``agent_prompt_snapshots`` — read-only view that groups published prompt
  sections by ``(agent_type, date_trunc('minute', published_at))`` so the
  whole-bot rollback UI can list "this minute we shipped sections X, Y, Z".
  Multiple sections published within the same wall-clock minute collapse
  into a single snapshot row; section ids/keys are returned as arrays
  ordered by ``ordinal``.

See ``docs/superpowers/specs/2026-05-07-unified-prompt-editor-design.md`` §5.1.
"""
from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "0009_unified_prompt_editor"
down_revision = "0008_extraction_cache"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE agent_tool_overrides (
            id                BIGSERIAL PRIMARY KEY,
            agent_type        TEXT NOT NULL,
            tool_name         TEXT NOT NULL,
            description       TEXT NOT NULL,
            active            BOOLEAN NOT NULL DEFAULT false,
            is_draft          BOOLEAN NOT NULL DEFAULT false,
            parent_version_id BIGINT REFERENCES agent_tool_overrides(id),
            change_note       TEXT,
            created_by        TEXT,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
            published_at      TIMESTAMPTZ
        );
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX agent_tool_overrides_active_uniq
            ON agent_tool_overrides (agent_type, tool_name)
            WHERE active = true;
        """
    )
    op.execute(
        """
        CREATE INDEX agent_tool_overrides_lookup
            ON agent_tool_overrides (agent_type, tool_name, created_at DESC);
        """
    )
    op.execute(
        """
        CREATE VIEW agent_prompt_snapshots AS
        SELECT
            agent_type,
            date_trunc('minute', published_at)              AS snapshot_minute,
            array_agg(id ORDER BY ordinal)                  AS section_version_ids,
            array_agg(section_key ORDER BY ordinal)         AS section_keys,
            max(created_by)                                 AS published_by
        FROM agent_prompts
        WHERE published_at IS NOT NULL
        GROUP BY agent_type, date_trunc('minute', published_at);
        """
    )


def downgrade() -> None:
    # Drop the view first — it depends on agent_prompts (not on the new table),
    # but ordering view-before-table keeps the downgrade symmetrical with the
    # upgrade and avoids any cross-object surprise on future edits.
    op.execute("DROP VIEW IF EXISTS agent_prompt_snapshots;")
    op.execute("DROP INDEX IF EXISTS agent_tool_overrides_lookup;")
    op.execute("DROP INDEX IF EXISTS agent_tool_overrides_active_uniq;")
    op.execute("DROP TABLE IF EXISTS agent_tool_overrides;")
