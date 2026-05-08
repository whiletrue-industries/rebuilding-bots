"""collapse multi-section prompts into a single body row per agent

Revision ID: 0010_collapse_unified_prompt
Revises: 0009_unified_prompt_editor
Create Date: 2026-05-08

The Unified Prompt Editor (UPE) was originally implemented with one
``agent_prompts`` row per logical section (preamble, core_characteristics,
domain_routing, …). Sections were stitched back together by the editor's
client-side ``assemble`` helper, which inserted ``<!-- SECTION_KEY: ... -->``
HTML comments between bodies. The markers leaked into the visible textarea
and into the LLM's system prompt — neither of which the UI/UX called for.

The system prompt that actually reaches the LLM (``bot_config.py:237``)
was always just ``"\\n\\n---\\n\\n".join(bodies)`` with no markers, so
the multi-row decomposition was operationally moot.

This migration collapses every agent's active section rows into a single
``section_key='body'`` row whose body is the existing concatenation. Old
rows are kept (history is preserved) but flagged ``active=false`` so the
``WHERE active = true ORDER BY ordinal`` query in
``_load_instructions_from_aurora`` returns exactly one row.

The collapse runs in two explicit phases (Python-mediated, not CTE-mediated)
to avoid any risk of seeing the deactivation before the read:

1. SELECT: read every agent's full concatenated body into Python memory.
2. UPDATE then INSERT, per agent: deactivate the old rows, write the new
   single body row.

Even though Postgres CTEs in a single statement share one snapshot,
concretizing the read in Python first removes the cognitive load of
reasoning about that and protects against future refactors that might
split the work across statements.

Drafts (``is_draft=true``) are not collapsed. Open drafts pre-migration
are mechanically incompatible with the post-migration single-row schema
(they have arbitrary section_keys); the editor will silently start fresh
on the first post-migration draft save. This is acceptable for v1 — the
UPE has been in prod for under 24h.

Idempotent: a no-op if the agent already has exactly one active row with
section_key='body'.
"""
from __future__ import annotations

from alembic import op
from sqlalchemy import text


revision = '0010_collapse_unified_prompt'
down_revision = '0009_unified_prompt_editor'
branch_labels = None
depends_on = None


_SELECT_SQL = """
    WITH agents AS (
        SELECT DISTINCT agent_type
        FROM agent_prompts
        WHERE active = true
    ),
    needs_collapse AS (
        -- Skip agents already in single-row form (idempotent re-runs).
        SELECT a.agent_type
        FROM agents a
        WHERE NOT EXISTS (
            SELECT 1
            FROM agent_prompts p
            WHERE p.agent_type = a.agent_type
              AND p.active = true
              AND p.section_key = 'body'
              AND (
                  SELECT count(*) FROM agent_prompts q
                  WHERE q.agent_type = a.agent_type AND q.active = true
              ) = 1
        )
    )
    SELECT
        n.agent_type,
        string_agg(p.body, E'\n\n---\n\n' ORDER BY p.ordinal) AS body
    FROM needs_collapse n
    JOIN agent_prompts p ON p.agent_type = n.agent_type AND p.active = true
    GROUP BY n.agent_type
"""


_DEACTIVATE_SQL = """
    UPDATE agent_prompts
    SET active = false
    WHERE agent_type = :agent_type AND active = true
"""


_INSERT_SQL = """
    INSERT INTO agent_prompts (
        agent_type, section_key, ordinal, header_text, body,
        active, is_draft, change_note, created_by, published_at
    ) VALUES (
        :agent_type, 'body', 0, '', :body,
        true, false,
        'collapsed by alembic 0010', 'alembic-0010', now()
    )
"""


def upgrade() -> None:
    bind = op.get_bind()
    # Phase 1 — read concatenated bodies into memory before any write.
    rows = bind.execute(text(_SELECT_SQL)).fetchall()
    if not rows:
        return
    # Phase 2 — for each agent, deactivate then insert. The read in phase 1
    # has already returned the joined body, so the deactivation here cannot
    # corrupt the source data we're about to insert.
    for row in rows:
        params = {"agent_type": row.agent_type, "body": row.body}
        bind.execute(text(_DEACTIVATE_SQL), {"agent_type": row.agent_type})
        bind.execute(text(_INSERT_SQL), params)


def downgrade() -> None:
    # The downgrade cannot reconstruct the original section bodies — the
    # collapse is one-way at the data level. Restore the previous active
    # state by demoting the collapsed body and reactivating the most
    # recent pre-collapse rows for each agent.
    op.execute("""
        WITH collapsed AS (
            SELECT id, agent_type
            FROM agent_prompts
            WHERE active = true
              AND section_key = 'body'
              AND change_note = 'collapsed by alembic 0010'
        ),
        previously_active AS (
            SELECT DISTINCT ON (p.agent_type, p.section_key) p.id
            FROM agent_prompts p
            JOIN collapsed c USING (agent_type)
            WHERE p.id != c.id
              AND p.section_key != 'body'
            ORDER BY p.agent_type, p.section_key, p.created_at DESC
        ),
        deactivate_collapsed AS (
            UPDATE agent_prompts
            SET active = false
            WHERE id IN (SELECT id FROM collapsed)
            RETURNING 1
        )
        UPDATE agent_prompts
        SET active = true
        WHERE id IN (SELECT id FROM previously_active);
    """)
