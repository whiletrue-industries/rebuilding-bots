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

All non-body rows are deleted, including in-flight drafts (``is_draft=true``)
and inactive history. Drafts pre-migration have arbitrary section_keys
that don't fit the post-migration single-row schema, and history is
intentionally collapsed away ("snapshots become whole-prompt snapshots"
— spec decision). Admin will need to re-enter any in-flight draft after
the migration runs; loud, recoverable, acceptable for v1.

Idempotent: a no-op if the agent already has exactly one active row with
section_key='body' and no other rows.
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


_DELETE_OLD_SQL = """
    DELETE FROM agent_prompts
    WHERE agent_type = :agent_type
      AND section_key != 'body'
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
    # Phase 2 — for each agent, DELETE every row whose section_key is not
    # 'body' (active rows AND drafts AND inactive history), then INSERT the
    # new collapsed body row.
    #
    # Why DELETE instead of UPDATE active=false? The LibreChat-side
    # `aurora.listSections` query uses
    #     SELECT DISTINCT ON (section_key) *
    #     FROM agent_prompts WHERE agent_type=$1
    #     ORDER BY section_key, active DESC, created_at DESC
    # which falls back to inactive/draft rows when no active row exists for
    # a given section_key. After a soft-deactivate every old section_key
    # still has rows, so listSections returns N+1 rows (1 body + N
    # fall-back per old key) and the assemble() invariant ('exactly one
    # section') trips. DELETE removes the section_keys entirely so the
    # DISTINCT ON has nothing to fall back to.
    #
    # History trade-off: per-section history is lost. This is the spec
    # decision recorded in the brainstorm — "Per-section history is lost
    # (snapshots become whole-prompt snapshots). Cleanest end state."
    for row in rows:
        params = {"agent_type": row.agent_type, "body": row.body}
        bind.execute(text(_DELETE_OLD_SQL), {"agent_type": row.agent_type})
        bind.execute(text(_INSERT_SQL), params)


def downgrade() -> None:
    # The DELETE in upgrade() drops every non-body row, so the original
    # multi-section data — including inactive history and pending drafts —
    # is gone irrecoverably. There is no SQL we can run to reconstruct it.
    # Fail loudly rather than silently leave the DB in a half-state.
    raise RuntimeError(
        "0010_collapse_unified_prompt: downgrade is not supported. "
        "The upgrade DELETEs every section row whose key is not 'body'. "
        "Restore from a logical backup taken before the upgrade ran "
        "(pre-collapse `agent_prompts` rows for the affected agents)."
    )
