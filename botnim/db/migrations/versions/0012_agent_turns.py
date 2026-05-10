"""agent_turns + trace_audit_log

Revision ID: 0012_agent_turns
Revises: 0011_sanity_runs
Create Date: 2026-05-10

Adds two tables for Phoenix LLM-loop tracing:

- ``agent_turns``: canonical row per chat turn — stores trace_id, summary
  stats, and cited chunk references so the LibreChat admin UI can render
  per-turn observability data.
- ``trace_audit_log``: append-only audit log recording every admin who
  fetches a trace (separate from agent_turns for audit isolation).

Both tables are written by the LibreChat agent loop (future task), not by
botnim-api directly.
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "0012_agent_turns"
down_revision = "0011_sanity_runs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_turns",
        sa.Column("turn_id", UUID(as_uuid=True), primary_key=True),
        sa.Column("conversation_id", sa.String(64), nullable=False),
        sa.Column("message_id", sa.String(64), nullable=False),
        sa.Column("trace_id", sa.String(32), nullable=False),
        sa.Column("summary", JSONB, nullable=False, server_default="{}"),
        sa.Column("cited_chunks", JSONB, nullable=False, server_default="[]"),
        sa.Column("env", sa.String(16), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    op.create_index("ix_agent_turns_trace_id", "agent_turns", ["trace_id"])
    op.create_index("ix_agent_turns_message_id", "agent_turns", ["message_id"])
    op.create_index("ix_agent_turns_conversation_id", "agent_turns", ["conversation_id"])

    op.create_table(
        "trace_audit_log",
        sa.Column("id", sa.BigInteger, sa.Identity(), primary_key=True),
        sa.Column("admin_user_id", sa.String(64), nullable=False),
        sa.Column("trace_id", sa.String(32), nullable=False),
        sa.Column("conversation_id", sa.String(64), nullable=False),
        sa.Column("owning_user_id", sa.String(64), nullable=False),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    op.create_index("ix_trace_audit_log_trace_id", "trace_audit_log", ["trace_id"])
    op.create_index("ix_trace_audit_log_admin_user_id", "trace_audit_log", ["admin_user_id"])
    op.create_index("ix_trace_audit_log_ts", "trace_audit_log", ["ts"])


def downgrade() -> None:
    op.drop_index("ix_trace_audit_log_ts", table_name="trace_audit_log")
    op.drop_index("ix_trace_audit_log_admin_user_id", table_name="trace_audit_log")
    op.drop_index("ix_trace_audit_log_trace_id", table_name="trace_audit_log")
    op.drop_table("trace_audit_log")
    op.drop_index("ix_agent_turns_conversation_id", table_name="agent_turns")
    op.drop_index("ix_agent_turns_message_id", table_name="agent_turns")
    op.drop_index("ix_agent_turns_trace_id", table_name="agent_turns")
    op.drop_table("agent_turns")
