"""sanity_runs table

Revision ID: 0011_sanity_runs
Revises: 0010_collapse_unified_prompt
Create Date: 2026-05-09
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0011_sanity_runs"
down_revision = "0010_collapse_unified_prompt"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE sanity_runs (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            env             TEXT NOT NULL,
            started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            finished_at     TIMESTAMPTZ,
            status          TEXT NOT NULL,
            url_old         TEXT NOT NULL,
            url_new         TEXT NOT NULL,
            total_rows      INTEGER,
            ab_new_wins     INTEGER,
            ab_old_wins     INTEGER,
            ab_ties         INTEGER,
            rubric_pass     INTEGER,
            rubric_fail     INTEGER,
            rubric_xfail    INTEGER,
            rubric_infra    INTEGER,
            pass_rate       NUMERIC(4,3),
            alert_severity  TEXT,
            alert_reasons   JSONB,
            capture_json    JSONB,
            judged_json     JSONB,
            html            TEXT,
            error           TEXT,
            CONSTRAINT sanity_runs_status_chk
                CHECK (status IN ('running', 'succeeded', 'failed')),
            CONSTRAINT sanity_runs_alert_severity_chk
                CHECK (alert_severity IS NULL OR alert_severity IN ('orange', 'red'))
        )
        """
    )
    op.create_index(
        "sanity_runs_started_at",
        "sanity_runs",
        [sa.text("started_at DESC")],
    )
    op.create_index(
        "sanity_runs_env_started_at",
        "sanity_runs",
        ["env", sa.text("started_at DESC")],
    )


def downgrade() -> None:
    op.drop_index("sanity_runs_env_started_at", table_name="sanity_runs")
    op.drop_index("sanity_runs_started_at", table_name="sanity_runs")
    op.execute("DROP TABLE sanity_runs")
