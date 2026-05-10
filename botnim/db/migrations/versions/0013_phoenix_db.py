"""phoenix database + phoenix_app role

Revision ID: 0013_phoenix_db
Revises: 0012_agent_turns
Create Date: 2026-05-10

Provisions the empty ``phoenix`` database and dedicated ``phoenix_app``
login role on the shared Aurora cluster.  Phoenix (the LLM-tracing
collector) runs its own schema migrations on first boot — we only need
the DB + role to exist beforehand.

SECURITY NOTE — DO NOT substitute a real password into this migration.
The placeholder 'placeholder_rotated_via_secrets_manager' is intentionally
visible so it's obvious in CloudWatch / alembic logs that no real secret
has been committed. The real password is set OUT-OF-BAND, post-upgrade,
via ``ALTER ROLE phoenix_app WITH PASSWORD '<from-secrets-manager>'``.
That ALTER is run by an operator with master credentials, never via this
file. See CLAUDE.md "Phase 0 — Prerequisites" in the phoenix-llm-tracing
plan for the exact recipe.
"""
from alembic import op

revision = "0013_phoenix_db"
down_revision = "0012_agent_turns"
branch_labels = None
depends_on = None

_PLACEHOLDER_PW = "placeholder_rotated_via_secrets_manager"


def upgrade() -> None:
    # Preflight: this migration creates a database and a role, which require
    # rds_superuser (Aurora) or rolsuper (plain Postgres) privileges.  The
    # deploy.sh phase-6.5 alembic step normally runs as botnim_app, which
    # lacks these privileges and would silently no-op — the same incident
    # pattern that burned us in migration 0007 (hnsw_replaces_ivfflat).
    # Fail loudly here so alembic_version is never advanced without the
    # schema actually being applied.
    conn = op.get_bind()
    is_super = conn.exec_driver_sql(
        # Check superuser in a way that works on both plain Postgres (local dev)
        # and Aurora (where 'rds_superuser' exists but rolsuper is always false).
        # pg_has_role() raises ERROR if the role doesn't exist, so we guard with
        # EXISTS before calling it — otherwise local Postgres tests would blow up
        # because 'rds_superuser' is Aurora-only.
        "SELECT "
        "  (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) "
        "  OR ( "
        "    EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'rds_superuser') "
        "    AND pg_has_role(current_user, 'rds_superuser', 'member') "
        "  )"
    ).scalar()
    if not is_super:
        raise RuntimeError(
            "Migration 0013_phoenix_db requires rds_superuser privileges "
            "(creates a database and a role). Run alembic with master "
            "credentials, not the app user. See CLAUDE.md migration-0007 "
            "incident notes — silently advancing alembic_version while the "
            "schema diverges has burned us before."
        )

    # CREATE ROLE and CREATE DATABASE cannot run inside a transaction, so
    # we use autocommit_block.  Each statement is its own implicit txn.
    with op.get_context().autocommit_block():
        # Idempotent role creation — re-running after a partial failure is safe.
        op.execute(
            "DO $$ BEGIN "
            "CREATE ROLE phoenix_app LOGIN PASSWORD '" + _PLACEHOLDER_PW + "'; "
            "EXCEPTION WHEN duplicate_object THEN NULL; END $$"
        )

        # CREATE DATABASE cannot run inside a DO block, so guard with a
        # driver-level check outside the autocommit_block (which is already
        # committed above).  We re-use the same connection.
        db_exists = conn.exec_driver_sql(
            "SELECT 1 FROM pg_database WHERE datname = 'phoenix'"
        ).scalar()
        if not db_exists:
            op.execute("CREATE DATABASE phoenix OWNER phoenix_app")

        # REVOKE / GRANT are idempotent — running them twice produces the
        # same state, so no guard is needed.
        op.execute("REVOKE ALL ON DATABASE phoenix FROM PUBLIC")
        op.execute("GRANT CONNECT ON DATABASE phoenix TO phoenix_app")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP DATABASE IF EXISTS phoenix")
        op.execute("DROP ROLE IF EXISTS phoenix_app")
