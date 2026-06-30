"""Reassign law_name_catalog matview ownership to the app role (botnim_app).

Migration 0018 creates the matview as whoever runs alembic. On staging the
alembic-first deploy phase runs as the **postgres master**, so the matview ends
up owned by `postgres`, and the runtime app role `botnim_app` gets
`permission denied for materialized view law_name_catalog` on every read — the
resolver and the query-side detector both query it, so every unfiltered
israeli_laws search errored. Reassigning ownership to `botnim_app` restores both
SELECT (reads) and the ability to `REFRESH MATERIALIZED VIEW CONCURRENTLY` (the
end-of-sync hook).

Guarded on the role existing so this is a safe no-op in test databases (where
`botnim_app` is absent) and on any env where the matview is already owned by
`botnim_app` (e.g. prod, if alembic there runs as the app user) — `ALTER … OWNER
TO botnim_app` is idempotent. `GRANT SELECT` is belt-and-suspenders.
"""
from alembic import op

revision = "0019_law_name_catalog_owner"
down_revision = "0018_law_name_catalog"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'botnim_app') THEN
            EXECUTE 'ALTER MATERIALIZED VIEW law_name_catalog OWNER TO botnim_app';
            EXECUTE 'GRANT SELECT ON law_name_catalog TO botnim_app';
          END IF;
        END $$;
        """
    )


def downgrade() -> None:
    # Ownership reassignment is not meaningfully reversible (the prior owner is
    # env-dependent and the matview is functional either way). No-op.
    pass
