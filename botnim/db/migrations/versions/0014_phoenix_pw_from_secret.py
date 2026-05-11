"""phoenix_app password rotation from Secrets Manager

Revision ID: 0014_phoenix_pw_from_secret
Revises: 0013_phoenix_db
Create Date: 2026-05-11

(ID kept under 32 chars — alembic_version.version_num is VARCHAR(32).
The earlier draft used a 37-char ID and tripped string-data-right-
truncation when alembic tried to write the version row.)

Reads the connection string from Secrets Manager
(botnim/<env>/phoenix-db-url) and runs ALTER ROLE phoenix_app PASSWORD
to make Aurora match what the secret says. Idempotent — runs every
deploy; if the role's current password already matches, this is a
no-op (PostgreSQL doesn't expose role passwords for comparison, so
we just always set, which is harmless).

Why this migration exists
-------------------------
0013 creates phoenix_app with a placeholder password
('placeholder_rotated_via_secrets_manager') intentionally — the real
password is set OUT-OF-BAND because secrets must not live in code.
This migration is the IaC bridge: it pulls the real password from
Secrets Manager (via the task's IAM role) and aligns Aurora with it.
After this migration runs, the Phoenix ECS service can boot with the
secret value as PHOENIX_SQL_DATABASE_URL.

Prereq
------
The secret botnim/<env>/phoenix-db-url must exist in Secrets Manager
in the form: postgresql://phoenix_app:<password>@<host>:5432/phoenix.
Created out-of-band as a one-time provisioning step (mirrors how
OPENAI_API_KEY_<ENV> works in this repo). When the secret is missing,
the migration logs a warning and exits 0 (a no-op) so a fresh env can
still bring the schema up; the password rotation is retried on the
next deploy after the operator creates the secret.

Alternative considered
----------------------
Terraform-managed secret with random_password + a Lambda rotation
trigger that ALTERs the role on rotation. Rejected as overkill for
v1 — adds a Lambda + IAM + EventBridge for one infrequent rotation.
This migration is the smaller, simpler equivalent.
"""
from __future__ import annotations
import logging
import os
import urllib.parse

from alembic import op

revision = "0014_phoenix_pw_from_secret"
down_revision = "0013_phoenix_db"
branch_labels = None
depends_on = None

_logger = logging.getLogger("alembic.runtime.migration")


def _read_phoenix_db_url() -> str | None:
    """Fetch botnim/<env>/phoenix-db-url from Secrets Manager.

    Returns the secret string, or None if the secret doesn't exist
    yet (fresh env — operator hasn't run the one-time provisioning
    step). Logs a warning and lets the migration no-op gracefully.
    """
    env = os.getenv("ENVIRONMENT", "local")
    secret_name = f"botnim/{env}/phoenix-db-url"
    try:
        import boto3  # type: ignore
    except ImportError:
        _logger.warning(
            "[0014] boto3 not installed — cannot read %s; skipping. "
            "Install boto3 in the migration runtime to enable IaC password rotation.",
            secret_name,
        )
        return None
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "il-central-1"
    try:
        client = boto3.client("secretsmanager", region_name=region)
        resp = client.get_secret_value(SecretId=secret_name)
        return resp.get("SecretString")
    except client.exceptions.ResourceNotFoundException:
        _logger.warning(
            "[0014] secret %s not found in region %s — skipping rotation. "
            "Create the secret with the postgresql://… connection string and "
            "re-run alembic to align Aurora with it.",
            secret_name, region,
        )
        return None
    except Exception as e:  # pragma: no cover — best-effort
        _logger.warning(
            "[0014] failed to read %s: %s; skipping rotation",
            secret_name, e,
        )
        return None


def _extract_password(url: str) -> str | None:
    """Parse a postgresql://user:password@host:port/db URL and return password."""
    try:
        parsed = urllib.parse.urlparse(url)
        if not parsed.password:
            return None
        return urllib.parse.unquote(parsed.password)
    except Exception:
        return None


def upgrade() -> None:
    url = _read_phoenix_db_url()
    if not url:
        _logger.info("[0014] no secret available; this migration is a no-op for now")
        return
    password = _extract_password(url)
    if not password:
        _logger.warning(
            "[0014] could not extract password from secret value (URL parse failed); "
            "skipping rotation"
        )
        return

    # Sanity: refuse the placeholder. Indicates the operator hasn't actually
    # written a real password into the secret yet (e.g., they ran
    # `aws secretsmanager create-secret --secret-string placeholder_rotated_via_secrets_manager`).
    if password == "placeholder_rotated_via_secrets_manager":
        _logger.warning(
            "[0014] secret still contains the placeholder password — refusing to "
            "rotate. Update the secret to a real value first."
        )
        return

    # CREATE/ALTER ROLE cannot run inside a transaction.
    with op.get_context().autocommit_block():
        # Use SQL-level escaping for the password literal: PostgreSQL allows
        # E'...' escape strings that handle special chars safely. We escape
        # backslashes and single quotes manually for safety.
        escaped = password.replace("\\", "\\\\").replace("'", "''")
        op.execute(f"ALTER ROLE phoenix_app WITH PASSWORD '{escaped}'")
    _logger.info(
        "[0014] phoenix_app password rotated to match Secrets Manager value (%d chars)",
        len(password),
    )


def downgrade() -> None:
    """Reset to the placeholder set by 0013.

    Provided for symmetry; not expected to be used in practice (running
    downgrade would lock Phoenix out until the operator runs upgrade
    again to re-rotate from the secret).
    """
    with op.get_context().autocommit_block():
        op.execute(
            "ALTER ROLE phoenix_app WITH PASSWORD 'placeholder_rotated_via_secrets_manager'"
        )
