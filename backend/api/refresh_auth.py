"""API-key auth dependency for the admin refresh endpoint.

Separate from the Firebase-backed auth used by /admin/users because the
refresh caller is a VPC-local Lambda, not a browser session. The key is
mounted into the task's environment via Secrets Manager (see
infra/envs/<env>/main.tf and secrets.tf).
"""
from __future__ import annotations

import hmac
import os
from typing import Optional

from fastapi import Header, HTTPException


def require_refresh_api_key(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> None:
    """FastAPI dependency: reject the request unless X-API-Key matches the
    BOTNIM_ADMIN_API_KEY env var.

    Returns 503 if the server is not configured (env var missing) so the
    caller can distinguish "I don't know who you are" from "I don't know
    who I am". Uses hmac.compare_digest for constant-time comparison.
    """
    expected = os.environ.get("BOTNIM_ADMIN_API_KEY")
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="refresh endpoint not configured (BOTNIM_ADMIN_API_KEY unset)",
        )
    if not x_api_key or not hmac.compare_digest(expected, x_api_key):
        raise HTTPException(status_code=401, detail="invalid or missing X-API-Key")
