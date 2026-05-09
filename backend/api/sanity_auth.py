"""API-key auth dependency for the admin sanity endpoint.

Separate secret from refresh-admin so the two schedules can rotate keys
independently. Mounted via Secrets Manager (see infra/envs/<env>/sanity.tf).
"""
from __future__ import annotations

import hmac
import os
from typing import Optional

from fastapi import Header, HTTPException


def require_sanity_api_key(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> None:
    expected = os.environ.get("BOTNIM_SANITY_ADMIN_API_KEY")
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="sanity endpoint not configured (BOTNIM_SANITY_ADMIN_API_KEY unset)",
        )
    if not x_api_key or not hmac.compare_digest(expected, x_api_key):
        raise HTTPException(status_code=401, detail="invalid or missing X-API-Key")
