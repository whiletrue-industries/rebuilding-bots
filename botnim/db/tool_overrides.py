"""Aurora-backed lookup of active per-tool description overrides.

Used by :func:`botnim.bot_config.load_bot_config` to swap the
auto-generated tool description (built from ``config.yaml`` / OpenAPI
YAML) for an admin-edited one stored in ``agent_tool_overrides``.

Returns ``{}`` when:

* the table is empty (e.g. fresh local DB),
* there are no rows with ``active = true`` for ``bot_slug``,
* Aurora is unreachable (e.g. unit-test runner with no DB).

The local import + bare ``try/except`` mirrors
:func:`botnim.bot_config._load_instructions_from_aurora` so this module
stays importable without Postgres connectivity at module-load time.
This matters for callers like the LibreChat runtime which ship the
Python distribution but never hit Aurora directly.

Cache lifetime: callers (currently ``load_bot_config``) invoke this
once per bot per sync invocation; we deliberately do NOT memoize at
module level so a long-lived service that calls ``load_bot_config``
repeatedly (e.g. the FastAPI endpoint) sees fresh values on each call.
If a caller wants intra-sync caching, they should hold the returned
dict on a local variable and pass it through (which is exactly what
``load_bot_config`` does).
"""
from __future__ import annotations

from typing import Dict

from ..config import get_logger

logger = get_logger(__name__)


def get_active_tool_overrides(bot_slug: str) -> Dict[str, str]:
    """Return ``{tool_name: description}`` for active overrides on ``bot_slug``.

    Empty dict on miss / DB unreachable. Never raises.
    """
    try:
        from sqlalchemy import text as _text
        from .session import get_session
        with get_session() as sess:
            rows = sess.execute(_text(
                "SELECT tool_name, description FROM agent_tool_overrides "
                "WHERE agent_type = :a AND active = true"
            ), {"a": bot_slug}).fetchall()
    except Exception:
        return {}
    return {r[0]: r[1] for r in rows}
