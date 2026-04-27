"""Local-dev launcher for backend.api.server.

Stubs the Firebase auth modules (which require a real GCP service-account
JSON in cwd at import time) before importing the FastAPI app, then hands
the app to uvicorn. Intended only for hitting the search endpoints against
a local Aurora snapshot — auth-protected endpoints will reject everything
because the stub `verify_id_token` always raises.

Usage:
    cd rebuilding-bots
    set -a && source .env.local-dev && source .env && set +a
    .venv/bin/python scripts/dev_server.py
"""
from __future__ import annotations

import os
import sys
import types
from pathlib import Path
from typing import Annotated

# Repo root must be on sys.path so `import backend.api.server` resolves.
_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
# backend/api/ must be on sys.path so server.py's `from resolve_firebase_user
# import FireBaseUser` resolves the same way it does at runtime.
_BACKEND_API = _REPO / "backend" / "api"
if str(_BACKEND_API) not in sys.path:
    sys.path.insert(0, str(_BACKEND_API))


def _stub_firebase() -> None:
    """Register fake firebase_admin / resolve_firebase_user modules.

    Mirrors the strategy in tests/test_query_error_handling.py — keeps the
    real Aurora and OpenAI client paths intact while making the module-load
    Firebase init a no-op.
    """
    fa = types.ModuleType("firebase_admin")
    def _init_noop(*a, **k):
        return types.SimpleNamespace(name="local-dev-stub")
    fa.initialize_app = _init_noop
    sys.modules["firebase_admin"] = fa

    fa_firestore = types.ModuleType("firebase_admin.firestore")
    fa_firestore.client = lambda *a, **k: None
    sys.modules["firebase_admin.firestore"] = fa_firestore

    fa_creds = types.ModuleType("firebase_admin.credentials")
    fa_creds.Certificate = lambda *a, **k: None
    sys.modules["firebase_admin.credentials"] = fa_creds

    fa_auth = types.ModuleType("firebase_admin.auth")
    def _verify(*a, **k):
        raise RuntimeError("firebase auth not available in local-dev launcher")
    fa_auth.verify_id_token = _verify
    sys.modules["firebase_admin.auth"] = fa_auth

    # The actual user-resolution helper. Matches the runtime API surface
    # closely enough for FastAPI's Depends(...) to introspect.
    rfu = types.ModuleType("resolve_firebase_user")
    rfu.FireBaseUser = Annotated[dict, lambda: {"uid": "local-dev"}]
    sys.modules["resolve_firebase_user"] = rfu

    # refresh_auth: same trick — we don't hit /refresh in local dev.
    ra = types.ModuleType("refresh_auth")
    ra.require_refresh_api_key = lambda: None
    sys.modules["refresh_auth"] = ra


def main() -> None:
    _stub_firebase()
    # Default to local-dev DB if the user forgot to source .env.local-dev.
    os.environ.setdefault("DATABASE_URL", "postgresql+psycopg://test:test@localhost:54329/botnim_local")
    os.environ.setdefault("ENVIRONMENT", "local")
    os.environ.setdefault("BOTNIM_QUERY_BACKEND", "aurora")

    from backend.api.server import app  # noqa: E402

    import uvicorn
    port = int(os.environ.get("PORT", "8001"))
    print(f"[dev_server] starting on http://localhost:{port}")
    print(f"[dev_server] env: ENVIRONMENT={os.environ.get('ENVIRONMENT')} "
          f"BOTNIM_QUERY_BACKEND={os.environ.get('BOTNIM_QUERY_BACKEND')}")
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")


if __name__ == "__main__":
    main()
