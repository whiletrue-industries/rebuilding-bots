"""Repo-root conftest.

Adds the repo root to sys.path so tests can import `backend.api.server`
(the FastAPI server module that runs at /srv/backend/api at runtime), and
pre-imports the `backend` and `backend.api` packages to seed sys.modules.

Without the explicit pre-import, pytest's collection-time importer can
fail to discover `backend.api` even with sys.path configured correctly —
because the `tests/` package and `backend/` package live as siblings under
the repo root and pytest's rootpath-based discovery prefers the test
module's parent. Pre-importing the packages here puts them in sys.modules
so subsequent `from backend.api.server import app` resolves directly.
"""
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Seed sys.modules. These are empty __init__.py packages — importing them
# has no side effects beyond registration.
import backend  # noqa: F401,E402
import backend.api  # noqa: F401,E402
