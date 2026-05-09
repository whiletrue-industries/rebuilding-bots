"""Test that query.py error handling returns proper HTTP status codes.

Verifies DoD criteria:
  1. HTTP 500/502/504 on actual errors (not 200 with error text)
  2. Structured error details in response
  3. Happy path unchanged

We mock the entire botnim package since it has heavy dependencies (dataflows,
elasticsearch, etc.) that aren't available outside the Docker container.
We only need to test the server.py routing logic, not the actual search.
"""
import sys
import types
from typing import Annotated
from unittest.mock import MagicMock

# Mock all heavy dependencies before any imports.
# server.py imports several botnim submodules at module load time; each must be
# present in sys.modules or `from botnim.X import Y` raises ModuleNotFoundError
# because MagicMock doesn't behave as a package for submodule resolution.
for mod in [
    "firebase_admin", "firebase_admin.firestore", "firebase_admin.credentials",
    "firebase_admin.auth",
    "dataflows", "dataflows_airtable",
    "botnim", "botnim.collect_sources", "botnim.vector_store",
    "botnim.vector_store.vector_store_base", "botnim.vector_store.vector_store_openai",
    "botnim.vector_store.vector_store_es", "botnim.vector_store.search_modes",
    "botnim.query",
    "botnim.bot_config", "botnim.config",
    "botnim.fetch_and_process", "botnim.sync",
    "botnim.db", "botnim.db.session",
]:
    sys.modules[mod] = MagicMock()

# server.py uses a few names from botnim.config as plain values (not callables);
# make them concrete so module-load-time references behave.
sys.modules["botnim.config"].AVAILABLE_BOTS = ["unified"]
sys.modules["botnim.config"].VALID_ENVIRONMENTS = ["staging", "production", "local"]
sys.modules["botnim.config"].DEFAULT_ENVIRONMENT = "local"

# Create a proper resolve_firebase_user module with a real type annotation
resolve_mod = types.ModuleType("resolve_firebase_user")
resolve_mod.FireBaseUser = Annotated[dict, lambda: None]  # simple annotation
sys.modules["resolve_firebase_user"] = resolve_mod

# refresh_auth + sanity_auth are top-level modules imported by server.py from
# its own directory at runtime. Mock their public surface so the import
# resolves; we only test request-routing here, not auth.
refresh_auth_mod = types.ModuleType("refresh_auth")
refresh_auth_mod.require_refresh_api_key = lambda: None
sys.modules["refresh_auth"] = refresh_auth_mod
sanity_auth_mod = types.ModuleType("sanity_auth")
sanity_auth_mod.require_sanity_api_key = lambda: None
sys.modules["sanity_auth"] = sanity_auth_mod

# botnim.word_doc.* — server.py uses WordDocResponse as FastAPI response_model,
# which requires a real pydantic model class (not a MagicMock). Provide
# minimal real BaseModel subclasses so the FastAPI route registration succeeds;
# render/storage are never invoked in these routing tests, so MagicMock-style
# attributes are fine.
from pydantic import BaseModel
from typing import List


class _StubWordDocSection(BaseModel):
    heading: str
    level: int = 1
    body_md: str = ""


class _StubWordDocRequest(BaseModel):
    title: str
    sections: List[_StubWordDocSection]


class _StubWordDocResponse(BaseModel):
    url: str
    filename: str
    expires_at: str


word_doc_pkg = types.ModuleType("botnim.word_doc")
word_doc_models = types.ModuleType("botnim.word_doc.models")
word_doc_models.WordDocRequest = _StubWordDocRequest
word_doc_models.WordDocResponse = _StubWordDocResponse
word_doc_render = types.ModuleType("botnim.word_doc.render")
word_doc_render.render_word_doc = MagicMock(return_value=b"")
word_doc_render.sanitize_filename = lambda s: "stub.docx"
word_doc_storage = types.ModuleType("botnim.word_doc.storage")
word_doc_storage.upload_word_doc = MagicMock()
sys.modules["botnim.word_doc"] = word_doc_pkg
sys.modules["botnim.word_doc.models"] = word_doc_models
sys.modules["botnim.word_doc.render"] = word_doc_render
sys.modules["botnim.word_doc.storage"] = word_doc_storage

# Now set up the search modes mock properly
mock_search_modes = sys.modules["botnim.vector_store.search_modes"]
mock_search_modes.SEARCH_MODES = {}
mock_search_modes.DEFAULT_SEARCH_MODE = MagicMock(num_results=5)

# And mock run_query as a proper function we can patch
mock_run_query = MagicMock(return_value="mock results")
sys.modules["botnim.query"].run_query = mock_run_query

import logging
import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient

# Now import the server - all its dependencies are mocked
from backend.api.server import app

client = TestClient(app)


class TestSearchErrorHandling:
    """Verify error propagation from run_query through the HTTP layer."""

    def test_happy_path_returns_200(self):
        """Happy path: successful search returns 200 with results."""
        with patch("backend.api.server.run_query", return_value="result1\nresult2\n"):
            response = client.get("/retrieve/unified/common_knowledge?query=test")

        assert response.status_code == 200
        assert "result1" in response.text

    def test_general_exception_returns_500(self):
        """General exceptions from run_query should return HTTP 500."""
        with patch("backend.api.server.run_query",
                   side_effect=RuntimeError("ES index not found: unified__fake")):
            response = client.get("/retrieve/unified/fake?query=test")

        assert response.status_code == 500
        body = response.json()
        assert body["error"] == "search_error"
        assert "ES index not found" in body["detail"]
        assert body["store_id"] == "unified__fake"

    def test_connection_error_returns_502(self):
        """ConnectionError (ES down) should return HTTP 502."""
        with patch("backend.api.server.run_query",
                   side_effect=ConnectionError("Connection refused: localhost:9200")):
            response = client.get("/retrieve/unified/common_knowledge?query=test")

        assert response.status_code == 502
        body = response.json()
        assert body["error"] == "upstream_connection_error"
        assert "Connection refused" in body["detail"]

    def test_timeout_error_returns_504(self):
        """TimeoutError should return HTTP 504."""
        with patch("backend.api.server.run_query",
                   side_effect=TimeoutError("Search timed out after 30s")):
            response = client.get("/retrieve/unified/common_knowledge?query=test")

        assert response.status_code == 504
        body = response.json()
        assert body["error"] == "search_timeout"
        assert "timed out" in body["detail"]

    def test_deadline_exceeded_uses_synthesized_detail(self):
        """An empty-message TimeoutError (e.g. raised by asyncio.wait_for on
        deadline) must fall back to a synthesized detail that names the
        deadline + the METADATA_BROWSE escape hatch — otherwise the LLM has
        no signal to recover from."""
        # asyncio.TimeoutError is an alias for TimeoutError on 3.11+, so the
        # empty-message constructor models exactly what wait_for raises.
        with patch("backend.api.server.run_query", side_effect=TimeoutError()):
            response = client.get("/retrieve/unified/common_knowledge?query=test")

        assert response.status_code == 504
        body = response.json()
        assert body["error"] == "search_timeout"
        assert "deadline exceeded" in body["detail"]
        assert "METADATA_BROWSE" in body["detail"]
        assert body["store_id"] == "unified__common_knowledge"

    def test_error_response_is_structured_json(self):
        """Error responses must be structured JSON with error, detail, store_id."""
        with patch("backend.api.server.run_query",
                   side_effect=ValueError("Invalid query format")):
            response = client.get("/retrieve/unified/legal_text?query=bad")

        assert response.status_code == 500
        body = response.json()
        assert set(body.keys()) == {"error", "detail", "store_id"}
        assert body["store_id"] == "unified__legal_text"

    def test_error_not_returned_as_200(self):
        """Critical: errors must NOT return HTTP 200 (the bug we're fixing)."""
        with patch("backend.api.server.run_query",
                   side_effect=Exception("Something broke")):
            response = client.get("/retrieve/unified/common_knowledge?query=test")

        # This was the bug: errors returned 200 with error text
        assert response.status_code != 200
        # Should NOT contain the old-style plain text error
        assert not response.text.startswith("Error performing search:")


class TestAdvisoryLock:
    """The daily-refresh background job must coordinate across multiple
    Fargate tasks via a postgres advisory lock — exactly one task runs the
    pipeline per Lambda invocation."""

    def _build_engine_with_lock(self, lock_acquired: bool):
        """Build a fake SQLAlchemy engine whose pg_try_advisory_lock returns
        the supplied bool. Returns (engine_mock, conn_mock) so tests can
        inspect the calls made on the connection."""
        conn = MagicMock()
        # pg_try_advisory_lock → bool, pg_advisory_unlock → None.
        # We assume callers run them in this order; conn.execute(...).scalar()
        # always returns the lock_acquired flag (only the first call uses
        # scalar() so this is fine).
        conn.execute.return_value.scalar.return_value = lock_acquired
        engine = MagicMock()
        engine.connect.return_value = conn
        return engine, conn

    def test_runs_body_when_lock_acquired(self, caplog):
        from backend.api import server as server_module
        engine, conn = self._build_engine_with_lock(True)

        # Patch the get_engine function on the mocked db.session module so
        # the helper's deferred `from botnim.db.session import get_engine`
        # picks up our fake.
        with patch.object(sys.modules["botnim.db.session"], "get_engine", return_value=engine):
            calls = []
            server_module._try_run_with_advisory_lock(0xDEADBEEF, "TEST", lambda: calls.append("ran"))

        assert calls == ["ran"], "body fn must run when lock is acquired"
        # Connection must be closed at the end.
        conn.close.assert_called_once()

    def test_skips_body_when_lock_held_by_other(self, caplog):
        from backend.api import server as server_module
        engine, conn = self._build_engine_with_lock(False)

        with caplog.at_level(logging.INFO):
            with patch.object(sys.modules["botnim.db.session"], "get_engine", return_value=engine):
                calls = []
                server_module._try_run_with_advisory_lock(0xDEADBEEF, "TEST", lambda: calls.append("ran"))

        assert calls == [], "body fn must NOT run when another task holds the lock"
        assert "TEST_SKIPPED" in caplog.text, "must log SKIPPED so operators can see why this task did nothing"
        conn.close.assert_called_once()

    def test_releases_lock_even_if_body_raises(self):
        from backend.api import server as server_module
        engine, conn = self._build_engine_with_lock(True)

        def boom():
            raise RuntimeError("body crashed")

        with patch.object(sys.modules["botnim.db.session"], "get_engine", return_value=engine):
            with pytest.raises(RuntimeError, match="body crashed"):
                server_module._try_run_with_advisory_lock(0xDEADBEEF, "TEST", boom)

        # Verify pg_advisory_unlock was called and conn was closed even on raise.
        unlock_calls = [c for c in conn.execute.call_args_list
                        if any("pg_advisory_unlock" in str(a) for a in c.args)]
        assert unlock_calls, "must call pg_advisory_unlock even when body raises"
        conn.close.assert_called_once()
