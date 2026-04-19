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

# Mock all heavy dependencies before any imports
for mod in [
    "firebase_admin", "firebase_admin.firestore", "firebase_admin.credentials",
    "firebase_admin.auth",
    "dataflows", "dataflows_airtable",
    "botnim", "botnim.collect_sources", "botnim.vector_store",
    "botnim.vector_store.vector_store_base", "botnim.vector_store.vector_store_openai",
    "botnim.vector_store.vector_store_es", "botnim.vector_store.search_modes",
    "botnim.query",
]:
    sys.modules[mod] = MagicMock()

# Create a proper resolve_firebase_user module with a real type annotation
resolve_mod = types.ModuleType("resolve_firebase_user")
resolve_mod.FireBaseUser = Annotated[dict, lambda: None]  # simple annotation
sys.modules["resolve_firebase_user"] = resolve_mod

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
