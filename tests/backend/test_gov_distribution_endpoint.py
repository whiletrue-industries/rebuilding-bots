"""Test that the /retrieve endpoint injects a ``government_distribution``
sidecar onto the response when:

  * the caller filtered by ``decision_number`` in ``metadata_filter``, AND
  * the request targeted a ``government_decisions`` context, AND
  * the sidecar SQL probe returned >= 2 distinct governments.

The mock scaffolding at the top mirrors
``tests/backend/test_metadata_filter_endpoint.py``: all heavy botnim
dependencies must be mocked in ``sys.modules`` before ``backend.api.server``
is imported, otherwise the FastAPI module-load fails outside the Docker
container.
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
    "botnim.observability", "botnim.observability.tracing",
    "botnim.observability.middleware",
]:
    sys.modules[mod] = MagicMock()

# server.py invokes these at startup; make them no-op callables so the
# FastAPI import doesn't blow up when the module-level calls run.
sys.modules["botnim.observability.tracing"].init_tracing = MagicMock(return_value=None)
sys.modules["botnim.observability.middleware"].install_trace_middleware = MagicMock(return_value=None)

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
from pydantic import BaseModel, Field
from typing import List


class _StubWordDocSection(BaseModel):
    heading: str = Field(..., min_length=1)
    level: int = 1
    body_md: str = Field(..., min_length=1)


# Mirror the real WordDocRequest's min_length=1 constraint on sections so
# co-running tests (tests/backend/api/test_generate_word_doc.py) that expect
# 422 on an empty-sections payload still observe that semantics when this
# stub leaks into their session via sys.modules.
class _StubWordDocRequest(BaseModel):
    title: str = Field(..., min_length=1)
    sections: List[_StubWordDocSection] = Field(..., min_length=1)


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
# Return a real pydantic-conformant object so co-running word_doc tests that
# re-import server with this stub still in sys.modules don't trip the FastAPI
# response_model validator. Without this, *any* call to /tools/generate_word_doc
# would return a MagicMock that fails string-type validation on url/filename/
# expires_at and surface as a test-isolation poisoning failure in
# tests/backend/api/test_generate_word_doc.py.
word_doc_storage.upload_word_doc = lambda **_: _StubWordDocResponse(
    url="https://example.com/stub", filename="stub.docx", expires_at="2099-01-01T00:00:00Z",
)
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
# Gov-distribution sidecar: same MagicMock-on-the-module pattern as run_query.
sys.modules["botnim.query"].government_distribution_sidecar = MagicMock(return_value=None)

from unittest.mock import patch
from fastapi.testclient import TestClient

# Now import the server - all its dependencies are mocked
from backend.api.server import app

client = TestClient(app)
ENDPOINT = "/retrieve/unified/government_decisions__dev"

FAKE_DISTRIBUTION = [
    {"government_number": "36", "government": "ממשלת בנט", "doc_count": 12, "latest_publish_date": "2022-06-13"},
    {"government_number": "37", "government": "ממשלת נתניהו", "doc_count": 8, "latest_publish_date": "2023-11-19"},
]


# We patch `backend.api.server.run_query` and `.government_distribution_sidecar`
# directly (rather than reaching into `sys.modules["botnim.query"]` and mutating
# its attributes) because server.py captured both names at module-load time via
# `from botnim.query import ...`. Replacing the attribute on the sys.modules
# entry after server.py is imported no longer affects what server.py calls —
# but `patch("backend.api.server.X", ...)` does, and it auto-reverts at test
# exit so cross-test pollution stays out.
class TestGovernmentDistributionEndpoint:
    def test_sidecar_injected_into_yaml_when_multi_government(self):
        with patch("backend.api.server.run_query", return_value="- header: some result\n  text: content\n"), \
             patch("backend.api.server.government_distribution_sidecar",
                   return_value=FAKE_DISTRIBUTION):
            resp = client.get(ENDPOINT, params={
                "query": "החלטה 550",
                "metadata_filter": '{"decision_number": "550"}',
                "format": "yaml",
            })

        assert resp.status_code == 200
        body = resp.text
        assert "government_distribution" in body
        assert "36" in body

    def test_sidecar_not_injected_when_single_government(self):
        with patch("backend.api.server.run_query", return_value="- header: result\n  text: content\n"), \
             patch("backend.api.server.government_distribution_sidecar",
                   return_value=None):
            resp = client.get(ENDPOINT, params={
                "query": "החלטה 999",
                "metadata_filter": '{"decision_number": "999"}',
                "format": "yaml",
            })

        assert resp.status_code == 200
        assert "government_distribution" not in resp.text

    def test_sidecar_not_called_without_decision_number_filter(self):
        # run_query returns a string here because the /retrieve handler wraps
        # the result in starlette Response(content=...), which requires
        # bytes/str — list payloads (dict format pre-serialization) crash
        # render(). The semantic point of this test is the sidecar dispatch
        # decision, not the run_query payload shape.
        with patch("backend.api.server.run_query", return_value="- header: result\n"), \
             patch("backend.api.server.government_distribution_sidecar") as mock_sidecar:
            resp = client.get(ENDPOINT, params={
                "query": "מה המדיניות",
                "format": "dict",
            })

            assert resp.status_code == 200
            mock_sidecar.assert_not_called()

    def test_sidecar_not_called_for_non_gov_decisions_context(self):
        # See test_sidecar_not_called_without_decision_number_filter for why
        # we return a string here rather than [].
        with patch("backend.api.server.run_query", return_value="- header: result\n"), \
             patch("backend.api.server.government_distribution_sidecar") as mock_sidecar:
            resp = client.get("/retrieve/unified/common_knowledge__dev", params={
                "query": "מסמך",
                "metadata_filter": '{"decision_number": "550"}',
            })

            assert resp.status_code == 200
            mock_sidecar.assert_not_called()
