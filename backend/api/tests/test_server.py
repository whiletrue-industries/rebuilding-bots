"""Tests for the live knesset_sessions_live endpoint URL fallback.

server.py imports many heavy modules (firebase_admin, custom auth, botnim
submodules) at module load time. We mirror the mocking pattern used in
``tests/backend/test_metadata_filter_endpoint.py`` — install MagicMock
placeholders in ``sys.modules`` before importing ``backend.api.server``.

CRITICAL: we pre-import the real
``botnim.document_parser.knesset_odata.process_odata`` BEFORE the heavy
``botnim`` MagicMock replacement so that the endpoint's lazy
``from botnim.document_parser.knesset_odata.process_odata import ...``
still resolves through ``sys.modules`` to the real module — that's the
module we patch in the test below.
"""
import sys
import types
from typing import Annotated, List
from unittest.mock import MagicMock, patch

# Pre-load the REAL process_odata module so sys.modules has it under
# the dotted path. The endpoint's lazy
# ``from botnim.document_parser.knesset_odata.process_odata import ...``
# resolves via sys.modules['<dotted-name>'] regardless of what
# sys.modules['botnim'] looks like, so this survives the top-level botnim
# MagicMock clobber below.
#
# Co-running tests (tests/backend/test_metadata_filter_endpoint.py et al.)
# may have already replaced sys.modules["botnim"] with a MagicMock by the
# time this module imports — which breaks ``import botnim.document_parser…``.
# To handle that, we evict any stale ``botnim*`` MagicMocks before the
# real import, then re-apply the heavy mocking below.
_stale_botnim_keys = [
    k for k in list(sys.modules)
    if (k == "botnim" or k.startswith("botnim."))
    and isinstance(sys.modules[k], MagicMock)
]
for _k in _stale_botnim_keys:
    sys.modules.pop(_k, None)
# (We don't reinstall the stale MagicMocks — the heavy mock block below
# re-creates fresh ones, which is what the existing co-running tests do
# anyway, and they each install their own at module load time.)

import botnim.document_parser.knesset_odata.process_odata as _real_process_odata  # noqa: F401,E402

# Mock all heavy server-load-time dependencies.
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

# Re-pin the real process_odata under its dotted path so the endpoint's
# lazy import resolves to the real module (which the test patches).
sys.modules["botnim.document_parser.knesset_odata.process_odata"] = _real_process_odata

sys.modules["botnim.observability.tracing"].init_tracing = MagicMock(return_value=None)
sys.modules["botnim.observability.middleware"].install_trace_middleware = MagicMock(return_value=None)

sys.modules["botnim.config"].AVAILABLE_BOTS = ["unified"]
sys.modules["botnim.config"].VALID_ENVIRONMENTS = ["staging", "production", "local"]
sys.modules["botnim.config"].DEFAULT_ENVIRONMENT = "local"

resolve_mod = types.ModuleType("resolve_firebase_user")
resolve_mod.FireBaseUser = Annotated[dict, lambda: None]
sys.modules["resolve_firebase_user"] = resolve_mod

refresh_auth_mod = types.ModuleType("refresh_auth")
refresh_auth_mod.require_refresh_api_key = lambda: None
sys.modules["refresh_auth"] = refresh_auth_mod
sanity_auth_mod = types.ModuleType("sanity_auth")
sanity_auth_mod.require_sanity_api_key = lambda: None
sys.modules["sanity_auth"] = sanity_auth_mod

from pydantic import BaseModel, Field


class _StubWordDocSection(BaseModel):
    heading: str = Field(..., min_length=1)
    level: int = 1
    body_md: str = Field(..., min_length=1)


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
word_doc_storage.upload_word_doc = lambda **_: _StubWordDocResponse(
    url="https://example.com/stub", filename="stub.docx", expires_at="2099-01-01T00:00:00Z",
)
sys.modules["botnim.word_doc"] = word_doc_pkg
sys.modules["botnim.word_doc.models"] = word_doc_models
sys.modules["botnim.word_doc.render"] = word_doc_render
sys.modules["botnim.word_doc.storage"] = word_doc_storage

mock_search_modes = sys.modules["botnim.vector_store.search_modes"]
mock_search_modes.SEARCH_MODES = {}
mock_search_modes.DEFAULT_SEARCH_MODE = MagicMock(num_results=5)

sys.modules["botnim.query"].run_query = MagicMock(return_value="mock results")
sys.modules["botnim.query"].government_distribution_sidecar = MagicMock(return_value=None)

from fastapi.testclient import TestClient

from backend.api.server import app  # noqa: E402

client = TestClient(app)


def _fake_sessions():
    return [
        {"PlenumSessionID": 2241628, "Number": 383, "KnessetNum": 25,
         "Name": "ישיבה רגילה", "StartDate": "2026-03-25T11:00:00",
         "FinishDate": "2026-03-26T10:23:10", "IsSpecialMeeting": False},
        {"PlenumSessionID": 2256195, "Number": 390, "KnessetNum": 25,
         "Name": "ישיבה רגילה", "StartDate": "2026-05-13T11:00:00",
         "FinishDate": "", "IsSpecialMeeting": False},
    ]


def _fake_stenograms():
    # Only the past session has a stenogram.
    return [{"PlenumSessionID": 2241628,
             "FilePath": "https://fs.knesset.gov.il/25/plenum/25_st_383.doc",
             "LastUpdatedDate": "2026-03-27T00:00:00"}]


def test_knesset_sessions_live_populates_source_url_for_all_sessions():
    """Future sessions get the session-detail URL as source_url fallback."""
    with patch(
        "botnim.document_parser.knesset_odata.process_odata.fetch_plenum_sessions",
        return_value=_fake_sessions(),
    ), patch(
        "botnim.document_parser.knesset_odata.process_odata.fetch_session_items",
        return_value=[],
    ), patch(
        "botnim.document_parser.knesset_odata.process_odata.fetch_session_stenograms",
        return_value=_fake_stenograms(),
    ):
        resp = client.get(
            "/botnim/knesset/sessions",
            params={"from": "2026-03-01", "to": "2026-06-01", "include_items": "false"},
        )

    assert resp.status_code == 200
    body = resp.json()
    by_session = {s["PlenumSessionID"]: s for s in body["sessions"]}
    assert by_session[2241628]["source_url"] == (
        "https://fs.knesset.gov.il/25/plenum/25_st_383.doc"
    )
    # The new behaviour: future session also has source_url.
    assert by_session[2256195]["source_url"] == (
        "https://www.knesset.gov.il/plenum/heb/sessionDet.aspx?SessionID=2256195"
    )
