"""API client unit tests — every external call mocked.

The real curl_cffi session is constructed once via ``GovIlClient()``
and patched in tests with a MagicMock that records call args. We assert
the URL shape, the params (CollectorType list, Type GUID, skip/limit),
and that 404s on ``fetch_content`` are surfaced as ``None`` rather than
exceptions (some pages 404 because they were retracted — Tal's guide
calls this out).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from botnim.document_parser.gov_il_decisions.api import (
    GOV_RESOLUTIONS_TYPE,
    GovIlClient,
)


def _ok_json(payload):
    resp = MagicMock()
    resp.json.return_value = payload
    resp.status_code = 200
    resp.raise_for_status = MagicMock()
    return resp


def _status(code, content=b""):
    resp = MagicMock()
    resp.status_code = code
    resp.content = content
    resp.raise_for_status = MagicMock()
    return resp


def test_list_decisions_url_and_params():
    client = GovIlClient()
    fake_session = MagicMock()
    fake_session.get.return_value = _ok_json({"total": 25800, "results": []})
    client._session = fake_session

    out = client.list_decisions(skip=100, limit=50)

    assert out == {"total": 25800, "results": []}
    fake_session.get.assert_called_once()
    args, kwargs = fake_session.get.call_args
    assert args[0] == "https://www.gov.il/CollectorsWebApi/api/DataCollector/GetResults"
    assert kwargs["params"]["CollectorType"] == ["policy", "pmopolicy"]
    assert kwargs["params"]["Type"] == GOV_RESOLUTIONS_TYPE
    assert kwargs["params"]["skip"] == 100
    assert kwargs["params"]["limit"] == 50
    assert kwargs["params"]["culture"] == "he"


def test_fetch_content_url():
    client = GovIlClient()
    fake_session = MagicMock()
    fake_session.get.return_value = _ok_json({"contentMain": {"htmlContents": []}})
    client._session = fake_session

    out = client.fetch_content("dec3994-2026")

    assert out == {"contentMain": {"htmlContents": []}}
    args, kwargs = fake_session.get.call_args
    assert args[0] == "https://www.gov.il/ContentPageWebApi/api/content-pages/dec3994-2026"
    assert kwargs["params"]["culture"] == "he"


def test_fetch_content_404_returns_none():
    client = GovIlClient()
    fake_session = MagicMock()
    fake_session.get.return_value = _status(404)
    client._session = fake_session

    assert client.fetch_content("dec-deleted") is None


def test_download_attachment_returns_bytes():
    client = GovIlClient()
    fake_session = MagicMock()
    fake_session.get.return_value = _status(200, content=b"%PDF-1.4 test")
    client._session = fake_session

    out = client.download_attachment("https://www.gov.il/BlobFolder/x.pdf")

    assert out == b"%PDF-1.4 test"


def test_session_uses_chrome_impersonation():
    # The constructor must build a curl_cffi Session with impersonate="chrome".
    # We verify by inspecting the wrapper attribute set on the session — see
    # api.py for how this is exposed.
    client = GovIlClient()
    assert client._impersonate == "chrome"
