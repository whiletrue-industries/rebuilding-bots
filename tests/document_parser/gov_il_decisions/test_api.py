"""API client unit tests — every external call mocked.

The real curl_cffi session is constructed via ``GovIlClient()`` and
patched in tests with a MagicMock that records call args. Tests pass
``warmup=False`` so the lazy cookie-warmup GET doesn't touch the network
(and doesn't perturb the ``get`` call count) — except the dedicated
warmup test, which asserts the warmup fires.

We assert the (post-2026-05-migration) gateway URL shape, params
(CollectorType list, Type GUID, skip/limit), the required ``x-client-id``
header, that 404s on ``fetch_content`` surface as ``None``, and that a
non-JSON 200 body raises ``GovIlApiError`` instead of an opaque
JSONDecodeError.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from botnim.document_parser.gov_il_decisions.api import (
    DEFAULT_CLIENT_ID,
    GOV_RESOLUTIONS_TYPE,
    GovIlClient,
    _WARMUP_URL,
)
from botnim.document_parser.gov_il_decisions.exceptions import GovIlApiError


def _ok_json(payload, ctype="application/json; charset=utf-8"):
    resp = MagicMock()
    resp.json.return_value = payload
    resp.status_code = 200
    resp.headers = {"content-type": ctype}
    resp.text = "{}"
    resp.raise_for_status = MagicMock()
    return resp


def _html(status=200, body="<!DOCTYPE html><html><head></head></html>"):
    resp = MagicMock()
    resp.status_code = status
    resp.headers = {"content-type": "text/html; charset=utf-8"}
    resp.text = body
    resp.raise_for_status = MagicMock()
    return resp


def _status(code, content=b""):
    resp = MagicMock()
    resp.status_code = code
    resp.content = content
    resp.raise_for_status = MagicMock()
    return resp


def test_list_decisions_url_and_params():
    client = GovIlClient(warmup=False)
    fake_session = MagicMock()
    fake_session.get.return_value = _ok_json({"total": 25800, "results": []})
    client._session = fake_session

    out = client.list_decisions(skip=100, limit=50)

    assert out == {"total": 25800, "results": []}
    fake_session.get.assert_called_once()
    args, kwargs = fake_session.get.call_args
    assert args[0] == (
        "https://openapi-gc.digital.gov.il/pub/cio/govil/rest"
        "/collectors/v1/api/DataCollector/GetResults"
    )
    assert kwargs["params"]["CollectorType"] == ["policy", "pmopolicy"]
    assert kwargs["params"]["Type"] == GOV_RESOLUTIONS_TYPE
    assert kwargs["params"]["skip"] == 100
    assert kwargs["params"]["limit"] == 50
    assert kwargs["params"]["culture"] == "he"


def test_requests_carry_client_id_and_origin_headers():
    client = GovIlClient(warmup=False)
    fake_session = MagicMock()
    fake_session.get.return_value = _ok_json({"total": 0, "results": []})
    client._session = fake_session

    client.list_decisions(skip=0, limit=5)

    _, kwargs = fake_session.get.call_args
    headers = kwargs["headers"]
    assert headers["x-client-id"] == DEFAULT_CLIENT_ID
    assert headers["Referer"] == "https://www.gov.il/"
    assert headers["Origin"] == "https://www.gov.il"


def test_client_id_overridable_via_env(monkeypatch):
    monkeypatch.setenv("GOV_IL_CLIENT_ID", "ROTATED-KEY-123")
    client = GovIlClient(warmup=False)
    fake_session = MagicMock()
    fake_session.get.return_value = _ok_json({"total": 0, "results": []})
    client._session = fake_session

    client.list_decisions(skip=0, limit=5)

    _, kwargs = fake_session.get.call_args
    assert kwargs["headers"]["x-client-id"] == "ROTATED-KEY-123"


def test_fetch_content_url():
    client = GovIlClient(warmup=False)
    fake_session = MagicMock()
    fake_session.get.return_value = _ok_json({"contentMain": {"htmlContents": []}})
    client._session = fake_session

    out = client.fetch_content("dec3994-2026")

    assert out == {"contentMain": {"htmlContents": []}}
    args, kwargs = fake_session.get.call_args
    assert args[0] == (
        "https://openapi-gc.digital.gov.il/pub/cio/govil/rest"
        "/contentpage/v1/api/content-pages/dec3994-2026"
    )
    assert kwargs["params"]["culture"] == "he"


def test_fetch_content_404_returns_none():
    client = GovIlClient(warmup=False)
    fake_session = MagicMock()
    fake_session.get.return_value = _status(404)
    client._session = fake_session

    assert client.fetch_content("dec-deleted") is None


def test_list_decisions_non_json_raises_gov_il_api_error():
    """A 200 with an HTML body (endpoint moved / WAF block) must raise a
    clear GovIlApiError, NOT the opaque JSONDecodeError that hid the
    2026-05 migration for a month."""
    client = GovIlClient(warmup=False)
    fake_session = MagicMock()
    fake_session.get.return_value = _html()
    client._session = fake_session

    with pytest.raises(GovIlApiError, match="non-JSON"):
        client.list_decisions(skip=0, limit=5)


def test_fetch_content_non_json_raises_gov_il_api_error():
    client = GovIlClient(warmup=False)
    fake_session = MagicMock()
    fake_session.get.return_value = _html()
    client._session = fake_session

    with pytest.raises(GovIlApiError, match="non-JSON"):
        client.fetch_content("dec3994-2026")


def test_download_attachment_returns_bytes():
    client = GovIlClient(warmup=False)
    fake_session = MagicMock()
    fake_session.get.return_value = _status(200, content=b"%PDF-1.4 test")
    client._session = fake_session

    out = client.download_attachment("https://www.gov.il/BlobFolder/x.pdf")

    assert out == b"%PDF-1.4 test"


def test_warmup_seeds_cookie_once_before_first_call():
    """warmup=True: the first real call issues a warmup GET to the gateway
    root first, and it happens exactly once across multiple calls."""
    client = GovIlClient(warmup=True, delay_seconds=0)
    fake_session = MagicMock()
    fake_session.get.return_value = _ok_json({"total": 0, "results": []})
    client._session = fake_session

    client.list_decisions(skip=0, limit=5)
    assert fake_session.get.call_count == 2  # warmup + listing
    assert fake_session.get.call_args_list[0].args[0] == _WARMUP_URL

    client.list_decisions(skip=5, limit=5)
    assert fake_session.get.call_count == 3  # +listing only, no 2nd warmup


def test_warmup_failure_is_non_fatal():
    """A raising warmup GET must not abort the run — the real call proceeds."""
    client = GovIlClient(warmup=True, delay_seconds=0)
    fake_session = MagicMock()
    fake_session.get.side_effect = [
        ConnectionError("warmup boom"),
        _ok_json({"total": 1, "results": []}),
    ]
    client._session = fake_session

    out = client.list_decisions(skip=0, limit=5)
    assert out == {"total": 1, "results": []}


def test_session_uses_chrome_impersonation():
    # The constructor must build a curl_cffi Session with impersonate="chrome".
    client = GovIlClient(warmup=False)
    assert client._impersonate == "chrome"
