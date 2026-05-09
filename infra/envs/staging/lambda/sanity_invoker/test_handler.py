"""Smoke tests for the sanity invoker Lambda handler.

Same shape as refresh_invoker/test_handler.py; mocks boto3 + urllib.
"""
from __future__ import annotations

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _path_and_env(monkeypatch: pytest.MonkeyPatch):
    here = os.path.dirname(__file__)
    sys.path.insert(0, here)
    monkeypatch.setenv("SANITY_ADMIN_API_KEY_SECRET_ARN", "arn:aws:secretsmanager:il-central-1:000:secret:x")
    monkeypatch.setenv("SANITY_ENDPOINT_URL", "https://botnim.staging.build-up.team/botnim/admin/sanity")
    # Clear cached module between tests so env vars are re-read at import time
    sys.modules.pop("handler", None)


def _fake_secrets_client(secret_string: str = "s3cret") -> MagicMock:
    client = MagicMock()
    client.get_secret_value.return_value = {"SecretString": secret_string}
    return client


def test_happy_path_posts_with_x_api_key_header():
    import handler  # noqa: WPS433 (local import for monkeypatched env)

    mock_response = MagicMock()
    mock_response.status = 202
    mock_response.read.return_value = b'{"status":"accepted"}'
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=None)

    with patch.object(handler.boto3, "client", return_value=_fake_secrets_client("s3cret")) as mock_boto, \
         patch.object(handler.urllib.request, "urlopen", return_value=mock_response) as mock_urlopen:
        result = handler.handler(event={}, context=None)

    assert result["status"] == 202
    assert json.loads(result["body"])["status"] == "accepted"
    # boto3 client called for secretsmanager
    assert mock_boto.call_args.args == ("secretsmanager",)
    # urllib.request.Request passed as first arg to urlopen; grab it
    req = mock_urlopen.call_args.args[0]
    assert req.full_url == "https://botnim.staging.build-up.team/botnim/admin/sanity"
    assert req.get_method() == "POST"
    assert req.headers.get("X-api-key") == "s3cret"


def test_non_2xx_raises_so_lambda_error_metric_fires():
    import handler

    import urllib.error

    bad_response = MagicMock()
    bad_response.status = 500
    bad_response.read.return_value = b"internal error"
    http_error = urllib.error.HTTPError(
        url="https://botnim.staging.build-up.team/botnim/admin/sanity",
        code=500,
        msg="Internal Server Error",
        hdrs=None,  # type: ignore[arg-type]
        fp=None,  # type: ignore[arg-type]
    )
    http_error.read = MagicMock(return_value=b"internal error")

    with patch.object(handler.boto3, "client", return_value=_fake_secrets_client()), \
         patch.object(handler.urllib.request, "urlopen", side_effect=http_error):
        with pytest.raises(urllib.error.HTTPError):
            handler.handler(event={}, context=None)
