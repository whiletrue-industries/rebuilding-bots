"""Unit tests for the refresh-invoker Lambda handler."""
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
    monkeypatch.setenv("ADMIN_API_KEY_SECRET_ARN", "arn:aws:secretsmanager:il-central-1:000:secret:x")
    monkeypatch.setenv("REFRESH_ENDPOINT_URL", "http://botnim-api:8000/botnim/admin/refresh")
    # Clear cached module
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

    assert result["statusCode"] == 202
    # boto3 client called for secretsmanager
    assert mock_boto.call_args.args == ("secretsmanager",)
    # urllib.request.Request passed as first arg to urlopen; grab it
    req = mock_urlopen.call_args.args[0]
    assert req.full_url == "http://botnim-api:8000/botnim/admin/refresh"
    assert req.get_method() == "POST"
    assert req.headers.get("X-api-key") == "s3cret"


def test_non_202_raises_so_lambda_error_metric_fires():
    import handler

    bad_response = MagicMock()
    bad_response.status = 500
    bad_response.read.return_value = b"nope"
    bad_response.__enter__ = MagicMock(return_value=bad_response)
    bad_response.__exit__ = MagicMock(return_value=None)

    with patch.object(handler.boto3, "client", return_value=_fake_secrets_client()), \
         patch.object(handler.urllib.request, "urlopen", return_value=bad_response):
        with pytest.raises(RuntimeError):
            handler.handler(event={}, context=None)
