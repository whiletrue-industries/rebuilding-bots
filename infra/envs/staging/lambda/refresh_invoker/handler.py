"""Lambda entrypoint that invokes botnim-api /admin/refresh.

Triggered by EventBridge Schedule. Reads the admin API key from Secrets
Manager, POSTs to the refresh endpoint via VPC (Service Connect), and
raises on non-2xx so Lambda's built-in `Errors` metric increments — an
independent CloudWatch alarm on that metric covers the case where the
in-API logging path can't fire because the API task itself is down.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SECRET_ARN = os.environ["ADMIN_API_KEY_SECRET_ARN"]
ENDPOINT_URL = os.environ["REFRESH_ENDPOINT_URL"]


def handler(event, context):  # noqa: ARG001 (Lambda entrypoint signature)
    sm = boto3.client("secretsmanager")
    secret = sm.get_secret_value(SecretId=SECRET_ARN)
    api_key = secret["SecretString"]

    req = urllib.request.Request(
        url=ENDPOINT_URL,
        method="POST",
        headers={
            "X-API-Key": api_key,
            "Content-Type": "application/json",
        },
        data=b"{}",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.status
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        status = e.code
        logger.error(f"refresh endpoint returned {status}: {body}")
        raise RuntimeError(f"refresh endpoint returned {status}: {body}") from e
    except urllib.error.URLError as e:
        logger.error(f"refresh endpoint unreachable: {e}")
        raise

    logger.info(f"refresh endpoint returned {status}: {body}")
    if status < 200 or status >= 300:
        raise RuntimeError(f"refresh endpoint returned {status}: {body}")
    return {"statusCode": status, "body": body}
