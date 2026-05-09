"""Lambda entrypoint that invokes botnim-api /botnim/admin/sanity.

Triggered by EventBridge Schedule (cron 03:00 + 12:00 UTC). Reads the
admin API key from Secrets Manager, POSTs, raises on non-2xx so the
Lambda Errors metric increments.
"""
from __future__ import annotations

import logging
import os
import socket
import urllib.error
import urllib.parse
import urllib.request

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SECRET_ARN = os.environ["SANITY_ADMIN_API_KEY_SECRET_ARN"]
ENDPOINT_URL = os.environ["SANITY_ENDPOINT_URL"]


def _diagnose_endpoint(url: str) -> str:
    """Resolve and TCP-probe the endpoint host before opening the HTTP
    connection. Lambda VPC + Route53 split-horizon DNS quirks tend to
    surface as opaque urlopen errors (e.g. EBUSY on connect when the
    name resolves to an IP that isn't reachable from the Lambda ENI).
    Returning a structured trace string up front makes the actual
    failure mode visible in CloudWatch instead of buried in a stack
    trace inside `do_open`.
    """
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname or ""
    port = parsed.port or (443 if parsed.scheme == "https" else 80)

    # Read the Lambda runtime's resolver config — useful if getaddrinfo
    # blows up with EBUSY (often filesystem contention reading these).
    resolv = ""
    try:
        with open("/etc/resolv.conf", "r") as f:
            resolv = f.read().strip()
    except OSError as e:
        resolv = f"<resolv.conf read failed: {e}>"

    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        ips = sorted({info[4][0] for info in infos})
        return f"DNS_OK host={host} port={port} ips={ips} resolv={resolv!r}"
    except OSError as e:
        return f"DNS_FAIL host={host} errno={getattr(e, 'errno', '?')} err={e!r} resolv={resolv!r}"


def handler(event, context):  # noqa: ARG001 (Lambda entrypoint signature)
    sm = boto3.client("secretsmanager")
    secret = sm.get_secret_value(SecretId=SECRET_ARN)
    api_key = secret["SecretString"]

    diag = _diagnose_endpoint(ENDPOINT_URL)
    logger.info(f"endpoint diag: {diag}")

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
        logger.error(f"sanity endpoint returned {e.code}: {body}")
        raise
    except (urllib.error.URLError, OSError) as e:
        logger.error(f"sanity endpoint network error: {e!r}")
        raise

    logger.info(f"sanity endpoint accepted: status={status} body={body}")
    return {"status": status, "body": body}
