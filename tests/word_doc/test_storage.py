"""Tests for S3 upload + presigned URL helper."""
from __future__ import annotations

import os
from datetime import timezone
from urllib.parse import urlparse, parse_qs

import boto3
import pytest
from moto import mock_aws


@mock_aws
def test_upload_writes_object_and_returns_url():
    os.environ["AWS_DEFAULT_REGION"] = "il-central-1"
    s3 = boto3.client("s3", region_name="il-central-1")
    s3.create_bucket(
        Bucket="botnim-word-docs-test",
        CreateBucketConfiguration={"LocationConstraint": "il-central-1"},
    )

    from botnim.word_doc.storage import upload_word_doc

    res = upload_word_doc(
        bucket="botnim-word-docs-test",
        body=b"PK fake docx content",
        filename="א.docx",
        s3_client=s3,
    )

    assert res.url.startswith("https://")
    parsed = urlparse(res.url)
    qs = parse_qs(parsed.query)
    assert "X-Amz-Signature" in qs or "AWSAccessKeyId" in qs
    assert "ResponseContentDisposition" in qs or "response-content-disposition" in qs
    # 7-day expiry — in the future
    from datetime import datetime, timedelta
    delta = res.expires_at - datetime.now(timezone.utc).replace(microsecond=0)
    assert timedelta(days=6, hours=23) < delta < timedelta(days=7, minutes=1)


@mock_aws
def test_upload_object_actually_present():
    os.environ["AWS_DEFAULT_REGION"] = "il-central-1"
    s3 = boto3.client("s3", region_name="il-central-1")
    s3.create_bucket(
        Bucket="bkt",
        CreateBucketConfiguration={"LocationConstraint": "il-central-1"},
    )
    from botnim.word_doc.storage import upload_word_doc
    res = upload_word_doc(bucket="bkt", body=b"x", filename="y.docx", s3_client=s3)

    # Extract key from URL
    parsed = urlparse(res.url)
    key = parsed.path.lstrip("/")
    if key.startswith("bkt/"):  # path-style
        key = key[len("bkt/"):]

    obj = s3.get_object(Bucket="bkt", Key=key)
    assert obj["Body"].read() == b"x"
