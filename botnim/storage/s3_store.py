"""S3Store — boto3-backed ArtifactStore for staging / prod.

Client construction mirrors botnim/word_doc/storage.py: a regional
client (il-central-1 requires the regional endpoint for SigV4). Creds
come from the default chain (ECS task role) unless an explicit client
is injected (used by tests).
"""
from __future__ import annotations

import io
from typing import BinaryIO, List, Optional

import boto3
from botocore.exceptions import ClientError


class S3Store:
    def __init__(
        self,
        bucket: str,
        *,
        region_name: Optional[str] = None,
        client: Optional[object] = None,
    ) -> None:
        if not bucket:
            raise ValueError("bucket must be non-empty")
        self._bucket = bucket
        if client is not None:
            self._client = client
        else:
            self._client = boto3.client("s3", region_name=region_name)

    def get_bytes(self, key: str) -> bytes:
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if _is_not_found(exc):
                raise FileNotFoundError(key) from exc
            raise
        expected = resp.get("ContentLength")
        data = resp["Body"].read()
        if expected is not None and len(data) != expected:
            raise OSError(
                f"short read for {key!r}: read {len(data)} of {expected} bytes"
            )
        return data

    def open_stream(self, key: str) -> BinaryIO:
        # Read side consumes file-likes; we buffer the verified bytes so
        # callers get a seekable stream and so the short-read guard runs.
        return io.BytesIO(self.get_bytes(key))

    def put_atomic(self, key: str, data: bytes) -> None:
        # S3 PutObject is itself atomic — a reader sees either the old
        # object or the fully-written new one, never a partial body.
        self._client.put_object(Bucket=self._bucket, Key=key, Body=data)

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as exc:
            if _is_not_found(exc):
                return False
            raise

    def list(self, prefix: str) -> List[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        keys: List[str] = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys


def _is_not_found(exc: "ClientError") -> bool:
    """True for the several shapes S3 uses to signal a missing key.

    get_object → NoSuchKey; head_object → 404 / NotFound. moto and real
    S3 differ in which they raise, so check both code and HTTP status.
    """
    err = exc.response.get("Error", {})
    code = err.get("Code")
    if code in ("NoSuchKey", "NotFound", "404"):
        return True
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return status == 404
