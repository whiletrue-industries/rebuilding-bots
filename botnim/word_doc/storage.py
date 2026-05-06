"""S3 upload + presigned URL helper for word-doc artifacts."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote

import boto3

from .models import WordDocResponse


_PRESIGN_TTL = 7 * 24 * 3600  # 7 days


def upload_word_doc(
    *,
    bucket: str,
    body: bytes,
    filename: str,
    s3_client: Optional[object] = None,
) -> WordDocResponse:
    """PUT body into the env-scoped bucket; return a presigned-URL response.

    Key shape: `<uuid4>/<filename>`. The UUID prefix collision-proofs
    concurrent generations and stops one user from guessing another's
    URL by title.
    """
    if not bucket:
        raise RuntimeError("WORD_DOCS_BUCKET is not set")

    if s3_client is None:
        s3_client = boto3.client("s3")

    key = f"{uuid.uuid4().hex}/{filename}"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        ),
    )

    encoded_filename = quote(filename, safe="")
    url = s3_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": bucket,
            "Key": key,
            "ResponseContentDisposition": f"attachment; filename*=UTF-8''{encoded_filename}",
        },
        ExpiresIn=_PRESIGN_TTL,
    )
    expires_at = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=_PRESIGN_TTL)
    return WordDocResponse(url=url, filename=filename, expires_at=expires_at)
