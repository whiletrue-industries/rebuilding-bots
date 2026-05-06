"""S3 upload + presigned URL helper for word-doc artifacts."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote

import boto3
from botocore.client import Config

from .models import WordDocResponse


_PRESIGN_TTL = 7 * 24 * 3600  # 7 days

# Default region for the word-docs bucket. Overridable via env so the
# helper still works in regions other than il-central-1 (e.g. tests
# under moto). Match the bucket's actual region — il-central-1 (and
# other newer regions) only honor the regional endpoint
# `s3.<region>.amazonaws.com` for SigV4 presigned URLs; the legacy
# global `s3.amazonaws.com` returns IllegalLocationConstraintException.
_AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "il-central-1"


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

    # SigV4 + virtual-hosted-style addressing forces the regional
    # endpoint (e.g. s3.il-central-1.amazonaws.com) which il-central-1
    # requires; otherwise URLs point at the legacy global endpoint and
    # download attempts return IllegalLocationConstraintException.
    _client_config = Config(signature_version="s3v4", s3={"addressing_style": "virtual"})

    if s3_client is None:
        # Upload client uses the default credentials chain (task role).
        # Task role has s3:PutObject on this bucket via word_docs.tf.
        s3_client = boto3.client("s3", region_name=_AWS_REGION, config=_client_config)

    # Signing client: when WORD_DOCS_SIGNING_AWS_* are set, sign presigned
    # download URLs with a long-lived IAM user instead of the task role's
    # STS creds. Why: STS-signed URLs include a multi-KB
    # X-Amz-Security-Token query param that pushes the URL past ~2KB —
    # LibreChat's markdown renderer drops the trailing &X-Amz-Signature
    # param at that length, breaking every download link. IAM-user-signed
    # URLs have no security token (~700 chars shorter) and also honor the
    # full ?X-Amz-Expires=7d we advertise (STS sessions expire ≤36h on
    # Fargate task roles). Falls back to the upload client (task role)
    # when those env vars are absent — keeps local/test behavior unchanged.
    signing_key = os.environ.get("WORD_DOCS_SIGNING_AWS_ACCESS_KEY_ID")
    signing_secret = os.environ.get("WORD_DOCS_SIGNING_AWS_SECRET_ACCESS_KEY")
    if signing_key and signing_secret:
        signing_client = boto3.client(
            "s3",
            region_name=_AWS_REGION,
            config=_client_config,
            aws_access_key_id=signing_key,
            aws_secret_access_key=signing_secret,
        )
    else:
        signing_client = s3_client

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
    url = signing_client.generate_presigned_url(
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
