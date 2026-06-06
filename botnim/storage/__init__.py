"""botnim.storage — artifact storage abstraction (ArtifactStore).

get_artifact_store() returns a process-wide singleton selected by env:
  * BOTNIM_ARTIFACT_BUCKET set  -> S3Store(bucket)        (staging/prod)
  * otherwise                    -> LocalFsStore(<root>)   (dev / CI)

The LocalFs root is BOTNIM_ARTIFACT_LOCAL_ROOT if set, else
<repo-root>/tmp/artifacts (botnim.config.ROOT is the repo root).
"""
from __future__ import annotations

import os
from typing import Optional

from .base import (
    ArtifactStore,
    seed_key,
    cache_key,
    wikitext_cache_key,
)
from .local_fs import LocalFsStore
from .s3_store import S3Store

__all__ = [
    "ArtifactStore",
    "S3Store",
    "LocalFsStore",
    "seed_key",
    "cache_key",
    "wikitext_cache_key",
    "get_artifact_store",
]

_SINGLETON: Optional[ArtifactStore] = None


def _default_local_root() -> str:
    explicit = os.environ.get("BOTNIM_ARTIFACT_LOCAL_ROOT")
    if explicit:
        return explicit
    # Import lazily so importing botnim.storage doesn't pull config (and
    # its dotenv / SPECS scan) for callers that only need the key-builder.
    from botnim.config import ROOT

    return str(ROOT / "tmp" / "artifacts")


def _build_artifact_store() -> ArtifactStore:
    bucket = os.environ.get("BOTNIM_ARTIFACT_BUCKET")
    if bucket:
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        return S3Store(bucket, region_name=region)
    return LocalFsStore(_default_local_root())


def get_artifact_store() -> ArtifactStore:
    """Return the configured ArtifactStore singleton (lazy, env-selected)."""
    global _SINGLETON
    if _SINGLETON is None:
        _SINGLETON = _build_artifact_store()
    return _SINGLETON


def _reset_artifact_store_singleton() -> None:
    """Test-only hook to clear the cached singleton between cases."""
    global _SINGLETON
    _SINGLETON = None
