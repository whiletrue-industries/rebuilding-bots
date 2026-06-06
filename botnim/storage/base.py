"""ArtifactStore — backend-agnostic artifact storage contract.

Two concrete backends implement this Protocol:
  * S3Store     — boto3, ECS task-role creds (prod / staging).
  * LocalFsStore — local filesystem (dev / CI).

Keys mirror today's relative path under ``config_dir/extraction`` and
carry one of three prefixes:

  seed/<bot>/<relpath>    immutable operator data (S3 versioning ON)
  cache/<bot>/<relpath>   re-derivable artifacts
  cache/wikitext/<bot>/<html_sha256>__<version>.json
                          versioned wikitext extraction cache
                          (version is passed by the caller — the canonical
                          WIKITEXT_EXTRACTOR_VERSION lives in
                          botnim/document_parser/wikitext/process_document.py)

The key-builder helpers below are the ONLY supported way to construct
keys; callers must not hand-format prefixes so the prefix scheme stays
in one place.
"""
from __future__ import annotations

from typing import BinaryIO, List, Protocol, runtime_checkable


def _norm_relpath(relpath: str) -> str:
    """Normalise a relative path into a forward-slash key fragment.

    Strips a leading slash and converts Windows-style backslashes so a
    caller passing an os.path-joined fragment on either platform lands
    on the same key.
    """
    if not relpath:
        raise ValueError("relpath must be non-empty")
    return relpath.replace("\\", "/").lstrip("/")


def _norm_bot(bot: str) -> str:
    if not bot:
        raise ValueError("bot must be non-empty")
    return bot


def seed_key(bot: str, relpath: str) -> str:
    """Key for immutable operator-supplied data (S3 versioning ON)."""
    return f"seed/{_norm_bot(bot)}/{_norm_relpath(relpath)}"


def cache_key(bot: str, relpath: str) -> str:
    """Key for re-derivable cache artifacts."""
    return f"cache/{_norm_bot(bot)}/{_norm_relpath(relpath)}"


def wikitext_cache_key(bot: str, html_sha256: str, version: str) -> str:
    """Key for the versioned wikitext extraction cache.

    The version parameter must be the caller's canonical
    WIKITEXT_EXTRACTOR_VERSION constant (from
    botnim/document_parser/wikitext/process_document.py) so that a
    version bump is automatically a cache miss with no invalidation
    logic here.
    """
    if not html_sha256:
        raise ValueError("html_sha256 must be non-empty")
    if not version:
        raise ValueError("version must be non-empty")
    return (
        f"cache/wikitext/{_norm_bot(bot)}/"
        f"{html_sha256}__{version}.json"
    )


@runtime_checkable
class ArtifactStore(Protocol):
    """Pinned contract every backend implements verbatim."""

    def get_bytes(self, key: str) -> bytes:
        """Return the full object body.

        Raises FileNotFoundError if the key is missing. Raises on a
        truncated / short read (body length != advertised length).
        """
        ...

    def open_stream(self, key: str) -> BinaryIO:
        """Return a binary file-like positioned at byte 0 for the read
        side to consume. Raises FileNotFoundError if the key is missing."""
        ...

    def put_atomic(self, key: str, data: bytes) -> None:
        """Write data so a concurrent reader never sees a partial object.

        S3: a single PutObject to the final key. LocalFs: write to a
        temp file in the same directory, then os.replace onto the final
        path."""
        ...

    def exists(self, key: str) -> bool:
        """True iff the key currently resolves to an object."""
        ...

    def list(self, prefix: str) -> List[str]:
        """Return keys under prefix (used only for the wikitext glob)."""
        ...
