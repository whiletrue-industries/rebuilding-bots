"""LocalFsStore — filesystem-backed ArtifactStore for dev / CI.

Keys are POSIX-style ("a/b/c.json"); they map 1:1 onto paths rooted at
``root``. put_atomic writes a temp file in the destination directory
and os.replace()s it onto the final path so a concurrent reader never
observes a partial object (os.replace is atomic within a filesystem).
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import IO, List


class LocalFsStore:
    def __init__(self, root: str) -> None:
        self._root = Path(root).resolve()
        self._root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, key: str) -> Path:
        """Map a key to an absolute path, refusing traversal outside root."""
        if not key:
            raise ValueError("key must be non-empty")
        target = (self._root / key).resolve()
        # target must live under root; reject "../" escapes.
        if self._root != target and self._root not in target.parents:
            raise ValueError(f"key escapes store root: {key!r}")
        return target

    def get_bytes(self, key: str) -> bytes:
        path = self._resolve(key)
        if not path.is_file():
            raise FileNotFoundError(key)
        expected = path.stat().st_size
        data = path.read_bytes()
        if len(data) != expected:
            # File shrank between stat and read (concurrent truncation).
            raise OSError(
                f"short read for {key!r}: read {len(data)} of {expected} bytes"
            )
        return data

    def open_stream(self, key: str) -> IO[bytes]:
        path = self._resolve(key)
        if not path.is_file():
            raise FileNotFoundError(key)
        return open(path, "rb")

    def put_atomic(self, key: str, data: bytes) -> None:
        path = self._resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=".tmp-", dir=str(path.parent)
        )
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, path)
        except BaseException:
            # Clean up the temp file on any failure so put_atomic leaves
            # no partial artifacts behind.
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise

    def exists(self, key: str) -> bool:
        return self._resolve(key).is_file()

    def list(self, prefix: str) -> List[str]:
        base = self._resolve(prefix) if prefix else self._root
        if not base.exists():
            return []
        keys: List[str] = []
        for p in base.rglob("*"):
            if p.is_file():
                keys.append(p.relative_to(self._root).as_posix())
        return keys
