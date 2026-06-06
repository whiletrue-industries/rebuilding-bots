"""Shared CSV artifact writer.

Every fetcher used to hand-roll ``tmp = path.with_suffix(... + '.tmp')`` +
``csv.DictWriter`` + ``os.replace``. With the ArtifactStore the atomic
boundary moves into ``store.put_atomic(key, data)`` (S3: single PutObject to
the final key; LocalFs: temp + os.replace), so writers only need to (1)
compute a key and (2) hand us rows + fieldnames. We render the exact same
bytes ``csv.DictWriter(..., newline="")`` produced and pass them through.
"""
from __future__ import annotations

import csv
import io
from typing import Iterable, Mapping, Sequence

from .base import ArtifactStore


def key_for_extraction(bot: str, relpath: str) -> str:
    """Map a writer's (bot, relative-path-under-config_dir) to a cache key.

    Mirrors today's on-disk layout: the file lived at
    ``config_dir/<relpath>`` where ``config_dir.name == bot``. The store key
    is ``cache/<bot>/<relpath>`` (re-derivable artifact prefix).

    A leading slash on ``relpath`` is normalised away so callers can pass
    either form without error.
    """
    return f"cache/{bot}/{relpath.lstrip('/')}"


def render_csv_bytes(
    rows: Iterable[Mapping[str, object]],
    fieldnames: Sequence[str],
    *,
    extend_fieldnames: bool = False,
) -> bytes:
    """Render rows to CSV bytes identical to the legacy DictWriter output.

    ``extend_fieldnames`` reproduces process_pdfs' behaviour: start from the
    given ``fieldnames`` and append any extra keys seen across rows, in
    first-seen order (process_pdfs.py:181-185).
    """
    rows = list(rows)
    field_list = list(fieldnames)
    if extend_fieldnames:
        for r in rows:
            for k in r.keys():
                if k not in field_list:
                    field_list.append(k)
    buf = io.StringIO(newline="")
    writer = csv.DictWriter(buf, fieldnames=field_list)
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    return buf.getvalue().encode("utf-8")


def write_csv_artifact(
    store: ArtifactStore,
    key: str,
    rows: Iterable[Mapping[str, object]],
    *,
    fieldnames: Sequence[str],
    extend_fieldnames: bool = False,
) -> None:
    """Render ``rows`` to CSV and atomically write them to ``key``."""
    data = render_csv_bytes(rows, fieldnames, extend_fieldnames=extend_fieldnames)
    store.put_atomic(key, data)
