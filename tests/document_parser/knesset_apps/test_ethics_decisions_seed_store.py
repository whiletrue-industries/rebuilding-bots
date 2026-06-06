"""Seed-store integration for the ethics_decisions fetcher.

The fetcher must read its immutable archive seed from the ``seed/`` key
(``seed/<bot>/ethics_decisions/index.csv`` via the ArtifactStore) and write
the merged working index to the ``cache/`` key — NOT read its seed from the
on-disk output path or the cache key.  The ``EthicsDecisionsConfig.seed_key``
field carries the seed address; when it is set, the fetcher reads from there.
A missing seed object (pre-upload / first-run) is not an error.
"""
from __future__ import annotations

import csv
import io
from pathlib import Path
from unittest.mock import MagicMock

from botnim.storage import LocalFsStore, seed_key as _seed_key, cache_key as _cache_key
from botnim.document_parser.knesset_apps.ethics_decisions_html import (
    EthicsDecisionsConfig,
    fetch_ethics_decisions_index,
)


def _resp(payload, status=200):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = payload
    r.raise_for_status = MagicMock()
    return r


_SEED_CSV = (
    "url,filename,date,knesset_num,title\r\n"
    "https://main.knesset.gov.il/Activity/committees/Ethics/Documents/old15.pdf,"
    "old15.pdf,1999-10-26,15,seed-archive-row\r\n"
)

_LIVE_HTML = """
<html><body><table>
  <tr><td>10.2.2026</td>
      <td><a href="/Activity/committees/Ethics/Documents/live25.pdf">live K25</a></td>
  </tr>
</table></body></html>
"""

# Canonical key shapes for the unified bot
_BOT = "unified"
_SEED_RELPATH = "ethics_decisions/index.csv"
_CACHE_RELPATH = "extraction/ethics_decisions/index.csv"
_EXPECTED_SEED_KEY = f"seed/{_BOT}/{_SEED_RELPATH}"
_EXPECTED_CACHE_KEY = f"cache/{_BOT}/{_CACHE_RELPATH}"


def test_seed_read_from_store_and_merged_written_to_cache(tmp_path: Path):
    """Seed lives at seed/ key; merged output goes to cache/ key."""
    store = LocalFsStore(tmp_path / "store")
    # Seed lives ONLY at the seed/ key; the cache key is empty (first run).
    store.put_atomic(_EXPECTED_SEED_KEY, _SEED_CSV.encode("utf-8"))

    cfg = EthicsDecisionsConfig(
        store=store,
        key=_EXPECTED_CACHE_KEY,
        seed_key=_EXPECTED_SEED_KEY,
    )
    http_get = MagicMock(return_value=_resp({"Html": _LIVE_HTML}))

    rows = fetch_ethics_decisions_index(cfg, http_get=http_get)

    urls = [r.url for r in rows]
    # Live K25 row is present.
    assert any("live25.pdf" in u for u in urls), urls
    # Seed archive row is pulled from the seed/ key (NOT the cache).
    assert any("old15.pdf" in u for u in urls), urls

    # Merged working index written to cache/ key.
    cached = store.get_bytes(_EXPECTED_CACHE_KEY).decode("utf-8")
    cached_urls = [r["url"] for r in csv.DictReader(cached.splitlines())]
    assert set(cached_urls) == set(urls)


def test_seed_key_not_same_as_cache_key():
    """Confirm the upload-script target (seed/) and the reader source agree."""
    # The upload script uploads to seed/unified/ethics_decisions/index.csv.
    # The reader must read from the same key.
    assert _EXPECTED_SEED_KEY == "seed/unified/ethics_decisions/index.csv"
    assert _EXPECTED_CACHE_KEY == "cache/unified/extraction/ethics_decisions/index.csv"
    assert _EXPECTED_SEED_KEY != _EXPECTED_CACHE_KEY


def test_missing_seed_in_store_is_not_an_error(tmp_path: Path):
    """No seed object → fetcher proceeds with live rows only (first-run state)."""
    store = LocalFsStore(tmp_path / "store")
    cfg = EthicsDecisionsConfig(
        store=store,
        key=_EXPECTED_CACHE_KEY,
        seed_key=_EXPECTED_SEED_KEY,
    )
    http_get = MagicMock(return_value=_resp({"Html": _LIVE_HTML}))

    rows = fetch_ethics_decisions_index(cfg, http_get=http_get)
    urls = [r.url for r in rows]
    assert any("live25.pdf" in u for u in urls)
    assert store.exists(_EXPECTED_CACHE_KEY)


def test_seed_key_none_falls_back_to_cache_key_for_seed(tmp_path: Path):
    """Backwards compat: seed_key=None reads seed from the cache key (old behaviour)."""
    store = LocalFsStore(tmp_path / "store")
    # Seed is placed at the CACHE key (old behaviour — seed and output are same key).
    store.put_atomic(_EXPECTED_CACHE_KEY, _SEED_CSV.encode("utf-8"))

    cfg = EthicsDecisionsConfig(
        store=store,
        key=_EXPECTED_CACHE_KEY,
        seed_key=None,  # old behaviour
    )
    http_get = MagicMock(return_value=_resp({"Html": _LIVE_HTML}))

    rows = fetch_ethics_decisions_index(cfg, http_get=http_get)
    urls = [r.url for r in rows]
    assert any("old15.pdf" in u for u in urls), "seed rows should be present in legacy mode"
    assert any("live25.pdf" in u for u in urls)


def test_seed_key_not_overwritten_by_fetcher(tmp_path: Path):
    """The fetcher writes to cache/ only; seed/ key must remain unchanged."""
    store = LocalFsStore(tmp_path / "store")
    original_seed = _SEED_CSV.encode("utf-8")
    store.put_atomic(_EXPECTED_SEED_KEY, original_seed)

    cfg = EthicsDecisionsConfig(
        store=store,
        key=_EXPECTED_CACHE_KEY,
        seed_key=_EXPECTED_SEED_KEY,
    )
    http_get = MagicMock(return_value=_resp({"Html": _LIVE_HTML}))
    fetch_ethics_decisions_index(cfg, http_get=http_get)

    # The seed key must be untouched.
    assert store.get_bytes(_EXPECTED_SEED_KEY) == original_seed


def test_upload_script_key_matches_reader_key():
    """Explicit assertion: upload script writes seed/unified/ethics_decisions/index.csv,
    the reader reads from the same key when fetch_and_process wires seed_key.

    The upload script table:
      on-disk: specs/unified/extraction/ethics_decisions/index.csv
      seed key: seed/unified/ethics_decisions/index.csv

    The fetcher in fetch_and_process.py strips "extraction/" from source['source']
    and calls seed_key(bot, relpath):
      seed_key("unified", "ethics_decisions/index.csv") == "seed/unified/ethics_decisions/index.csv"
    """
    from botnim.storage.base import seed_key
    computed = seed_key("unified", "ethics_decisions/index.csv")
    assert computed == "seed/unified/ethics_decisions/index.csv"
