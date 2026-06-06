"""Lexicon section-overrides must be read from seed/<bot>/ via the store.

Verifies the resolution order:
  1. seed/<bot>/lexicon_section_overrides.json via store (operator seed).
  2. Legacy on-disk candidates (fallback when store has no object).
"""
from __future__ import annotations

import json
from pathlib import Path

from botnim.storage import LocalFsStore
from botnim.document_parser.lexicon import lexicon as lexmod


def test_overrides_loaded_from_store_seed_prefix(tmp_path: Path):
    store = LocalFsStore(tmp_path / "store")
    payload = {
        "https://main.knesset.gov.il/About/Lexicon/Pages/revision.aspx":
            "https://he.wikisource.org/wiki/x#section_115",
    }
    store.put_atomic(
        "seed/unified/lexicon_section_overrides.json",
        json.dumps(payload).encode("utf-8"),
    )

    got = lexmod._load_section_overrides(store=store, bot="unified")
    assert got == payload


def test_missing_store_object_falls_back_to_empty(tmp_path: Path):
    """No seed object AND no on-disk candidate → empty dict, no crash."""
    store = LocalFsStore(tmp_path / "store")
    got = lexmod._load_section_overrides(
        store=store, bot="unified",
        _disk_candidates=[tmp_path / "does-not-exist.json"],
    )
    assert got == {}


def test_non_string_values_filtered(tmp_path: Path):
    store = LocalFsStore(tmp_path / "store")
    store.put_atomic(
        "seed/unified/lexicon_section_overrides.json",
        json.dumps({"a": "https://ok", "b": 123, "c": ""}).encode("utf-8"),
    )
    got = lexmod._load_section_overrides(store=store, bot="unified")
    assert got == {"a": "https://ok"}


def test_store_wins_over_disk_candidate(tmp_path: Path):
    """Store takes priority over on-disk file when both exist."""
    store = LocalFsStore(tmp_path / "store")
    store_payload = {"from": "store"}
    store.put_atomic(
        "seed/unified/lexicon_section_overrides.json",
        json.dumps({"from": "store"}).encode("utf-8"),
    )
    disk_file = tmp_path / "overrides.json"
    disk_file.write_text(json.dumps({"from": "disk"}), encoding="utf-8")

    got = lexmod._load_section_overrides(
        store=store, bot="unified",
        _disk_candidates=[disk_file],
    )
    assert got == store_payload


def test_no_store_falls_back_to_disk(tmp_path: Path):
    """When store=None, the disk candidate is used."""
    disk_file = tmp_path / "overrides.json"
    disk_payload = {"from": "disk-only"}
    disk_file.write_text(json.dumps(disk_payload), encoding="utf-8")

    got = lexmod._load_section_overrides(
        store=None,
        _disk_candidates=[disk_file],
    )
    assert got == disk_payload


def test_upload_script_key_matches_reader_key():
    """Explicit confirmation: upload script writes seed/unified/lexicon_section_overrides.json,
    the reader reads from the same key.

    Upload script table:
      on-disk: specs/unified/extraction/lexicon_section_overrides.json
      seed key: seed/unified/lexicon_section_overrides.json

    Reader:  store.get_bytes(f'seed/{bot}/{_OVERRIDES_FILENAME}')
           = store.get_bytes('seed/unified/lexicon_section_overrides.json')
    """
    from botnim.storage.base import seed_key
    assert seed_key("unified", "lexicon_section_overrides.json") == \
        "seed/unified/lexicon_section_overrides.json"
    # Confirm the filename constant matches.
    assert lexmod._OVERRIDES_FILENAME == "lexicon_section_overrides.json"
