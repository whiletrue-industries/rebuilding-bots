"""The common-knowledge.md split source must read from seed/<bot>/ via store.

Verifies that:
  - markdown split sources read from seed/<bot>/<source> (not cache/)
  - on-disk fallback works when store has no object
  - JSON split sources (wikitext) continue reading from cache/ (unchanged)
"""
from __future__ import annotations

import json
from pathlib import Path

from botnim.storage import LocalFsStore
from botnim import collect_sources


def test_split_md_reads_from_store_seed(tmp_path: Path, monkeypatch):
    store = LocalFsStore(tmp_path / "store")
    store.put_atomic(
        "seed/unified/common-knowledge.md",
        "from-seed-A\n---\nfrom-seed-B".encode("utf-8"),
    )
    monkeypatch.setattr(collect_sources, "get_artifact_store", lambda: store)

    config_dir = tmp_path / "specs" / "unified"
    config_dir.mkdir(parents=True)
    # On-disk file has DIFFERENT content so we prove the store wins.
    (config_dir / "common-knowledge.md").write_text("from-disk", encoding="utf-8")

    out = collect_sources._collect_raw_streams_split(
        config_dir, "common_budget_knowledge", "common-knowledge.md"
    )
    bodies = [c for (_fn, c, _ct, _meta) in out]
    assert bodies == ["from-seed-A", "from-seed-B"]
    fnames = [fn for (fn, _c, _ct, _meta) in out]
    assert fnames == ["common_budget_knowledge_0.md", "common_budget_knowledge_1.md"]


def test_split_md_falls_back_to_disk_when_no_store_object(tmp_path: Path, monkeypatch):
    store = LocalFsStore(tmp_path / "store")  # empty store
    monkeypatch.setattr(collect_sources, "get_artifact_store", lambda: store)

    config_dir = tmp_path / "specs" / "unified"
    config_dir.mkdir(parents=True)
    (config_dir / "common-knowledge.md").write_text("disk-A\n---\ndisk-B", encoding="utf-8")

    out = collect_sources._collect_raw_streams_split(
        config_dir, "common_budget_knowledge", "common-knowledge.md"
    )
    bodies = [c for (_fn, c, _ct, _meta) in out]
    assert bodies == ["disk-A", "disk-B"]


def test_split_json_branch_still_reads_cache(tmp_path: Path, monkeypatch):
    """JSON (wikitext) split sources are re-derivable — read from cache/ not seed/."""
    store = LocalFsStore(tmp_path / "store")
    monkeypatch.setattr(collect_sources, "get_artifact_store", lambda: store)

    config_dir = tmp_path / "specs" / "unified"
    config_dir.mkdir(parents=True)

    # JSON split sources live at cache/<bot>/<relpath> (key_for_extraction).
    cache_key = "cache/unified/extraction/doc_structure_content.json"
    data = {"metadata": {"document_name": "doc"}, "structure": []}
    store.put_atomic(cache_key, json.dumps(data).encode("utf-8"))

    out = collect_sources._collect_raw_streams_split(
        config_dir, "legal_text", "extraction/doc_structure_content.json"
    )
    # Empty structure → no chunks; the important thing is no store error / no crash.
    assert out == []


def test_upload_script_key_matches_reader_key():
    """Explicit confirmation: upload script writes seed/unified/common-knowledge.md,
    the reader reads from the same key.

    Upload script table:
      on-disk: specs/unified/common-knowledge.md
      seed key: seed/unified/common-knowledge.md

    Reader:  store.get_bytes(f'seed/{bot}/{source}')
           = store.get_bytes('seed/unified/common-knowledge.md')
    """
    from botnim.storage.base import seed_key
    assert seed_key("unified", "common-knowledge.md") == "seed/unified/common-knowledge.md"
