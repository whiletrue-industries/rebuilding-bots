from pathlib import Path
from botnim.document_parser.wikisource_law_book.manifest import (
    LawBookEntry, write_manifest, read_manifest,
)


def test_manifest_round_trip(tmp_path: Path):
    entries = [
        LawBookEntry("חוק האזנת סתר", "https://he.wikisource.org/wiki/חוק_האזנת_סתר", "law", "ok"),
        LawBookEntry("תקנות הגנת הפרטיות", "https://he.wikisource.org/wiki/תקנות_הגנת_הפרטיות", "regulation", "failed"),
    ]
    p = tmp_path / "manifest.csv"
    write_manifest(p, entries)
    assert p.exists()
    out = read_manifest(p)
    assert out == entries


def test_read_missing_returns_empty(tmp_path: Path):
    assert read_manifest(tmp_path / "nope.csv") == []
