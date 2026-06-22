"""Committed coverage manifest for the law-book corpus.

One row per discovered statute/regulation. Committing it makes the corpus
reviewable and diffable, and decouples enumeration from extraction (an
operator can run discovery, eyeball the CSV, then extract).
"""
import csv
from dataclasses import dataclass, asdict
from pathlib import Path

MANIFEST_COLUMNS = ["title", "url", "kind", "status"]


@dataclass
class LawBookEntry:
    title: str
    url: str
    kind: str          # 'law' | 'regulation'
    status: str = "pending"   # 'pending' | 'ok' | 'failed' | 'skipped'


def write_manifest(path: Path, entries: list[LawBookEntry]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS)
        w.writeheader()
        for e in entries:
            w.writerow(asdict(e))


def read_manifest(path: Path) -> list[LawBookEntry]:
    path = Path(path)
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        return [LawBookEntry(**row) for row in csv.DictReader(f)]
