#!/usr/bin/env python3
"""Operator one-shot: upload the immutable extraction seed files to seed/.

Uploads the three operator-owned seed artifacts to the ArtifactStore's
``seed/<bot>/`` prefix (versioning ON), where the fetchers read them at
sync time:

  * ethics_decisions/index.csv      (older-Knesset K15-K23 archive seed)
  * lexicon_section_overrides.json  (hand-curated lexicon→wikisource map)
  * common-knowledge.md             (budget common-knowledge split source)

Idempotent: re-running uploads a new object version (bucket versioning
keeps history). Run after editing any of these files in specs/:

  python scripts/upload_extraction_seed.py
  python scripts/upload_extraction_seed.py --bot unified

The store backend (S3 vs LocalFs) is selected by env via
``botnim.storage.get_artifact_store``.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ROOT/specs is the canonical specs tree (mirrors botnim.config.SPECS).
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SPECS_ROOT = ROOT / "specs"

# (on-disk relpath under specs/<bot>/, seed key relpath under seed/<bot>/)
# These MUST exactly match the keys the readers/fetchers read from:
#   ethics_decisions_html.py reads seed/<bot>/ethics_decisions/index.csv
#   lexicon.py reads seed/<bot>/lexicon_section_overrides.json
#   collect_sources.py reads seed/<bot>/common-knowledge.md
_SEED_FILES: list[tuple[str, str]] = [
    ("extraction/ethics_decisions/index.csv", "ethics_decisions/index.csv"),
    ("extraction/lexicon_section_overrides.json", "lexicon_section_overrides.json"),
    ("common-knowledge.md", "common-knowledge.md"),
]


def upload_seed(*, specs_root: Path, bot: str, store) -> list[str]:
    """Upload each seed file to seed/<bot>/<key_relpath>. Returns the keys uploaded.

    Raises FileNotFoundError if any source file is missing — the seed set is
    fixed, so a missing file is an operator error, not a skip-and-continue.

    Idempotency: relies on bucket versioning (seed/ prefix has versioning ON
    per the ArtifactStore contract). Re-running simply uploads a new version.
    """
    uploaded: list[str] = []
    for disk_relpath, key_relpath in _SEED_FILES:
        src = specs_root / bot / disk_relpath
        if not src.exists():
            raise FileNotFoundError(f"seed source not found: {src}")
        key = f"seed/{bot}/{key_relpath}"
        store.put_atomic(key, src.read_bytes())
        print(f"uploaded {src} -> {key}", flush=True)
        uploaded.append(key)
    return uploaded


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--bot", default="unified",
        help="bot slug whose seed files to upload (default: unified)",
    )
    p.add_argument(
        "--specs-root", default=str(DEFAULT_SPECS_ROOT),
        help="path to the specs/ root (default: repo specs/)",
    )
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    from botnim.storage import get_artifact_store
    store = get_artifact_store()
    keys = upload_seed(
        specs_root=Path(args.specs_root), bot=args.bot, store=store
    )
    print(f"done: uploaded {len(keys)} seed file(s)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
