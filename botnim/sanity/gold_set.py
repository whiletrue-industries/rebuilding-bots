"""Loader for deploy/gold-set.json.

The file lives in parlibot, not rebuilding-bots; the Dockerfile COPYs it
to /srv/deploy/gold-set.json. Set BOTNIM_GOLD_SET_PATH to override (used
by tests and local dev).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from botnim.sanity.types import GoldEntry

_DEFAULT_PATH = "/srv/deploy/gold-set.json"


def load_gold_set() -> list[GoldEntry]:
    path = Path(os.environ.get("BOTNIM_GOLD_SET_PATH", _DEFAULT_PATH))
    if not path.exists():
        raise FileNotFoundError(f"gold-set.json not found at {path}")
    raw = json.loads(path.read_text())
    if not isinstance(raw, list):
        raise ValueError("gold-set.json must be a JSON array of entries")
    out: list[GoldEntry] = []
    for i, entry in enumerate(raw):
        try:
            out.append(
                GoldEntry(
                    row=int(entry["row"]),
                    question=str(entry["question"]),
                    expected_behavior=str(entry["expected_behavior"]),
                    must_not_contain=list(entry.get("must_not_contain", [])),
                    observed_notes=str(entry.get("observed", {}).get("notes", "")),
                )
            )
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError(f"gold-set.json entry {i} malformed: {e}") from e
    return out
