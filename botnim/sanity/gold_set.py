"""Loader for deploy/gold-set.json.

The file lives in parlibot, not rebuilding-bots; the Dockerfile COPYs it
to /srv/deploy/gold-set.json. Set BOTNIM_GOLD_SET_PATH to override (used
by tests and local dev).

Schema (post-2026-05-10):
    [
      {
        "source_excel_row": 0,                 # used as `row`; "row" also accepted
        "question": "...",                     # turn 1 (the user's question)
        "expected_behavior": "...",            # IDEAL one-turn answer (PASS_T1 bar)
        "followup_prompt": "..." | null,       # turn 2 prompt; null = no follow-up
        "expected_after_followup": "..." | null,
        "must_not_contain": [...],
        "observed": { "notes": "...", ... }
      },
      ...
    ]
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from botnim.sanity.types import GoldEntry

_DEFAULT_PATH = "/srv/deploy/gold-set.json"


def _entry_row(entry: dict) -> int:
    """Resolve the row index from either `source_excel_row` (preferred) or
    `row` (legacy). Raise if neither is present."""
    if "source_excel_row" in entry:
        return int(entry["source_excel_row"])
    if "row" in entry:
        return int(entry["row"])
    raise KeyError("missing 'source_excel_row' (or legacy 'row')")


def _opt_str(value) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


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
                    row=_entry_row(entry),
                    question=str(entry["question"]),
                    expected_behavior=str(entry["expected_behavior"]),
                    must_not_contain=list(entry.get("must_not_contain", [])),
                    observed_notes=str(entry.get("observed", {}).get("notes") or ""),
                    followup_prompt=_opt_str(entry.get("followup_prompt")),
                    expected_after_followup=_opt_str(entry.get("expected_after_followup")),
                )
            )
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError(f"gold-set.json entry {i} malformed: {e}") from e
    return out
