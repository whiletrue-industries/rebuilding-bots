"""Idempotently insert (or refresh) a single agent_prompts section.

Use this script to add a new prompt section to a bot's active prompt set
without rewriting the whole agent.txt and re-running
``backfill_agent_prompts.py --force`` (which deactivates everything else).

Behavior:

* Parses a single SECTION_KEY-formatted file (same format
  ``backfill_agent_prompts.py`` uses).
* Finds the target ``agent_type`` in ``agent_prompts``.
* If a row already exists with the same ``section_key``, deactivates the
  old row and inserts a new active row with the same ordinal — so the
  section's position in the assembled prompt is preserved.
* If no row exists, inserts a new active row at ``ordinal = max(ordinal) + 1``
  for that ``agent_type`` (or 1 if the table is empty for the bot).

Idempotent: running twice with the same input is a no-op (checks if the
new body is byte-equal to the active row before deactivating).

Usage::

    python3 scripts/insert_prompt_section.py \
        --agent-type unified \
        --file specs/unified/prompt_sections/plenary_schedule.md \
        --created-by "feat-plenary-schedule-2026-04-30"

Set ``DATABASE_URL`` first (Aurora target). For local dev:
``DATABASE_URL=postgresql+psycopg://botnim:botnim@localhost:54320/botnim_local``.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from sqlalchemy import text

from botnim.db.session import get_session

SECTION_RE = re.compile(r"^<!--\s*SECTION_KEY:\s*([\w_]+)\s*-->\s*$", re.M)
HEADER_RE = re.compile(r"^<!--\s*(.+?)\s*-->\s*$")


def parse_single_section(blob: str) -> tuple[str, str | None, str]:
    """Parse a file containing exactly one SECTION_KEY block.

    Returns ``(section_key, header_text|None, body)``. Raises ValueError
    if zero or more than one SECTION_KEY markers are found.
    """
    matches = list(SECTION_RE.finditer(blob))
    if len(matches) != 1:
        raise ValueError(
            f"expected exactly one <!-- SECTION_KEY: ... --> marker, "
            f"got {len(matches)}"
        )
    m = matches[0]
    section_key = m.group(1)
    chunk = blob[m.end():].lstrip("\n")
    header_text: str | None = None
    first_line, _, rest = chunk.partition("\n")
    hm = HEADER_RE.match(first_line)
    if hm:
        header_text = hm.group(1)
        chunk = rest.lstrip("\n")
    body = re.sub(r"\n+---\s*$", "", chunk).strip()
    return section_key, header_text, body


def upsert(
    agent_type: str,
    file_path: Path,
    created_by: str,
) -> None:
    blob = file_path.read_text(encoding="utf-8")
    section_key, header_text, body = parse_single_section(blob)
    print(
        f"parsed section {section_key!r} (header_text len={len(header_text or '')}, "
        f"body len={len(body)})",
        file=sys.stderr,
    )

    with get_session() as sess:
        existing = sess.execute(text(
            "SELECT body FROM agent_prompts "
            "WHERE agent_type = :a AND section_key = :k AND active = true "
            "ORDER BY ordinal LIMIT 1"
        ), {"a": agent_type, "k": section_key}).fetchone()

        if existing and existing[0] == body:
            print(
                f"no-op: agent_type={agent_type!r} section_key={section_key!r} "
                f"already has identical active body",
                file=sys.stderr,
            )
            return

        if existing:
            print(
                f"deactivating existing active row for "
                f"agent_type={agent_type!r} section_key={section_key!r}",
                file=sys.stderr,
            )
            preserved_ordinal = sess.execute(text(
                "SELECT ordinal FROM agent_prompts "
                "WHERE agent_type = :a AND section_key = :k AND active = true "
                "ORDER BY ordinal LIMIT 1"
            ), {"a": agent_type, "k": section_key}).scalar()
            sess.execute(text(
                "UPDATE agent_prompts SET active = false "
                "WHERE agent_type = :a AND section_key = :k AND active = true"
            ), {"a": agent_type, "k": section_key})
            ordinal = preserved_ordinal
        else:
            max_ord = sess.execute(text(
                "SELECT COALESCE(MAX(ordinal), 0) FROM agent_prompts "
                "WHERE agent_type = :a AND active = true"
            ), {"a": agent_type}).scalar() or 0
            ordinal = max_ord + 1

        sess.execute(text(
            "INSERT INTO agent_prompts "
            "(agent_type, section_key, ordinal, header_text, body, "
            " active, is_draft, created_by, published_at) "
            "VALUES (:a, :k, :o, :h, :b, true, false, :u, now())"
        ), {
            "a": agent_type,
            "k": section_key,
            "o": ordinal,
            "h": header_text,
            "b": body,
            "u": created_by,
        })
        print(
            f"inserted active row for agent_type={agent_type!r} "
            f"section_key={section_key!r} ordinal={ordinal}",
            file=sys.stderr,
        )


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--agent-type", required=True)
    p.add_argument("--file", required=True, type=Path)
    p.add_argument("--created-by", default="insert_prompt_section")
    args = p.parse_args()
    upsert(args.agent_type, args.file, args.created_by)
    return 0


if __name__ == "__main__":
    sys.exit(main())
