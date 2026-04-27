"""Backfill agent_prompts table from a sectioned agent.txt.

Parses an agent.txt that uses `<!-- SECTION_KEY: name -->` and
`<!-- header_text -->` markers, then INSERTs one active row per section
into agent_prompts. Idempotent: if active rows already exist for the
agent_type, the script aborts unless --force is passed (which deactivates
the existing rows first).

Usage from inside the ECS task:
    cd /srv && python3 scripts/backfill_agent_prompts.py \
        --agent-type unified \
        --file /tmp/agent.txt \
        --created-by backfill-2026-04-27

For local-dev validation, set DATABASE_URL env var first.
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


def parse_sections(text_blob: str) -> list[tuple[str, str | None, str]]:
    """Split agent.txt into [(section_key, header_text|None, body)] tuples.

    The body for each section starts after the optional header_text comment
    line and runs until the next SECTION_KEY marker or end-of-file. The
    `---` horizontal rules between sections in the source file are stripped
    from the trailing body so each row stores clean content.
    """
    matches = list(SECTION_RE.finditer(text_blob))
    if not matches:
        raise ValueError("no <!-- SECTION_KEY: ... --> markers found")
    sections: list[tuple[str, str | None, str]] = []
    for i, m in enumerate(matches):
        section_key = m.group(1)
        body_start = m.end()
        body_end = matches[i + 1].start() if i + 1 < len(matches) else len(text_blob)
        chunk = text_blob[body_start:body_end].lstrip("\n")

        # Optional header_text comment immediately after the SECTION_KEY line.
        header_text: str | None = None
        first_line, _, rest = chunk.partition("\n")
        hm = HEADER_RE.match(first_line)
        if hm:
            header_text = hm.group(1)
            chunk = rest.lstrip("\n")

        # Strip a trailing `---` separator and surrounding whitespace.
        body = re.sub(r"\n+---\s*$", "", chunk).strip()
        sections.append((section_key, header_text, body))
    return sections


def backfill(
    agent_type: str,
    file_path: Path,
    created_by: str,
    force: bool,
) -> None:
    blob = file_path.read_text(encoding="utf-8")
    sections = parse_sections(blob)
    print(f"parsed {len(sections)} sections from {file_path}", file=sys.stderr)
    for s in sections:
        print(f"  - {s[0]} (header_text len={len(s[1] or '')}, body len={len(s[2])})",
              file=sys.stderr)

    with get_session() as sess:
        existing = sess.execute(text(
            "SELECT count(*) FROM agent_prompts WHERE agent_type = :a AND active = true"
        ), {"a": agent_type}).scalar()
        if existing and not force:
            raise SystemExit(
                f"refusing to insert: {existing} active rows already exist for "
                f"agent_type={agent_type!r}. Pass --force to deactivate them first."
            )
        if force and existing:
            print(f"deactivating {existing} existing active rows for {agent_type}",
                  file=sys.stderr)
            sess.execute(text(
                "UPDATE agent_prompts SET active = false "
                "WHERE agent_type = :a AND active = true"
            ), {"a": agent_type})

        for ordinal, (section_key, header_text, body) in enumerate(sections, start=1):
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

    print(f"inserted {len(sections)} active rows for agent_type={agent_type}",
          file=sys.stderr)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--agent-type", required=True)
    p.add_argument("--file", required=True, type=Path)
    p.add_argument("--created-by", default="backfill")
    p.add_argument("--force", action="store_true",
                   help="Deactivate existing active rows before inserting.")
    args = p.parse_args()
    backfill(args.agent_type, args.file, args.created_by, args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())
