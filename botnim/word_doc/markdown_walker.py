"""Narrow markdown subset → block list. No external markdown library —
LLM output uses a known, narrow subset and we want full control over RTL."""
from __future__ import annotations

import re
from typing import List, Dict, Any

_HEADING_RE = re.compile(r"^(#{2,3})\s+(.*)$")
_BULLET_RE = re.compile(r"^[-*]\s+(.*)$")
_NUMBERED_RE = re.compile(r"^\d+\.\s+(.*)$")

# Inline markdown: **bold**, *italic*, [text](url). We tokenize left-to-right.
_INLINE_RE = re.compile(
    r"(\*\*[^*]+\*\*)|(\*[^*]+\*)|(\[[^\]]+\]\([^)]+\))"
)


def _tokenize_runs(text: str) -> List[Dict[str, Any]]:
    """Return list of run dicts: {text, bold, italic, url}."""
    runs = []
    pos = 0
    for m in _INLINE_RE.finditer(text):
        if m.start() > pos:
            runs.append({"text": text[pos:m.start()], "bold": False, "italic": False, "url": None})
        token = m.group(0)
        if token.startswith("**") and token.endswith("**"):
            runs.append({"text": token[2:-2], "bold": True, "italic": False, "url": None})
        elif token.startswith("*") and token.endswith("*"):
            runs.append({"text": token[1:-1], "bold": False, "italic": True, "url": None})
        elif token.startswith("[") and "](" in token:
            inner = token[1:]
            label, rest = inner.split("](", 1)
            url = rest[:-1]
            runs.append({"text": label, "bold": False, "italic": False, "url": url})
        pos = m.end()
    if pos < len(text):
        runs.append({"text": text[pos:], "bold": False, "italic": False, "url": None})
    if not runs:
        runs.append({"text": text, "bold": False, "italic": False, "url": None})
    return runs


def parse_markdown(md: str) -> List[Dict[str, Any]]:
    """Walk markdown line-by-line and produce a block list."""
    blocks: List[Dict[str, Any]] = []
    paragraph_buffer: List[str] = []

    def _flush_paragraph():
        if paragraph_buffer:
            text = " ".join(paragraph_buffer)
            blocks.append({"type": "paragraph", "runs": _tokenize_runs(text)})
            paragraph_buffer.clear()

    for line in md.splitlines():
        line = line.rstrip()
        if not line:
            _flush_paragraph()
            continue

        m = _HEADING_RE.match(line)
        if m:
            _flush_paragraph()
            level = len(m.group(1)) - 1  # ## → 1, ### → 2
            blocks.append({"type": "heading", "level": level, "text": m.group(2).strip()})
            continue

        m = _BULLET_RE.match(line)
        if m:
            _flush_paragraph()
            blocks.append({"type": "list_item", "ordered": False, "runs": _tokenize_runs(m.group(1))})
            continue

        m = _NUMBERED_RE.match(line)
        if m:
            _flush_paragraph()
            blocks.append({"type": "list_item", "ordered": True, "runs": _tokenize_runs(m.group(1))})
            continue

        paragraph_buffer.append(line)

    _flush_paragraph()
    return blocks
