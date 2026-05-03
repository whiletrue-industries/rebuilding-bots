"""Pure extractors for the three body formats gov.il serves.

* HTML — inline body in ``contentMain.htmlContents[].sectionData``.
* PDF  — attachment body for ~1% of decisions (per Tal's data,
  377 / 26,037 ≈ 1.4%). pymupdf's ``page.get_text()`` handles RTL
  Hebrew correctly when called as plain-text (table-mode reverses
  characters — avoid that path).
* DOCX — rare but real (a handful of pre-2010 decisions). Reuses
  python-docx, which is already a hard dep for knesset_protocols.

All three return text with control characters stripped — Excel writers
elsewhere in the codebase reject ``\\x00–\\x08, \\x0B, \\x0C, \\x0E–\\x1F``,
and these bytes routinely appear in PDF text streams.
"""
from __future__ import annotations

import io
import re

import docx
import fitz

_HTML_TAG = re.compile(r"<[^>]+>")
_HTML_ENTITY = re.compile(r"&#?\w+;")
_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def strip_control_chars(text: str) -> str:
    """Remove control characters except \\n and \\t."""
    if not text:
        return ""
    return _CONTROL_CHARS.sub("", text)


def html_to_text(html: str | None) -> str:
    """Strip HTML, decode common entities, collapse whitespace.

    Design copied from ``bk_datapackage.process_bk_csv._html_to_text`` —
    same input class (gov.il body HTML proxied through BudgetKey), same
    constraints (no BeautifulSoup; regex pass + manual entity map covers
    the actual tag/entity universe these payloads use).
    """
    if not html:
        return ""
    text = re.sub(r"</(p|div|li|h[1-6]|tr|br)\s*>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<br\s*/?\s*>", "\n", text, flags=re.IGNORECASE)
    text = _HTML_TAG.sub("", text)
    text = (
        text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&apos;", "'")
        .replace("&#13;", "")
        .replace("&#10;", "\n")
    )
    text = _HTML_ENTITY.sub("", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return strip_control_chars(text.strip())


def pdf_to_text(blob: bytes) -> str:
    """Extract text from an in-memory PDF blob via pymupdf.

    Uses ``page.get_text()`` (plain text mode) — table-aware modes
    reverse RTL Hebrew character order. See Tal's GOV_DEC_DEV_GUIDE.md
    troubleshooting table.
    """
    if not blob:
        return ""
    doc = fitz.open(stream=blob, filetype="pdf")
    try:
        parts = [page.get_text() for page in doc]
    finally:
        doc.close()
    return strip_control_chars("\n\n".join(parts).strip())


def docx_to_text(blob: bytes) -> str:
    """Extract text from an in-memory DOCX blob via python-docx."""
    if not blob:
        return ""
    d = docx.Document(io.BytesIO(blob))
    parts = [p.text for p in d.paragraphs if p.text]
    return strip_control_chars("\n".join(parts).strip())
