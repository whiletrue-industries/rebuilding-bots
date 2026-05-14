"""Map a Hebrew section reference to a Wikisource anchor URL.

The Knesset Lexicon glossary entries (scraped by ``lexicon.py``) describe
*concepts* like "שאילתה" but typically reference a specific section of
תקנון הכנסת / חוק־יסוד: הכנסת / חוק הכנסת / כללי אתיקה / חוק הפרשנות.
This helper turns the section reference into the corresponding Wikisource
anchor URL so the bot can cite the actual section text, not the glossary
page about the concept.

The helper is deliberately conservative: it returns ``None`` for any law
it doesn't have a Wikisource URL for, leaving the caller to fall back to
the original Lexicon URL.
"""
from __future__ import annotations

import re
import unicodedata
from urllib.parse import quote


def _wiki_url(title: str) -> str:
    """Build a he.wikisource.org page URL from a Hebrew title.

    MediaWiki canonicalises spaces in page titles to underscores, so we
    must replace ``%20`` with ``_`` after url-quoting — otherwise the
    anchor URL points at a 404 (``תקנון%20הכנסת`` != ``תקנון_הכנסת``).
    ``:`` is left unescaped because the page title for the basic laws is
    canonically rendered ``חוק-יסוד: הכנסת`` with a literal colon.
    """
    return "https://he.wikisource.org/wiki/" + quote(title, safe=":").replace("%20", "_")


def _wiki_anchor(section: str) -> str:
    """Build the section fragment of a Wikisource URL. Same ``%20→_`` rule
    as :func:`_wiki_url` applies because the anchor is also a MediaWiki
    section title — spaces in the anchor name are canonicalised to
    underscores by MediaWiki."""
    return quote(f"סעיף {section}", safe=":").replace("%20", "_")


# Wikisource page URLs for laws referenced by the lexicon corpus. These
# MUST match the URLs in specs/unified/config.yaml `legal_text` fetchers
# so the citations land on the same documents the bot already has indexed.
_LAW_URLS: dict[str, str] = {
    "takanon":      _wiki_url("תקנון הכנסת"),
    "yesod":        _wiki_url("חוק-יסוד: הכנסת"),
    "chok_knesset": _wiki_url("חוק הכנסת"),
    "ethics":       _wiki_url("כללי אתיקה לחברי הכנסת"),
    "parshanut":    _wiki_url("חוק הפרשנות"),
}


def _strip_niqqud(text: str) -> str:
    """Remove Hebrew combining marks (cantillation + niqqud), but keep
    Hebrew punctuation such as U+05BE (maqaf, "־") that lives in the same
    Unicode block. Using Unicode category Mn ("Mark, Nonspacing") catches
    every combining mark without enumerating ranges manually — and never
    eats the maqaf, which is category Pd ("Punctuation, Dash")."""
    return "".join(ch for ch in text if unicodedata.category(ch) != "Mn")


# Detect a law name. Order matters: longer / more specific names first
# so "תקנון הכנסת" wins over the bare "הכנסת" inside חוק־יסוד.
_LAW_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("takanon",      re.compile(r"תקנון\s+הכנסת")),
    ("ethics",       re.compile(r"כללי\s+אתיקה(?:\s+לחברי\s+הכנסת)?")),
    ("yesod",        re.compile(r"חוק[־\-]יסוד\s*:\s*הכנסת")),
    ("parshanut",    re.compile(r"חוק\s+הפרשנות")),
    ("chok_knesset", re.compile(r"חוק\s+הכנסת")),
]

# Section pattern: digits, optionally followed by ONE Hebrew letter
# (e.g. "סעיף 6א", "סעיף 14א"). We DROP any "(ב)(1)" sub-clause part
# because Wikisource section anchors are at the top-level section, not
# at the sub-clause.
_SECTION_RE = re.compile(r"סעיף\s+(\d+[א-ת]?)")


def derive_section_url(text: str) -> str | None:
    """Return a Wikisource section URL for a section reference in ``text``,
    or ``None`` if no recognisable (law, section) pair is found.

    Disambiguation rule: the law name that appears EARLIEST in the text
    wins. This matches how a human reads "לפי תקנון הכנסת סעיף 86,
    ובהמשך לחוק הכנסת" — the first reference is the operative one.
    """
    if not text:
        return None
    clean = _strip_niqqud(text)

    section_match = _SECTION_RE.search(clean)
    if section_match is None:
        return None
    section = section_match.group(1)

    # Find the earliest law mention. ``finditer`` over each pattern gives
    # all positions; we pick the lowest start index across all laws.
    earliest: tuple[int, str] | None = None
    for law_key, pattern in _LAW_PATTERNS:
        m = pattern.search(clean)
        if m is None:
            continue
        if earliest is None or m.start() < earliest[0]:
            earliest = (m.start(), law_key)

    if earliest is None:
        return None

    base_url = _LAW_URLS[earliest[1]]
    return f"{base_url}#{_wiki_anchor(section)}"
