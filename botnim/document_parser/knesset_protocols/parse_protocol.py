"""Parse a single Knesset committee/plenum protocol .doc file.

The Knesset transcripts at ``fs.knesset.gov.il/<knesset>/Committees/...doc``
and ``.../Plenum/...doc`` are saved as Word 2007+ OOXML (despite the .doc
extension), with a fairly disciplined paragraph-style convention:

  Style name        Marker text         Meaning
  ────────────────  ──────────────────  ─────────────────────────────────
  ``יור``           ``<< יור >>``       Chair turn header
  ``דובר``          ``<< דובר >>``      Speaker turn header
  ``דובר-המשך``     ``<< דובר_המשך >>`` Same speaker continuing (after
                                        an interjection)
  ``קריאות``        ``<< קריאה >>``     Heckling/interjection
  ``נושא``          ``<< נושא >>``      Agenda item (committee)
  ``נושא-תת``       —                   Sub-heading on agenda item
  ``Normal``        —                   Body text of the current turn

Plenum docs use the same chair/speaker markers but agenda items are
tagged ``<< הצח >>`` (= הצעת חוק) rather than ``<< נושא >>`` and live
in their own paragraph styles like ``שער_כותרת_מספר``.

The output is a list of :class:`SpeakerTurn` records. Each turn carries
the agenda item it belongs to (the most recent ``נושא``/``הצח``) plus
the speaker name and party (parsed from the marker line) and the joined
text of every ``Normal`` paragraph until the next turn starts.

This module is pure parsing — no I/O, no fetching. The caller is
responsible for downloading the .doc file and supplying its bytes.
"""
from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from typing import Iterator, Optional

import docx

# Style names we treat as turn-header / agenda-item paragraphs. Match by
# substring (lowercased Hebrew) so minor formatting variations like a
# trailing dash or underscore still hit.
_TURN_STYLES = {
    "יור": "chair",
    "דובר": "speaker",
    "דובר-המשך": "speaker_continued",
    "דובר_המשך": "speaker_continued",
    # Plenum docs use "קריאות" (plural) as the paragraph style name but
    # "<< קריאה >>" (singular) inside the marker text, so accept both.
    "קריאות": "interjection",
    "קריאה": "interjection",
}
_AGENDA_STYLES = {"נושא", "נושא-תת"}

# Header text markers; useful when paragraph styles are stripped or the
# doc was reformatted. Pattern matches both the Hebrew name and an
# optional speaker line in the same paragraph.
_MARKER_RE = re.compile(
    r"<<\s*(?P<kind>יור|דובר_המשך|דובר|קריאה|נושא|הצח)\s*>>(?P<rest>.*)$"
)
# A "speaker line" looks like ``היו"ר NAME:`` or ``NAME (PARTY):``.
# The trailing colon is optional because _strip_markers (used on the
# input before this regex) drops trailing punctuation, including ":".
_SPEAKER_LINE_RE = re.compile(
    r"""
    ^
    (?:<<\s*(?:יור|דובר_המשך|דובר|קריאה)\s*>>\s*)?   # optional leading marker
    (?P<name>[^()<>:]+?)                              # name (no parens / colons)
    (?:\s*\((?P<party>[^)]+)\))?                      # optional party in parens
    \s*:?\s*                                          # optional trailing colon
    (?:<<\s*(?:יור|דובר_המשך|דובר|קריאה)\s*>>\s*)?   # optional trailing marker
    $
    """,
    re.VERBOSE,
)


@dataclass
class SpeakerTurn:
    """One contiguous speaker turn within a protocol."""

    ordinal: int                                # 1-based index inside this protocol
    role: str                                   # "chair" / "speaker" / "interjection" / "speaker_continued"
    speaker_name: str = ""
    speaker_party: str = ""
    agenda_item: str = ""                       # most recent agenda heading
    text: str = ""                              # joined body paragraphs
    paragraph_count: int = 0


@dataclass
class ProtocolHeader:
    """Per-document metadata extracted from the preamble."""

    knesset_num: str = ""           # e.g. "25"
    session_label: str = ""         # e.g. "פרוטוקול מס' 920" or "ישיבה שפ\"ו"
    committee_name: str = ""        # e.g. "ועדת הכספים" (committee docs only)
    session_date: str = ""          # raw Hebrew/Gregorian date line
    participants: list[str] = field(default_factory=list)


def _normalize_style(style_name: str) -> str:
    """Best-effort canonical form of a paragraph style name."""
    if not style_name:
        return ""
    # python-docx returns the user-visible name. Strip trailing whitespace
    # / underscore variants the Knesset templates sometimes carry.
    return style_name.strip().rstrip("_-").strip()


def _classify(paragraph) -> tuple[str, str]:
    """Classify a paragraph as (kind, payload).

    kind is one of: ``"agenda"``, ``"turn_header"``, ``"body"``, ``"skip"``.
    payload is the cleaned text relevant to that kind (the speaker line
    for turn headers, the agenda title for agenda, the body text for
    body, empty for skip).
    """
    text = paragraph.text.strip()
    if not text:
        return ("skip", "")
    style = _normalize_style(paragraph.style.name)

    if style in _AGENDA_STYLES or text.startswith(("<< נושא", "<<נושא")):
        return ("agenda", _strip_markers(text))

    # Plenum agenda items use << הצח >> markers — there is no consistent
    # style name for them across template generations, so always check
    # the marker text.
    if "<< הצח" in text or "<<הצח" in text:
        return ("agenda", _strip_markers(text))

    if style in _TURN_STYLES:
        return ("turn_header", text)

    m = _MARKER_RE.match(text)
    if m and m.group("kind") in ("יור", "דובר", "דובר_המשך", "קריאה"):
        return ("turn_header", text)

    return ("body", text)


def _strip_markers(text: str) -> str:
    """Remove ``<< ... >>`` decorator pairs from a heading line."""
    return re.sub(r"<<\s*[^<>]*?\s*>>", "", text).strip(" -–:;")


def _parse_speaker_line(text: str) -> tuple[str, str, str]:
    """Extract (role, name, party) from a turn-header paragraph.

    role is one of the values in :data:`_TURN_STYLES`. Falls back to
    empty strings on parse failure rather than raising — a malformed
    turn header should still produce a turn so the body text is not
    lost.
    """
    role = "speaker"
    m = _MARKER_RE.match(text)
    if m:
        kind = m.group("kind")
        if kind in _TURN_STYLES:
            role = _TURN_STYLES[kind]
        # The rest may contain "NAME (PARTY): << ... >>" or "NAME: << ... >>"
        rest = m.group("rest").strip()
        rest = _strip_markers(rest)
    else:
        rest = _strip_markers(text)

    sm = _SPEAKER_LINE_RE.match(rest)
    if sm:
        name = sm.group("name").strip()
        party = (sm.group("party") or "").strip()
    else:
        name, party = rest, ""
    # Drop honorifics like 'היו"ר ' / 'מ"מ היו"ר ' so the name field is
    # consistent across chair-vs-speaker rows for the same person.
    name = re.sub(r"^(?:מ\"מ\s+)?(?:היו\"ר|יו\"ר|היו״ר|יו״ר)\s+", "", name).strip()
    return role, name, party


def _extract_header(paragraphs: list) -> ProtocolHeader:
    """Pull metadata from the preamble of a protocol.

    Walks paragraphs until the first turn-header is encountered. We
    look for date-shaped lines, committee/session labels, and the
    participants block.
    """
    h = ProtocolHeader()
    in_participants = False
    for p in paragraphs:
        text = p.text.strip()
        if not text:
            continue
        # Stop scanning once the body starts.
        kind, _ = _classify(p)
        if kind == "turn_header":
            break

        # Knesset number line: "הכנסת העשרים-וחמש" or similar
        if "הכנסת" in text and "מושב" not in text and not h.knesset_num:
            m = re.search(r"הכנסת\s+ה?(\S+)", text)
            if m:
                h.knesset_num = m.group(1)

        # Protocol/session label
        if not h.session_label and (text.startswith("פרוטוקול") or "ישיבה" in text):
            h.session_label = text

        # Committee name
        if not h.committee_name and "ועדת" in text:
            m = re.search(r"(ועדת\s+\S+(?:\s+\S+){0,3})", text)
            if m:
                h.committee_name = m.group(1).strip()

        # Date — looks for Gregorian date in parens or after Hebrew date
        if not h.session_date and re.search(r"\d{4}", text) and (
            "תשפ" in text or "תשפ\"" in text or re.search(r"\d{1,2}\s+ב[א-ת]+", text)
        ):
            h.session_date = text

        # Participants block
        if "נכחו" in text or "חברי הוועדה" in text:
            in_participants = True
            continue
        if in_participants:
            # End of block when we hit something that doesn't look like a name
            # (e.g. a section header or a long sentence). MK names are short.
            if len(text) > 60 or text.endswith(":"):
                in_participants = False
                continue
            h.participants.append(text)

    return h


def parse_protocol(doc_bytes: bytes) -> tuple[ProtocolHeader, list[SpeakerTurn]]:
    """Parse a protocol .doc bytestream into header + speaker turns."""
    document = docx.Document(io.BytesIO(doc_bytes))
    paragraphs = list(document.paragraphs)
    header = _extract_header(paragraphs)

    turns: list[SpeakerTurn] = []
    current_agenda = ""
    current_turn: Optional[SpeakerTurn] = None
    ordinal = 0

    def _flush():
        nonlocal current_turn
        if current_turn is None:
            return
        # Drop empty turns (no body) — they're usually orphan markers.
        if current_turn.text.strip():
            turns.append(current_turn)
        current_turn = None

    for p in paragraphs:
        kind, payload = _classify(p)
        if kind == "skip":
            continue
        if kind == "agenda":
            if payload:
                current_agenda = payload
            continue
        if kind == "turn_header":
            _flush()
            ordinal += 1
            role, name, party = _parse_speaker_line(payload)
            current_turn = SpeakerTurn(
                ordinal=ordinal,
                role=role,
                speaker_name=name,
                speaker_party=party,
                agenda_item=current_agenda,
            )
            continue
        # body
        if current_turn is None:
            # Body paragraph before any turn header — skip (likely TOC remnant).
            continue
        if current_turn.text:
            current_turn.text += "\n" + payload
        else:
            current_turn.text = payload
        current_turn.paragraph_count += 1

    _flush()
    return header, turns


def parse_protocol_path(path) -> tuple[ProtocolHeader, list[SpeakerTurn]]:
    """Convenience wrapper for callers that have a filesystem path."""
    from pathlib import Path
    return parse_protocol(Path(path).read_bytes())
