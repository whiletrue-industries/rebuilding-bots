"""Unit tests for the protocol parser.

Builds tiny in-memory .docx documents with python-docx, fed through
parse_protocol(), and asserts the speaker-turn extraction handles the
real-world Knesset transcript conventions: ``<<יור>>``/``<<דובר>>``
markers, agenda-item resets, honorific stripping, party extraction in
parens, interjections, and continuation turns.
"""
from __future__ import annotations

import io
from typing import Optional

import docx
import pytest

from botnim.document_parser.knesset_protocols.parse_protocol import (
    parse_protocol,
    _parse_speaker_line,
    _strip_markers,
)


def _make_doc(paragraphs: list[tuple[str, Optional[str]]]) -> bytes:
    """Build a .docx in memory from (style, text) tuples."""
    d = docx.Document()
    for style, text in paragraphs:
        p = d.add_paragraph(text)
        if style:
            try:
                p.style = d.styles[style]
            except KeyError:
                d.styles.add_style(style, 1)
                p.style = d.styles[style]
    buf = io.BytesIO()
    d.save(buf)
    return buf.getvalue()


def test_basic_committee_protocol_yields_turns():
    blob = _make_doc([
        (None, "הכנסת העשרים-וחמש"),
        (None, "פרוטוקול מס' 99"),
        (None, "מישיבת ועדת הכספים"),
        (None, 'יום שלישי, ו\' בניסן התשפ"ו (24 במרץ 2026), שעה 12:00'),
        ("נושא", "<< נושא >> הצעת חוק לדוגמה << נושא >>"),
        ("יור", '<< יור >> היו"ר ישראל ישראלי: << יור >>'),
        (None, "פותח את הישיבה."),
        ("דובר", "<< דובר >> פלוני אלמוני (סיעת הליכוד): << דובר >>"),
        (None, "טקסט הדובר הראשון."),
        (None, "המשך טקסט הדובר הראשון."),
        ("יור", '<< יור >> היו"ר ישראל ישראלי: << יור >>'),
        (None, "תודה."),
    ])
    header, turns = parse_protocol(blob)
    assert "כספים" in header.committee_name
    assert "פרוטוקול מס' 99" == header.session_label
    assert len(turns) == 3

    t1, t2, t3 = turns
    assert t1.role == "chair"
    assert t1.speaker_name == "ישראל ישראלי"
    assert t1.text == "פותח את הישיבה."
    assert t1.agenda_item == "הצעת חוק לדוגמה"

    assert t2.role == "speaker"
    assert t2.speaker_name == "פלוני אלמוני"
    assert t2.speaker_party == "סיעת הליכוד"
    assert "טקסט הדובר הראשון" in t2.text
    assert "המשך טקסט" in t2.text
    assert t2.agenda_item == "הצעת חוק לדוגמה"

    assert t3.role == "chair"
    assert t3.text == "תודה."


def test_agenda_item_resets_for_subsequent_turns():
    blob = _make_doc([
        ("נושא", "<< נושא >> נושא ראשון"),
        ("דובר", "<< דובר >> אחד אחד: << דובר >>"),
        (None, "טקסט בנושא הראשון."),
        ("נושא", "<< נושא >> נושא שני"),
        ("דובר", "<< דובר >> שניים שניים: << דובר >>"),
        (None, "טקסט בנושא השני."),
    ])
    _, turns = parse_protocol(blob)
    assert len(turns) == 2
    assert turns[0].agenda_item == "נושא ראשון"
    assert turns[1].agenda_item == "נושא שני"


def test_plenum_haza_marker_recognized_as_agenda():
    blob = _make_doc([
        (None, "<< הצח >> הצעת חוק כלשהי << הצח >>"),
        ("יור", "<< יור >> יושב הראש: << יור >>"),
        (None, "דברים."),
    ])
    _, turns = parse_protocol(blob)
    assert len(turns) == 1
    assert turns[0].agenda_item == "הצעת חוק כלשהי"


def test_interjection_role_classified():
    blob = _make_doc([
        ("דובר", "<< דובר >> דובר ראשי: << דובר >>"),
        (None, "מתחיל לדבר..."),
        ("קריאות", "<< קריאה >> מפריע: << קריאה >>"),
        (None, "אתה לא צודק!"),
    ])
    _, turns = parse_protocol(blob)
    assert len(turns) == 2
    assert turns[0].role == "speaker"
    assert turns[1].role == "interjection"
    assert turns[1].speaker_name == "מפריע"


def test_continuation_role_classified():
    blob = _make_doc([
        ("דובר", "<< דובר >> אריאל אריאל (סיעת המרכז): << דובר >>"),
        (None, "פתיחה."),
        ("קריאות", "<< קריאה >> מאן דהוא: << קריאה >>"),
        (None, "הפרעה."),
        ("דובר-המשך", "<< דובר_המשך >> אריאל אריאל (סיעת המרכז): << דובר_המשך >>"),
        (None, "המשך אחרי ההפרעה."),
    ])
    _, turns = parse_protocol(blob)
    assert len(turns) == 3
    assert turns[2].role == "speaker_continued"
    assert turns[2].speaker_name == "אריאל אריאל"
    assert turns[2].speaker_party == "סיעת המרכז"


def test_honorific_stripped_from_chair_name():
    role, name, party = _parse_speaker_line('<< יור >> היו"ר אבי כהן: << יור >>')
    assert role == "chair"
    assert name == "אבי כהן"
    assert party == ""


def test_speaker_with_party_in_parens():
    role, name, party = _parse_speaker_line(
        '<< דובר >> רותי לוי (יש עתיד): << דובר >>'
    )
    assert role == "speaker"
    assert name == "רותי לוי"
    assert party == "יש עתיד"


def test_strip_markers_removes_decorator_pairs():
    assert _strip_markers("<< נושא >> שלום << נושא >>") == "שלום"
    assert _strip_markers("טקסט נקי") == "טקסט נקי"


def test_empty_turn_dropped():
    """A turn header with no body paragraph should not produce a row."""
    blob = _make_doc([
        ("דובר", "<< דובר >> דובר א': << דובר >>"),
        ("דובר", "<< דובר >> דובר ב': << דובר >>"),
        (None, "אני זה דובר ב'."),
    ])
    _, turns = parse_protocol(blob)
    assert len(turns) == 1
    assert turns[0].speaker_name == "דובר ב'"


def test_body_before_first_turn_is_skipped():
    """Lone Normal paragraphs before any turn header are TOC remnants."""
    blob = _make_doc([
        (None, "תוכן עניינים"),
        (None, "פריט תוכן 1"),
        ("דובר", "<< דובר >> ראשון: << דובר >>"),
        (None, "תוכן בפועל."),
    ])
    _, turns = parse_protocol(blob)
    assert len(turns) == 1
    assert turns[0].text == "תוכן בפועל."
