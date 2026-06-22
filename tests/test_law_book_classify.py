import pytest
from botnim.document_parser.wikisource_law_book.classify import classify_title


@pytest.mark.parametrize("title,expected", [
    ("חוק האזנת סתר", "law"),
    ("חוק הגנת הפרטיות", "law"),
    ("חוק-יסוד: הכנסת", "law"),
    ("חוק יסוד: כבוד האדם וחירותו", "law"),
    ("פקודת מס הכנסה", "law"),
    ("פקודת הראיות [נוסח חדש]", "law"),
    ("תקנות הגנת הפרטיות (אבטחת מידע)", "regulation"),
    ("צו המועצות המקומיות", "regulation"),
    ("כללי לשכת עורכי הדין", "regulation"),
    ("תקנון הכנסת", "regulation"),
    ("ויקיטקסט:אודות", "other"),
    ("עזרה:תוכן", "other"),
    ("מדיניות הפרטיות", "other"),
    ("", "other"),
    # legal_text carry-over: starts with החלט (→ "other" by default) but must be
    # ingested into israeli_laws for the single-source consolidation.
    ("החלטת שכר חברי הכנסת (הענקות ותשלומים)", "regulation"),
    # guard: the carry-over allow-list stays narrow — generic החלטות stay "other".
    ("החלטת ממשלה מספר 550", "other"),
])
def test_classify_title(title, expected):
    assert classify_title(title) == expected
