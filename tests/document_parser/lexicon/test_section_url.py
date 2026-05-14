"""Tests for section_url.derive_section_url — Hebrew section→Wikisource anchor."""
import pytest
from botnim.document_parser.lexicon.section_url import derive_section_url


# Wikisource URL prefixes for the 5 known laws (URL-encoded).
TAKANON = (
    "https://he.wikisource.org/wiki/"
    "%D7%AA%D7%A7%D7%A0%D7%95%D7%9F_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA"
)
YESOD = (
    "https://he.wikisource.org/wiki/"
    "%D7%97%D7%95%D7%A7-%D7%99%D7%A1%D7%95%D7%93:_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA"
)
CHOK_KNESSET = (
    "https://he.wikisource.org/wiki/"
    "%D7%97%D7%95%D7%A7_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA"
)
ETHICS = (
    "https://he.wikisource.org/wiki/"
    "%D7%9B%D7%9C%D7%9C%D7%99_%D7%90%D7%AA%D7%99%D7%A7%D7%94_"
    "%D7%9C%D7%97%D7%91%D7%A8%D7%99_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA"
)
PARSHANUT = (
    "https://he.wikisource.org/wiki/"
    "%D7%97%D7%95%D7%A7_%D7%94%D7%A4%D7%A8%D7%A9%D7%A0%D7%95%D7%AA"
)
SECTION = "#%D7%A1%D7%A2%D7%99%D7%A3_"  # "#סעיף_" url-encoded


@pytest.mark.parametrize("text,expected", [
    # תקנון variants
    ("בהתאם לסעיף 137 לתקנון הכנסת",          f"{TAKANON}{SECTION}137"),
    ("תקנון הכנסת, סעיף 86",                  f"{TAKANON}{SECTION}86"),
    ("ראו סעיף 42(ב)(1) לתקנון הכנסת",        f"{TAKANON}{SECTION}42"),
    # חוק־יסוד: הכנסת (with U+05BE Hebrew dash or hyphen)
    ("חוק־יסוד: הכנסת, סעיף 22",              f"{YESOD}{SECTION}22"),
    ("חוק-יסוד: הכנסת, סעיף 6א",              f"{YESOD}{SECTION}6%D7%90"),
    ("סעיף 20א של חוק־יסוד: הכנסת",           f"{YESOD}{SECTION}20%D7%90"),
    # חוק הכנסת (be careful not to confuse with חוק־יסוד: הכנסת)
    ("חוק הכנסת, סעיף 61",                    f"{CHOK_KNESSET}{SECTION}61"),
    ("ראו את חוק הכנסת סעיף 8א",              f"{CHOK_KNESSET}{SECTION}8%D7%90"),
    # כללי אתיקה
    ("כללי אתיקה לחברי הכנסת, סעיף 14א",      f"{ETHICS}{SECTION}14%D7%90"),
    # חוק הפרשנות
    ("חוק הפרשנות, סעיף 25",                  f"{PARSHANUT}{SECTION}25"),
])
def test_derive_section_url_known_law(text, expected):
    assert derive_section_url(text) == expected


@pytest.mark.parametrize("text", [
    "",
    "סעיף 137",                                # no law name → ambiguous
    "מידע כללי על הכנסת",                       # no section reference
    "חוק חובת המכרזים, התשנ\"ב-1992",          # known unsupported law
    "החלטות ועדת האתיקה",                       # not section-anchored
])
def test_derive_section_url_no_match_returns_none(text):
    assert derive_section_url(text) is None


def test_derive_section_url_prefers_takanon_when_disambiguated():
    """If text mentions תקנון first, prefer that even if חוק הכנסת appears later."""
    text = "לפי תקנון הכנסת סעיף 86, ובהמשך לחוק הכנסת"
    expected = "https://he.wikisource.org/wiki/%D7%AA%D7%A7%D7%A0%D7%95%D7%9F_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA#%D7%A1%D7%A2%D7%99%D7%A3_86"
    assert derive_section_url(text) == expected


def test_derive_section_url_handles_niqqud():
    """Hebrew vowel marks shouldn't break the regex."""
    text = "סָעִיף 137 לְתַקָּנוֹן הַכְּנֶסֶת"
    expected = "https://he.wikisource.org/wiki/%D7%AA%D7%A7%D7%A0%D7%95%D7%9F_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA#%D7%A1%D7%A2%D7%99%D7%A3_137"
    assert derive_section_url(text) == expected
