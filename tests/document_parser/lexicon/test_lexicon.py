"""Tests for the lexicon scraper's CSV output shape."""
import csv
from pathlib import Path
from unittest.mock import patch

from botnim.document_parser.lexicon import lexicon as lex_mod


_FAKE_INDEX_HTML = '''
<html><body>
  <table>
    <tr><td class="lexColumns">
        <a href="/About/Lexicon/Pages/query.aspx">שאילתות חבר הכנסת</a>
    </td></tr>
    <tr><td class="lexColumns">
        <a href="/About/Lexicon/Pages/reservation.aspx">הסתייגות בוועדה</a>
    </td></tr>
    <tr><td class="lexColumns">
        <a href="/About/Lexicon/Pages/dictionary.aspx">פיתוח-הפרטה</a>
    </td></tr>
  </table>
</body></html>
'''

_FAKE_ENTRY_HTML_QUERY = '''
<html><body><div class="LexiconContent">
  שאילתות: לפי סעיף 137 לתקנון הכנסת, חבר הכנסת רשאי לפנות לשר בשאילתה.
</div></body></html>
'''

_FAKE_ENTRY_HTML_RESERVATION = '''
<html><body><div class="LexiconContent">
  הסתייגות בוועדה: לפי סעיף 86 לתקנון הכנסת, חבר הכנסת רשאי להציע תיקונים.
</div></body></html>
'''

_FAKE_ENTRY_HTML_GENERIC = '''
<html><body><div class="LexiconContent">
  פיתוח-הפרטה: מונח כללי בלי הפניה לסעיף ספציפי.
</div></body></html>
'''


def _mock_get(url, headers=None):  # noqa: ARG001
    class _Resp:
        status_code = 200
        text: str = ""
    r = _Resp()
    if url.endswith("/about/lexicon/pages/default.aspx"):
        r.text = _FAKE_INDEX_HTML
    elif url.endswith("query.aspx"):
        r.text = _FAKE_ENTRY_HTML_QUERY
    elif url.endswith("reservation.aspx"):
        r.text = _FAKE_ENTRY_HTML_RESERVATION
    elif url.endswith("dictionary.aspx"):
        r.text = _FAKE_ENTRY_HTML_GENERIC
    return r


def _read_csv(path: Path) -> list[dict[str, str]]:
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_csv_has_three_columns(tmp_path):
    out = tmp_path / "lexicon.csv"
    with patch.object(lex_mod, "requests", create=True) as mock_req, \
         patch.object(lex_mod.time, "sleep", lambda _s: None):
        mock_req.get = _mock_get
        lex_mod.scrape_lexicon(out)
    rows = _read_csv(out)
    assert {"מידע", "lexicon_url", "source_url"} == set(rows[0].keys())
    assert len(rows) == 3


def test_content_does_not_contain_markdown_link(tmp_path):
    """The `[קישור למידע](URL)` segment must NOT appear in column מידע."""
    out = tmp_path / "lexicon.csv"
    with patch.object(lex_mod, "requests", create=True) as mock_req, \
         patch.object(lex_mod.time, "sleep", lambda _s: None):
        mock_req.get = _mock_get
        lex_mod.scrape_lexicon(out)
    rows = _read_csv(out)
    for r in rows:
        assert "[קישור למידע]" not in r["מידע"], r["מידע"]
        assert "https://" not in r["מידע"], r["מידע"]


def test_source_url_uses_wikisource_when_section_detected(tmp_path):
    """Entries that reference a known law+section get a Wikisource URL."""
    out = tmp_path / "lexicon.csv"
    with patch.object(lex_mod, "requests", create=True) as mock_req, \
         patch.object(lex_mod.time, "sleep", lambda _s: None):
        mock_req.get = _mock_get
        lex_mod.scrape_lexicon(out)
    rows = _read_csv(out)
    by_url = {r["lexicon_url"].rsplit("/", 1)[-1]: r for r in rows}

    # שאילתות → סעיף 137 לתקנון
    expected_takanon_137 = (
        "https://he.wikisource.org/wiki/"
        "%D7%AA%D7%A7%D7%A0%D7%95%D7%9F_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA"
        "#%D7%A1%D7%A2%D7%99%D7%A3_137"
    )
    assert by_url["query.aspx"]["source_url"] == expected_takanon_137

    # הסתייגות → סעיף 86 לתקנון
    expected_takanon_86 = (
        "https://he.wikisource.org/wiki/"
        "%D7%AA%D7%A7%D7%A0%D7%95%D7%9F_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA"
        "#%D7%A1%D7%A2%D7%99%D7%A3_86"
    )
    assert by_url["reservation.aspx"]["source_url"] == expected_takanon_86


def test_source_url_falls_back_to_lexicon_url(tmp_path):
    """Generic entries without a section reference keep the Lexicon URL."""
    out = tmp_path / "lexicon.csv"
    with patch.object(lex_mod, "requests", create=True) as mock_req, \
         patch.object(lex_mod.time, "sleep", lambda _s: None):
        mock_req.get = _mock_get
        lex_mod.scrape_lexicon(out)
    rows = _read_csv(out)
    by_url = {r["lexicon_url"].rsplit("/", 1)[-1]: r for r in rows}
    assert by_url["dictionary.aspx"]["source_url"] == by_url["dictionary.aspx"]["lexicon_url"]
