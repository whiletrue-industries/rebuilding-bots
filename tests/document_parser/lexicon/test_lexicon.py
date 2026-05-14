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


def test_curated_override_wins_over_regex_derived_and_fallback(tmp_path):
    """Hand-curated overrides take priority over derive_section_url and the
    lexicon-URL fallback.

    Reproduces the production bug fixed by the overrides file: the שאילתות
    entry's body describes §137 mechanics without naming the section, so
    ``derive_section_url`` returns None and the source_url falls back to
    the Knesset glossary page. With an override for that URL, the source
    points at the takanon §137 Wikisource anchor.
    """
    out = tmp_path / "lexicon.csv"
    # The third mocked entry's content_url ends with dictionary.aspx and
    # its body has no section reference (see _FAKE_ENTRY_HTML_GENERIC).
    fake_overrides = {
        "https://main.knesset.gov.il/About/Lexicon/Pages/dictionary.aspx":
            "https://he.wikisource.org/wiki/%D7%AA%D7%A7%D7%A0%D7%95%D7%9F_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA#%D7%A1%D7%A2%D7%99%D7%A3_137",
    }
    with patch.object(lex_mod, "requests", create=True) as mock_req, \
         patch.object(lex_mod.time, "sleep", lambda _s: None), \
         patch.object(lex_mod, "_load_section_overrides", lambda: fake_overrides):
        mock_req.get = _mock_get
        lex_mod.scrape_lexicon(out)
    rows = _read_csv(out)
    by_url = {r["lexicon_url"].rsplit("/", 1)[-1]: r for r in rows}
    assert by_url["dictionary.aspx"]["source_url"] == fake_overrides[
        "https://main.knesset.gov.il/About/Lexicon/Pages/dictionary.aspx"
    ]


def test_curated_override_beats_regex_derived(tmp_path):
    """If BOTH a regex-derived anchor AND an override exist, override wins.

    Lets us correct mis-derivations without changing the regex (which is
    used by many lexicon entries).
    """
    out = tmp_path / "lexicon.csv"
    # query.aspx's mocked body cites "סעיף 137 לתקנון הכנסת" — derive_section_url
    # would produce תקנון §137. Override targets §86 instead. Override must win.
    target_url = "https://he.wikisource.org/wiki/%D7%AA%D7%A7%D7%A0%D7%95%D7%9F_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA#%D7%A1%D7%A2%D7%99%D7%A3_86"
    fake_overrides = {
        "https://main.knesset.gov.il/About/Lexicon/Pages/query.aspx": target_url,
    }
    with patch.object(lex_mod, "requests", create=True) as mock_req, \
         patch.object(lex_mod.time, "sleep", lambda _s: None), \
         patch.object(lex_mod, "_load_section_overrides", lambda: fake_overrides):
        mock_req.get = _mock_get
        lex_mod.scrape_lexicon(out)
    rows = _read_csv(out)
    by_url = {r["lexicon_url"].rsplit("/", 1)[-1]: r for r in rows}
    assert by_url["query.aspx"]["source_url"] == target_url


def test_load_section_overrides_reads_committed_file():
    """Sanity-check that the committed overrides JSON in the repo loads."""
    overrides = lex_mod._load_section_overrides()
    assert isinstance(overrides, dict)
    assert len(overrides) > 0, "expected at least one curated override"
    # All values must be Wikisource URLs (the only legal target shape).
    for k, v in overrides.items():
        assert v.startswith("https://he.wikisource.org/"), f"bad target for {k}: {v}"
    # Known sanity-DoD entries must be present in the override file.
    assert "https://main.knesset.gov.il/About/Lexicon/Pages/query.aspx" in overrides
    assert "https://main.knesset.gov.il/About/Lexicon/Pages/reservation.aspx" in overrides


def test_legacy_one_column_csv_triggers_rescrape_even_when_sentinel_matches(tmp_path):
    """Schema-upgrade guard: pre-existing 1-col CSV must NOT short-circuit.

    On a real staging EFS, the existing ``lexicon.csv`` is the legacy
    1-column shape AND its sentinel matches Knesset's current index hash.
    Without this guard the new scraper would early-return and never write
    the 3-column CSV. This test reproduces that exact state.
    """
    out = tmp_path / "lexicon.csv"
    sentinel = tmp_path / "lexicon.csv.index.sha256"

    # Pre-seed legacy 1-column CSV + a sentinel that will match the
    # _fetch_index hash we're about to compute.
    out.write_text("מידע\nשורה ישנה\n", encoding="utf-8")

    with patch.object(lex_mod, "requests", create=True) as mock_req, \
         patch.object(lex_mod.time, "sleep", lambda _s: None):
        mock_req.get = _mock_get
        # First call: compute current hash to pre-seed sentinel.
        _, current_hash = lex_mod._fetch_index()
        sentinel.write_text(current_hash, encoding="utf-8")
        # The actual run under test.
        lex_mod.scrape_lexicon(out)

    rows = _read_csv(out)
    assert {"מידע", "lexicon_url", "source_url"} == set(rows[0].keys()), \
        "scraper should have re-scraped into the 3-col schema"
    assert len(rows) == 3


def test_csv_matches_current_schema_helper(tmp_path):
    """The helper distinguishes legacy 1-col, current 3-col, and missing."""
    missing = tmp_path / "missing.csv"
    legacy = tmp_path / "legacy.csv"
    legacy.write_text("מידע\nx\n", encoding="utf-8")
    current = tmp_path / "current.csv"
    current.write_text("מידע,lexicon_url,source_url\na,b,c\n", encoding="utf-8")

    assert lex_mod._csv_matches_current_schema(missing) is False
    assert lex_mod._csv_matches_current_schema(legacy) is False
    assert lex_mod._csv_matches_current_schema(current) is True


def test_matching_schema_csv_with_matching_sentinel_short_circuits(tmp_path):
    """Idempotent path: 3-col CSV + matching sentinel → no re-scrape."""
    out = tmp_path / "lexicon.csv"
    sentinel = tmp_path / "lexicon.csv.index.sha256"
    out.write_text("מידע,lexicon_url,source_url\noriginal,a,b\n", encoding="utf-8")

    call_count = {"n": 0}
    original_iter = lex_mod._iter_entries

    def counting_iter(html):
        call_count["n"] += 1
        yield from original_iter(html)

    with patch.object(lex_mod, "requests", create=True) as mock_req, \
         patch.object(lex_mod.time, "sleep", lambda _s: None), \
         patch.object(lex_mod, "_iter_entries", counting_iter):
        mock_req.get = _mock_get
        _, current_hash = lex_mod._fetch_index()
        sentinel.write_text(current_hash, encoding="utf-8")
        lex_mod.scrape_lexicon(out)

    assert call_count["n"] == 0, "scrape should have short-circuited"
    assert out.read_text(encoding="utf-8").startswith("מידע,lexicon_url,source_url\noriginal,")
