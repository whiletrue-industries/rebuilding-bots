"""Tests for the index-hash short-circuit in lexicon.scrape_lexicon."""
from __future__ import annotations

import csv
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from botnim.document_parser.lexicon import lexicon


_INDEX_HTML_V1 = """
<html><body>
<input type="hidden" name="__VIEWSTATE" value="aaa-session-v1-token-bbb" />
<table><tr>
<td class="lexColumns"><a href="/About/Lexicon/Pages/edelshtein.aspx">אדלשטיין, יולי</a></td>
<td class="lexColumns"><a href="/About/Lexicon/Pages/opposition.aspx">אופוזיציה</a></td>
</tr></table>
</body></html>
"""

# V2 has a NEW entry — hrefs differ → re-scrape expected.
_INDEX_HTML_V2 = """
<html><body>
<input type="hidden" name="__VIEWSTATE" value="ccc-session-v2-token-ddd" />
<table><tr>
<td class="lexColumns"><a href="/About/Lexicon/Pages/edelshtein.aspx">אדלשטיין, יולי</a></td>
<td class="lexColumns"><a href="/About/Lexicon/Pages/opposition.aspx">אופוזיציה</a></td>
<td class="lexColumns"><a href="/About/Lexicon/Pages/herzog.aspx">הרצוג</a></td>
</tr></table>
</body></html>
"""

# V1_DRIFT has the SAME hrefs but different ViewState + whitespace —
# regression test for the prod bug where ASP.NET ViewState drift made
# raw-HTML hashing useless. Dehydrated href-list hash MUST match V1.
_INDEX_HTML_V1_DRIFT = """
<html><body>
<input type="hidden" name="__VIEWSTATE" value="zzz-different-session-token" />
<table>
   <tr>
<td class="lexColumns">  <a href="/About/Lexicon/Pages/edelshtein.aspx">אדלשטיין, יולי</a>  </td>
<td class="lexColumns"><a href="/About/Lexicon/Pages/opposition.aspx">אופוזיציה</a></td>
   </tr>
</table>
<!-- timestamp: 2026-05-06T10:23:11Z -->
</body></html>
"""

_ENTRY_HTML = """
<html><body><div class="LexiconContent">תוכן דף ביוגרפיה לדוגמא.</div></body></html>
"""


def _resp(text: str, status: int = 200):
    r = MagicMock()
    r.status_code = status
    r.text = text
    return r


def _expected_hash(html: str) -> str:
    """Mirror lexicon._fetch_index's dehydrated hash for assertions."""
    import hashlib
    from pyquery import PyQuery as pq
    doc = pq(html)
    hrefs = sorted({(pq(a).attr('href') or '').strip() for a in doc(lexicon.LINK_CLASS)})
    hrefs = [h for h in hrefs if h]
    return hashlib.sha256('\n'.join(hrefs).encode('utf-8')).hexdigest()


@patch.object(lexicon, "time")  # silence the 5s sleep
@patch.object(lexicon, "requests")
def test_first_run_writes_csv_and_sentinel(mock_requests, _mock_time, tmp_path: Path):
    out = tmp_path / "lexicon.csv"
    sentinel = tmp_path / "lexicon.csv.index.sha256"

    # Index page + one entry response per link (2 links, so 3 total GETs).
    mock_requests.get.side_effect = [
        _resp(_INDEX_HTML_V1),
        _resp(_ENTRY_HTML),
        _resp(_ENTRY_HTML),
    ]

    lexicon.scrape_lexicon(out)

    assert out.exists()
    assert sentinel.exists()
    # Sentinel matches the dehydrated href-list hash (NOT raw HTML).
    assert sentinel.read_text().strip() == _expected_hash(_INDEX_HTML_V1)
    # CSV has header + 2 entry rows. 3-column format introduced 2026-05-13:
    # מידע (content), lexicon_url (traceability), source_url (Wikisource
    # anchor when detectable, else falls back to lexicon_url).
    with open(out, encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["מידע", "lexicon_url", "source_url"]
    assert len(rows) == 3  # header + 2


@patch.object(lexicon, "time")
@patch.object(lexicon, "requests")
def test_unchanged_index_short_circuits(mock_requests, _mock_time, tmp_path: Path):
    """Second run with the same index hash + existing CSV must NOT iterate entries."""
    out = tmp_path / "lexicon.csv"
    sentinel = tmp_path / "lexicon.csv.index.sha256"

    # Pre-populate sentinel + CSV from a prior run. The CSV must carry
    # the current 3-col header — a legacy 1-col CSV would (correctly)
    # trip the schema-upgrade guard and force a re-scrape.
    legacy_body = "מידע,lexicon_url,source_url\nold row,a,b\n"
    out.write_text(legacy_body, encoding="utf-8")
    sentinel.write_text(_expected_hash(_INDEX_HTML_V1), encoding="utf-8")

    # Only the index GET should fire — no per-entry GETs.
    mock_requests.get.side_effect = [_resp(_INDEX_HTML_V1)]

    lexicon.scrape_lexicon(out)

    # CSV untouched.
    assert out.read_text(encoding="utf-8") == legacy_body
    # Exactly one GET (index only).
    assert mock_requests.get.call_count == 1


@patch.object(lexicon, "time")
@patch.object(lexicon, "requests")
def test_changed_index_re_scrapes(mock_requests, _mock_time, tmp_path: Path):
    """Different index hash forces a full re-scrape."""
    out = tmp_path / "lexicon.csv"
    sentinel = tmp_path / "lexicon.csv.index.sha256"

    out.write_text("מידע\nold row\n", encoding="utf-8")
    sentinel.write_text(_expected_hash(_INDEX_HTML_V1), encoding="utf-8")

    # Server now returns V2 — has 3 entries (added Herzog) so the
    # dehydrated href-list hash differs.
    mock_requests.get.side_effect = [
        _resp(_INDEX_HTML_V2),
        _resp(_ENTRY_HTML),
        _resp(_ENTRY_HTML),
        _resp(_ENTRY_HTML),
    ]

    lexicon.scrape_lexicon(out)

    # CSV rewritten (no longer the "old row" content). 3-column format.
    with open(out, encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["מידע", "lexicon_url", "source_url"]
    assert len(rows) == 4  # header + 3 fresh entries
    assert all("old row" not in r[0] for r in rows[1:])
    # Sentinel updated to V2's dehydrated hash.
    assert sentinel.read_text().strip() == _expected_hash(_INDEX_HTML_V2)


@patch.object(lexicon, "time")
@patch.object(lexicon, "requests")
def test_missing_csv_re_scrapes_even_if_sentinel_present(mock_requests, _mock_time, tmp_path: Path):
    """If user / ops deletes the CSV but the sentinel is left, we must re-scrape."""
    out = tmp_path / "lexicon.csv"
    sentinel = tmp_path / "lexicon.csv.index.sha256"

    sentinel.write_text(_expected_hash(_INDEX_HTML_V1), encoding="utf-8")
    # No CSV.

    mock_requests.get.side_effect = [
        _resp(_INDEX_HTML_V1),
        _resp(_ENTRY_HTML),
        _resp(_ENTRY_HTML),
    ]

    lexicon.scrape_lexicon(out)
    assert out.exists()
    # Three GETs: index + 2 entries.
    assert mock_requests.get.call_count == 3


@patch.object(lexicon, "time")
@patch.object(lexicon, "requests")
def test_viewstate_drift_does_not_force_rescrape(mock_requests, _mock_time, tmp_path: Path):
    """Regression for prod bug: ASP.NET emits per-request ViewState/timestamp
    bytes, so two consecutive raw-HTML hashes never match. The dehydrated
    href-list hash MUST be stable across that drift — same hrefs → same hash
    → short-circuit fires."""
    out = tmp_path / "lexicon.csv"
    sentinel = tmp_path / "lexicon.csv.index.sha256"

    legacy_body = "מידע,lexicon_url,source_url\nold row,a,b\n"
    out.write_text(legacy_body, encoding="utf-8")
    sentinel.write_text(_expected_hash(_INDEX_HTML_V1), encoding="utf-8")

    # Server returns V1_DRIFT — same hrefs as V1 but different ViewState +
    # whitespace + timestamp. Raw-HTML hash would differ; dehydrated hash
    # must NOT.
    mock_requests.get.side_effect = [_resp(_INDEX_HTML_V1_DRIFT)]

    lexicon.scrape_lexicon(out)

    # CSV untouched (short-circuit fired despite the drift).
    assert out.read_text(encoding="utf-8") == legacy_body
    # Only the index GET — no per-entry GETs.
    assert mock_requests.get.call_count == 1
    # Sentinel unchanged (stayed at V1's hash).
    assert sentinel.read_text().strip() == _expected_hash(_INDEX_HTML_V1)
    # Sanity: V1's dehydrated hash equals V1_DRIFT's despite raw HTML differing.
    assert _expected_hash(_INDEX_HTML_V1) == _expected_hash(_INDEX_HTML_V1_DRIFT)
    import hashlib
    assert (
        hashlib.sha256(_INDEX_HTML_V1.encode("utf-8")).hexdigest()
        != hashlib.sha256(_INDEX_HTML_V1_DRIFT.encode("utf-8")).hexdigest()
    )


@patch.object(lexicon, "time")
@patch.object(lexicon, "requests")
def test_index_500_propagates(mock_requests, _mock_time, tmp_path: Path):
    """Server failure on the index must surface, not silently leave stale CSV."""
    out = tmp_path / "lexicon.csv"
    out.write_text("מידע\nold row\n", encoding="utf-8")
    mock_requests.get.return_value = _resp("oops", status=500)
    with pytest.raises(Exception, match="500"):
        lexicon.scrape_lexicon(out)
    # CSV must be untouched.
    assert out.read_text(encoding="utf-8") == "מידע\nold row\n"
