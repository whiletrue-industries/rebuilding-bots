"""Tests for the index-hash short-circuit in lexicon.scrape_lexicon."""
from __future__ import annotations

import csv
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from botnim.document_parser.lexicon import lexicon


_INDEX_HTML_V1 = """
<html><body>
<table><tr>
<td class="lexColumns"><a href="/About/Lexicon/Pages/edelshtein.aspx">אדלשטיין, יולי</a></td>
<td class="lexColumns"><a href="/About/Lexicon/Pages/opposition.aspx">אופוזיציה</a></td>
</tr></table>
</body></html>
"""

_INDEX_HTML_V2 = _INDEX_HTML_V1 + "<!-- v2 -->"  # any byte change → different sha256

_ENTRY_HTML = """
<html><body><div class="LexiconContent">תוכן דף ביוגרפיה לדוגמא.</div></body></html>
"""


def _resp(text: str, status: int = 200):
    r = MagicMock()
    r.status_code = status
    r.text = text
    return r


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
    # Sentinel matches sha256 of v1 index html
    import hashlib
    assert sentinel.read_text().strip() == hashlib.sha256(
        _INDEX_HTML_V1.encode("utf-8")
    ).hexdigest()
    # CSV has header + 2 entry rows
    with open(out, encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["מידע"]
    assert len(rows) == 3  # header + 2


@patch.object(lexicon, "time")
@patch.object(lexicon, "requests")
def test_unchanged_index_short_circuits(mock_requests, _mock_time, tmp_path: Path):
    """Second run with the same index hash + existing CSV must NOT iterate entries."""
    out = tmp_path / "lexicon.csv"
    sentinel = tmp_path / "lexicon.csv.index.sha256"

    # Pre-populate sentinel + CSV from a prior run.
    out.write_text("מידע\nold row\n", encoding="utf-8")
    import hashlib
    sentinel.write_text(
        hashlib.sha256(_INDEX_HTML_V1.encode("utf-8")).hexdigest(),
        encoding="utf-8",
    )

    # Only the index GET should fire — no per-entry GETs.
    mock_requests.get.side_effect = [_resp(_INDEX_HTML_V1)]

    lexicon.scrape_lexicon(out)

    # CSV untouched.
    assert out.read_text(encoding="utf-8") == "מידע\nold row\n"
    # Exactly one GET (index only).
    assert mock_requests.get.call_count == 1


@patch.object(lexicon, "time")
@patch.object(lexicon, "requests")
def test_changed_index_re_scrapes(mock_requests, _mock_time, tmp_path: Path):
    """Different index hash forces a full re-scrape."""
    out = tmp_path / "lexicon.csv"
    sentinel = tmp_path / "lexicon.csv.index.sha256"

    out.write_text("מידע\nold row\n", encoding="utf-8")
    import hashlib
    sentinel.write_text(
        hashlib.sha256(_INDEX_HTML_V1.encode("utf-8")).hexdigest(),
        encoding="utf-8",
    )

    # Server now returns v2 (different bytes).
    mock_requests.get.side_effect = [
        _resp(_INDEX_HTML_V2),
        _resp(_ENTRY_HTML),
        _resp(_ENTRY_HTML),
    ]

    lexicon.scrape_lexicon(out)

    # CSV rewritten (no longer the "old row" content).
    with open(out, encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["מידע"]
    assert len(rows) == 3  # header + 2 fresh
    assert all("old row" not in r[0] for r in rows[1:])
    # Sentinel updated to v2 hash.
    assert sentinel.read_text().strip() == hashlib.sha256(
        _INDEX_HTML_V2.encode("utf-8")
    ).hexdigest()


@patch.object(lexicon, "time")
@patch.object(lexicon, "requests")
def test_missing_csv_re_scrapes_even_if_sentinel_present(mock_requests, _mock_time, tmp_path: Path):
    """If user / ops deletes the CSV but the sentinel is left, we must re-scrape."""
    out = tmp_path / "lexicon.csv"
    sentinel = tmp_path / "lexicon.csv.index.sha256"

    import hashlib
    sentinel.write_text(
        hashlib.sha256(_INDEX_HTML_V1.encode("utf-8")).hexdigest(),
        encoding="utf-8",
    )
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
def test_index_500_propagates(mock_requests, _mock_time, tmp_path: Path):
    """Server failure on the index must surface, not silently leave stale CSV."""
    out = tmp_path / "lexicon.csv"
    out.write_text("מידע\nold row\n", encoding="utf-8")
    mock_requests.get.return_value = _resp("oops", status=500)
    with pytest.raises(Exception, match="500"):
        lexicon.scrape_lexicon(out)
    # CSV must be untouched.
    assert out.read_text(encoding="utf-8") == "מידע\nold row\n"
