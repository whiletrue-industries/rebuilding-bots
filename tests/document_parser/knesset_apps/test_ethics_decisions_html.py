"""Unit tests for knesset_apps.ethics_decisions_html."""
from __future__ import annotations

import csv
import io
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from botnim.document_parser.knesset_apps.ethics_decisions_html import (
    EthicsDecisionsConfig,
    _absolute,
    fetch_ethics_decisions_index,
)
from botnim.document_parser.knesset_apps.common import (
    DocRow,
    EmptyUpstreamIndex,
    atomic_write_csv,
)
from botnim.storage.local_fs import LocalFsStore

_KEY = "cache/unified/extraction/ethics_decisions.csv"


def _resp(payload, status=200):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = payload
    r.raise_for_status = MagicMock()
    return r


# ---------- _absolute ----------

def test_absolute_passes_full_urls():
    assert _absolute("https://fs.knesset.gov.il/x.pdf") == "https://fs.knesset.gov.il/x.pdf"
    assert _absolute("http://fs.knesset.gov.il/x.pdf") == "http://fs.knesset.gov.il/x.pdf"


def test_absolute_resolves_protocol_relative():
    assert _absolute("//fs.knesset.gov.il/x.pdf") == "https://fs.knesset.gov.il/x.pdf"


def test_absolute_resolves_root_relative():
    assert _absolute("/Activity/committees/Ethics/x.pdf") == \
        "https://main.knesset.gov.il/Activity/committees/Ethics/x.pdf"


def test_absolute_resolves_naked_path():
    assert _absolute("Activity/committees/Ethics/x.pdf") == \
        "https://main.knesset.gov.il/Activity/committees/Ethics/x.pdf"


# ---------- happy path ----------

def test_extracts_pdf_anchors_with_dates(tmp_path: Path):
    """Mirror the actual EthicsDecisions25 HTML structure we sniffed."""
    store = LocalFsStore(tmp_path)
    cfg = EthicsDecisionsConfig(store=store, key=_KEY)
    html = """
    <html><body><table>
      <tr>
        <td>10.2.2026</td>
        <td><a href="https://main.knesset.gov.il/Activity/committees/Ethics/Decisions25/Decisions25-50.pdf">החלטה מס' 50/25</a></td>
      </tr>
      <tr>
        <td>2.1.2026</td>
        <td><a href="https://main.knesset.gov.il/Activity/committees/Ethics/Decisions25/Decisions25-49.pdf">החלטה מס' 49/25</a></td>
      </tr>
    </table></body></html>
    """
    http = MagicMock(return_value=_resp({"Title": "...", "Lang": "", "Html": html}))
    rows = fetch_ethics_decisions_index(cfg, http_get=http)
    assert len(rows) == 2
    by_url = {r.url: r for r in rows}
    assert "Decisions25-50.pdf" in by_url["https://main.knesset.gov.il/Activity/committees/Ethics/Decisions25/Decisions25-50.pdf"].url
    assert by_url["https://main.knesset.gov.il/Activity/committees/Ethics/Decisions25/Decisions25-50.pdf"].date == "10.2.2026"
    assert by_url["https://main.knesset.gov.il/Activity/committees/Ethics/Decisions25/Decisions25-50.pdf"].title.startswith("החלטה מס' 50/25")
    assert all(r.knesset_num == 25 for r in rows)
    # All rows write to the store:
    assert store.exists(_KEY)
    loaded = list(csv.DictReader(io.StringIO(store.get_bytes(_KEY).decode("utf-8"))))
    assert len(loaded) == 2


def test_skips_non_pdf_anchors(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    cfg = EthicsDecisionsConfig(store=store, key=_KEY)
    html = """
    <html><body>
      <a href="/Activity/committees/Ethics/x.pdf">PDF</a>
      <a href="/Activity/committees/Ethics/index.html">HTML</a>
      <a href="https://example.com/page.aspx">SP</a>
    </body></html>
    """
    http = MagicMock(return_value=_resp({"Html": html}))
    rows = fetch_ethics_decisions_index(cfg, http_get=http)
    assert len(rows) == 1
    assert rows[0].url.endswith("x.pdf")


def test_dedupes_repeated_pdf_anchors(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    cfg = EthicsDecisionsConfig(store=store, key=_KEY)
    html = """
    <a href="https://fs/x.pdf">A</a>
    <a href="https://fs/x.pdf">A again</a>
    """
    http = MagicMock(return_value=_resp({"Html": html}))
    rows = fetch_ethics_decisions_index(cfg, http_get=http)
    assert len(rows) == 1


def test_filename_falls_back_to_md5_for_uglyurls(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    cfg = EthicsDecisionsConfig(store=store, key=_KEY)
    html = """<a href="/Activity/committees/Ethics/Decisions25/לוח-מלא.pdf">x</a>"""
    http = MagicMock(return_value=_resp({"Html": html}))
    rows = fetch_ethics_decisions_index(cfg, http_get=http)
    assert len(rows) == 1
    # Either the URL-tail filename if it's clean, or md5 fallback.
    assert rows[0].filename.endswith(".pdf")


def test_request_passes_correct_query_params(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    cfg = EthicsDecisionsConfig(store=store, key=_KEY)
    http = MagicMock(return_value=_resp({"Html": '<a href="/y.pdf">y</a>'}))
    fetch_ethics_decisions_index(cfg, http_get=http)
    params = http.call_args.kwargs["params"]
    assert params["PageName"] == "EthicsDecisions25"
    assert params["Project"] == "committees"
    assert "/APPS/committees/2217/pages/EthicsDecisions25" in params["Route"]


# ---------- empty-result guard ----------

def test_empty_html_with_existing_csv_preserves_seed(tmp_path: Path):
    """Live API hiccup + non-empty seed: the seed is preserved.

    Previously this raised EmptyUpstreamIndex to guard against an
    upstream blip nuking the index. Now the seed-merge gives a better
    protection: if live returns 0 rows we just write the seed back
    unchanged. No data lost, no spurious build failure on a transient
    upstream outage.
    """
    store = LocalFsStore(tmp_path)
    atomic_write_csv(store, _KEY, [DocRow(
        url="https://x/y.pdf", filename="y.pdf",
        date="2008-01-01", knesset_num=17, title="seed row",
    )])
    cfg = EthicsDecisionsConfig(store=store, key=_KEY)
    http = MagicMock(return_value=_resp({"Html": ""}))
    rows = fetch_ethics_decisions_index(cfg, http_get=http)
    assert len(rows) == 1
    assert rows[0].url == "https://x/y.pdf"
    # Store object now matches.
    loaded = list(csv.DictReader(io.StringIO(store.get_bytes(_KEY).decode("utf-8"))))
    assert len(loaded) == 1


def test_empty_live_with_empty_seed_still_raises(tmp_path: Path):
    """Live=0 AND seed=0 (and a pre-existing populated CSV) is still
    an error — that's the corruption case the guard exists to catch."""
    store = LocalFsStore(tmp_path)
    # Pre-existing store object but with a URL-less row so seed=0 even
    # though file exists and is non-empty (seed-loader filters URL-less rows).
    store.put_atomic(_KEY, b"url,filename,date,knesset_num,title\n,empty_url.pdf,d,25,t\n")
    cfg = EthicsDecisionsConfig(store=store, key=_KEY)
    http = MagicMock(return_value=_resp({"Html": ""}))
    with pytest.raises(EmptyUpstreamIndex):
        fetch_ethics_decisions_index(cfg, http_get=http)


def test_empty_html_first_run_writes_empty_csv(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    cfg = EthicsDecisionsConfig(store=store, key=_KEY)
    http = MagicMock(return_value=_resp({"Html": ""}))
    rows = fetch_ethics_decisions_index(cfg, http_get=http)
    assert rows == []
    assert store.exists(_KEY)


# ---------- seed-from-store merge ----------

def _seed_store(store, rows: list[dict]) -> None:
    """Write a seed CSV to the store."""
    fieldnames = ["url", "filename", "date", "knesset_num", "title"]
    buf = io.StringIO(newline="")
    w = csv.DictWriter(buf, fieldnames=fieldnames)
    w.writeheader()
    for r in rows:
        w.writerow(r)
    store.put_atomic(_KEY, buf.getvalue().encode("utf-8"))


def test_seed_merge_appends_older_knessets(tmp_path: Path):
    """Seed rows (uploaded to store at key) for URLs not in the
    live fetch are appended after the live K25 rows."""
    store = LocalFsStore(tmp_path)
    _seed_store(store, [
        {"url": "https://main.knesset.gov.il/.../hachlatot17_40.pdf",
         "filename": "hachlatot17_40.pdf",
         "date": "2008-05-12", "knesset_num": "17",
         "title": "החלטה 17/40"},
        {"url": "https://main.knesset.gov.il/.../hachlatot18_1.pdf",
         "filename": "hachlatot18_1.pdf",
         "date": "2009-04-14", "knesset_num": "18",
         "title": "החלטה 18/1"},
    ])
    cfg = EthicsDecisionsConfig(store=store, key=_KEY)
    live_html = (
        '<table><tr><td>9.7.2025</td>'
        '<td><a href="/Activity/committees/Ethics/Decisions25/Decisions25-43.pdf">'
        'החלטה 43/25</a></td></tr></table>'
    )
    http = MagicMock(return_value=_resp({"Html": live_html}))
    rows = fetch_ethics_decisions_index(cfg, http_get=http)
    urls = [r.url for r in rows]
    assert any("Decisions25-43.pdf" in u for u in urls), urls
    assert any("hachlatot17_40.pdf" in u for u in urls), urls
    assert any("hachlatot18_1.pdf" in u for u in urls), urls
    # Live row comes first (we prepend live), seed after.
    assert "Decisions25-43.pdf" in urls[0]


def test_seed_merge_live_wins_on_url_collision(tmp_path: Path):
    """If a URL appears in both live and seed, the live row wins —
    so freshly-edited K25 content isn't overwritten by stale seed."""
    store = LocalFsStore(tmp_path)
    _seed_store(store, [
        {"url": "https://main.knesset.gov.il/Activity/committees/Ethics/"
                "Decisions25/Decisions25-43.pdf",
         "filename": "Decisions25-43.pdf",
         "date": "1900-01-01", "knesset_num": "25",
         "title": "STALE TITLE — should not appear"},
    ])
    cfg = EthicsDecisionsConfig(store=store, key=_KEY)
    live_html = (
        '<table><tr><td>9.7.2025</td>'
        '<td><a href="/Activity/committees/Ethics/Decisions25/Decisions25-43.pdf">'
        'LIVE TITLE</a></td></tr></table>'
    )
    http = MagicMock(return_value=_resp({"Html": live_html}))
    rows = fetch_ethics_decisions_index(cfg, http_get=http)
    matching = [r for r in rows if "Decisions25-43.pdf" in r.url]
    assert len(matching) == 1, "URL should appear exactly once after dedup"
    assert matching[0].title == "LIVE TITLE"


def test_seed_merge_first_run_no_existing_object(tmp_path: Path):
    """First run (no committed seed in store) still works — falls back to
    live-only, same as before this feature existed."""
    store = LocalFsStore(tmp_path)
    cfg = EthicsDecisionsConfig(store=store, key=_KEY)
    live_html = (
        '<table><tr><td><a href="/x.pdf">live</a></td></tr></table>'
    )
    http = MagicMock(return_value=_resp({"Html": live_html}))
    rows = fetch_ethics_decisions_index(cfg, http_get=http)
    assert len(rows) == 1
    assert rows[0].url.endswith("/x.pdf")


def test_seed_merge_malformed_csv_raises(tmp_path: Path):
    """Seed CSV missing required columns is a hard error — better to
    fail loud than silently drop rows."""
    store = LocalFsStore(tmp_path)
    store.put_atomic(_KEY, b"url,filename\nhttps://x/y.pdf,y.pdf\n")
    cfg = EthicsDecisionsConfig(store=store, key=_KEY)
    http = MagicMock(return_value=_resp({"Html": '<a href="/z.pdf">z</a>'}))
    with pytest.raises(ValueError, match="missing columns"):
        fetch_ethics_decisions_index(cfg, http_get=http)
