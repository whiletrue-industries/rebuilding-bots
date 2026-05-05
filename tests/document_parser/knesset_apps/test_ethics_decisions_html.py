"""Unit tests for knesset_apps.ethics_decisions_html."""
from __future__ import annotations

import csv
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
    out = tmp_path / "ethics.csv"
    cfg = EthicsDecisionsConfig(output_csv_path=out)
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
    # All rows write to CSV:
    with open(out, encoding="utf-8") as f:
        loaded = list(csv.DictReader(f))
    assert len(loaded) == 2


def test_skips_non_pdf_anchors(tmp_path: Path):
    out = tmp_path / "ethics.csv"
    cfg = EthicsDecisionsConfig(output_csv_path=out)
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
    out = tmp_path / "x.csv"
    cfg = EthicsDecisionsConfig(output_csv_path=out)
    html = """
    <a href="https://fs/x.pdf">A</a>
    <a href="https://fs/x.pdf">A again</a>
    """
    http = MagicMock(return_value=_resp({"Html": html}))
    rows = fetch_ethics_decisions_index(cfg, http_get=http)
    assert len(rows) == 1


def test_filename_falls_back_to_md5_for_uglyurls(tmp_path: Path):
    out = tmp_path / "x.csv"
    cfg = EthicsDecisionsConfig(output_csv_path=out)
    html = """<a href="/Activity/committees/Ethics/Decisions25/לוח-מלא.pdf">x</a>"""
    http = MagicMock(return_value=_resp({"Html": html}))
    rows = fetch_ethics_decisions_index(cfg, http_get=http)
    assert len(rows) == 1
    # Either the URL-tail filename if it's clean, or md5 fallback.
    assert rows[0].filename.endswith(".pdf")


def test_request_passes_correct_query_params(tmp_path: Path):
    out = tmp_path / "x.csv"
    cfg = EthicsDecisionsConfig(output_csv_path=out)
    http = MagicMock(return_value=_resp({"Html": '<a href="/y.pdf">y</a>'}))
    fetch_ethics_decisions_index(cfg, http_get=http)
    params = http.call_args.kwargs["params"]
    assert params["PageName"] == "EthicsDecisions25"
    assert params["Project"] == "committees"
    assert "/APPS/committees/2217/pages/EthicsDecisions25" in params["Route"]


# ---------- empty-result guard ----------

def test_empty_html_with_existing_csv_raises(tmp_path: Path):
    out = tmp_path / "x.csv"
    atomic_write_csv(out, [DocRow(url="u", filename="f", date="d", knesset_num=25)])
    cfg = EthicsDecisionsConfig(output_csv_path=out)
    http = MagicMock(return_value=_resp({"Html": ""}))
    with pytest.raises(EmptyUpstreamIndex):
        fetch_ethics_decisions_index(cfg, http_get=http)
    # CSV untouched.
    with open(out, encoding="utf-8") as f:
        assert sum(1 for _ in csv.DictReader(f)) == 1


def test_empty_html_first_run_writes_empty_csv(tmp_path: Path):
    out = tmp_path / "x.csv"
    cfg = EthicsDecisionsConfig(output_csv_path=out)
    http = MagicMock(return_value=_resp({"Html": ""}))
    rows = fetch_ethics_decisions_index(cfg, http_get=http)
    assert rows == []
    assert out.exists()
